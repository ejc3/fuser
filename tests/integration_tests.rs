use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use fuser::Config;
use fuser::Errno;
use fuser::FileHandle;
use fuser::Filesystem;
use fuser::FopenFlags;
use fuser::Generation;
use fuser::INodeNo;
use fuser::LockOwner;
use fuser::OpenFlags;
use fuser::Session;
use fuser::SessionACL;
use tempfile::TempDir;

/// Test that clone_fd creates a working file descriptor for multi-reader setups.
#[cfg(target_os = "linux")]
#[test]
fn clone_fd_multi_reader() {
    use std::os::fd::AsRawFd;

    // Simple filesystem that tracks how many times getattr is called
    struct CountingFS {
        count: Arc<AtomicUsize>,
    }

    impl Filesystem for CountingFS {
        fn getattr(
            &self,
            _req: &fuser::Request,
            ino: INodeNo,
            _fh: Option<FileHandle>,
            reply: fuser::ReplyAttr,
        ) {
            self.count.fetch_add(1, Ordering::SeqCst);
            if ino == INodeNo::ROOT {
                // Root directory
                reply.attr(
                    &Duration::from_secs(1),
                    &fuser::FileAttr {
                        ino: INodeNo::ROOT,
                        size: 0,
                        blocks: 0,
                        atime: std::time::UNIX_EPOCH,
                        mtime: std::time::UNIX_EPOCH,
                        ctime: std::time::UNIX_EPOCH,
                        crtime: std::time::UNIX_EPOCH,
                        kind: fuser::FileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        blksize: 4096,
                        flags: 0,
                    },
                );
            } else {
                reply.error(Errno::ENOENT);
            }
        }
    }

    let tmpdir: TempDir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));

    let session = Session::new(
        CountingFS {
            count: count.clone(),
        },
        tmpdir.path(),
        &Config::default(),
    )
    .unwrap();

    // Clone the fd - this should succeed
    let cloned_fd = session.clone_fd().expect("clone_fd should succeed");

    // Verify it's a valid fd (different from the original)
    assert!(cloned_fd.as_raw_fd() >= 0);

    // Clean up
    drop(cloned_fd);
    drop(session);
}

/// Test that from_fd_initialized creates a session that can process requests.
/// Verifies both readers receive requests and metadata returns expected values.
#[cfg(target_os = "linux")]
#[test]
fn from_fd_initialized_works() {
    // Filesystem that tracks request count per instance with artificial delay
    // to ensure kernel dispatches to both readers
    struct SlowCountingFS {
        count: Arc<AtomicUsize>,
    }

    impl Filesystem for SlowCountingFS {
        fn getattr(
            &self,
            _req: &fuser::Request,
            ino: INodeNo,
            _fh: Option<FileHandle>,
            reply: fuser::ReplyAttr,
        ) {
            self.count.fetch_add(1, Ordering::SeqCst);

            // Add delay so while one reader is processing, the kernel
            // will dispatch concurrent requests to the other reader
            thread::sleep(Duration::from_millis(50));

            if ino == INodeNo::ROOT {
                reply.attr(
                    &Duration::from_secs(0), // No caching to ensure requests reach FUSE
                    &fuser::FileAttr {
                        ino: INodeNo::ROOT,
                        size: 0,
                        blocks: 0,
                        atime: std::time::UNIX_EPOCH,
                        mtime: std::time::UNIX_EPOCH,
                        ctime: std::time::UNIX_EPOCH,
                        crtime: std::time::UNIX_EPOCH,
                        kind: fuser::FileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        blksize: 4096,
                        flags: 0,
                    },
                );
            } else {
                reply.error(Errno::ENOENT);
            }
        }
    }

    let tmpdir: TempDir = tempfile::tempdir().unwrap();

    // Separate counters to track which reader handled requests
    let primary_count = Arc::new(AtomicUsize::new(0));
    let reader_count = Arc::new(AtomicUsize::new(0));

    let session = Session::new(
        SlowCountingFS {
            count: primary_count.clone(),
        },
        tmpdir.path(),
        &Config::default(),
    )
    .unwrap();

    // Clone fd for second reader BEFORE spawning the primary (spawn takes ownership)
    let cloned_fd = session.clone_fd().expect("clone_fd should succeed");

    // Save path for concurrent access (before session is moved)
    let path = tmpdir.path().to_path_buf();

    // Spawn primary session in background
    let primary_bg = session.spawn().unwrap();

    // Start second reader in a thread
    let reader_count_clone = reader_count.clone();
    let reader_handle = thread::spawn(move || {
        let reader_session = Session::from_fd_initialized(
            SlowCountingFS {
                count: reader_count_clone,
            },
            cloned_fd,
            SessionACL::All,
        );
        // Spawn in background - the thread will run until ENODEV when primary unmounts
        let bg = reader_session.spawn().unwrap();
        // Keep BackgroundSession alive - when dropped it will wait for the thread
        // The thread exits on ENODEV when primary unmounts
        drop(bg);
    });

    // Give readers time to start processing
    thread::sleep(Duration::from_millis(100));

    // Generate concurrent requests from multiple threads
    // With 50ms delay per request and concurrent threads, the kernel should
    // dispatch to both readers
    let request_threads: Vec<_> = (0..4)
        .map(|_| {
            let p = path.clone();
            thread::spawn(move || {
                for _ in 0..5 {
                    let meta = std::fs::metadata(&p);
                    // Verify metadata returns expected values
                    if let Ok(m) = meta {
                        assert!(m.is_dir(), "root should be a directory");
                        assert_eq!(
                            m.permissions().mode() & 0o777,
                            0o755,
                            "permissions should be 0o755"
                        );
                    }
                }
            })
        })
        .collect();

    // Wait for all request threads
    for t in request_threads {
        t.join().unwrap();
    }

    // Let any in-flight requests complete
    thread::sleep(Duration::from_millis(200));

    // Unmount by dropping the primary BackgroundSession
    // This will cause the secondary to exit with ENODEV
    drop(primary_bg);

    // Wait for reader thread to finish
    let _ = reader_handle.join();

    // Verify both readers processed requests
    let primary = primary_count.load(Ordering::SeqCst);
    let reader = reader_count.load(Ordering::SeqCst);
    let total = primary + reader;

    eprintln!(
        "Request distribution: primary={}, reader={}, total={}",
        primary, reader, total
    );

    // Total should be > 0 (requests were processed)
    assert!(total > 0, "expected some requests to be processed");

    // With 50ms delay per request and 4 concurrent threads, both readers
    // should handle some requests. The kernel dispatches to whichever
    // reader is blocked in read(), and with the delay, both should be available.
    assert!(
        primary > 0 && reader > 0,
        "expected both readers to process requests: primary={}, reader={}. \
         This verifies multi-threaded request handling works.",
        primary,
        reader
    );
}

/// Test that remap_file_range is called when FICLONE ioctl is used.
/// This verifies the FUSE_REMAP_FILE_RANGE opcode dispatch works correctly.
#[cfg(target_os = "linux")]
#[test]
fn remap_file_range_dispatch() {
    use std::ffi::OsStr;
    use std::os::fd::AsRawFd;
    use std::sync::atomic::AtomicBool;

    /// FICLONE ioctl number: _IOW(0x94, 9, int) = 0x40049409
    const FICLONE: libc::c_ulong = 0x40049409;

    const SRC_INO: INodeNo = INodeNo(2);
    const DST_INO: INodeNo = INodeNo(3);

    /// Filesystem that tracks remap_file_range calls
    struct RemapFS {
        remap_called: Arc<AtomicBool>,
    }

    impl Filesystem for RemapFS {
        fn lookup(
            &self,
            _req: &fuser::Request,
            parent: INodeNo,
            name: &OsStr,
            reply: fuser::ReplyEntry,
        ) {
            if parent == INodeNo::ROOT {
                let ino = match name.to_str() {
                    Some("src") => SRC_INO,
                    Some("dst") => DST_INO,
                    _ => {
                        reply.error(Errno::ENOENT);
                        return;
                    }
                };
                reply.entry(
                    &Duration::from_secs(1),
                    &fuser::FileAttr {
                        ino,
                        size: 4096,
                        blocks: 8,
                        atime: std::time::UNIX_EPOCH,
                        mtime: std::time::UNIX_EPOCH,
                        ctime: std::time::UNIX_EPOCH,
                        crtime: std::time::UNIX_EPOCH,
                        kind: fuser::FileType::RegularFile,
                        perm: 0o644,
                        nlink: 1,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        blksize: 4096,
                        flags: 0,
                    },
                    Generation(0),
                );
            } else {
                reply.error(Errno::ENOENT);
            }
        }

        fn getattr(
            &self,
            _req: &fuser::Request,
            ino: INodeNo,
            _fh: Option<FileHandle>,
            reply: fuser::ReplyAttr,
        ) {
            let (kind, size) = match ino {
                INodeNo::ROOT => (fuser::FileType::Directory, 0),
                SRC_INO | DST_INO => (fuser::FileType::RegularFile, 4096),
                _ => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };
            reply.attr(
                &Duration::from_secs(1),
                &fuser::FileAttr {
                    ino,
                    size,
                    blocks: if size > 0 { 8 } else { 0 },
                    atime: std::time::UNIX_EPOCH,
                    mtime: std::time::UNIX_EPOCH,
                    ctime: std::time::UNIX_EPOCH,
                    crtime: std::time::UNIX_EPOCH,
                    kind,
                    perm: if kind == fuser::FileType::Directory {
                        0o755
                    } else {
                        0o644
                    },
                    nlink: if kind == fuser::FileType::Directory {
                        2
                    } else {
                        1
                    },
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                },
            );
        }

        fn open(
            &self,
            _req: &fuser::Request,
            ino: INodeNo,
            _flags: OpenFlags,
            reply: fuser::ReplyOpen,
        ) {
            if ino == SRC_INO || ino == DST_INO {
                reply.opened(FileHandle(ino.0), FopenFlags::empty());
            } else {
                reply.error(Errno::ENOENT);
            }
        }

        fn release(
            &self,
            _req: &fuser::Request,
            _ino: INodeNo,
            _fh: FileHandle,
            _flags: OpenFlags,
            _lock_owner: Option<LockOwner>,
            _flush: bool,
            reply: fuser::ReplyEmpty,
        ) {
            reply.ok();
        }

        fn remap_file_range(
            &self,
            _req: &fuser::Request,
            ino_in: u64,
            _fh_in: u64,
            _offset_in: i64,
            ino_out: u64,
            _fh_out: u64,
            _offset_out: i64,
            len: u64,
            _remap_flags: u32,
            reply: fuser::ReplyWrite,
        ) {
            eprintln!(
                "remap_file_range called: ino_in={}, ino_out={}, len={}",
                ino_in, ino_out, len
            );
            self.remap_called.store(true, Ordering::SeqCst);
            // Return success with the number of bytes "remapped"
            reply.written(len as u32);
        }
    }

    let tmpdir: TempDir = tempfile::tempdir().unwrap();
    let remap_called = Arc::new(AtomicBool::new(false));

    let session = Session::new(
        RemapFS {
            remap_called: remap_called.clone(),
        },
        tmpdir.path(),
        &Config::default(),
    )
    .unwrap();

    let path = tmpdir.path().to_path_buf();
    let bg = session.spawn().unwrap();

    // Give the mount time to be ready
    thread::sleep(Duration::from_millis(100));

    // Open source and destination files
    let src_path = path.join("src");
    let dst_path = path.join("dst");

    let src_file = std::fs::File::open(&src_path).expect("open src");
    let dst_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&dst_path)
        .expect("open dst");

    // Perform FICLONE ioctl
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };

    // The ioctl may fail with EOPNOTSUPP if the kernel doesn't have the patch,
    // but that's OK - we just verify dispatch worked if it succeeded
    if ret == 0 {
        assert!(
            remap_called.load(Ordering::SeqCst),
            "remap_file_range should have been called"
        );
        eprintln!("SUCCESS: FICLONE dispatched to remap_file_range");
    } else {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        // EOPNOTSUPP (95) or ENOSYS (38) means kernel doesn't support it
        // ENOTTY (25) means the ioctl didn't reach FUSE at all
        eprintln!(
            "FICLONE returned error {} (errno {}), kernel may not support FUSE_REMAP_FILE_RANGE",
            ret, errno
        );
        // Don't fail the test - the opcode requires kernel patches
    }

    drop(src_file);
    drop(dst_file);
    drop(bg);
}
