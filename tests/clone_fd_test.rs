//! Tests for clone_fd multi-reader support

use std::os::fd::AsFd;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use fuser::{Config, Errno, FileHandle, FileType, Filesystem, INodeNo, Session, SessionACL};
use tempfile::TempDir;

/// Simple filesystem for testing
struct CountingFS {
    count: Arc<AtomicUsize>,
    ready: Arc<AtomicBool>,
}

impl Filesystem for CountingFS {
    fn init(
        &mut self,
        _req: &fuser::Request,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), std::io::Error> {
        self.ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn getattr(
        &self,
        _req: &fuser::Request,
        ino: INodeNo,
        _fh: Option<FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        self.count.fetch_add(1, Ordering::SeqCst);

        if ino == INodeNo::ROOT {
            reply.attr(
                &Duration::from_secs(0),
                &fuser::FileAttr {
                    ino: INodeNo::ROOT,
                    size: 0,
                    blocks: 0,
                    atime: std::time::UNIX_EPOCH,
                    mtime: std::time::UNIX_EPOCH,
                    ctime: std::time::UNIX_EPOCH,
                    crtime: std::time::UNIX_EPOCH,
                    kind: FileType::Directory,
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

/// Wait for filesystem to be ready (init called)
fn wait_ready(ready: &AtomicBool, timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    while !ready.load(Ordering::SeqCst) {
        if start.elapsed().as_millis() > timeout_ms as u128 {
            return false;
        }
        thread::yield_now();
    }
    true
}

/// Test that clone_fd creates a valid file descriptor.
#[cfg(target_os = "linux")]
#[test]
fn clone_fd_creates_valid_fd() {
    use std::os::fd::AsRawFd;

    let tmpdir: TempDir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(AtomicBool::new(false));

    let session = Session::new(
        CountingFS {
            count: count.clone(),
            ready: ready.clone(),
        },
        tmpdir.path(),
        &Config::default(),
    )
    .unwrap();

    let cloned_fd = session.clone_fd().expect("clone_fd should succeed");

    assert!(cloned_fd.as_raw_fd() >= 0);
    assert_ne!(
        cloned_fd.as_raw_fd(),
        session.as_fd().as_raw_fd(),
        "cloned fd should be different from original"
    );

    drop(cloned_fd);
    drop(session);
}

/// Test that from_fd_initialized creates a working session.
#[cfg(target_os = "linux")]
#[test]
fn from_fd_initialized_processes_requests() {
    let tmpdir: TempDir = tempfile::tempdir().unwrap();
    let primary_count = Arc::new(AtomicUsize::new(0));
    let reader_count = Arc::new(AtomicUsize::new(0));
    let primary_ready = Arc::new(AtomicBool::new(false));
    let reader_ready = Arc::new(AtomicBool::new(false));

    let session = Session::new(
        CountingFS {
            count: primary_count.clone(),
            ready: primary_ready.clone(),
        },
        tmpdir.path(),
        &Config::default(),
    )
    .unwrap();

    let cloned_fd = session.clone_fd().expect("clone_fd should succeed");
    let path = tmpdir.path().to_path_buf();

    let primary_bg = session.spawn().unwrap();

    // Wait for primary to be ready before starting reader
    assert!(wait_ready(&primary_ready, 5000), "primary should init");

    let reader_ready_clone = reader_ready.clone();
    let reader_count_clone = reader_count.clone();
    let reader_handle = thread::spawn(move || {
        let reader_session = Session::from_fd_initialized(
            CountingFS {
                count: reader_count_clone,
                ready: reader_ready_clone,
            },
            cloned_fd,
            SessionACL::All,
            None, // Use default proto_version
        );
        let bg = reader_session.spawn().unwrap();
        drop(bg);
    });

    // Use barrier to synchronize request threads
    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));

    let request_threads: Vec<_> = (0..num_threads)
        .map(|_| {
            let p = path.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait(); // All threads start together
                for _ in 0..5 {
                    if let Ok(m) = std::fs::metadata(&p) {
                        assert!(m.is_dir());
                        assert_eq!(m.permissions().mode() & 0o777, 0o755);
                    }
                }
            })
        })
        .collect();

    for t in request_threads {
        t.join().unwrap();
    }

    drop(primary_bg);
    let _ = reader_handle.join();

    let total = primary_count.load(Ordering::SeqCst) + reader_count.load(Ordering::SeqCst);
    assert!(total > 0, "expected requests to be processed");
}
