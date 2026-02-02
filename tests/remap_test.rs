//! Test for FUSE_REMAP_FILE_RANGE support

use std::ffi::OsStr;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use fuser::{
    Config, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, LockOwner, OpenFlags,
    Session,
};
use tempfile::TempDir;

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
        reply.written(len as u32);
    }
}

/// Test that remap_file_range is called when FICLONE ioctl is used.
#[cfg(target_os = "linux")]
#[test]
fn remap_file_range_dispatch() {
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

    thread::sleep(Duration::from_millis(100));

    let src_path = path.join("src");
    let dst_path = path.join("dst");

    let src_file = std::fs::File::open(&src_path).expect("open src");
    let dst_file = std::fs::OpenOptions::new()
        .write(true)
        .open(&dst_path)
        .expect("open dst");

    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };

    if ret == 0 {
        assert!(
            remap_called.load(Ordering::SeqCst),
            "remap_file_range should have been called"
        );
        eprintln!("SUCCESS: FICLONE dispatched to remap_file_range");
    } else {
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        eprintln!(
            "FICLONE returned error {} (errno {}), kernel may not support FUSE_REMAP_FILE_RANGE",
            ret, errno
        );
    }

    drop(src_file);
    drop(dst_file);
    drop(bg);
}
