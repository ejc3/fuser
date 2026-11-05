use std::io;

use crate::session::MAX_WRITE_SIZE;

/// Size of the buffer for reading a request from the kernel. Since the kernel may send
/// up to `MAX_WRITE_SIZE` bytes in a write request, we use that value plus some extra space.
const BUFFER_SIZE: usize = MAX_WRITE_SIZE + 4096;

/// The buffer one event loop reads requests into.
///
/// On Linux it is its own `MAP_NORESERVE` mapping. Each reader thread holds
/// one, so a session with N threads holds N x 16 MiB of address space while a
/// request only touches its own length of it. Allocated from the heap, that
/// address space is committed memory (`VM_ACCOUNT`): adjacent buffers merge
/// into one mapping, and under the default heuristic overcommit
/// (`vm.overcommit_memory=0`) `fork()` refuses with ENOMEM to copy any single
/// accounted mapping larger than RAM plus swap. 64 readers in a 1 GiB guest
/// is 1,049,088 kB of buffers against a MemTotal of 1,007,956 kB, so the
/// process could no longer fork once 62 of them lay side by side.
/// `MAP_NORESERVE` leaves the buffers out of commit accounting; the pages are
/// still allocated on first touch. Under `vm.overcommit_memory=2` the kernel
/// ignores the flag and accounts them as before.
///
/// The mapping is page aligned, which satisfies `fuse_in_header`'s alignment.
#[derive(Debug)]
pub(crate) struct FuseReadBuf {
    #[cfg(target_os = "linux")]
    ptr: std::ptr::NonNull<u8>,
    #[cfg(not(target_os = "linux"))]
    buffer: Vec<u64>,
}

// The mapping is owned by this value alone.
#[cfg(target_os = "linux")]
unsafe impl Send for FuseReadBuf {}

impl FuseReadBuf {
    /// Creates a new `FuseReadBuf` of `BUFFER_SIZE` bytes.
    #[cfg(target_os = "linux")]
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: a fresh anonymous mapping; no existing memory is touched.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                BUFFER_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: std::ptr::NonNull::new(ptr.cast()).expect("mmap returned a null mapping"),
        })
    }

    /// Creates a new `FuseReadBuf` of `BUFFER_SIZE` bytes.
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn new() -> io::Result<Self> {
        // u64 elements keep the buffer aligned for `fuse_in_header`.
        Ok(Self {
            buffer: vec![0; BUFFER_SIZE.div_ceil(8)],
        })
    }

    /// Returns the buffer as a mutable slice.
    pub(crate) fn as_mut(&mut self) -> &mut [u8] {
        #[cfg(target_os = "linux")]
        // SAFETY: the mapping is BUFFER_SIZE bytes, readable and writable,
        // zero filled, and lives as long as self.
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr.as_ptr(), BUFFER_SIZE)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let len = self.buffer.len() * 8;
            // SAFETY: the Vec holds len initialised bytes.
            unsafe { std::slice::from_raw_parts_mut(self.buffer.as_mut_ptr().cast(), len) }
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for FuseReadBuf {
    fn drop(&mut self) {
        // SAFETY: the mapping was created by new() with this length.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), BUFFER_SIZE);
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    /// The VmFlags line of the mapping that holds `addr`.
    fn vm_flags(addr: usize) -> String {
        let smaps = std::fs::read_to_string("/proc/self/smaps").expect("read smaps");
        let mut inside = false;
        for line in smaps.lines() {
            if let Some((range, _)) = line.split_once(' ') {
                if let Some((start, end)) = range.split_once('-') {
                    if let (Ok(start), Ok(end)) = (
                        usize::from_str_radix(start, 16),
                        usize::from_str_radix(end, 16),
                    ) {
                        inside = start <= addr && addr < end;
                        continue;
                    }
                }
            }
            if inside {
                if let Some(flags) = line.strip_prefix("VmFlags:") {
                    return flags.trim().to_string();
                }
            }
        }
        panic!("no mapping holds {addr:#x}");
    }

    #[test]
    fn read_buffers_are_not_commit_accounted() {
        let mut bufs: Vec<FuseReadBuf> = (0..4).map(|_| FuseReadBuf::new().unwrap()).collect();
        for buf in &mut bufs {
            let slice = buf.as_mut();
            assert_eq!(slice.len(), BUFFER_SIZE);
            // Touch both ends: the mapping is usable and zero filled.
            assert_eq!(slice[0], 0);
            assert_eq!(slice[BUFFER_SIZE - 1], 0);
            slice[0] = 1;
            slice[BUFFER_SIZE - 1] = 1;
            let flags = vm_flags(slice.as_ptr() as usize);
            let flags: Vec<&str> = flags.split_whitespace().collect();
            assert!(
                !flags.contains(&"ac"),
                "a read buffer is commit-accounted (VmFlags: {flags:?}), so it counts against fork()"
            );
            assert!(flags.contains(&"nr"), "VmFlags: {flags:?}");
        }
    }
}
