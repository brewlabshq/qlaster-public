//! Single-producer / single-consumer ring backed by an mmap'd file in
//! /dev/shm. Wire frames are stored back-to-back as `[u32 LE frame_len][bytes]`,
//! padded to 4-byte boundaries; producer wraps by writing a sentinel
//! (`frame_len = 0xFFFF_FFFF`) when the contiguous tail is too small.
//!
//! Producer and consumer live in different processes; the AcqRel-ordered
//! atomic positions in the header synchronize all body bytes between them.

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::error::QlasterError;

const RING_MAGIC: u32 = 0x515F_5253; // 'Q','_','R','S' (LE)
const RING_VERSION: u32 = 1;
pub const RING_HEADER_SIZE: usize = 128;
const FRAME_LEN_BYTES: usize = 4;
const SENTINEL: u32 = u32::MAX;

#[repr(C)]
struct RingHeader {
    magic: AtomicU32,
    version: AtomicU32,
    capacity: AtomicU64,
    producer_pos: AtomicU64,
    consumer_pos: AtomicU64,
    dropped_frames: AtomicU64,
    closed: AtomicU32,
    _pad: [u8; 84],
}

const _: () = {
    assert!(std::mem::size_of::<RingHeader>() == RING_HEADER_SIZE);
    assert!(std::mem::align_of::<RingHeader>() >= 8);
};

/// Round `n` up to the next multiple of 4. Frames are 4-byte aligned in the
/// ring so the producer cursor always lands on a position where a u32 length
/// prefix can be written without straddling.
#[inline]
fn align4(n: usize) -> usize {
    (n + 3) & !3
}

/// Owning handle to a writer-side mmap'd ring. Drop unmaps the region and
/// (for the producer that created the file) unlinks it.
pub struct ShmRingProducer {
    base: *mut u8,
    map_len: usize,
    capacity: usize,
    path: PathBuf,
    owns_file: bool,
}

impl std::fmt::Debug for ShmRingProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmRingProducer")
            .field("path", &self.path)
            .field("capacity", &self.capacity)
            .finish()
    }
}

unsafe impl Send for ShmRingProducer {}
unsafe impl Sync for ShmRingProducer {}

/// Owning handle to a reader-side mmap'd ring. Drop unmaps but does not
/// unlink (the producer owns the file lifecycle).
pub struct ShmRingConsumer {
    base: *mut u8,
    map_len: usize,
    capacity: usize,
}

impl std::fmt::Debug for ShmRingConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmRingConsumer")
            .field("capacity", &self.capacity)
            .finish()
    }
}

unsafe impl Send for ShmRingConsumer {}
unsafe impl Sync for ShmRingConsumer {}

impl ShmRingProducer {
    /// Create a new ring file at `path` of size `RING_HEADER_SIZE + capacity`.
    /// `capacity` must be a power of two and at least `MIN_CAPACITY` to admit
    /// any non-trivial frame.
    pub fn create(path: impl Into<PathBuf>, capacity: usize) -> Result<Self, QlasterError> {
        let path = path.into();
        if !capacity.is_power_of_two() {
            return Err(QlasterError::ShmError(format!(
                "capacity {capacity} must be a power of two"
            )));
        }
        if capacity < 64 {
            return Err(QlasterError::ShmError(format!(
                "capacity {capacity} too small (min 64)"
            )));
        }

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| QlasterError::ShmError(format!("create parent dir: {e}")))?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(|e| QlasterError::ShmError(format!("open {}: {e}", path.display())))?;

        let map_len = RING_HEADER_SIZE + capacity;
        if let Err(e) = file.set_len(map_len as u64) {
            let _ = std::fs::remove_file(&path);
            return Err(QlasterError::ShmError(format!("ftruncate: {e}")));
        }

        let base = unsafe { mmap_shared(file.as_raw_fd(), map_len) };
        let base = match base {
            Ok(p) => p,
            Err(e) => {
                let _ = std::fs::remove_file(&path);
                return Err(QlasterError::ShmError(format!("mmap: {e}")));
            }
        };

        // Initialize header fields. Single-writer at construction; no other
        // process has the fd yet so plain stores are sufficient.
        unsafe {
            let header = &*(base as *const RingHeader);
            header.magic.store(RING_MAGIC, Ordering::Relaxed);
            header.version.store(RING_VERSION, Ordering::Relaxed);
            header.capacity.store(capacity as u64, Ordering::Relaxed);
            header.producer_pos.store(0, Ordering::Relaxed);
            header.consumer_pos.store(0, Ordering::Relaxed);
            header.dropped_frames.store(0, Ordering::Relaxed);
            header.closed.store(0, Ordering::Release);
        }

        Ok(Self {
            base,
            map_len,
            capacity,
            path,
            owns_file: true,
        })
    }

    /// Capacity of the body in bytes (excluding header).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Filesystem path of the backing ring file (for handing to consumers).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Push one frame consisting of the concatenation of `parts` into the
    /// ring. The 4-byte length prefix is written automatically; `parts`
    /// should be the wire-frame body (e.g. frame header + payload).
    /// Returns `Err(QlasterError::ShmFull)` if the ring lacks room.
    pub fn try_push(&self, parts: &[&[u8]]) -> Result<(), QlasterError> {
        let body_len: usize = parts.iter().map(|p| p.len()).sum();
        let frame_size = FRAME_LEN_BYTES + align4(body_len);
        if frame_size > self.capacity {
            return Err(QlasterError::ShmError(format!(
                "frame {frame_size} > ring capacity {}",
                self.capacity
            )));
        }
        let header = self.header();
        let producer_pos = header.producer_pos.load(Ordering::Relaxed);
        let consumer_pos = header.consumer_pos.load(Ordering::Acquire);
        let used = (producer_pos - consumer_pos) as usize;
        let free = self.capacity - used;

        let tail_offset = (producer_pos as usize) & (self.capacity - 1);
        let tail_room = self.capacity - tail_offset;

        let need = if tail_room >= frame_size {
            frame_size
        } else {
            tail_room + frame_size
        };
        if free < need {
            header.dropped_frames.fetch_add(1, Ordering::Relaxed);
            return Err(QlasterError::ShmFull);
        }

        let body_ptr = unsafe { self.body_ptr() };

        if tail_room < frame_size {
            // Two-step wrap: write the actual frame at body offset 0 first
            // (consumer has not yet been told to read past the sentinel), then
            // write the sentinel at tail_offset, then publish in one Release
            // store covering both regions.
            unsafe {
                write_frame_at(body_ptr, 0, body_len as u32, parts);
                ptr::write_unaligned(body_ptr.add(tail_offset) as *mut u32, SENTINEL.to_le());
            }
            let new_pos = producer_pos + (tail_room + frame_size) as u64;
            header.producer_pos.store(new_pos, Ordering::Release);
        } else {
            unsafe {
                write_frame_at(body_ptr, tail_offset, body_len as u32, parts);
            }
            let new_pos = producer_pos + frame_size as u64;
            header.producer_pos.store(new_pos, Ordering::Release);
        }
        Ok(())
    }

    /// Mark the ring closed. Subsequent pops by the consumer will see no
    /// new data; the consumer can detect closure by reading `is_closed()`
    /// after draining.
    pub fn close(&self) {
        self.header().closed.store(1, Ordering::Release);
    }

    pub fn dropped_frames(&self) -> u64 {
        self.header().dropped_frames.load(Ordering::Relaxed)
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*(self.base as *const RingHeader) }
    }

    unsafe fn body_ptr(&self) -> *mut u8 {
        unsafe { self.base.add(RING_HEADER_SIZE) }
    }
}

impl Drop for ShmRingProducer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, self.map_len);
        }
        if self.owns_file {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl ShmRingConsumer {
    /// Open an existing ring file and validate its header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, QlasterError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| QlasterError::ShmError(format!("open {}: {e}", path.display())))?;
        let metadata = file
            .metadata()
            .map_err(|e| QlasterError::ShmError(format!("stat: {e}")))?;
        let map_len = metadata.len() as usize;
        if map_len <= RING_HEADER_SIZE {
            return Err(QlasterError::ShmError(format!(
                "ring file {} too small ({} bytes)",
                path.display(),
                map_len
            )));
        }

        let base = unsafe { mmap_shared(file.as_raw_fd(), map_len) }
            .map_err(|e| QlasterError::ShmError(format!("mmap: {e}")))?;

        let header = unsafe { &*(base as *const RingHeader) };
        let magic = header.magic.load(Ordering::Acquire);
        let version = header.version.load(Ordering::Acquire);
        let capacity = header.capacity.load(Ordering::Acquire) as usize;
        if magic != RING_MAGIC {
            unsafe {
                libc::munmap(base as *mut libc::c_void, map_len);
            }
            return Err(QlasterError::ShmError(format!(
                "bad ring magic 0x{magic:08x}"
            )));
        }
        if version != RING_VERSION {
            unsafe {
                libc::munmap(base as *mut libc::c_void, map_len);
            }
            return Err(QlasterError::ShmError(format!(
                "unsupported ring version {version}"
            )));
        }
        if !capacity.is_power_of_two() || capacity == 0 {
            unsafe {
                libc::munmap(base as *mut libc::c_void, map_len);
            }
            return Err(QlasterError::ShmError(format!(
                "bad ring capacity {capacity}"
            )));
        }
        if map_len < RING_HEADER_SIZE + capacity {
            unsafe {
                libc::munmap(base as *mut libc::c_void, map_len);
            }
            return Err(QlasterError::ShmError(format!(
                "ring file shorter than header+capacity ({} < {})",
                map_len,
                RING_HEADER_SIZE + capacity
            )));
        }

        Ok(Self {
            base,
            map_len,
            capacity,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_closed(&self) -> bool {
        self.header().closed.load(Ordering::Acquire) != 0
    }

    /// Pop one frame from the ring, returning its body bytes (without the
    /// length prefix). Returns `None` if the ring is empty.
    pub fn try_pop(&self) -> Option<Vec<u8>> {
        let header = self.header();
        loop {
            let producer_pos = header.producer_pos.load(Ordering::Acquire);
            let consumer_pos = header.consumer_pos.load(Ordering::Relaxed);
            if consumer_pos == producer_pos {
                return None;
            }
            let body_offset = (consumer_pos as usize) & (self.capacity - 1);
            let body_ptr = unsafe { self.body_ptr() };

            let raw_len = unsafe { ptr::read_unaligned(body_ptr.add(body_offset) as *const u32) };
            let frame_len = u32::from_le(raw_len);

            if frame_len == SENTINEL {
                let tail_room = self.capacity - body_offset;
                let new_pos = consumer_pos + tail_room as u64;
                header.consumer_pos.store(new_pos, Ordering::Release);
                continue;
            }

            let body_len = frame_len as usize;
            let frame_size = FRAME_LEN_BYTES + align4(body_len);
            if frame_size > self.capacity {
                tracing::error!(
                    "shm ring corruption: frame size {frame_size} exceeds capacity {}",
                    self.capacity
                );
                return None;
            }

            let mut buf = vec![0u8; body_len];
            unsafe {
                ptr::copy_nonoverlapping(
                    body_ptr.add(body_offset + FRAME_LEN_BYTES),
                    buf.as_mut_ptr(),
                    body_len,
                );
            }
            let new_pos = consumer_pos + frame_size as u64;
            header.consumer_pos.store(new_pos, Ordering::Release);
            return Some(buf);
        }
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*(self.base as *const RingHeader) }
    }

    unsafe fn body_ptr(&self) -> *mut u8 {
        unsafe { self.base.add(RING_HEADER_SIZE) }
    }
}

impl Drop for ShmRingConsumer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, self.map_len);
        }
    }
}

unsafe fn mmap_shared(fd: libc::c_int, len: usize) -> io::Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr as *mut u8)
    }
}

unsafe fn write_frame_at(body_ptr: *mut u8, offset: usize, body_len: u32, parts: &[&[u8]]) {
    unsafe {
        let dst = body_ptr.add(offset);
        ptr::write_unaligned(dst as *mut u32, body_len.to_le());
        let mut cursor = dst.add(FRAME_LEN_BYTES);
        for part in parts {
            if !part.is_empty() {
                ptr::copy_nonoverlapping(part.as_ptr(), cursor, part.len());
                cursor = cursor.add(part.len());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_ring_path(name: &str) -> PathBuf {
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("qlaster-test-ring-{pid}-{nonce}-{name}"))
    }

    #[test]
    fn create_and_simple_push_pop_roundtrip() {
        let path = temp_ring_path("simple");
        let producer = ShmRingProducer::create(&path, 4096).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");

        let body = b"hello qlaster";
        producer.try_push(&[body]).expect("push");

        let popped = consumer.try_pop().expect("pop");
        assert_eq!(popped, body);
        assert!(consumer.try_pop().is_none());
    }

    #[test]
    fn multi_part_frames_concatenate_correctly() {
        let path = temp_ring_path("multipart");
        let producer = ShmRingProducer::create(&path, 4096).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");

        producer
            .try_push(&[&[1, 2, 3], &[4, 5], &[6, 7, 8, 9]])
            .expect("push");
        let popped = consumer.try_pop().expect("pop");
        assert_eq!(popped, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn ring_full_returns_shm_full_and_drops_frame() {
        let path = temp_ring_path("full");
        let producer = ShmRingProducer::create(&path, 64).expect("create");
        // 64 byte capacity; each frame is 4 (prefix) + body. push 16-byte frames.
        let body = vec![0xAAu8; 16];
        // 4 + 16 = 20 bytes/frame, 64 / 20 = 3 frames fit (60 bytes used).
        producer.try_push(&[&body]).expect("push 1");
        producer.try_push(&[&body]).expect("push 2");
        producer.try_push(&[&body]).expect("push 3");
        // Fourth push should not fit (would need 80 bytes total).
        match producer.try_push(&[&body]) {
            Err(QlasterError::ShmFull) => {}
            other => panic!("expected ShmFull, got {other:?}"),
        }
        assert_eq!(producer.dropped_frames(), 1);
    }

    #[test]
    fn wrap_around_writes_sentinel_and_consumer_resyncs() {
        let path = temp_ring_path("wrap");
        let cap = 64;
        let producer = ShmRingProducer::create(&path, cap).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");

        // Push & immediately consume frames to advance both positions
        // until the producer's tail_offset is close to the end.
        let small = vec![0x11u8; 16]; // 20 bytes/frame
        for i in 0..3 {
            producer.try_push(&[&small]).expect("push small");
            let popped = consumer.try_pop().expect("pop small");
            assert_eq!(popped.len(), 16, "iter {i}");
        }
        // After 3 frames: producer_pos=60. tail_offset=60. tail_room=4.
        // A 24-byte frame won't fit contiguously and triggers wrap.
        let big = vec![0x22u8; 20]; // 4 + 20 = 24 bytes/frame
        producer.try_push(&[&big]).expect("push wrapping");
        let popped = consumer.try_pop().expect("pop wrapped");
        assert_eq!(popped.len(), 20);
        assert!(popped.iter().all(|&b| b == 0x22));
        assert!(consumer.try_pop().is_none());
    }

    #[test]
    fn frame_larger_than_capacity_is_rejected() {
        let path = temp_ring_path("toobig");
        let producer = ShmRingProducer::create(&path, 64).expect("create");
        let huge = vec![0u8; 128];
        assert!(matches!(
            producer.try_push(&[&huge]),
            Err(QlasterError::ShmError(_))
        ));
    }

    #[test]
    fn open_rejects_files_with_bad_magic() {
        let path = temp_ring_path("badmagic");
        std::fs::write(&path, vec![0u8; RING_HEADER_SIZE + 64]).expect("write junk");
        assert!(matches!(
            ShmRingConsumer::open(&path),
            Err(QlasterError::ShmError(_))
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn close_signals_consumer() {
        let path = temp_ring_path("close");
        let producer = ShmRingProducer::create(&path, 256).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");
        assert!(!consumer.is_closed());
        producer.close();
        assert!(consumer.is_closed());
    }

    #[test]
    fn unaligned_body_lengths_round_to_4_byte_frames() {
        let path = temp_ring_path("align");
        let producer = ShmRingProducer::create(&path, 256).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");
        // 7-byte body → frame_size = 4 + 8 = 12 bytes consumed.
        producer
            .try_push(&[&[1u8, 2, 3, 4, 5, 6, 7]])
            .expect("push");
        producer.try_push(&[&[9u8, 9, 9]]).expect("push");
        let a = consumer.try_pop().expect("pop a");
        let b = consumer.try_pop().expect("pop b");
        assert_eq!(a, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(b, vec![9, 9, 9]);
    }
}
