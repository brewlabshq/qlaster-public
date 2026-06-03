//! Shared-memory byte queue adapter backed by `shaq`.
//!
//! Qlaster frames are stored as `[u32 LE frame_len][bytes]` in a `shaq`
//! byte queue. `shaq` owns the shared-memory synchronization; Qlaster keeps
//! framing, file lifecycle, and eventfd wakeups at the transport layer.

use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use shaq::mpmc;

use crate::error::QlasterError;

pub const RING_HEADER_SIZE: usize = 0;
const FRAME_LEN_BYTES: usize = 4;
const CLOSE_SENTINEL: u32 = u32::MAX;

/// Owning handle to a writer-side shared-memory queue. Drop unmaps the region
/// and unlinks the queue file.
pub struct ShmRingProducer {
    producer: mpmc::Producer<u8>,
    capacity: usize,
    path: PathBuf,
    dropped_frames: AtomicU64,
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

/// Owning handle to a reader-side shared-memory queue.
pub struct ShmRingConsumer {
    consumer: mpmc::Consumer<u8>,
    capacity: usize,
    path: PathBuf,
    closed: AtomicBool,
}

impl std::fmt::Debug for ShmRingConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmRingConsumer")
            .field("path", &self.path)
            .field("capacity", &self.capacity)
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl ShmRingProducer {
    /// Create a new shared-memory byte queue at `path` with at least
    /// `capacity` bytes of queue storage.
    pub fn create(path: impl Into<PathBuf>, capacity: usize) -> Result<Self, QlasterError> {
        let path = path.into();
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

        let normalized_capacity = capacity.next_power_of_two();
        let file_size = mpmc::minimum_file_size::<u8>(normalized_capacity);
        let producer = match unsafe { mpmc::Producer::<u8>::create(&file, file_size) } {
            Ok(producer) => producer,
            Err(err) => {
                let _ = std::fs::remove_file(&path);
                return Err(QlasterError::ShmError(format!("shaq create: {err}")));
            }
        };

        Ok(Self {
            capacity: normalized_capacity,
            producer,
            path,
            dropped_frames: AtomicU64::new(0),
            owns_file: true,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Push one Qlaster frame into the shared byte queue.
    pub fn try_push(&self, parts: &[&[u8]]) -> Result<(), QlasterError> {
        let body_len: usize = parts.iter().map(|p| p.len()).sum();
        let body_len_u32: u32 = body_len
            .try_into()
            .map_err(|_| QlasterError::PayloadTooLarge {
                found: body_len,
                max: u32::MAX as usize,
            })?;
        let frame_size = FRAME_LEN_BYTES + body_len;
        if frame_size > self.capacity {
            return Err(QlasterError::ShmError(format!(
                "frame {frame_size} > ring capacity {}",
                self.capacity
            )));
        }

        let mut frame = Vec::with_capacity(frame_size);
        frame.extend_from_slice(&body_len_u32.to_le_bytes());
        for part in parts {
            frame.extend_from_slice(part);
        }

        if self.producer.try_write_slice(&frame) {
            Ok(())
        } else {
            self.dropped_frames.fetch_add(1, Ordering::Relaxed);
            Err(QlasterError::ShmFull)
        }
    }

    /// Mark the queue closed for the consumer by publishing a sentinel frame.
    pub fn close(&self) {
        let _ = self.producer.try_write_slice(&CLOSE_SENTINEL.to_le_bytes());
    }

    pub fn dropped_frames(&self) -> u64 {
        self.dropped_frames.load(Ordering::Relaxed)
    }
}

impl Drop for ShmRingProducer {
    fn drop(&mut self) {
        if self.owns_file {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl ShmRingConsumer {
    /// Open an existing queue file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, QlasterError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| QlasterError::ShmError(format!("open {}: {e}", path.display())))?;

        let consumer = unsafe { mpmc::Consumer::<u8>::join(&file) }
            .map_err(|e| QlasterError::ShmError(format!("shaq join: {e}")))?;

        Ok(Self {
            capacity: file
                .metadata()
                .map(|m| m.len() as usize)
                .unwrap_or_default(),
            consumer,
            path: path.to_path_buf(),
            closed: AtomicBool::new(false),
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) || !self.path.exists()
    }

    /// Pop one frame from the queue, returning its body bytes without the
    /// length prefix. Returns `None` if the queue is empty or a close sentinel
    /// was consumed.
    pub fn try_pop(&self) -> Option<Vec<u8>> {
        let len_bytes = self.read_exact(FRAME_LEN_BYTES)?;
        let frame_len = u32::from_le_bytes(len_bytes.try_into().ok()?);
        if frame_len == CLOSE_SENTINEL {
            self.closed.store(true, Ordering::Release);
            return None;
        }
        let body_len = frame_len as usize;
        if body_len == 0 {
            return Some(Vec::new());
        }
        self.read_exact(body_len)
    }

    fn read_exact(&self, len: usize) -> Option<Vec<u8>> {
        let batch = self.consumer.reserve_read_batch(len)?;
        if batch.len() != len {
            tracing::error!(
                requested = len,
                received = batch.len(),
                "shaq byte queue returned partial frame"
            );
            return None;
        }

        let mut out = Vec::with_capacity(len);
        for idx in 0..len {
            out.push(unsafe { batch.read(idx) });
        }
        Some(out)
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
        let body = vec![0xAAu8; 16];
        producer.try_push(&[&body]).expect("push 1");
        producer.try_push(&[&body]).expect("push 2");
        producer.try_push(&[&body]).expect("push 3");
        match producer.try_push(&[&body]) {
            Err(QlasterError::ShmFull) => {}
            other => panic!("expected ShmFull, got {other:?}"),
        }
        assert_eq!(producer.dropped_frames(), 1);
    }

    #[test]
    fn queue_wraps_without_splitting_frames() {
        let path = temp_ring_path("wrap");
        let producer = ShmRingProducer::create(&path, 64).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");

        let small = vec![0x11u8; 16];
        for i in 0..3 {
            producer.try_push(&[&small]).expect("push small");
            let popped = consumer.try_pop().expect("pop small");
            assert_eq!(popped.len(), 16, "iter {i}");
        }

        let big = vec![0x22u8; 20];
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
    fn close_sentinel_marks_consumer_closed() {
        let path = temp_ring_path("close");
        let producer = ShmRingProducer::create(&path, 4096).expect("create");
        let consumer = ShmRingConsumer::open(&path).expect("open");

        assert!(!consumer.is_closed());
        producer.close();
        assert!(consumer.try_pop().is_none());
        assert!(consumer.is_closed());
    }
}
