use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use crate::shm::{EventFd, ShmRingProducer};

/// Single data frame as it sits in a shared-memory sink, pre-split into the
/// contiguous chunks the SHM producer concatenates into the ring slot.
#[derive(Clone, Debug)]
pub struct OutboundFrame {
    pub header: Bytes,
    pub payload: Bytes,
    /// Stamped at frame creation in the sender dispatcher.
    pub frame_start: Instant,
}

impl OutboundFrame {
    pub fn total_len(&self) -> usize {
        self.header.len() + self.payload.len()
    }
}

/// Per-slot shared-memory fan-out sink.
#[derive(Debug)]
pub struct SlotSink {
    ring: Arc<ShmRingProducer>,
    wakeup: Arc<EventFd>,
}

impl SlotSink {
    pub fn shm(ring: Arc<ShmRingProducer>, wakeup: Arc<EventFd>) -> Self {
        Self { ring, wakeup }
    }

    /// Push a frame to the ring. On failure (sink closed / ring out of room),
    /// the frame is returned so the caller can drop the slow consumer.
    pub fn try_push(&self, frame: OutboundFrame) -> Result<(), OutboundFrame> {
        let parts: [&[u8]; 2] = [&frame.header, &frame.payload];
        match self.ring.try_push(&parts) {
            Ok(()) => {
                if let Err(err) = self.wakeup.notify() {
                    tracing::warn!("shm wakeup notify failed: {err}");
                }
                Ok(())
            }
            Err(_) => Err(frame),
        }
    }

    pub fn close(&self) {
        self.ring.close();
        let _ = self.wakeup.notify();
    }
}
