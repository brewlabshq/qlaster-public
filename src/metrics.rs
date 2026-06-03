use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub fn unix_time_nanos() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    nanos.min(u64::MAX as u128) as u64
}

pub fn elapsed_since_unix_time_nanos_us(start_nanos: u64) -> u64 {
    if start_nanos == 0 {
        return 0;
    }
    let elapsed_nanos = unix_time_nanos().saturating_sub(start_nanos);
    if elapsed_nanos == 0 {
        0
    } else {
        elapsed_nanos.saturating_add(999) / 1_000
    }
}

#[derive(Debug)]
pub struct LatencyAggregator {
    sum_us: AtomicU64,
    count: AtomicU64,
    min_us: AtomicU64,
    max_us: AtomicU64,
}

impl Default for LatencyAggregator {
    fn default() -> Self {
        Self {
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
            min_us: AtomicU64::new(u64::MAX),
            max_us: AtomicU64::new(0),
        }
    }
}

impl LatencyAggregator {
    pub fn record(&self, elapsed_us: u64) {
        self.sum_us.fetch_add(elapsed_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.min_us.fetch_min(elapsed_us, Ordering::Relaxed);
        self.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
    }

    pub fn flush(&self) -> LatencySnapshot {
        let count = self.count.swap(0, Ordering::Relaxed);
        let total_us = self.sum_us.swap(0, Ordering::Relaxed);
        let min_us = self.min_us.swap(u64::MAX, Ordering::Relaxed);
        let max_us = self.max_us.swap(0, Ordering::Relaxed);
        LatencySnapshot {
            count,
            total_us,
            min_us: if count == 0 { 0 } else { min_us },
            max_us,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LatencySnapshot {
    pub count: u64,
    pub total_us: u64,
    pub min_us: u64,
    pub max_us: u64,
}

impl LatencySnapshot {
    pub fn avg_us(&self) -> u64 {
        self.total_us.checked_div(self.count).unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct CounterAggregator {
    value: AtomicU64,
}

impl CounterAggregator {
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Read-and-zero. Mirrors `LatencyAggregator::flush` so callers can
    /// pull metrics on a fixed cadence without compounding.
    pub fn flush(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }

    pub fn peek(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub struct QlasterSenderMetrics {
    /// Universal: time spent inside `dispatch_update` (encode + filter + push
    /// to all matching shared-memory sinks).
    pub dispatch: LatencyAggregator,
    /// Time from frame creation to ring-publish (Release store of
    /// producer_pos). Mostly memcpy time.
    pub shm_send: LatencyAggregator,
    /// Count of frames dropped because the per-slot ring was full.
    /// The slot is also evicted (slow-consumer policy).
    pub shm_ring_full: CounterAggregator,
}

impl QlasterSenderMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default)]
pub struct QlasterConsumerMetrics {
    /// Data-frame only: time spent reading bytes from the transport into user
    /// memory. For SHM this is ring pop/copy.
    pub read: LatencyAggregator,
    /// Data-frame only: time spent decoding an account-update frame.
    pub decode: LatencyAggregator,
    /// Data-frame only: time spent pushing a decoded update into the local
    /// consumer queue.
    pub enqueue: LatencyAggregator,
    /// Data-frame only: sender frame creation through consumer queue publish.
    /// Uses sender wall-clock metadata in the wire frame, so cross-host clock
    /// skew can make this saturate to zero.
    pub full_read: LatencyAggregator,
}

impl QlasterConsumerMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}
