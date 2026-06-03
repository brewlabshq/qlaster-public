//! SHM-transport consumer. Uses a UDS control channel and an mmap'd ring for
//! the data plane.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, AtomicU8, Ordering},
    Arc,
};
use std::time::Instant;

use crossbeam_queue::ArrayQueue;
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use crate::{
    error::QlasterError,
    metrics::{elapsed_since_unix_time_nanos_us, QlasterConsumerMetrics},
    shm::{recv_frame_with_fd, AsyncEventFd, EventFd, ShmRingConsumer},
    types::{
        decode_server_frame, decode_server_frame_owned, decode_server_frame_owned_with_meta,
        AccountUpdate, ConnectionReady, PingRequest, ServerFrame, ServerFrameWithMeta, SlotToken,
        SlotUpdate, SubscriptionRequest, TransactionUpdate,
    },
    wire::{read_framed, write_framed},
};

const CONSUMER_UPDATE_QUEUE_CAPACITY: usize = 2048;
const UNSET_SLOT_INDEX: u8 = u8::MAX;

#[derive(Debug)]
pub struct QlasterShmConsumer {
    stream: Arc<Mutex<UnixStream>>,
    slot_token_index: Arc<AtomicU8>,
    slot_token_generation: Arc<AtomicU64>,
    pub updates: Arc<ArrayQueue<AccountUpdate>>,
    pub transactions: Arc<ArrayQueue<TransactionUpdate>>,
    pub slots: Arc<ArrayQueue<SlotUpdate>>,
    metrics: Arc<QlasterConsumerMetrics>,
    reader_task: tokio::task::JoinHandle<()>,
    /// Held only to extend the ring's lifetime; reader_task has its own
    /// `Arc<ShmRingConsumer>` clone.
    _ring: Arc<ShmRingConsumer>,
}

impl Drop for QlasterShmConsumer {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
}

/// Connect to a sender's SHM control socket, perform the SHM handshake (send
/// an empty subscribe → receive ConnectionReadyShm + ring-eventfd over
/// SCM_RIGHTS), open the per-slot ring, and start the reader task.
pub async fn setup_shm_consumer(
    uds_path: impl AsRef<Path>,
) -> Result<QlasterShmConsumer, QlasterError> {
    setup_shm_consumer_with_metrics(uds_path, Arc::new(QlasterConsumerMetrics::new())).await
}

pub async fn setup_shm_consumer_with_metrics(
    uds_path: impl AsRef<Path>,
    metrics: Arc<QlasterConsumerMetrics>,
) -> Result<QlasterShmConsumer, QlasterError> {
    let mut stream = UnixStream::connect(uds_path.as_ref())
        .await
        .map_err(|e| QlasterError::UdsError(format!("connect: {e}")))?;
    crate::shm::set_nosigpipe(stream.as_raw_fd())
        .map_err(|e| QlasterError::UdsError(format!("set SO_NOSIGPIPE: {e}")))?;

    // Bootstrap subscribe: empty filter, no slot token. Triggers slot creation
    // on the sender and the SCM_RIGHTS ConnectionReadyShm reply.
    let bootstrap = SubscriptionRequest::new(Vec::new(), Vec::new());
    write_framed(&mut stream, &bootstrap.encode()).await?;

    let (ready_bytes, fd_opt) = recv_frame_with_fd(&stream).await?;
    let ready = match decode_server_frame(&ready_bytes)? {
        ServerFrame::ConnectionReadyShm(r) => r,
        other => {
            return Err(QlasterError::UdsError(format!(
                "expected ConnectionReadyShm, got {other:?}"
            )));
        }
    };
    let fd = fd_opt
        .ok_or_else(|| QlasterError::UdsError("ConnectionReadyShm missing eventfd".into()))?;
    let eventfd = EventFd::from_owned_fd(fd);

    let ring_path = PathBuf::from(&ready.ring_path);
    let ring = Arc::new(ShmRingConsumer::open(&ring_path)?);
    let slot_token_index = Arc::new(AtomicU8::new(ready.slot_token.slot_index));
    let slot_token_generation = Arc::new(AtomicU64::new(ready.slot_token.generation));
    let updates = Arc::new(ArrayQueue::new(CONSUMER_UPDATE_QUEUE_CAPACITY));
    let transactions = Arc::new(ArrayQueue::new(CONSUMER_UPDATE_QUEUE_CAPACITY));
    let slots = Arc::new(ArrayQueue::new(CONSUMER_UPDATE_QUEUE_CAPACITY));

    let reader_task = tokio::spawn(run_reader(
        Arc::clone(&ring),
        Arc::clone(&updates),
        Arc::clone(&transactions),
        Arc::clone(&slots),
        eventfd,
        Arc::clone(&metrics),
    ));

    Ok(QlasterShmConsumer {
        stream: Arc::new(Mutex::new(stream)),
        slot_token_index,
        slot_token_generation,
        updates,
        transactions,
        slots,
        metrics,
        reader_task,
        _ring: ring,
    })
}

impl QlasterShmConsumer {
    pub async fn subscribe(
        &mut self,
        account_pubkeys: Vec<solana_pubkey::Pubkey>,
        account_owners: Vec<solana_pubkey::Pubkey>,
    ) -> Result<(), QlasterError> {
        let request = SubscriptionRequest::new(account_pubkeys, account_owners)
            .with_slot_token(self.slot_token());
        self.send_subscription(request).await
    }

    pub async fn subscribe_transactions(&mut self) -> Result<(), QlasterError> {
        let request = SubscriptionRequest::transactions().with_slot_token(self.slot_token());
        self.send_subscription(request).await
    }

    async fn send_subscription(
        &mut self,
        request: SubscriptionRequest,
    ) -> Result<(), QlasterError> {
        let encoded = request.encode();
        let mut guard = self.stream.lock().await;
        write_framed(&mut *guard, &encoded).await?;
        // The sender replies with a regular ConnectionReady (no shm handshake)
        // for re-subscribes that already have a slot. Drain it so the next
        // subscribe / ping doesn't pick up the stale reply.
        let reply = read_framed(&mut *guard).await?;
        match decode_server_frame_owned(reply)? {
            ServerFrame::ConnectionReady(ConnectionReady { slot_token }) => {
                self.slot_token_index
                    .store(slot_token.slot_index, Ordering::Release);
                self.slot_token_generation
                    .store(slot_token.generation, Ordering::Release);
            }
            ServerFrame::ConnectionReadyShm(_) => {
                return Err(QlasterError::UdsError(
                    "unexpected ConnectionReadyShm on re-subscribe".into(),
                ));
            }
            ServerFrame::AccountUpdate(_) => {
                return Err(QlasterError::UdsError(
                    "AccountUpdate on UDS control channel".into(),
                ));
            }
            ServerFrame::TransactionUpdate(_) => {
                return Err(QlasterError::UdsError(
                    "TransactionUpdate on UDS control channel".into(),
                ));
            }
            ServerFrame::SlotUpdate(_) => {
                return Err(QlasterError::UdsError(
                    "SlotUpdate on UDS control channel".into(),
                ));
            }
        }
        Ok(())
    }

    pub async fn send_ping(&mut self) -> Result<(), QlasterError> {
        let Some(token) = self.slot_token() else {
            return Err(QlasterError::MalformedPayload(
                "cannot send ping before slot token is assigned",
            ));
        };
        let ping = PingRequest::new(token);
        let encoded = ping.encode();
        let mut guard = self.stream.lock().await;
        write_framed(&mut *guard, &encoded).await?;
        Ok(())
    }

    pub fn slot_token(&self) -> Option<SlotToken> {
        let idx = self.slot_token_index.load(Ordering::Acquire);
        if idx == UNSET_SLOT_INDEX {
            return None;
        }
        let generation = self.slot_token_generation.load(Ordering::Acquire);
        Some(SlotToken::new(idx, generation))
    }

    pub fn try_next_update(&self) -> Option<AccountUpdate> {
        self.updates.pop()
    }

    pub fn try_next_transaction(&self) -> Option<TransactionUpdate> {
        self.transactions.pop()
    }

    pub fn try_next_slot(&self) -> Option<SlotUpdate> {
        self.slots.pop()
    }

    pub fn metrics(&self) -> Arc<QlasterConsumerMetrics> {
        Arc::clone(&self.metrics)
    }
}

async fn run_reader(
    ring: Arc<ShmRingConsumer>,
    updates: Arc<ArrayQueue<AccountUpdate>>,
    transactions: Arc<ArrayQueue<TransactionUpdate>>,
    slots: Arc<ArrayQueue<SlotUpdate>>,
    eventfd: EventFd,
    metrics: Arc<QlasterConsumerMetrics>,
) {
    let async_efd = match AsyncEventFd::new(eventfd) {
        Ok(a) => a,
        Err(err) => {
            tracing::error!("shm consumer: async eventfd setup failed: {err}");
            return;
        }
    };
    loop {
        // Drain whatever's in the ring before sleeping on the eventfd.
        // This handles the race where the producer notifies and then advances
        // producer_pos before the consumer drains; we don't want to miss a
        // wake.
        loop {
            let read_start = Instant::now();
            match ring.try_pop() {
                Some(bytes) => {
                    let read_elapsed_us = read_start.elapsed().as_micros() as u64;
                    let decode_start = Instant::now();
                    match decode_server_frame_owned_with_meta(bytes) {
                        Ok(ServerFrameWithMeta::AccountUpdate { update, meta }) => {
                            record_decode_and_enqueue(
                                &metrics,
                                read_elapsed_us,
                                decode_start,
                                meta.sender_created_at_unix_nanos,
                                || {
                                    let _ = updates.force_push(update);
                                },
                            );
                        }
                        Ok(ServerFrameWithMeta::TransactionUpdate { update, meta }) => {
                            record_decode_and_enqueue(
                                &metrics,
                                read_elapsed_us,
                                decode_start,
                                meta.sender_created_at_unix_nanos,
                                || {
                                    let _ = transactions.force_push(update);
                                },
                            );
                        }
                        Ok(ServerFrameWithMeta::SlotUpdate { update, meta }) => {
                            record_decode_and_enqueue(
                                &metrics,
                                read_elapsed_us,
                                decode_start,
                                meta.sender_created_at_unix_nanos,
                                || {
                                    let _ = slots.force_push(update);
                                },
                            );
                        }
                        Ok(other) => {
                            tracing::warn!(
                                "shm consumer received non-update frame on ring: {other:?}"
                            );
                        }
                        Err(err) => {
                            tracing::warn!("shm consumer frame decode failed: {err}");
                        }
                    }
                }
                None => break,
            }
        }

        if ring.is_closed() {
            return;
        }

        if let Err(err) = async_efd.wait().await {
            tracing::warn!("shm consumer eventfd wait failed: {err}");
            return;
        }
    }
}

fn record_decode_and_enqueue<F>(
    metrics: &QlasterConsumerMetrics,
    read_elapsed_us: u64,
    decode_start: Instant,
    sender_created_at_unix_nanos: u64,
    enqueue: F,
) where
    F: FnOnce(),
{
    let decode_elapsed_us = decode_start.elapsed().as_micros() as u64;
    metrics.read.record(read_elapsed_us);
    metrics.decode.record(decode_elapsed_us);
    let enqueue_start = Instant::now();
    enqueue();
    metrics
        .enqueue
        .record(enqueue_start.elapsed().as_micros() as u64);
    metrics.full_read.record(elapsed_since_unix_time_nanos_us(
        sender_created_at_unix_nanos,
    ));
}
