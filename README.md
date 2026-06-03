# Qlaster

Shared-memory data streaming for colocated Solana services.

Account, transaction, and slot updates are fanned out from a single sender to local
consumers over a per-consumer SPSC ring in `/dev/shm`, with a Unix-domain
control socket for handshake, subscription, and eventfd-based wakeups.

![qlaster](./image.png)

## Why

- **Zero-copy hot path.** Updates land in mmap'd ring memory; consumers read in
  place. No socket copy, no serializer per consumer.
- **Filter at the sender.** Per-consumer subscriptions (account pubkeys,
  owners, transaction opt-in) — large filters fall back to a bloom-pre-reject.
- **Backpressure-aware.** A slow consumer that fills its ring is dropped
  rather than stalling the dispatcher.
- **Opportunistic LZ4** on account payloads ≥ 500 KiB when entropy sampling
  predicts a real win.

## Layout

- `sender/` — binds the UDS, provisions a ring + eventfd per consumer, runs
  the dispatcher tasks fed by `broadcast::Sender<AccountUpdate>` /
  `broadcast::Sender<TransactionUpdate>` / `broadcast::Sender<SlotUpdate>`.
- `consumer/` — connects to the UDS, receives the ring path + eventfd via
  `SCM_RIGHTS`, drains frames into `crossbeam_queue::ArrayQueue`s the caller
  polls.
- `shm/` — ring buffer, eventfd, UDS framing primitives.
- `types.rs`, `wire.rs` — wire format and frame codecs.

## Usage

```rust
use qlaster::sender::{SenderConfig, ShmTransportConfig, setup_sender};
use qlaster::consumer::setup_shm_consumer;
use qlaster::metrics::QlasterSenderMetrics;
use std::sync::Arc;
use tokio::sync::broadcast;

let (updates_tx, _) = broadcast::channel(128);
let cfg = SenderConfig { shm: ShmTransportConfig::defaults("/tmp/qlaster.sock") };
let sender = setup_sender(cfg, updates_tx.clone(), None, Arc::new(QlasterSenderMetrics::new())).await?;
tokio::spawn(sender.run());

let mut consumer = setup_shm_consumer("/tmp/qlaster.sock").await?;
consumer.subscribe(vec![pubkey], vec![]).await?;
while let Some(update) = consumer.updates.pop() { /* ... */ }
```

See `tests/shm_flow.rs` for end-to-end examples (account filtering, transaction
opt-in, always-on slot updates, reconnection).

## Requirements

Linux only — depends on `eventfd(2)`, `memfd_create`, and `SCM_RIGHTS` over
Unix sockets. Sender and consumer must share a host (and the `/dev/shm`
directory the sender chooses).
