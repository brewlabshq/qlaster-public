use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arc_swap::{ArcSwap, ArcSwapOption};
use solana_bloom::bloom::Bloom;
use tokio::sync::{broadcast, mpsc};

use crate::{
    error::QlasterError,
    metrics::{QlasterSenderMetrics, unix_time_nanos},
    shm::{EventFd, ShmRingProducer},
    transport::{OutboundFrame, SlotSink},
    types::{
        AccountUpdate, PingRequest, SlotToken, SlotUpdate, SubscriptionRequest, TransactionUpdate,
    },
};

pub mod shm;

pub use shm::ShmListenerHandle;

/// Cross-channel envelope used to carry the receive timestamp of a
/// `SubscriptionRequest` across to consumers (e.g., igris) for end-to-end
/// processing latency measurement.
pub type SubscriptionRequestForward = (SubscriptionRequest, Instant);

const MAX_CONNECTION_SLOTS: usize = 10;
/// One bit per connection slot.
type SlotMask = u16;
const _: () = assert!(MAX_CONNECTION_SLOTS <= SlotMask::BITS as usize);
const SLOT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_SHM_RING_CAPACITY: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub shm: ShmTransportConfig,
}

#[derive(Clone, Debug)]
pub struct ShmTransportConfig {
    /// Path the sender binds for control. Existing files at this path are
    /// removed before bind.
    pub uds_path: PathBuf,
    /// Directory holding the per-slot ring files. Created if missing.
    pub shm_dir: PathBuf,
    /// Body capacity (bytes) of each per-slot ring. Must be a power of two.
    pub ring_capacity_bytes: usize,
}

impl ShmTransportConfig {
    pub fn defaults(uds_path: impl Into<PathBuf>) -> Self {
        // Linux keeps the RAM-backed tmpfs at /dev/shm. macOS (and other
        // unixes) have no /dev/shm, so fall back to the platform temp dir;
        // the ring is a regular mmap'd file, so any shared directory works.
        #[cfg(target_os = "linux")]
        let shm_dir = PathBuf::from(format!("/dev/shm/qlaster-{}", std::process::id()));
        #[cfg(not(target_os = "linux"))]
        let shm_dir = std::env::temp_dir().join(format!("qlaster-{}", std::process::id()));
        Self {
            uds_path: uds_path.into(),
            shm_dir,
            ring_capacity_bytes: DEFAULT_SHM_RING_CAPACITY,
        }
    }
}

#[derive(Debug)]
pub struct QlasterSender {
    state: Arc<SenderState>,
    shm_listener: ShmListenerHandle,
}

impl QlasterSender {
    pub fn metrics(&self) -> Arc<QlasterSenderMetrics> {
        Arc::clone(&self.state.metrics)
    }

    /// Cheap, cloneable handle a producer can consult *before* building an
    /// update, so account data is not copied and transactions are not
    /// serialized when no connected consumer would receive them. Grab it
    /// before calling [`QlasterSender::run`].
    pub fn interest(&self) -> SenderInterest {
        SenderInterest {
            manager: Arc::clone(&self.state.manager),
        }
    }
}

/// Producer-side pre-check backed by the sender's routing index. Every
/// method is lock-free: one atomic load plus hash lookups. A `true` answer
/// may occasionally be stale (a consumer just left); the dispatcher still
/// filters authoritatively, so this is only ever an optimization.
#[derive(Clone, Debug)]
pub struct SenderInterest {
    manager: Arc<ConnectionManager>,
}

impl SenderInterest {
    /// Any consumer subscribed to transactions (all, or by signer).
    #[inline]
    pub fn wants_transactions(&self) -> bool {
        !self.manager.tx_routing.load().tx_is_empty()
    }

    /// Any consumer would receive a transaction with these signers.
    #[inline]
    pub fn wants_transaction_signed_by(&self, signers: &[solana_pubkey::Pubkey]) -> bool {
        self.manager
            .tx_routing
            .load()
            .recipients_for_signers(signers)
            != 0
    }

    /// Any consumer would receive an update for this account.
    #[inline]
    pub fn wants_account(
        &self,
        pubkey: &solana_pubkey::Pubkey,
        owner: &solana_pubkey::Pubkey,
    ) -> bool {
        let mut mask = self.manager.tx_routing.load().account_mask;
        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            if let Some(entry) = self.manager.connections[idx].load_full() {
                if entry.filter.load().matches_account_keys(pubkey, owner) {
                    return true;
                }
            }
        }
        false
    }

    /// At least one consumer is connected (slot updates go to everyone).
    #[inline]
    pub fn has_consumers(&self) -> bool {
        self.manager.tx_routing.load().connected_mask != 0
    }
}

#[derive(Debug)]
pub(crate) struct SenderState {
    pub(crate) manager: Arc<ConnectionManager>,
    pub(crate) bloom_updates_tx: Option<mpsc::UnboundedSender<SubscriptionRequestForward>>,
    pub(crate) metrics: Arc<QlasterSenderMetrics>,
}

#[derive(Debug)]
pub struct ConnectionManager {
    pub connections: [ArcSwapOption<ManagedConnection>; MAX_CONNECTION_SLOTS],
    next_connection_id: AtomicU64,
    /// Inverted transaction-subscription index read on the dispatch hot path.
    tx_routing: ArcSwap<TxRouting>,
    /// Serializes `tx_routing` rebuilds so a stale snapshot never wins.
    tx_routing_rebuild: Mutex<()>,
}

/// Which connection slots want a transaction (and which have any account
/// filter / are connected at all), keyed so the dispatcher does
/// O(signers) hash lookups per transaction instead of evaluating every
/// consumer's filter. Mirrors the mentioned-address index Agave keeps for
/// `logsSubscribe` mentions. Rebuilt only when subscriptions or slots change.
#[derive(Debug, Default)]
struct TxRouting {
    /// Slots subscribed to every transaction.
    all_mask: SlotMask,
    /// Slots subscribed to transactions signed by a given key.
    by_signer: HashMap<solana_pubkey::Pubkey, SlotMask>,
    /// Slots with a non-empty account pubkey/owner filter.
    account_mask: SlotMask,
    /// Slots with a live connection.
    connected_mask: SlotMask,
}

impl TxRouting {
    #[inline]
    fn tx_is_empty(&self) -> bool {
        self.all_mask == 0 && self.by_signer.is_empty()
    }

    #[inline]
    fn recipients(&self, update: &TransactionUpdate) -> SlotMask {
        self.recipients_for_signers(&update.signers)
    }

    #[inline]
    fn recipients_for_signers(&self, signers: &[solana_pubkey::Pubkey]) -> SlotMask {
        let mut mask = self.all_mask;
        if !self.by_signer.is_empty() {
            for signer in signers {
                if let Some(bits) = self.by_signer.get(signer) {
                    mask |= *bits;
                }
            }
        }
        mask
    }
}

#[derive(Debug)]
pub struct ManagedConnection {
    pub slot_index: usize,
    pub connection_id: u64,
    pub filter: Arc<ArcSwap<SubscriptionFilter>>,
    sink: SlotSink,
    pub last_seen_ms: AtomicU64,
}

/// Outcome of a SHM upsert request: either an existing slot was refreshed, or
/// a fresh ring/eventfd pair was installed and must be sent over the handshake.
#[derive(Debug)]
pub(crate) enum ShmSlotProvision {
    Existing(SlotToken),
    Created {
        slot_token: SlotToken,
        ring_path: PathBuf,
        ring_capacity: u64,
        eventfd: Arc<EventFd>,
    },
}

#[derive(Debug)]
pub struct SubscriptionFilter {
    pub account_pubkeys: HashSet<solana_pubkey::Pubkey>,
    pub account_owners: HashSet<solana_pubkey::Pubkey>,
    pub bloom: Bloom<[u8; 32]>,
    pub include_transactions: bool,
    pub transaction_signers: HashSet<solana_pubkey::Pubkey>,
    single_pubkey: Option<solana_pubkey::Pubkey>,
    single_owner: Option<solana_pubkey::Pubkey>,
    use_bloom_fast_reject: bool,
}

impl SubscriptionFilter {
    fn from_sets(
        account_pubkeys: HashSet<solana_pubkey::Pubkey>,
        account_owners: HashSet<solana_pubkey::Pubkey>,
        include_transactions: bool,
        transaction_signers: HashSet<solana_pubkey::Pubkey>,
    ) -> Self {
        let requested_items = account_pubkeys.len() + account_owners.len();
        let mut bloom = Bloom::random(requested_items.max(1), 0.001, 1 << 20);

        for pubkey in &account_pubkeys {
            bloom.add(&pubkey.to_bytes());
        }
        for owner in &account_owners {
            bloom.add(&owner.to_bytes());
        }

        let single_pubkey = (account_pubkeys.len() == 1)
            .then(|| *account_pubkeys.iter().next().expect("single pubkey exists"));
        let single_owner = (account_owners.len() == 1)
            .then(|| *account_owners.iter().next().expect("single owner exists"));
        let use_bloom_fast_reject = requested_items > 16;

        Self {
            account_pubkeys,
            account_owners,
            bloom,
            include_transactions,
            transaction_signers,
            single_pubkey,
            single_owner,
            use_bloom_fast_reject,
        }
    }

    fn from_request(req: &SubscriptionRequest) -> Self {
        let mut account_pubkeys = HashSet::with_capacity(req.account_pubkeys.len());
        let mut account_owners = HashSet::with_capacity(req.account_owners.len());

        for pubkey in &req.account_pubkeys {
            account_pubkeys.insert(*pubkey);
        }
        for owner in &req.account_owners {
            account_owners.insert(*owner);
        }

        Self::from_sets(
            account_pubkeys,
            account_owners,
            req.include_transactions,
            req.transaction_signers.iter().copied().collect(),
        )
    }

    fn with_request(&self, req: &SubscriptionRequest) -> Self {
        let mut account_pubkeys = self.account_pubkeys.clone();
        let mut account_owners = self.account_owners.clone();
        account_pubkeys.extend(req.account_pubkeys.iter().copied());
        account_owners.extend(req.account_owners.iter().copied());
        let mut transaction_signers = self.transaction_signers.clone();
        transaction_signers.extend(req.transaction_signers.iter().copied());
        Self::from_sets(
            account_pubkeys,
            account_owners,
            self.include_transactions || req.include_transactions,
            transaction_signers,
        )
    }

    fn matches_account(&self, update: &AccountUpdate) -> bool {
        self.matches_account_keys(&update.account_pubkey, &update.account_owner)
    }

    fn has_account_filter(&self) -> bool {
        !self.account_pubkeys.is_empty() || !self.account_owners.is_empty()
    }

    fn matches_account_keys(
        &self,
        account_pubkey: &solana_pubkey::Pubkey,
        account_owner: &solana_pubkey::Pubkey,
    ) -> bool {
        match (self.single_pubkey, self.single_owner) {
            (Some(pubkey), None) if self.account_owners.is_empty() => {
                return pubkey == *account_pubkey;
            }
            (None, Some(owner)) if self.account_pubkeys.is_empty() => {
                return owner == *account_owner;
            }
            (Some(pubkey), Some(owner))
                if self.account_pubkeys.len() == 1 && self.account_owners.len() == 1 =>
            {
                return pubkey == *account_pubkey || owner == *account_owner;
            }
            _ => {}
        }

        if self.use_bloom_fast_reject {
            let maybe_pubkey = self.bloom.contains(&account_pubkey.to_bytes());
            let maybe_owner = self.bloom.contains(&account_owner.to_bytes());
            if !maybe_pubkey && !maybe_owner {
                return false;
            }
        }

        self.account_pubkeys.contains(account_pubkey) || self.account_owners.contains(account_owner)
    }

    #[cfg(test)]
    fn wants_transactions(&self) -> bool {
        self.include_transactions || !self.transaction_signers.is_empty()
    }

    fn matches_transaction(&self, update: &TransactionUpdate) -> bool {
        self.include_transactions
            || (!self.transaction_signers.is_empty()
                && update
                    .signers
                    .iter()
                    .any(|signer| self.transaction_signers.contains(signer)))
    }
}

impl ConnectionManager {
    fn apply_request(
        &self,
        filter: &Arc<ArcSwap<SubscriptionFilter>>,
        request: &SubscriptionRequest,
    ) {
        let current = filter.load_full();
        let next = Arc::new(current.with_request(request));
        let routing_changed = next.include_transactions != current.include_transactions
            || next.transaction_signers.len() != current.transaction_signers.len()
            || next.has_account_filter() != current.has_account_filter();
        filter.store(next);
        if routing_changed {
            self.rebuild_tx_routing();
        }
    }

    fn new() -> Self {
        Self {
            connections: std::array::from_fn(|_| ArcSwapOption::empty()),
            next_connection_id: AtomicU64::new(1),
            tx_routing: ArcSwap::from_pointee(TxRouting::default()),
            tx_routing_rebuild: Mutex::new(()),
        }
    }

    /// Recompute the transaction routing index from the live slots. Cold
    /// path: runs on subscription changes and slot removal only.
    fn rebuild_tx_routing(&self) {
        let _guard = self
            .tx_routing_rebuild
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut routing = TxRouting::default();
        for (idx, slot) in self.connections.iter().enumerate() {
            let Some(entry) = slot.load_full() else {
                continue;
            };
            let bit: SlotMask = 1 << idx;
            routing.connected_mask |= bit;
            let filter = entry.filter.load();
            if filter.has_account_filter() {
                routing.account_mask |= bit;
            }
            if filter.include_transactions {
                routing.all_mask |= bit;
            } else {
                for signer in &filter.transaction_signers {
                    *routing.by_signer.entry(*signer).or_default() |= bit;
                }
            }
        }
        self.tx_routing.store(Arc::new(routing));
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn is_stale(entry: &ManagedConnection) -> bool {
        let now = Self::now_ms();
        now.saturating_sub(entry.last_seen_ms.load(Ordering::Relaxed))
            > SLOT_IDLE_TIMEOUT.as_millis() as u64
    }

    fn cleanup_if_same(&self, slot_index: usize, connection_id: u64) {
        let current = self.connections[slot_index].load_full();
        if current
            .as_ref()
            .map(|entry| entry.connection_id == connection_id)
            .unwrap_or(false)
        {
            if let Some(entry) = current.as_ref() {
                entry.sink.close();
            }
            let _ = self.connections[slot_index].compare_and_swap(&current, None);
            self.rebuild_tx_routing();
        }
    }

    fn maybe_cleanup_stale_slot(&self, idx: usize, entry: &Arc<ManagedConnection>) -> bool {
        if !Self::is_stale(entry) {
            return false;
        }
        entry.sink.close();
        let expected = Some(Arc::clone(entry));
        let _ = self.connections[idx].compare_and_swap(&expected, None);
        self.rebuild_tx_routing();
        true
    }

    fn slot_token(entry: &ManagedConnection) -> SlotToken {
        SlotToken::new(entry.slot_index as u8, entry.connection_id)
    }

    fn lookup_by_token(&self, token: SlotToken) -> Option<Arc<ManagedConnection>> {
        let idx = token.slot_index as usize;
        if idx >= MAX_CONNECTION_SLOTS {
            return None;
        }
        let entry = self.connections[idx].load_full()?;
        if entry.connection_id != token.generation {
            return None;
        }
        if self.maybe_cleanup_stale_slot(idx, &entry) {
            return None;
        }
        Some(entry)
    }

    fn find_free_slot(&self) -> Option<usize> {
        for (idx, slot) in self.connections.iter().enumerate() {
            match slot.load_full() {
                None => return Some(idx),
                Some(existing) => {
                    if self.maybe_cleanup_stale_slot(idx, &existing)
                        && self.connections[idx].load_full().is_none()
                    {
                        return Some(idx);
                    }
                }
            }
        }
        None
    }

    fn touch_ping(&self, ping: &PingRequest) -> bool {
        if let Some(existing) = self.lookup_by_token(ping.slot_token) {
            existing
                .last_seen_ms
                .store(Self::now_ms(), Ordering::Relaxed);
            return true;
        }
        false
    }

    fn dispatch_account_update(&self, update: AccountUpdate, metrics: &QlasterSenderMetrics) {
        let dispatch_start = Instant::now();
        let mut shared_frame: Option<OutboundFrame> = None;

        for (idx, slot) in self.connections.iter().enumerate() {
            let Some(entry) = slot.load_full() else {
                continue;
            };
            if self.maybe_cleanup_stale_slot(idx, &entry) {
                continue;
            }
            if !entry.filter.load().matches_account(&update) {
                continue;
            }
            if shared_frame.is_none() {
                match encode_account_frame(&update) {
                    Ok(frame) => shared_frame = Some(frame),
                    Err(err) => {
                        tracing::warn!("failed encoding account update for dispatch: {err}");
                        metrics
                            .dispatch
                            .record(dispatch_start.elapsed().as_micros() as u64);
                        return;
                    }
                }
            }
            let framed = shared_frame
                .as_ref()
                .expect("encoded payload should exist when a recipient matches")
                .clone();
            self.push_to_entry(&entry, framed, metrics);
        }

        metrics
            .dispatch
            .record(dispatch_start.elapsed().as_micros() as u64);
    }

    fn dispatch_transaction_update(
        &self,
        update: TransactionUpdate,
        metrics: &QlasterSenderMetrics,
    ) {
        // Hot path: one atomic load, then O(signers) lookups. Nothing is
        // encoded and no slot is touched unless some consumer wants it.
        let routing = self.tx_routing.load();
        if routing.tx_is_empty() {
            return;
        }
        let mut mask = routing.recipients(&update);
        drop(routing);
        if mask == 0 {
            return;
        }

        let dispatch_start = Instant::now();
        let frame = match encode_transaction_frame(&update) {
            Ok(frame) => frame,
            Err(err) => {
                tracing::warn!("failed encoding transaction update for dispatch: {err}");
                metrics
                    .dispatch
                    .record(dispatch_start.elapsed().as_micros() as u64);
                return;
            }
        };

        while mask != 0 {
            let idx = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            let Some(entry) = self.connections[idx].load_full() else {
                continue;
            };
            if self.maybe_cleanup_stale_slot(idx, &entry) {
                continue;
            }
            // The index may briefly lag a slot being reused; confirm against
            // the recipient's own filter (cheap: only matched slots get here).
            if !entry.filter.load().matches_transaction(&update) {
                continue;
            }
            self.push_to_entry(&entry, frame.clone(), metrics);
        }

        metrics
            .dispatch
            .record(dispatch_start.elapsed().as_micros() as u64);
    }

    fn dispatch_slot_update(&self, update: SlotUpdate, metrics: &QlasterSenderMetrics) {
        let dispatch_start = Instant::now();
        let mut shared_frame: Option<OutboundFrame> = None;

        for (idx, slot) in self.connections.iter().enumerate() {
            let Some(entry) = slot.load_full() else {
                continue;
            };
            if self.maybe_cleanup_stale_slot(idx, &entry) {
                continue;
            }
            if shared_frame.is_none() {
                match encode_slot_frame(&update) {
                    Ok(frame) => shared_frame = Some(frame),
                    Err(err) => {
                        tracing::warn!("failed encoding slot update for dispatch: {err}");
                        metrics
                            .dispatch
                            .record(dispatch_start.elapsed().as_micros() as u64);
                        return;
                    }
                }
            }
            let framed = shared_frame
                .as_ref()
                .expect("encoded payload should exist when a recipient matches")
                .clone();
            self.push_to_entry(&entry, framed, metrics);
        }

        metrics
            .dispatch
            .record(dispatch_start.elapsed().as_micros() as u64);
    }

    fn push_to_entry(
        &self,
        entry: &Arc<ManagedConnection>,
        frame: OutboundFrame,
        metrics: &QlasterSenderMetrics,
    ) {
        let frame_start = frame.frame_start;
        match entry.sink.try_push(frame) {
            Ok(()) => metrics
                .shm_send
                .record(frame_start.elapsed().as_micros() as u64),
            Err(_) => {
                metrics.shm_ring_full.inc();
                tracing::warn!(
                    slot = entry.slot_index,
                    generation = entry.connection_id,
                    "shared-memory ring full; disconnecting slow consumer"
                );
                self.cleanup_if_same(entry.slot_index, entry.connection_id);
            }
        }
    }

    /// SHM provisioning info returned to the UDS handshake on first install.
    pub(crate) async fn upsert_shm(
        self: &Arc<Self>,
        request: SubscriptionRequest,
        shm_dir: &std::path::Path,
        ring_capacity: usize,
    ) -> Result<ShmSlotProvision, QlasterError> {
        if let Some(existing) = request
            .slot_token
            .and_then(|token| self.lookup_by_token(token))
        {
            existing
                .last_seen_ms
                .store(Self::now_ms(), Ordering::Relaxed);
            self.apply_request(&existing.filter, &request);
            return Ok(ShmSlotProvision::Existing(Self::slot_token(&existing)));
        }

        let filter = Arc::new(ArcSwap::from_pointee(SubscriptionFilter::from_request(
            &request,
        )));
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);

        loop {
            let Some(slot_index) = self.find_free_slot() else {
                return Err(QlasterError::ConfigError(format!(
                    "connection slots exhausted (max {MAX_CONNECTION_SLOTS})"
                )));
            };

            let pid = std::process::id();
            let ring_path = shm_dir.join(format!("{pid}-{slot_index}-{connection_id}"));
            let ring = Arc::new(ShmRingProducer::create(&ring_path, ring_capacity)?);
            let eventfd = Arc::new(
                EventFd::new().map_err(|e| QlasterError::ShmError(format!("eventfd: {e}")))?,
            );

            let managed = Arc::new(ManagedConnection {
                slot_index,
                connection_id,
                filter: Arc::clone(&filter),
                sink: SlotSink::shm(Arc::clone(&ring), Arc::clone(&eventfd)),
                last_seen_ms: AtomicU64::new(Self::now_ms()),
            });

            let previous = self.connections[slot_index]
                .compare_and_swap(&None::<Arc<ManagedConnection>>, Some(Arc::clone(&managed)));
            if previous.is_none() {
                self.rebuild_tx_routing();
                let slot_token = SlotToken::new(slot_index as u8, connection_id);
                tracing::info!(
                    transport = "shm",
                    slot = slot_index,
                    generation = connection_id,
                    ring = %ring_path.display(),
                    "qlaster slot installed"
                );
                return Ok(ShmSlotProvision::Created {
                    slot_token,
                    ring_path,
                    ring_capacity: ring_capacity as u64,
                    eventfd,
                });
            }
        }
    }
}

fn encode_account_frame(update: &AccountUpdate) -> Result<OutboundFrame, QlasterError> {
    let frame_start = Instant::now();
    let (header, payload) = update.encode_parts_at(unix_time_nanos())?;
    Ok(OutboundFrame {
        header,
        payload,
        frame_start,
    })
}

fn encode_transaction_frame(update: &TransactionUpdate) -> Result<OutboundFrame, QlasterError> {
    let frame_start = Instant::now();
    let (header, payload) = update.encode_parts_at(unix_time_nanos())?;
    Ok(OutboundFrame {
        header,
        payload,
        frame_start,
    })
}

fn encode_slot_frame(update: &SlotUpdate) -> Result<OutboundFrame, QlasterError> {
    let frame_start = Instant::now();
    let (header, payload) = update.encode_parts_at(unix_time_nanos())?;
    Ok(OutboundFrame {
        header,
        payload,
        frame_start,
    })
}

pub async fn setup_sender(
    config: SenderConfig,
    master_updates: broadcast::Sender<AccountUpdate>,
    bloom_updates_tx: Option<mpsc::UnboundedSender<SubscriptionRequestForward>>,
    metrics: Arc<QlasterSenderMetrics>,
) -> Result<QlasterSender, QlasterError> {
    setup_sender_with_transactions(config, master_updates, None, bloom_updates_tx, metrics).await
}

pub async fn setup_sender_with_transactions(
    config: SenderConfig,
    master_updates: broadcast::Sender<AccountUpdate>,
    transaction_updates: Option<broadcast::Sender<TransactionUpdate>>,
    bloom_updates_tx: Option<mpsc::UnboundedSender<SubscriptionRequestForward>>,
    metrics: Arc<QlasterSenderMetrics>,
) -> Result<QlasterSender, QlasterError> {
    setup_sender_with_streams(
        config,
        master_updates,
        transaction_updates,
        None,
        bloom_updates_tx,
        metrics,
    )
    .await
}

pub async fn setup_sender_with_streams(
    config: SenderConfig,
    master_updates: broadcast::Sender<AccountUpdate>,
    transaction_updates: Option<broadcast::Sender<TransactionUpdate>>,
    slot_updates: Option<broadcast::Sender<SlotUpdate>>,
    bloom_updates_tx: Option<mpsc::UnboundedSender<SubscriptionRequestForward>>,
    metrics: Arc<QlasterSenderMetrics>,
) -> Result<QlasterSender, QlasterError> {
    let manager = Arc::new(ConnectionManager::new());
    let shm_listener = shm::bind_listener(&config.shm).await?;

    let state = Arc::new(SenderState {
        manager,
        bloom_updates_tx,
        metrics,
    });

    let dispatcher_state = Arc::clone(&state);
    let mut master_rx = master_updates.subscribe();
    tokio::spawn(async move {
        loop {
            match master_rx.recv().await {
                Ok(update) => dispatcher_state
                    .manager
                    .dispatch_account_update(update, &dispatcher_state.metrics),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!("qlaster account dispatcher lagged; skipped {skipped} updates");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    if let Some(transaction_updates) = transaction_updates {
        let dispatcher_state = Arc::clone(&state);
        let mut transaction_rx = transaction_updates.subscribe();
        tokio::spawn(async move {
            loop {
                match transaction_rx.recv().await {
                    Ok(update) => dispatcher_state
                        .manager
                        .dispatch_transaction_update(update, &dispatcher_state.metrics),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            "qlaster transaction dispatcher lagged; skipped {skipped} updates"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    if let Some(slot_updates) = slot_updates {
        let dispatcher_state = Arc::clone(&state);
        let mut slot_rx = slot_updates.subscribe();
        tokio::spawn(async move {
            loop {
                match slot_rx.recv().await {
                    Ok(update) => dispatcher_state
                        .manager
                        .dispatch_slot_update(update, &dispatcher_state.metrics),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("qlaster slot dispatcher lagged; skipped {skipped} updates");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    Ok(QlasterSender {
        state,
        shm_listener,
    })
}

impl QlasterSender {
    pub async fn run(self) -> Result<(), QlasterError> {
        let QlasterSender {
            state,
            shm_listener,
        } = self;
        shm::run_listener(state, shm_listener).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AccountPayload;
    use solana_pubkey::Pubkey;

    fn sample_update(account_pubkey: Pubkey, account_owner: Pubkey) -> AccountUpdate {
        AccountUpdate {
            account_pubkey,
            account_owner,
            lamports: 1,
            executable: false,
            rent_epoch: 0,
            slot: 0,
            write_version: 0,
            payload: AccountPayload::from_slice(&[]).expect("empty payload"),
        }
    }

    #[test]
    fn filter_from_request_deduplicates_and_matches_pubkey_or_owner() {
        let matched_pubkey = Pubkey::new_unique();
        let matched_owner = Pubkey::new_unique();
        let other_pubkey = Pubkey::new_unique();
        let other_owner = Pubkey::new_unique();

        let request = SubscriptionRequest::new(
            vec![matched_pubkey, matched_pubkey],
            vec![matched_owner, matched_owner],
        );
        let filter = SubscriptionFilter::from_request(&request);

        assert_eq!(filter.account_pubkeys.len(), 1);
        assert_eq!(filter.account_owners.len(), 1);

        assert!(filter.matches_account(&sample_update(matched_pubkey, other_owner)));
        assert!(filter.matches_account(&sample_update(other_pubkey, matched_owner)));
        assert!(!filter.matches_account(&sample_update(other_pubkey, other_owner)));
    }

    #[test]
    fn add_request_updates_existing_filter_contents() {
        let first_pubkey = Pubkey::new_unique();
        let first_owner = Pubkey::new_unique();
        let second_pubkey = Pubkey::new_unique();
        let second_owner = Pubkey::new_unique();

        let filter = SubscriptionFilter::from_request(&SubscriptionRequest::new(
            vec![first_pubkey],
            vec![first_owner],
        ));
        assert!(!filter.matches_account(&sample_update(second_pubkey, second_owner)));

        let filter = filter.with_request(&SubscriptionRequest::new(
            vec![second_pubkey],
            vec![second_owner],
        ));

        assert!(filter.account_pubkeys.contains(&first_pubkey));
        assert!(filter.account_pubkeys.contains(&second_pubkey));
        assert!(filter.account_owners.contains(&first_owner));
        assert!(filter.account_owners.contains(&second_owner));
        assert!(filter.matches_account(&sample_update(second_pubkey, second_owner)));
    }

    fn sample_tx(signers: Vec<Pubkey>) -> TransactionUpdate {
        TransactionUpdate::new(1, 0, [0u8; 64], false, bytes::Bytes::from_static(b"tx"))
            .expect("tx")
            .with_signers(signers)
    }

    #[test]
    fn transaction_subscription_is_explicit_and_sticky() {
        let tx = sample_tx(vec![Pubkey::new_unique()]);
        let filter =
            SubscriptionFilter::from_request(&SubscriptionRequest::new(Vec::new(), Vec::new()));
        assert!(!filter.wants_transactions());
        assert!(!filter.matches_transaction(&tx));

        let filter = filter.with_request(&SubscriptionRequest::transactions());
        assert!(filter.matches_transaction(&tx));

        let filter = filter.with_request(&SubscriptionRequest::new(Vec::new(), Vec::new()));
        assert!(filter.matches_transaction(&tx));
    }

    #[test]
    fn signer_subscription_matches_only_signed_transactions() {
        let wallet = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let filter =
            SubscriptionFilter::from_request(&SubscriptionRequest::transactions_signed_by(vec![
                wallet,
            ]));
        assert!(filter.wants_transactions());
        assert!(!filter.include_transactions);

        // Fee payer and secondary signer both count.
        assert!(filter.matches_transaction(&sample_tx(vec![wallet])));
        assert!(filter.matches_transaction(&sample_tx(vec![other, wallet])));
        assert!(!filter.matches_transaction(&sample_tx(vec![other])));
        // Producer that never attached signers can never match a signer filter.
        assert!(!filter.matches_transaction(&sample_tx(Vec::new())));

        // Signer sets accumulate across requests.
        let filter = filter.with_request(&SubscriptionRequest::transactions_signed_by(vec![other]));
        assert!(filter.matches_transaction(&sample_tx(vec![other])));
    }

    #[test]
    fn tx_routing_ors_all_and_signer_masks() {
        let wallet_a = Pubkey::new_unique();
        let wallet_b = Pubkey::new_unique();
        let mut routing = TxRouting::default();
        assert!(routing.tx_is_empty());

        routing.by_signer.insert(wallet_a, 0b0010);
        routing.by_signer.insert(wallet_b, 0b0100);
        assert_eq!(routing.recipients(&sample_tx(vec![wallet_a])), 0b0010);
        assert_eq!(
            routing.recipients(&sample_tx(vec![wallet_a, wallet_b])),
            0b0110
        );
        assert_eq!(
            routing.recipients(&sample_tx(vec![Pubkey::new_unique()])),
            0
        );

        routing.all_mask = 0b0001;
        assert_eq!(routing.recipients(&sample_tx(Vec::new())), 0b0001);
        assert_eq!(routing.recipients(&sample_tx(vec![wallet_b])), 0b0101);
    }

    #[test]
    fn empty_filter_matches_nothing() {
        let filter =
            SubscriptionFilter::from_request(&SubscriptionRequest::new(Vec::new(), Vec::new()));

        assert!(filter.account_pubkeys.is_empty());
        assert!(filter.account_owners.is_empty());
        assert!(
            !filter.matches_account(&sample_update(Pubkey::new_unique(), Pubkey::new_unique()))
        );
    }
}
