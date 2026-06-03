use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use qlaster::consumer::setup_shm_consumer;
use qlaster::metrics::QlasterSenderMetrics;
use qlaster::sender::{
    setup_sender, setup_sender_with_streams, setup_sender_with_transactions, SenderConfig,
    ShmTransportConfig,
};
use qlaster::types::{
    AccountPayload, AccountUpdate, SlotUpdate, TransactionPayload, TransactionUpdate,
};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc};

fn unique_paths(label: &str) -> (PathBuf, PathBuf) {
    let pid = std::process::id();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let base = std::env::temp_dir().join(format!("qlaster-shm-{label}-{pid}-{nonce}"));
    let uds = base.join("control.sock");
    let shm = base.join("rings");
    (uds, shm)
}

fn shm_config(label: &str) -> ShmTransportConfig {
    let (uds_path, shm_dir) = unique_paths(label);
    ShmTransportConfig {
        uds_path,
        shm_dir,
        ring_capacity_bytes: 1024 * 1024, // 1 MiB is plenty for tests
    }
}

fn test_pubkey(seed: u8) -> Pubkey {
    Pubkey::new_from_array([seed; 32])
}

async fn drain_one_update<F>(poll: F, timeout: Duration) -> Option<AccountUpdate>
where
    F: Fn() -> Option<AccountUpdate>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(update) = poll() {
            return Some(update);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::task::yield_now().await;
    }
}

async fn drain_one_transaction<F>(poll: F, timeout: Duration) -> Option<TransactionUpdate>
where
    F: Fn() -> Option<TransactionUpdate>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(update) = poll() {
            return Some(update);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::task::yield_now().await;
    }
}

async fn drain_one_slot<F>(poll: F, timeout: Duration) -> Option<SlotUpdate>
where
    F: Fn() -> Option<SlotUpdate>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(update) = poll() {
            return Some(update);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shm_end_to_end_filter_and_delivery() {
    let cfg = shm_config("basic");
    let uds_path = cfg.uds_path.clone();

    let (updates_tx, _) = broadcast::channel::<AccountUpdate>(128);
    let (bloom_tx, mut bloom_rx) = mpsc::unbounded_channel();

    let metrics = Arc::new(QlasterSenderMetrics::new());
    let sender = setup_sender(
        SenderConfig { shm: cfg },
        updates_tx.clone(),
        Some(bloom_tx),
        Arc::clone(&metrics),
    )
    .await
    .expect("setup sender");
    let _sender_task = tokio::spawn(sender.run());

    let mut consumer = setup_shm_consumer(&uds_path)
        .await
        .expect("setup shm consumer");

    // Bootstrap subscribe (with empty filter) is sent inside setup_shm_consumer
    // and surfaces via the bloom channel.
    let (boot_req, _) = tokio::time::timeout(Duration::from_secs(2), bloom_rx.recv())
        .await
        .expect("bootstrap bloom timeout")
        .expect("bootstrap bloom closed");
    assert!(boot_req.account_pubkeys.is_empty());
    assert!(boot_req.account_owners.is_empty());

    let pk = test_pubkey(1);
    let owner = test_pubkey(2);
    consumer
        .subscribe(vec![pk], vec![owner])
        .await
        .expect("subscribe");

    // Bloom forward of the real subscribe.
    let (sub_req, _) = tokio::time::timeout(Duration::from_secs(2), bloom_rx.recv())
        .await
        .expect("subscribe bloom timeout")
        .expect("subscribe bloom closed");
    assert_eq!(sub_req.account_pubkeys, vec![pk]);
    assert_eq!(sub_req.account_owners, vec![owner]);

    let other_pk = test_pubkey(3);
    let other_owner = test_pubkey(4);

    // Non-matching update — should not appear at the consumer.
    let _ = updates_tx.send(AccountUpdate {
        account_pubkey: other_pk,
        account_owner: other_owner,
        lamports: 1,
        executable: false,
        rent_epoch: 0,
        slot: 1,
        write_version: 1,
        payload: AccountPayload::from_slice(b"miss").expect("payload"),
    });
    let none = drain_one_update(|| consumer.try_next_update(), Duration::from_millis(300)).await;
    assert!(none.is_none(), "unexpected non-matching delivery");

    // Matching update — should arrive.
    let _ = updates_tx.send(AccountUpdate {
        account_pubkey: pk,
        account_owner: owner,
        lamports: 42,
        executable: true,
        rent_epoch: 7,
        slot: 2,
        write_version: 2,
        payload: AccountPayload::from_slice(b"hit").expect("payload"),
    });
    let got = drain_one_update(|| consumer.try_next_update(), Duration::from_secs(3))
        .await
        .expect("matching update timeout");
    assert_eq!(got.account_pubkey, pk);
    assert_eq!(got.lamports, 42);
    assert_eq!(got.payload.as_slice(), b"hit");

    consumer.send_ping().await.expect("ping");

    let dispatch_snap = metrics.dispatch.flush();
    let shm_send_snap = metrics.shm_send.flush();
    assert!(
        dispatch_snap.count >= 1,
        "dispatch metric should fire at least once, got {dispatch_snap:?}"
    );
    assert!(
        shm_send_snap.count >= 1,
        "shm_send metric should fire on the matching update, got {shm_send_snap:?}"
    );
    assert_eq!(
        metrics.shm_ring_full.peek(),
        0,
        "no ring-full drops expected on a sized-to-fit ring"
    );

    let consumer_metrics = consumer.metrics();
    let read_snap = consumer_metrics.read.flush();
    let decode_snap = consumer_metrics.decode.flush();
    let enqueue_snap = consumer_metrics.enqueue.flush();
    let full_read_snap = consumer_metrics.full_read.flush();
    assert!(
        read_snap.count >= 1,
        "consumer read metric should fire for delivered SHM update, got {read_snap:?}"
    );
    assert!(
        decode_snap.count >= 1,
        "consumer decode metric should fire for delivered SHM update, got {decode_snap:?}"
    );
    assert!(
        enqueue_snap.count >= 1,
        "consumer enqueue metric should fire for delivered SHM update, got {enqueue_snap:?}"
    );
    assert!(
        full_read_snap.count >= 1 && full_read_snap.total_us > 0,
        "consumer full_read metric should cover sender-to-queue latency, got {full_read_snap:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shm_transaction_stream_requires_subscription() {
    let cfg = shm_config("transactions");
    let uds_path = cfg.uds_path.clone();

    let (updates_tx, _) = broadcast::channel::<AccountUpdate>(128);
    let (transactions_tx, _) = broadcast::channel::<TransactionUpdate>(128);
    let (bloom_tx, _bloom_rx) = mpsc::unbounded_channel();
    let metrics = Arc::new(QlasterSenderMetrics::new());

    let sender = setup_sender_with_transactions(
        SenderConfig { shm: cfg },
        updates_tx.clone(),
        Some(transactions_tx.clone()),
        Some(bloom_tx),
        Arc::clone(&metrics),
    )
    .await
    .expect("setup sender");
    let _sender_task = tokio::spawn(sender.run());

    let mut consumer = setup_shm_consumer(&uds_path)
        .await
        .expect("setup shm consumer");

    let tx_update = TransactionUpdate {
        slot: 7,
        index: 3,
        signature: [9u8; 64],
        is_vote: false,
        payload: TransactionPayload::from_slice(b"tx-bytes").expect("payload"),
    };

    let _ = transactions_tx.send(tx_update.clone());
    let none = drain_one_transaction(
        || consumer.try_next_transaction(),
        Duration::from_millis(300),
    )
    .await;
    assert!(none.is_none(), "transaction delivered before subscription");

    consumer
        .subscribe_transactions()
        .await
        .expect("transaction subscribe");

    let _ = transactions_tx.send(tx_update.clone());
    let got = drain_one_transaction(|| consumer.try_next_transaction(), Duration::from_secs(3))
        .await
        .expect("transaction timeout");

    assert_eq!(got.slot, tx_update.slot);
    assert_eq!(got.index, tx_update.index);
    assert_eq!(got.signature, tx_update.signature);
    assert_eq!(got.payload.as_slice(), b"tx-bytes");

    let dispatch_snap = metrics.dispatch.flush();
    let shm_send_snap = metrics.shm_send.flush();
    assert!(
        dispatch_snap.count >= 1,
        "dispatch metric should fire on transaction delivery, got {dispatch_snap:?}"
    );
    assert!(
        shm_send_snap.count >= 1,
        "shm_send metric should fire on transaction delivery, got {shm_send_snap:?}"
    );

    let consumer_metrics = consumer.metrics();
    let read_snap = consumer_metrics.read.flush();
    let decode_snap = consumer_metrics.decode.flush();
    let enqueue_snap = consumer_metrics.enqueue.flush();
    let full_read_snap = consumer_metrics.full_read.flush();
    assert!(
        read_snap.count >= 1,
        "consumer read metric should fire for delivered transaction, got {read_snap:?}"
    );
    assert!(
        decode_snap.count >= 1,
        "consumer decode metric should fire for delivered transaction, got {decode_snap:?}"
    );
    assert!(
        enqueue_snap.count >= 1,
        "consumer enqueue metric should fire for delivered transaction, got {enqueue_snap:?}"
    );
    assert!(
        full_read_snap.count >= 1 && full_read_snap.total_us > 0,
        "consumer full_read metric should cover transaction sender-to-queue latency, got {full_read_snap:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shm_slot_stream_is_always_on() {
    let cfg = shm_config("slots");
    let uds_path = cfg.uds_path.clone();

    let (updates_tx, _) = broadcast::channel::<AccountUpdate>(128);
    let (slots_tx, _) = broadcast::channel::<SlotUpdate>(128);
    let (bloom_tx, _bloom_rx) = mpsc::unbounded_channel();
    let metrics = Arc::new(QlasterSenderMetrics::new());

    let sender = setup_sender_with_streams(
        SenderConfig { shm: cfg },
        updates_tx.clone(),
        None,
        Some(slots_tx.clone()),
        Some(bloom_tx),
        Arc::clone(&metrics),
    )
    .await
    .expect("setup sender");
    let _sender_task = tokio::spawn(sender.run());

    let consumer = setup_shm_consumer(&uds_path)
        .await
        .expect("setup shm consumer");

    let slot_update = SlotUpdate::new(44);
    let _ = slots_tx.send(slot_update);
    let got = drain_one_slot(|| consumer.try_next_slot(), Duration::from_secs(3))
        .await
        .expect("slot timeout");

    assert_eq!(got, slot_update);

    let dispatch_snap = metrics.dispatch.flush();
    let shm_send_snap = metrics.shm_send.flush();
    assert!(
        dispatch_snap.count >= 1,
        "dispatch metric should fire on slot delivery, got {dispatch_snap:?}"
    );
    assert!(
        shm_send_snap.count >= 1,
        "shm_send metric should fire on slot delivery, got {shm_send_snap:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_shm_consumers_get_disjoint_filters() {
    let cfg = shm_config("multi");
    let uds_path = cfg.uds_path.clone();

    let (updates_tx, _) = broadcast::channel::<AccountUpdate>(128);
    let (bloom_tx, _bloom_rx) = mpsc::unbounded_channel();

    let sender = setup_sender(
        SenderConfig { shm: cfg },
        updates_tx.clone(),
        Some(bloom_tx),
        Arc::new(QlasterSenderMetrics::new()),
    )
    .await
    .expect("setup sender");
    let _sender_task = tokio::spawn(sender.run());

    let mut a = setup_shm_consumer(&uds_path).await.expect("setup a");
    let mut b = setup_shm_consumer(&uds_path).await.expect("setup b");

    let pk_a = test_pubkey(5);
    let pk_b = test_pubkey(6);
    let dummy_owner = test_pubkey(7);

    a.subscribe(vec![pk_a], vec![]).await.expect("a sub");
    b.subscribe(vec![pk_b], vec![]).await.expect("b sub");

    let mk = |pk| AccountUpdate {
        account_pubkey: pk,
        account_owner: dummy_owner,
        lamports: 1,
        executable: false,
        rent_epoch: 0,
        slot: 1,
        write_version: 1,
        payload: AccountPayload::from_slice(b"x").expect("payload"),
    };

    let _ = updates_tx.send(mk(pk_a));
    let _ = updates_tx.send(mk(pk_b));

    let a_got = drain_one_update(|| a.try_next_update(), Duration::from_secs(3))
        .await
        .expect("a missed");
    let b_got = drain_one_update(|| b.try_next_update(), Duration::from_secs(3))
        .await
        .expect("b missed");
    assert_eq!(a_got.account_pubkey, pk_a);
    assert_eq!(b_got.account_pubkey, pk_b);

    // Cross-talk check: neither should receive the other's update.
    let none_a = drain_one_update(|| a.try_next_update(), Duration::from_millis(200)).await;
    let none_b = drain_one_update(|| b.try_next_update(), Duration::from_millis(200)).await;
    assert!(none_a.is_none(), "a got cross-talk");
    assert!(none_b.is_none(), "b got cross-talk");
}
