//! Sender-side SHM control plane: a UDS listener that accepts colo consumer
//! connections, hands each one a per-slot shared-memory ring + eventfd, and
//! routes subsequent subscribe/ping frames into the shared `ConnectionManager`.

use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::{UnixListener, UnixStream};

use crate::{
    error::QlasterError,
    sender::{SenderState, ShmSlotProvision, ShmTransportConfig},
    shm::send_frame_with_fd,
    types::{decode_client_frame, ClientFrame, ConnectionReady, ConnectionReadyShm},
    wire::{read_framed, write_framed},
};

#[derive(Debug)]
pub struct ShmListenerHandle {
    pub(crate) listener: UnixListener,
    pub(crate) config: ShmTransportConfig,
}

/// Bind the UDS socket and prepare the per-process shm directory.
pub(crate) async fn bind_listener(
    cfg: &ShmTransportConfig,
) -> Result<ShmListenerHandle, QlasterError> {
    if !cfg.ring_capacity_bytes.is_power_of_two() {
        return Err(QlasterError::ConfigError(format!(
            "ring_capacity_bytes {} must be a power of two",
            cfg.ring_capacity_bytes
        )));
    }
    if let Some(parent) = cfg.uds_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| QlasterError::UdsError(format!("create uds parent: {e}")))?;
        }
    }
    // Best-effort cleanup of a stale socket file.
    let _ = std::fs::remove_file(&cfg.uds_path);
    let listener = UnixListener::bind(&cfg.uds_path)
        .map_err(|e| QlasterError::UdsError(format!("bind {}: {e}", cfg.uds_path.display())))?;

    std::fs::create_dir_all(&cfg.shm_dir)
        .map_err(|e| QlasterError::ShmError(format!("create shm dir: {e}")))?;

    Ok(ShmListenerHandle {
        listener,
        config: cfg.clone(),
    })
}

pub(crate) async fn run_listener(
    state: Arc<SenderState>,
    handle: ShmListenerHandle,
) -> Result<(), QlasterError> {
    let ShmListenerHandle { listener, config } = handle;
    loop {
        let (stream, _addr) = listener
            .accept()
            .await
            .map_err(|e| QlasterError::UdsError(format!("accept: {e}")))?;
        if let Err(err) = crate::shm::set_nosigpipe(stream.as_raw_fd()) {
            // Refuse to serve a connection we cannot protect from SIGPIPE; a
            // later write to a vanished peer could otherwise kill the process.
            tracing::error!("dropping connection: set SO_NOSIGPIPE failed: {err}");
            continue;
        }
        let state = Arc::clone(&state);
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_uds_connection(state, stream, config).await {
                tracing::warn!("shm uds connection failed: {err}");
            }
        });
    }
}

async fn handle_uds_connection(
    state: Arc<SenderState>,
    mut stream: UnixStream,
    cfg: ShmTransportConfig,
) -> Result<(), QlasterError> {
    let manager = Arc::clone(&state.manager);

    loop {
        let payload = match read_framed(&mut stream).await {
            Ok(p) => p,
            Err(QlasterError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        match decode_client_frame(&payload) {
            Ok(ClientFrame::Subscription(request)) => {
                let received_at = Instant::now();
                let provision = manager
                    .upsert_shm(request.clone(), &cfg.shm_dir, cfg.ring_capacity_bytes)
                    .await?;

                match provision {
                    ShmSlotProvision::Created {
                        slot_token,
                        ring_path,
                        ring_capacity,
                        eventfd,
                    } => {
                        let ready = ConnectionReadyShm::new(
                            slot_token,
                            ring_path.to_string_lossy().into_owned(),
                            ring_capacity,
                        );
                        let encoded = ready.encode();
                        // Hand the consumer the *passable* fd: the single
                        // eventfd on Linux, the pipe read end on macOS (the
                        // producer keeps the write end for notify()).
                        let raw_fd = eventfd.pass_fd();
                        send_frame_with_fd(&stream, &encoded, raw_fd).await?;
                        // Drop our temporary handshake-side Arc; the SlotSink
                        // installed in upsert_shm keeps the eventfd alive.
                        drop(eventfd);
                    }
                    ShmSlotProvision::Existing(slot_token) => {
                        let ready = ConnectionReady::new(slot_token);
                        write_framed(&mut stream, &ready.encode()).await?;
                    }
                }

                if let Some(Err(err)) = state
                    .bloom_updates_tx
                    .as_ref()
                    .map(|tx| tx.send((request, received_at)))
                {
                    tracing::warn!("failed forwarding shm subscription: {err}");
                }
            }
            Ok(ClientFrame::Ping(ping)) => {
                if !manager.touch_ping(&ping) {
                    tracing::warn!(
                        "ignoring shm ping for unknown slot (slot={} gen={})",
                        ping.slot_token.slot_index,
                        ping.slot_token.generation
                    );
                }
            }
            Err(e) => {
                tracing::warn!("failed decoding shm uds frame: {e}");
                return Err(e);
            }
        }
    }
}
