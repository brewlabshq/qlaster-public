use bytes::Bytes;
use solana_pubkey::Pubkey;
use wincode::{SchemaRead, SchemaWrite};

use crate::{error::QlasterError, metrics::unix_time_nanos};

const REQUEST_SUBSCRIBE_TAG: u8 = 1; // subscription/filter update sent from client to sender
const UPDATE_TAG: u8 = 2; // account update sent from sender to client
const READY_TAG: u8 = 3; // sender -> client control frame carrying assigned slot token
const REQUEST_PING_TAG: u8 = 4; // keepalive ping sent from client to sender
const READY_SHM_TAG: u8 = 5; // sender -> client SHM ring handshake (UDS-only)
const TRANSACTION_UPDATE_TAG: u8 = 6; // transaction update sent from sender to client
const SLOT_UPDATE_TAG: u8 = 7; // Solana slot update sent from sender to client
const WIRE_VERSION: u8 = 9; // allows safe protocol evolution with explicit version checks
const UNSET_SLOT_INDEX: u8 = u8::MAX;
pub const MAX_ACCOUNT_PAYLOAD_BYTES: usize = 10 * 1024 * 1024; // solana account data upper bound
pub const MAX_TRANSACTION_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;
const ACCOUNT_UPDATE_FLAG_LZ4: u8 = 0x1;
const ACCOUNT_UPDATE_COMPRESSION_THRESHOLD_BYTES: usize = 500 * 1024;
const ACCOUNT_UPDATE_COMPRESSION_SAMPLE_BYTES: usize = 8 * 1024;
const ACCOUNT_UPDATE_COMPRESSION_MIN_GAIN_BPS: usize = 75;
const ACCOUNT_UPDATE_FIXED_BYTES: usize = 2 + 8 + (32 * 2) + (8 * 4) + 1 + 1 + 4 + 4;
const TRANSACTION_UPDATE_FIXED_BYTES: usize = 2 + 8 + 8 + 4 + 1 + 64 + 4;
const SLOT_UPDATE_FIXED_BYTES: usize = 2 + 8 + 8;

fn should_attempt_lz4(payload: &[u8]) -> bool {
    // Never attempt compression below threshold; small payloads are faster uncompressed.
    if payload.len() < ACCOUNT_UPDATE_COMPRESSION_THRESHOLD_BYTES {
        return false;
    }

    // Allow forcing compression for large payloads by setting gain requirement to zero.
    if ACCOUNT_UPDATE_COMPRESSION_MIN_GAIN_BPS == 0 {
        return true;
    }

    let sample_len = payload.len().min(ACCOUNT_UPDATE_COMPRESSION_SAMPLE_BYTES);
    let sample = &payload[..sample_len];
    let compressed_sample = lz4_flex::block::compress(sample);
    if compressed_sample.len() >= sample_len {
        return false;
    }

    let gain_bps = ((sample_len - compressed_sample.len()) * 10_000) / sample_len;
    gain_bps >= ACCOUNT_UPDATE_COMPRESSION_MIN_GAIN_BPS
}

macro_rules! serialize_wire {
    ($wire:expr) => {
        wincode::serialize(&$wire).expect("wincode serialization must succeed for wire structs")
    };
}

macro_rules! deserialize_wire {
    ($ty:ty, $bytes:expr, $ctx:literal) => {
        wincode::deserialize::<$ty>($bytes)
            .map_err(|e| QlasterError::DecodeError(format!("{}: {}", $ctx, e)))
    };
}

fn ensure_wire(wire_version: u8, message_tag: u8, expected_tag: u8) -> Result<(), QlasterError> {
    if wire_version != WIRE_VERSION {
        return Err(QlasterError::InvalidWireVersion {
            found: wire_version,
            expected: WIRE_VERSION,
        });
    }
    if message_tag != expected_tag {
        return Err(QlasterError::InvalidMessageTag {
            found: message_tag,
            expected: expected_tag,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotToken {
    pub slot_index: u8,
    pub generation: u64,
}

impl SlotToken {
    pub fn new(slot_index: u8, generation: u64) -> Self {
        Self {
            slot_index,
            generation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionRequest {
    pub slot_token: Option<SlotToken>,
    pub account_pubkeys: Vec<Pubkey>,
    pub account_owners: Vec<Pubkey>,
    pub include_transactions: bool,
}

impl SubscriptionRequest {
    pub fn new(account_pubkeys: Vec<Pubkey>, account_owners: Vec<Pubkey>) -> Self {
        Self {
            slot_token: None,
            account_pubkeys,
            account_owners,
            include_transactions: false,
        }
    }

    pub fn transactions() -> Self {
        Self {
            slot_token: None,
            account_pubkeys: Vec::new(),
            account_owners: Vec::new(),
            include_transactions: true,
        }
    }

    pub fn with_slot_token(mut self, slot_token: Option<SlotToken>) -> Self {
        self.slot_token = slot_token;
        self
    }

    pub fn with_transactions(mut self) -> Self {
        self.include_transactions = true;
        self
    }

    pub fn encode(&self) -> Vec<u8> {
        let (slot_index, slot_generation) = self
            .slot_token
            .map(|token| (token.slot_index, token.generation))
            .unwrap_or((UNSET_SLOT_INDEX, 0));

        let wire = WireSubscriptionRequest {
            wire_version: WIRE_VERSION,
            message_tag: REQUEST_SUBSCRIBE_TAG,
            slot_index,
            slot_generation,
            account_pubkeys: self.account_pubkeys.iter().map(|k| k.to_bytes()).collect(),
            account_owners: self.account_owners.iter().map(|k| k.to_bytes()).collect(),
            include_transactions: self.include_transactions,
        };
        serialize_wire!(wire)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        let wire: WireSubscriptionRequest =
            deserialize_wire!(WireSubscriptionRequest, bytes, "subscription request")?;
        ensure_wire(wire.wire_version, wire.message_tag, REQUEST_SUBSCRIBE_TAG)?;

        let slot_token = (wire.slot_index != UNSET_SLOT_INDEX)
            .then_some(SlotToken::new(wire.slot_index, wire.slot_generation));

        Ok(Self {
            slot_token,
            account_pubkeys: wire
                .account_pubkeys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            account_owners: wire
                .account_owners
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            include_transactions: wire.include_transactions,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PingRequest {
    pub slot_token: SlotToken,
}

impl PingRequest {
    pub fn new(slot_token: SlotToken) -> Self {
        Self { slot_token }
    }

    pub fn encode(&self) -> Vec<u8> {
        let wire = WirePingRequest {
            wire_version: WIRE_VERSION,
            message_tag: REQUEST_PING_TAG,
            slot_index: self.slot_token.slot_index,
            slot_generation: self.slot_token.generation,
        };
        serialize_wire!(wire)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        let wire: WirePingRequest = deserialize_wire!(WirePingRequest, bytes, "ping request")?;
        ensure_wire(wire.wire_version, wire.message_tag, REQUEST_PING_TAG)?;
        if wire.slot_index == UNSET_SLOT_INDEX {
            return Err(QlasterError::MalformedPayload(
                "ping request missing slot token",
            ));
        }

        Ok(Self {
            slot_token: SlotToken::new(wire.slot_index, wire.slot_generation),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionReady {
    pub slot_token: SlotToken,
}

impl ConnectionReady {
    pub fn new(slot_token: SlotToken) -> Self {
        Self { slot_token }
    }

    pub fn encode(&self) -> Vec<u8> {
        let wire = WireConnectionReady {
            wire_version: WIRE_VERSION,
            message_tag: READY_TAG,
            slot_index: self.slot_token.slot_index,
            slot_generation: self.slot_token.generation,
        };
        serialize_wire!(wire)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        let wire: WireConnectionReady =
            deserialize_wire!(WireConnectionReady, bytes, "connection ready")?;
        ensure_wire(wire.wire_version, wire.message_tag, READY_TAG)?;
        if wire.slot_index == UNSET_SLOT_INDEX {
            return Err(QlasterError::MalformedPayload(
                "connection ready missing slot token",
            ));
        }

        Ok(Self {
            slot_token: SlotToken::new(wire.slot_index, wire.slot_generation),
        })
    }
}

/// Sender → client handshake reply over the UDS control channel announcing
/// the per-connection shared-memory ring file. The `eventfd` used to wake the
/// consumer is sent out-of-band on the same UDS message via `SCM_RIGHTS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionReadyShm {
    pub slot_token: SlotToken,
    pub ring_path: String,
    pub ring_capacity: u64,
}

impl ConnectionReadyShm {
    pub fn new(slot_token: SlotToken, ring_path: String, ring_capacity: u64) -> Self {
        Self {
            slot_token,
            ring_path,
            ring_capacity,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let wire = WireConnectionReadyShm {
            wire_version: WIRE_VERSION,
            message_tag: READY_SHM_TAG,
            slot_index: self.slot_token.slot_index,
            slot_generation: self.slot_token.generation,
            ring_path: self.ring_path.clone(),
            ring_capacity: self.ring_capacity,
        };
        serialize_wire!(wire)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        let wire: WireConnectionReadyShm =
            deserialize_wire!(WireConnectionReadyShm, bytes, "connection ready shm")?;
        ensure_wire(wire.wire_version, wire.message_tag, READY_SHM_TAG)?;
        if wire.slot_index == UNSET_SLOT_INDEX {
            return Err(QlasterError::MalformedPayload(
                "connection ready shm missing slot token",
            ));
        }
        if wire.ring_path.is_empty() {
            return Err(QlasterError::MalformedPayload(
                "connection ready shm missing ring path",
            ));
        }
        if wire.ring_capacity == 0 {
            return Err(QlasterError::MalformedPayload(
                "connection ready shm has zero ring capacity",
            ));
        }
        Ok(Self {
            slot_token: SlotToken::new(wire.slot_index, wire.slot_generation),
            ring_path: wire.ring_path,
            ring_capacity: wire.ring_capacity,
        })
    }
}

pub enum ClientFrame {
    Subscription(SubscriptionRequest),
    Ping(PingRequest),
}

#[derive(Debug)]
pub enum ServerFrame {
    AccountUpdate(AccountUpdate),
    TransactionUpdate(TransactionUpdate),
    SlotUpdate(SlotUpdate),
    ConnectionReady(ConnectionReady),
    ConnectionReadyShm(ConnectionReadyShm),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountUpdateWireMeta {
    pub sender_created_at_unix_nanos: u64,
}

#[derive(Debug)]
pub enum ServerFrameWithMeta {
    AccountUpdate {
        update: AccountUpdate,
        meta: AccountUpdateWireMeta,
    },
    TransactionUpdate {
        update: TransactionUpdate,
        meta: AccountUpdateWireMeta,
    },
    SlotUpdate {
        update: SlotUpdate,
        meta: AccountUpdateWireMeta,
    },
    ConnectionReady(ConnectionReady),
    ConnectionReadyShm(ConnectionReadyShm),
}

fn wire_tag(bytes: &[u8]) -> Result<u8, QlasterError> {
    bytes.get(1).copied().ok_or(QlasterError::MalformedPayload(
        "frame too short for wire tag",
    ))
}

pub fn decode_client_frame(bytes: &[u8]) -> Result<ClientFrame, QlasterError> {
    match wire_tag(bytes)? {
        REQUEST_SUBSCRIBE_TAG => SubscriptionRequest::decode(bytes).map(ClientFrame::Subscription),
        REQUEST_PING_TAG => PingRequest::decode(bytes).map(ClientFrame::Ping),
        found => Err(QlasterError::InvalidMessageTag {
            found,
            expected: REQUEST_SUBSCRIBE_TAG,
        }),
    }
}

pub fn decode_server_frame(bytes: &[u8]) -> Result<ServerFrame, QlasterError> {
    match wire_tag(bytes)? {
        UPDATE_TAG => AccountUpdate::decode(bytes).map(ServerFrame::AccountUpdate),
        TRANSACTION_UPDATE_TAG => {
            TransactionUpdate::decode(bytes).map(ServerFrame::TransactionUpdate)
        }
        SLOT_UPDATE_TAG => SlotUpdate::decode(bytes).map(ServerFrame::SlotUpdate),
        READY_TAG => ConnectionReady::decode(bytes).map(ServerFrame::ConnectionReady),
        READY_SHM_TAG => ConnectionReadyShm::decode(bytes).map(ServerFrame::ConnectionReadyShm),
        found => Err(QlasterError::InvalidMessageTag {
            found,
            expected: UPDATE_TAG,
        }),
    }
}

pub fn decode_server_frame_owned(bytes: Vec<u8>) -> Result<ServerFrame, QlasterError> {
    match wire_tag(&bytes)? {
        UPDATE_TAG => AccountUpdate::decode_owned(bytes).map(ServerFrame::AccountUpdate),
        TRANSACTION_UPDATE_TAG => {
            TransactionUpdate::decode_owned(bytes).map(ServerFrame::TransactionUpdate)
        }
        SLOT_UPDATE_TAG => SlotUpdate::decode_owned(bytes).map(ServerFrame::SlotUpdate),
        READY_TAG => ConnectionReady::decode(&bytes).map(ServerFrame::ConnectionReady),
        READY_SHM_TAG => ConnectionReadyShm::decode(&bytes).map(ServerFrame::ConnectionReadyShm),
        found => Err(QlasterError::InvalidMessageTag {
            found,
            expected: UPDATE_TAG,
        }),
    }
}

pub fn decode_server_frame_owned_with_meta(
    bytes: Vec<u8>,
) -> Result<ServerFrameWithMeta, QlasterError> {
    match wire_tag(&bytes)? {
        UPDATE_TAG => {
            let (update, meta) = AccountUpdate::decode_owned_with_meta(bytes)?;
            Ok(ServerFrameWithMeta::AccountUpdate { update, meta })
        }
        TRANSACTION_UPDATE_TAG => {
            let (update, meta) = TransactionUpdate::decode_owned_with_meta(bytes)?;
            Ok(ServerFrameWithMeta::TransactionUpdate { update, meta })
        }
        SLOT_UPDATE_TAG => {
            let (update, meta) = SlotUpdate::decode_owned_with_meta(bytes)?;
            Ok(ServerFrameWithMeta::SlotUpdate { update, meta })
        }
        READY_TAG => ConnectionReady::decode(&bytes).map(ServerFrameWithMeta::ConnectionReady),
        READY_SHM_TAG => {
            ConnectionReadyShm::decode(&bytes).map(ServerFrameWithMeta::ConnectionReadyShm)
        }
        found => Err(QlasterError::InvalidMessageTag {
            found,
            expected: UPDATE_TAG,
        }),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountPayload(Bytes);

impl AccountPayload {
    pub fn new(payload: Bytes) -> Result<Self, QlasterError> {
        let len = payload.len();
        if len > MAX_ACCOUNT_PAYLOAD_BYTES {
            return Err(QlasterError::PayloadTooLarge {
                found: len,
                max: MAX_ACCOUNT_PAYLOAD_BYTES,
            });
        }
        Ok(Self(payload))
    }

    pub fn from_slice(payload: &[u8]) -> Result<Self, QlasterError> {
        Self::new(Bytes::copy_from_slice(payload))
    }

    pub fn from_bytes(payload: Bytes) -> Result<Self, QlasterError> {
        Self::new(payload)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn clone_bytes(&self) -> Bytes {
        self.0.clone()
    }
}

impl TryFrom<Bytes> for AccountPayload {
    type Error = QlasterError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<[u8]> for AccountPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountUpdate {
    pub account_pubkey: Pubkey,
    pub account_owner: Pubkey,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub slot: u64,
    pub write_version: u64,
    pub payload: AccountPayload,
}

impl AccountUpdate {
    pub fn new(
        account_pubkey: Pubkey,
        account_owner: Pubkey,
        lamports: u64,
        executable: bool,
        rent_epoch: u64,
        payload: Bytes,
    ) -> Result<Self, QlasterError> {
        Ok(Self {
            account_pubkey,
            account_owner,
            lamports,
            executable,
            rent_epoch,
            slot: 0,
            write_version: 0,
            payload: AccountPayload::new(payload)?,
        })
    }

    pub fn with_slot_write_version(mut self, slot: u64, write_version: u64) -> Self {
        self.slot = slot;
        self.write_version = write_version;
        self
    }

    pub fn encode_parts_at(
        &self,
        sender_created_at_unix_nanos: u64,
    ) -> Result<(Bytes, Bytes), QlasterError> {
        if self.payload.len() > MAX_ACCOUNT_PAYLOAD_BYTES {
            return Err(QlasterError::PayloadTooLarge {
                found: self.payload.len(),
                max: MAX_ACCOUNT_PAYLOAD_BYTES,
            });
        }
        let raw_payload = self.payload.clone_bytes();
        let raw_payload_len = raw_payload.len();
        let raw_payload_len_u32: u32 =
            raw_payload_len
                .try_into()
                .map_err(|_| QlasterError::PayloadTooLarge {
                    found: raw_payload_len,
                    max: MAX_ACCOUNT_PAYLOAD_BYTES,
                })?;
        let mut flags = 0u8;
        let mut wire_payload = raw_payload;
        if should_attempt_lz4(wire_payload.as_ref()) {
            let compressed = lz4_flex::block::compress(wire_payload.as_ref());
            if compressed.len() < wire_payload.len() {
                flags |= ACCOUNT_UPDATE_FLAG_LZ4;
                wire_payload = Bytes::from(compressed);
            }
        }
        let wire_payload_len_u32: u32 =
            wire_payload
                .len()
                .try_into()
                .map_err(|_| QlasterError::PayloadTooLarge {
                    found: wire_payload.len(),
                    max: MAX_ACCOUNT_PAYLOAD_BYTES,
                })?;

        let mut header = Vec::with_capacity(ACCOUNT_UPDATE_FIXED_BYTES);
        header.push(WIRE_VERSION);
        header.push(UPDATE_TAG);
        header.extend_from_slice(&sender_created_at_unix_nanos.to_le_bytes());
        header.extend_from_slice(&self.account_pubkey.to_bytes());
        header.extend_from_slice(&self.account_owner.to_bytes());
        header.extend_from_slice(&self.lamports.to_le_bytes());
        header.push(u8::from(self.executable));
        header.extend_from_slice(&self.rent_epoch.to_le_bytes());
        header.extend_from_slice(&self.slot.to_le_bytes());
        header.extend_from_slice(&self.write_version.to_le_bytes());
        header.push(flags);
        header.extend_from_slice(&raw_payload_len_u32.to_le_bytes());
        header.extend_from_slice(&wire_payload_len_u32.to_le_bytes());
        Ok((Bytes::from(header), wire_payload))
    }

    pub fn encode_parts(&self) -> Result<(Bytes, Bytes), QlasterError> {
        self.encode_parts_at(unix_time_nanos())
    }

    pub fn encode(&self) -> Result<Vec<u8>, QlasterError> {
        let (header, payload) = self.encode_parts()?;
        let mut out = Vec::with_capacity(header.len() + payload.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    pub fn decode_with_meta(bytes: &[u8]) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        let decoded = DecodedAccountUpdate::parse(bytes)?;
        let wire_payload =
            &bytes[decoded.payload_offset..(decoded.payload_offset + decoded.wire_payload_len)];
        let payload = if decoded.compressed {
            let decompressed = lz4_flex::block::decompress(wire_payload, decoded.raw_payload_len)
                .map_err(|e| {
                QlasterError::DecodeError(format!("account update decompress: {e}"))
            })?;
            if decompressed.len() != decoded.raw_payload_len {
                return Err(QlasterError::MalformedPayload(
                    "account update decompressed payload size mismatch",
                ));
            }
            Bytes::from(decompressed)
        } else {
            if decoded.wire_payload_len != decoded.raw_payload_len {
                return Err(QlasterError::MalformedPayload(
                    "account update uncompressed payload size mismatch",
                ));
            }
            Bytes::copy_from_slice(wire_payload)
        };
        Self::from_decoded(decoded, payload)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        Self::decode_with_meta(bytes).map(|(update, _)| update)
    }

    pub fn decode_owned_with_meta(
        bytes: Vec<u8>,
    ) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        let bytes = Bytes::from(bytes);
        let decoded = DecodedAccountUpdate::parse(&bytes)?;
        let wire_payload = bytes
            .slice(decoded.payload_offset..(decoded.payload_offset + decoded.wire_payload_len));
        let payload = if decoded.compressed {
            let decompressed =
                lz4_flex::block::decompress(wire_payload.as_ref(), decoded.raw_payload_len)
                    .map_err(|e| {
                        QlasterError::DecodeError(format!("account update decompress: {e}"))
                    })?;
            if decompressed.len() != decoded.raw_payload_len {
                return Err(QlasterError::MalformedPayload(
                    "account update decompressed payload size mismatch",
                ));
            }
            Bytes::from(decompressed)
        } else {
            if decoded.wire_payload_len != decoded.raw_payload_len {
                return Err(QlasterError::MalformedPayload(
                    "account update uncompressed payload size mismatch",
                ));
            }
            wire_payload
        };
        Self::from_decoded(decoded, payload)
    }

    pub fn decode_owned(bytes: Vec<u8>) -> Result<Self, QlasterError> {
        Self::decode_owned_with_meta(bytes).map(|(update, _)| update)
    }

    fn from_decoded(
        decoded: DecodedAccountUpdate,
        payload: Bytes,
    ) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        Ok((
            Self {
                account_pubkey: Pubkey::new_from_array(decoded.account_pubkey),
                account_owner: Pubkey::new_from_array(decoded.account_owner),
                lamports: decoded.lamports,
                executable: decoded.executable,
                rent_epoch: decoded.rent_epoch,
                slot: decoded.slot,
                write_version: decoded.write_version,
                payload: AccountPayload::from_bytes(payload)?,
            },
            AccountUpdateWireMeta {
                sender_created_at_unix_nanos: decoded.sender_created_at_unix_nanos,
            },
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionPayload(Bytes);

impl TransactionPayload {
    pub fn new(payload: Bytes) -> Result<Self, QlasterError> {
        let len = payload.len();
        if len > MAX_TRANSACTION_PAYLOAD_BYTES {
            return Err(QlasterError::PayloadTooLarge {
                found: len,
                max: MAX_TRANSACTION_PAYLOAD_BYTES,
            });
        }
        Ok(Self(payload))
    }

    pub fn from_slice(payload: &[u8]) -> Result<Self, QlasterError> {
        Self::new(Bytes::copy_from_slice(payload))
    }

    pub fn from_bytes(payload: Bytes) -> Result<Self, QlasterError> {
        Self::new(payload)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn clone_bytes(&self) -> Bytes {
        self.0.clone()
    }
}

impl TryFrom<Bytes> for TransactionPayload {
    type Error = QlasterError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<[u8]> for TransactionPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotUpdate {
    pub slot: u64,
}

impl SlotUpdate {
    pub fn new(slot: u64) -> Self {
        Self { slot }
    }

    pub fn encode_parts_at(
        &self,
        sender_created_at_unix_nanos: u64,
    ) -> Result<(Bytes, Bytes), QlasterError> {
        let mut header = Vec::with_capacity(SLOT_UPDATE_FIXED_BYTES);
        header.push(WIRE_VERSION);
        header.push(SLOT_UPDATE_TAG);
        header.extend_from_slice(&sender_created_at_unix_nanos.to_le_bytes());
        header.extend_from_slice(&self.slot.to_le_bytes());
        Ok((Bytes::from(header), Bytes::new()))
    }

    pub fn encode_parts(&self) -> Result<(Bytes, Bytes), QlasterError> {
        self.encode_parts_at(unix_time_nanos())
    }

    pub fn encode(&self) -> Result<Vec<u8>, QlasterError> {
        let (header, payload) = self.encode_parts()?;
        let mut out = Vec::with_capacity(header.len() + payload.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    pub fn decode_with_meta(bytes: &[u8]) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        let decoded = DecodedSlotUpdate::parse(bytes)?;
        Ok((
            Self { slot: decoded.slot },
            AccountUpdateWireMeta {
                sender_created_at_unix_nanos: decoded.sender_created_at_unix_nanos,
            },
        ))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        Self::decode_with_meta(bytes).map(|(update, _)| update)
    }

    pub fn decode_owned_with_meta(
        bytes: Vec<u8>,
    ) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        Self::decode_with_meta(&bytes)
    }

    pub fn decode_owned(bytes: Vec<u8>) -> Result<Self, QlasterError> {
        Self::decode_owned_with_meta(bytes).map(|(update, _)| update)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionUpdate {
    pub slot: u64,
    pub index: u32,
    pub signature: [u8; 64],
    pub is_vote: bool,
    pub payload: TransactionPayload,
}

impl TransactionUpdate {
    pub fn new(
        slot: u64,
        index: u32,
        signature: [u8; 64],
        is_vote: bool,
        payload: Bytes,
    ) -> Result<Self, QlasterError> {
        Ok(Self {
            slot,
            index,
            signature,
            is_vote,
            payload: TransactionPayload::new(payload)?,
        })
    }

    pub fn encode_parts_at(
        &self,
        sender_created_at_unix_nanos: u64,
    ) -> Result<(Bytes, Bytes), QlasterError> {
        if self.payload.len() > MAX_TRANSACTION_PAYLOAD_BYTES {
            return Err(QlasterError::PayloadTooLarge {
                found: self.payload.len(),
                max: MAX_TRANSACTION_PAYLOAD_BYTES,
            });
        }
        let payload = self.payload.clone_bytes();
        let payload_len_u32: u32 =
            payload
                .len()
                .try_into()
                .map_err(|_| QlasterError::PayloadTooLarge {
                    found: payload.len(),
                    max: MAX_TRANSACTION_PAYLOAD_BYTES,
                })?;

        let mut header = Vec::with_capacity(TRANSACTION_UPDATE_FIXED_BYTES);
        header.push(WIRE_VERSION);
        header.push(TRANSACTION_UPDATE_TAG);
        header.extend_from_slice(&sender_created_at_unix_nanos.to_le_bytes());
        header.extend_from_slice(&self.slot.to_le_bytes());
        header.extend_from_slice(&self.index.to_le_bytes());
        header.push(u8::from(self.is_vote));
        header.extend_from_slice(&self.signature);
        header.extend_from_slice(&payload_len_u32.to_le_bytes());
        Ok((Bytes::from(header), payload))
    }

    pub fn encode_parts(&self) -> Result<(Bytes, Bytes), QlasterError> {
        self.encode_parts_at(unix_time_nanos())
    }

    pub fn encode(&self) -> Result<Vec<u8>, QlasterError> {
        let (header, payload) = self.encode_parts()?;
        let mut out = Vec::with_capacity(header.len() + payload.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    pub fn decode_with_meta(bytes: &[u8]) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        let decoded = DecodedTransactionUpdate::parse(bytes)?;
        let payload = Bytes::copy_from_slice(
            &bytes[decoded.payload_offset..decoded.payload_offset + decoded.payload_len],
        );
        Self::from_decoded(decoded, payload)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, QlasterError> {
        Self::decode_with_meta(bytes).map(|(update, _)| update)
    }

    pub fn decode_owned_with_meta(
        bytes: Vec<u8>,
    ) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        let bytes = Bytes::from(bytes);
        let decoded = DecodedTransactionUpdate::parse(&bytes)?;
        let payload =
            bytes.slice(decoded.payload_offset..decoded.payload_offset + decoded.payload_len);
        Self::from_decoded(decoded, payload)
    }

    pub fn decode_owned(bytes: Vec<u8>) -> Result<Self, QlasterError> {
        Self::decode_owned_with_meta(bytes).map(|(update, _)| update)
    }

    fn from_decoded(
        decoded: DecodedTransactionUpdate,
        payload: Bytes,
    ) -> Result<(Self, AccountUpdateWireMeta), QlasterError> {
        Ok((
            Self {
                slot: decoded.slot,
                index: decoded.index,
                signature: decoded.signature,
                is_vote: decoded.is_vote,
                payload: TransactionPayload::from_bytes(payload)?,
            },
            AccountUpdateWireMeta {
                sender_created_at_unix_nanos: decoded.sender_created_at_unix_nanos,
            },
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct DecodedAccountUpdate {
    sender_created_at_unix_nanos: u64,
    account_pubkey: [u8; 32],
    account_owner: [u8; 32],
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
    slot: u64,
    write_version: u64,
    compressed: bool,
    raw_payload_len: usize,
    payload_offset: usize,
    wire_payload_len: usize,
}

impl DecodedAccountUpdate {
    fn parse(bytes: &[u8]) -> Result<Self, QlasterError> {
        if bytes.len() < ACCOUNT_UPDATE_FIXED_BYTES {
            return Err(QlasterError::MalformedPayload(
                "account update frame shorter than fixed header",
            ));
        }

        ensure_wire(bytes[0], bytes[1], UPDATE_TAG)?;

        let mut cursor = 2usize;
        let sender_created_at_unix_nanos = take_u64_le(bytes, &mut cursor)?;
        let account_pubkey = take_array_32(bytes, &mut cursor)?;
        let account_owner = take_array_32(bytes, &mut cursor)?;
        let lamports = take_u64_le(bytes, &mut cursor)?;
        let executable = match take_u8(bytes, &mut cursor)? {
            0 => false,
            1 => true,
            _ => {
                return Err(QlasterError::MalformedPayload(
                    "account update executable flag must be 0 or 1",
                ));
            }
        };
        let rent_epoch = take_u64_le(bytes, &mut cursor)?;
        let slot = take_u64_le(bytes, &mut cursor)?;
        let write_version = take_u64_le(bytes, &mut cursor)?;
        let flags = take_u8(bytes, &mut cursor)?;
        if flags & !ACCOUNT_UPDATE_FLAG_LZ4 != 0 {
            return Err(QlasterError::MalformedPayload(
                "account update has unsupported compression flags",
            ));
        }
        let raw_payload_len = take_u32_le(bytes, &mut cursor)? as usize;
        if raw_payload_len > MAX_ACCOUNT_PAYLOAD_BYTES {
            return Err(QlasterError::PayloadTooLarge {
                found: raw_payload_len,
                max: MAX_ACCOUNT_PAYLOAD_BYTES,
            });
        }
        let wire_payload_len = take_u32_le(bytes, &mut cursor)? as usize;

        if bytes.len() != cursor + wire_payload_len {
            return Err(QlasterError::MalformedPayload(
                "account update payload size mismatch",
            ));
        }

        Ok(Self {
            sender_created_at_unix_nanos,
            account_pubkey,
            account_owner,
            lamports,
            executable,
            rent_epoch,
            slot,
            write_version,
            compressed: (flags & ACCOUNT_UPDATE_FLAG_LZ4) != 0,
            raw_payload_len,
            payload_offset: cursor,
            wire_payload_len,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct DecodedTransactionUpdate {
    sender_created_at_unix_nanos: u64,
    slot: u64,
    index: u32,
    is_vote: bool,
    signature: [u8; 64],
    payload_offset: usize,
    payload_len: usize,
}

impl DecodedTransactionUpdate {
    fn parse(bytes: &[u8]) -> Result<Self, QlasterError> {
        if bytes.len() < TRANSACTION_UPDATE_FIXED_BYTES {
            return Err(QlasterError::MalformedPayload(
                "transaction update frame shorter than fixed header",
            ));
        }

        ensure_wire(bytes[0], bytes[1], TRANSACTION_UPDATE_TAG)?;

        let mut cursor = 2usize;
        let sender_created_at_unix_nanos = take_u64_le(bytes, &mut cursor)?;
        let slot = take_u64_le(bytes, &mut cursor)?;
        let index = take_u32_le(bytes, &mut cursor)?;
        let is_vote = match take_u8(bytes, &mut cursor)? {
            0 => false,
            1 => true,
            _ => {
                return Err(QlasterError::MalformedPayload(
                    "transaction update vote flag must be 0 or 1",
                ));
            }
        };
        let signature = take_array_64(bytes, &mut cursor)?;
        let payload_len = take_u32_le(bytes, &mut cursor)? as usize;
        if payload_len > MAX_TRANSACTION_PAYLOAD_BYTES {
            return Err(QlasterError::PayloadTooLarge {
                found: payload_len,
                max: MAX_TRANSACTION_PAYLOAD_BYTES,
            });
        }
        if bytes.len() != cursor + payload_len {
            return Err(QlasterError::MalformedPayload(
                "transaction update payload size mismatch",
            ));
        }

        Ok(Self {
            sender_created_at_unix_nanos,
            slot,
            index,
            is_vote,
            signature,
            payload_offset: cursor,
            payload_len,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct DecodedSlotUpdate {
    sender_created_at_unix_nanos: u64,
    slot: u64,
}

impl DecodedSlotUpdate {
    fn parse(bytes: &[u8]) -> Result<Self, QlasterError> {
        if bytes.len() < SLOT_UPDATE_FIXED_BYTES {
            return Err(QlasterError::MalformedPayload(
                "slot update frame shorter than fixed header",
            ));
        }

        ensure_wire(bytes[0], bytes[1], SLOT_UPDATE_TAG)?;

        let mut cursor = 2usize;
        let sender_created_at_unix_nanos = take_u64_le(bytes, &mut cursor)?;
        let slot = take_u64_le(bytes, &mut cursor)?;
        if bytes.len() != cursor {
            return Err(QlasterError::MalformedPayload(
                "slot update payload size mismatch",
            ));
        }

        Ok(Self {
            sender_created_at_unix_nanos,
            slot,
        })
    }
}

fn take_exact<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    context: &'static str,
) -> Result<&'a [u8], QlasterError> {
    let end = cursor
        .checked_add(len)
        .ok_or(QlasterError::MalformedPayload(context))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or(QlasterError::MalformedPayload(context))?;
    *cursor = end;
    Ok(slice)
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, QlasterError> {
    Ok(take_exact(bytes, cursor, 1, "account update u8 out of bounds")?[0])
}

fn take_u32_le(bytes: &[u8], cursor: &mut usize) -> Result<u32, QlasterError> {
    let raw = take_exact(bytes, cursor, 4, "account update u32 out of bounds")?;
    Ok(u32::from_le_bytes(raw.try_into().map_err(|_| {
        QlasterError::MalformedPayload("account update bad u32")
    })?))
}

fn take_u64_le(bytes: &[u8], cursor: &mut usize) -> Result<u64, QlasterError> {
    let raw = take_exact(bytes, cursor, 8, "account update u64 out of bounds")?;
    Ok(u64::from_le_bytes(raw.try_into().map_err(|_| {
        QlasterError::MalformedPayload("account update bad u64")
    })?))
}

fn take_array_32(bytes: &[u8], cursor: &mut usize) -> Result<[u8; 32], QlasterError> {
    let raw = take_exact(bytes, cursor, 32, "account update [u8;32] out of bounds")?;
    raw.try_into()
        .map_err(|_| QlasterError::MalformedPayload("account update bad [u8;32]"))
}

fn take_array_64(bytes: &[u8], cursor: &mut usize) -> Result<[u8; 64], QlasterError> {
    let raw = take_exact(
        bytes,
        cursor,
        64,
        "transaction update [u8;64] out of bounds",
    )?;
    raw.try_into()
        .map_err(|_| QlasterError::MalformedPayload("transaction update bad [u8;64]"))
}

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
struct WireSubscriptionRequest {
    wire_version: u8,
    message_tag: u8,
    slot_index: u8,
    slot_generation: u64,
    account_pubkeys: Vec<[u8; 32]>,
    account_owners: Vec<[u8; 32]>,
    include_transactions: bool,
}

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
struct WirePingRequest {
    wire_version: u8,
    message_tag: u8,
    slot_index: u8,
    slot_generation: u64,
}

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
struct WireConnectionReady {
    wire_version: u8,
    message_tag: u8,
    slot_index: u8,
    slot_generation: u64,
}

#[derive(Clone, Debug, SchemaWrite, SchemaRead)]
struct WireConnectionReadyShm {
    wire_version: u8,
    message_tag: u8,
    slot_index: u8,
    slot_generation: u64,
    ring_path: String,
    ring_capacity: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::QlasterError;

    #[test]
    fn subscription_request_roundtrip() {
        let req = SubscriptionRequest::new(
            vec![Pubkey::new_unique(), Pubkey::new_unique()],
            vec![Pubkey::new_unique()],
        )
        .with_transactions()
        .with_slot_token(Some(SlotToken::new(2, 42)));
        let bytes = req.encode();
        let decoded = SubscriptionRequest::decode(&bytes).expect("decode request");
        assert_eq!(decoded, req);
        assert!(decoded.include_transactions);
    }

    #[test]
    fn connection_ready_shm_roundtrip() {
        let token = SlotToken::new(3, 7);
        // Path is only encode/decode round-tripped here (no filesystem access);
        // use a portable string rather than a Linux-only /dev/shm path.
        let ready = ConnectionReadyShm::new(token, "qlaster/test-ring".into(), 64 * 1024);
        let bytes = ready.encode();
        assert_eq!(
            ConnectionReadyShm::decode(&bytes).expect("decode shm ready"),
            ready
        );
        let routed = decode_server_frame(&bytes).expect("route shm ready");
        assert!(matches!(routed, ServerFrame::ConnectionReadyShm(_)));
    }

    #[test]
    fn connection_ready_shm_rejects_missing_fields() {
        let bytes = ConnectionReadyShm::new(SlotToken::new(0, 1), String::new(), 64).encode();
        assert!(matches!(
            ConnectionReadyShm::decode(&bytes),
            Err(QlasterError::MalformedPayload(_))
        ));
        let bytes = ConnectionReadyShm::new(SlotToken::new(0, 1), "/path".into(), 0).encode();
        assert!(matches!(
            ConnectionReadyShm::decode(&bytes),
            Err(QlasterError::MalformedPayload(_))
        ));
    }

    #[test]
    fn ping_and_ready_roundtrip() {
        let token = SlotToken::new(1, 99);
        let ping = PingRequest::new(token);
        assert_eq!(
            PingRequest::decode(&ping.encode()).expect("decode ping"),
            ping
        );

        let ready = ConnectionReady::new(token);
        assert_eq!(
            ConnectionReady::decode(&ready.encode()).expect("decode ready"),
            ready
        );
    }

    #[test]
    fn account_update_roundtrip() {
        let update = AccountUpdate {
            account_pubkey: Pubkey::new_unique(),
            account_owner: Pubkey::new_unique(),
            lamports: 42,
            executable: true,
            rent_epoch: 7,
            slot: 99,
            write_version: 11,
            payload: AccountPayload::from_slice(b"hello").expect("payload"),
        };
        let bytes = update.encode().expect("encode update");
        let decoded = AccountUpdate::decode(&bytes).expect("decode update");
        assert_eq!(decoded, update);
    }

    #[test]
    fn account_update_carries_sender_timestamp_metadata() {
        let update = AccountUpdate {
            account_pubkey: Pubkey::new_unique(),
            account_owner: Pubkey::new_unique(),
            lamports: 42,
            executable: false,
            rent_epoch: 7,
            slot: 99,
            write_version: 11,
            payload: AccountPayload::from_slice(b"hello").expect("payload"),
        };
        let sender_created_at_unix_nanos = 123_456_789;
        let (header, payload) = update
            .encode_parts_at(sender_created_at_unix_nanos)
            .expect("encode update");
        let mut bytes = Vec::with_capacity(header.len() + payload.len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&payload);

        let (decoded, meta) = AccountUpdate::decode_with_meta(&bytes).expect("decode update");
        assert_eq!(decoded, update);
        assert_eq!(
            meta.sender_created_at_unix_nanos,
            sender_created_at_unix_nanos
        );
    }

    #[test]
    fn transaction_update_roundtrip_and_metadata() {
        let update = TransactionUpdate {
            slot: 123,
            index: 4,
            signature: [7u8; 64],
            is_vote: true,
            payload: TransactionPayload::from_slice(b"transaction bytes").expect("payload"),
        };
        let sender_created_at_unix_nanos = 987_654_321;
        let (header, payload) = update
            .encode_parts_at(sender_created_at_unix_nanos)
            .expect("encode transaction");
        let mut bytes = Vec::with_capacity(header.len() + payload.len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&payload);

        let (decoded, meta) =
            TransactionUpdate::decode_with_meta(&bytes).expect("decode transaction");
        assert_eq!(decoded, update);
        assert_eq!(
            meta.sender_created_at_unix_nanos,
            sender_created_at_unix_nanos
        );

        let routed = decode_server_frame(&bytes).expect("route transaction");
        assert!(matches!(routed, ServerFrame::TransactionUpdate(_)));
    }

    #[test]
    fn slot_update_roundtrip_and_metadata() {
        let update = SlotUpdate::new(456);
        let sender_created_at_unix_nanos = 222_333_444;
        let (header, payload) = update
            .encode_parts_at(sender_created_at_unix_nanos)
            .expect("encode slot");
        assert!(payload.is_empty());
        let mut bytes = Vec::with_capacity(header.len() + payload.len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&payload);

        let (decoded, meta) = SlotUpdate::decode_with_meta(&bytes).expect("decode slot");
        assert_eq!(decoded, update);
        assert_eq!(
            meta.sender_created_at_unix_nanos,
            sender_created_at_unix_nanos
        );

        let decoded_owned = SlotUpdate::decode_owned(bytes.clone()).expect("decode owned slot");
        assert_eq!(decoded_owned, update);

        let routed = decode_server_frame(&bytes).expect("route slot");
        assert!(matches!(routed, ServerFrame::SlotUpdate(_)));
    }

    #[test]
    fn frame_decode_routes_by_tag() {
        let sub = SubscriptionRequest::new(vec![Pubkey::new_unique()], vec![]);
        assert!(matches!(
            decode_client_frame(&sub.encode()).expect("decode client sub"),
            ClientFrame::Subscription(_)
        ));

        let ping = PingRequest::new(SlotToken::new(0, 1));
        assert!(matches!(
            decode_client_frame(&ping.encode()).expect("decode client ping"),
            ClientFrame::Ping(_)
        ));

        let ready = ConnectionReady::new(SlotToken::new(0, 1));
        assert!(matches!(
            decode_server_frame(&ready.encode()).expect("decode server ready"),
            ServerFrame::ConnectionReady(_)
        ));
    }

    #[test]
    fn request_rejects_invalid_version_and_tag() {
        let req = SubscriptionRequest::new(vec![Pubkey::new_unique()], vec![]);
        let mut bytes = req.encode();

        bytes[0] = 99;
        match SubscriptionRequest::decode(&bytes) {
            Err(QlasterError::InvalidWireVersion { found, expected }) => {
                assert_eq!(found, 99);
                assert_eq!(expected, WIRE_VERSION);
            }
            other => panic!("expected InvalidWireVersion, got {other:?}"),
        }

        let mut bytes = req.encode();
        bytes[1] = 99;
        match SubscriptionRequest::decode(&bytes) {
            Err(QlasterError::InvalidMessageTag { found, expected }) => {
                assert_eq!(found, 99);
                assert_eq!(expected, REQUEST_SUBSCRIBE_TAG);
            }
            other => panic!("expected InvalidMessageTag, got {other:?}"),
        }
    }

    #[test]
    fn update_rejects_invalid_version_and_tag() {
        let update = AccountUpdate {
            account_pubkey: Pubkey::new_unique(),
            account_owner: Pubkey::new_unique(),
            lamports: 1,
            executable: false,
            rent_epoch: 0,
            slot: 1,
            write_version: 1,
            payload: AccountPayload::from_slice(&[1, 2, 3]).expect("payload"),
        };
        let mut bytes = update.encode().expect("encode update");
        bytes[0] = 77;
        match AccountUpdate::decode(&bytes) {
            Err(QlasterError::InvalidWireVersion { found, expected }) => {
                assert_eq!(found, 77);
                assert_eq!(expected, WIRE_VERSION);
            }
            other => panic!("expected InvalidWireVersion, got {other:?}"),
        }

        let mut bytes = update.encode().expect("encode update");
        bytes[1] = 77;
        match AccountUpdate::decode(&bytes) {
            Err(QlasterError::InvalidMessageTag { found, expected }) => {
                assert_eq!(found, 77);
                assert_eq!(expected, UPDATE_TAG);
            }
            other => panic!("expected InvalidMessageTag, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_and_garbage_payloads() {
        let req = SubscriptionRequest::new(vec![Pubkey::new_unique()], vec![]);
        let mut bytes = req.encode();
        bytes.truncate(bytes.len() / 2);
        assert!(SubscriptionRequest::decode(&bytes).is_err());
        assert!(SubscriptionRequest::decode(&[1, 2, 3, 4, 5]).is_err());

        let update = AccountUpdate {
            account_pubkey: Pubkey::new_unique(),
            account_owner: Pubkey::new_unique(),
            lamports: 1,
            executable: false,
            rent_epoch: 0,
            slot: 1,
            write_version: 2,
            payload: AccountPayload::from_slice(&[9, 8, 7]).expect("payload"),
        };
        let mut bytes = update.encode().expect("encode update");
        bytes.truncate(bytes.len() / 2);
        assert!(AccountUpdate::decode(&bytes).is_err());
        assert!(AccountUpdate::decode(&[9, 9, 9]).is_err());
    }

    #[test]
    fn account_payload_rejects_oversized_data() {
        let too_large = vec![7u8; MAX_ACCOUNT_PAYLOAD_BYTES + 1];
        match AccountPayload::from_bytes(Bytes::from(too_large)) {
            Err(QlasterError::PayloadTooLarge { found, max }) => {
                assert_eq!(found, MAX_ACCOUNT_PAYLOAD_BYTES + 1);
                assert_eq!(max, MAX_ACCOUNT_PAYLOAD_BYTES);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn compression_probe_accepts_low_entropy_payload() {
        let payload = vec![0u8; ACCOUNT_UPDATE_COMPRESSION_THRESHOLD_BYTES];
        assert!(should_attempt_lz4(&payload));
    }

    #[test]
    fn compression_probe_rejects_high_entropy_payload() {
        let mut payload = vec![0u8; ACCOUNT_UPDATE_COMPRESSION_THRESHOLD_BYTES];
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for byte in &mut payload {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            *byte = state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8;
        }
        assert!(!should_attempt_lz4(&payload));
    }

    #[test]
    fn compression_probe_skips_below_threshold() {
        let payload = vec![0u8; ACCOUNT_UPDATE_COMPRESSION_THRESHOLD_BYTES - 1];
        assert!(!should_attempt_lz4(&payload));
    }
}
