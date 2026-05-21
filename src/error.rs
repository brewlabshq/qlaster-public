#[derive(thiserror::Error, Debug)]
pub enum QlasterError {
    #[error("unable to connect to endpoint")]
    ConnectionFailed,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("account payload too large: {found} bytes (max {max} bytes)")]
    PayloadTooLarge { found: usize, max: usize },
    #[error("invalid wire version {found}; expected {expected}")]
    InvalidWireVersion { found: u8, expected: u8 },
    #[error("invalid message tag {found}; expected {expected}")]
    InvalidMessageTag { found: u8, expected: u8 },
    #[error("decode error: {0}")]
    DecodeError(String),
    #[error("operation timed out: {0}")]
    Timeout(&'static str),
    #[error("configuration error: {0}")]
    ConfigError(String),
    #[error("malformed payload: {0}")]
    MalformedPayload(&'static str),
    #[error("incoming endpoint accept loop closed")]
    IncomingClosed,
    #[error("shared memory error: {0}")]
    ShmError(String),
    #[error("shared memory ring full")]
    ShmFull,
    #[error("uds error: {0}")]
    UdsError(String),
}
