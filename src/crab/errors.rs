use rustls::server::VerifierBuilderError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrabError {
    TLS(#[from] rustls::Error),
    IO(#[from] std::io::Error),
    TLSVerifier(#[from] VerifierBuilderError),
    QuicConnectionError(#[from] quinn::ConnectionError),
    QuicReadExactError(#[from] quinn::ReadExactError),
    QuicWriteError(#[from] quinn::WriteError),
    EncodeError(#[from] binrw::error::Error),
    JoinError(#[from] tokio::task::JoinError),
    ErrorCode(u32),
}

impl CrabError {
    pub const NO_ERROR: u32 = 0;
    pub const ASYNC_RUNTIME_ERROR: u32 = 1;
    pub const UNSUPPORTED_ERROR: u32 = 2;
    pub const KEY_NOT_FOUND: u32 = 3;
    pub const CRYPTO_ERROR: u32 = 4;
    pub const BAD_ADDR: u32 = 5;
    pub const PARSE_ERROR: u32 = 6;
    pub const IO_BAD_MESSAGE: u32 = 7;
    pub const NO_PAYLOAD: u32 = 8;
    pub const CONNECT_HANDSHAKE_ERROR: u32 = 9;
    pub const HANDSHAKE_ERROR: u32 = 10;
    pub const BAD_REMOTE_ADDR: u32 = 11;
    pub const CONNECT_ERROR: u32 = 12;
    pub const HANDSHAKE_TIMEOUT: u32 = 13;
    pub const CANCELED_ERROR: u32 = 14;
    pub const PAYLOAD_TOO_LARGE: u32 = 15;
    pub const BAD_MESSAGE_HEADER: u32 = 16;
    pub const DESERIALIZATION_ERROR: u32 = 17;
    pub const HEARTBEAT_TIMEOUT: u32 = 18;
    pub const CONN_HANDSHAKE_ERROR: u32 = 19;
    pub const NODE_ALREADY_EXIT: u32 = 20;
    pub const NODE_EXISTS: u32 = 21;
    pub const UNEXCEPTED_RESPONSE: u32 = 22;
    pub const ILLEGAL_ERROR: u32 = 0xffff_fffe;
    pub const UNKNOWN_ERROR: u32 = 0xffff_ffff;

    fn error_message(&self) -> &'static str {
        match self {
            CrabError::ErrorCode(code) => match *code {
                Self::NO_ERROR => "No error",
                Self::ASYNC_RUNTIME_ERROR => "Asynchronous runtime error",
                Self::UNSUPPORTED_ERROR => "Unsupported error",
                Self::KEY_NOT_FOUND => "Key not found",
                Self::CRYPTO_ERROR => "Crypto error",
                Self::BAD_ADDR => "Bad addr",
                Self::PARSE_ERROR => "Parse error",
                Self::IO_BAD_MESSAGE => "Bad message format",
                Self::NO_PAYLOAD => "No payload",
                Self::CONNECT_HANDSHAKE_ERROR => "connection handshake error",
                Self::HANDSHAKE_ERROR => "Handshake error",
                Self::BAD_REMOTE_ADDR => "Bad remote address",
                Self::CONNECT_ERROR => "Connection error",
                Self::HANDSHAKE_TIMEOUT => "Handshake timeout",
                Self::CANCELED_ERROR => "Canceled error",
                Self::PAYLOAD_TOO_LARGE => "Payload too large",
                Self::BAD_MESSAGE_HEADER => "Bad message header",
                Self::DESERIALIZATION_ERROR => "Deserialization error",
                Self::HEARTBEAT_TIMEOUT => "Heartbeat timeout",
                Self::CONN_HANDSHAKE_ERROR => "Connection handshake error",
                Self::NODE_ALREADY_EXIT => "Node already exited",
                Self::ILLEGAL_ERROR => "Illegal error",
                Self::UNKNOWN_ERROR => "Unknown error",
                Self::UNEXCEPTED_RESPONSE => "Unexpected response",
                _ => "Unknown error code",
            },
            _ => "",
        }
    }
}

impl std::fmt::Display for CrabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ErrorCode(code) => write!(f, "Error code {}: {}", code, self.error_message()),
            Self::TLS(e) => e.fmt(f),
            Self::IO(e) => e.fmt(f),
            Self::TLSVerifier(e) => e.fmt(f),
            Self::QuicConnectionError(e) => e.fmt(f),
            Self::QuicReadExactError(e) => e.fmt(f),
            Self::QuicWriteError(e) => e.fmt(f),
            Self::EncodeError(e) => e.fmt(f),
            Self::JoinError(e) => e.fmt(f),
        }
    }
}
