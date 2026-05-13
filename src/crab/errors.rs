use rustls::server::VerifierBuilderError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrabError {
    TLS(#[from] rustls::Error),
    IO(#[from] std::io::Error),
    TLSVerifier(#[from] VerifierBuilderError),
    ErrorCode(u32),
}

impl CrabError {
    pub const NO_ERROR: u32 = 0;
    pub const ASYNC_RUNTIME_ERROR: u32 = 1;
    pub const UNSUPPORTED_ERROR: u32 = 2;
    pub const KEY_NOT_FOUND: u32 = 3;
    pub const CRYPTO_ERROR: u32 = 4;
    pub const BAD_ADDR: u32 = 5;
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
            Self::TLS(e) => write!(f, "TLS error {}", e),
            Self::IO(e) => write!(f, "IO Error {}", e),
            Self::TLSVerifier(e) => write!(f, "TLS verifier error {}", e),
        }
    }
}
