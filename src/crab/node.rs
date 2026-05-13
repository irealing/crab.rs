use std::sync::Arc;

use dashmap::DashMap;
use rustls::{ClientConfig, ServerConfig};

use super::errors::CrabError;
use super::protocol::Command;
type Manager = DashMap<String, dyn Node>;
#[async_trait::async_trait]
pub trait Node {
    fn id(&self) -> &str;
}
pub trait TLSProvider {
    fn server_config() -> ServerConfig;
    fn client_config() -> Option<ClientConfig>;
}
