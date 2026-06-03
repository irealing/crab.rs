use crate::crab::{CrabError, utils::runit::Worker};
use quinn::Connection;
use std::sync::Arc;

pub trait Node: Worker {
    fn id(&self) -> &str;
}
pub struct HandshakeRet {
    id: String,
}
#[async_trait::async_trait]
pub trait Manager: Send + Sync {
    async fn handshake(&self, _: Connection) -> Result<HandshakeRet, CrabError>;
    fn get(&self, _: &str) -> Option<Arc<dyn Node>>;
    fn del(&self, _: &str) -> Option<Arc<dyn Node>>;
}
