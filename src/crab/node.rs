use crate::crab::{CrabError, utils::runit::Worker};
use quinn::Connection;
use std::sync::Arc;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Ready,
    Runing,
    Stopping,
    Stopped,
}

pub trait Node: Worker {
    fn id(&self) -> &str;
    fn status(&self) -> NodeStatus;
}
pub struct HandshakeRet {
    node_id: String,
}
#[async_trait::async_trait]
pub trait Manager: Send + Sync {
    async fn handshake(&self, _: Connection) -> Result<HandshakeRet, CrabError>;
    fn get(&self, _: &str) -> Option<Arc<dyn Node>>;
    fn del(&self, _: &str) -> Option<Arc<dyn Node>>;
}
