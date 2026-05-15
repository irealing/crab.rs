use crate::crab::{CrabError, utils::runit::Worker};
use quinn::Connection;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait Node: Worker {
    fn id(&self) -> &str;
}
#[async_trait::async_trait]
pub trait Manager: Send + Sync {
    async fn handshake(&self, _: Connection) -> Result<Arc<dyn Node>, CrabError>;
    fn get(&self, _: &str) -> Option<Arc<dyn Node>>;
    fn del(&self, _: &str) -> Option<Arc<dyn Node>>;
}
