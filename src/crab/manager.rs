use super::{CrabError, Node};
use crate::crab::node::Manager;
use dashmap::DashMap;
use quinn::Connection;
use std::sync::Arc;

struct DefaultNodeManager {
    mapping: DashMap<String, Arc<dyn Node>>,
}
impl DefaultNodeManager {
    pub fn new() -> impl Manager {
        DefaultNodeManager {
            mapping: DashMap::new(),
        }
    }
}
#[async_trait::async_trait]
impl Manager for DefaultNodeManager {
    async fn handshake(&self, _: Connection) -> Result<Arc<dyn Node>, CrabError> {
        todo!("implements me: handshake");
    }
    fn get(&self, id: &str) -> Option<Arc<dyn Node>> {
        return match self.mapping.get(id) {
            Some(kv) => Some(Arc::clone(kv.value())),
            None => None,
        };
    }
    fn del(&self, id: &str) -> Option<Arc<dyn Node>> {
        return match self.mapping.remove(id) {
            Some((_, val)) => Some(val),
            None => None,
        };
    }
}
pub fn default_node_manager() -> impl Manager {
    DefaultNodeManager::new()
}
