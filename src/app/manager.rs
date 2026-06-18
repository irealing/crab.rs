use super::types::Handshake;
use crab::Handle;
use dashmap::DashMap;
use std::sync::Arc;

struct Node {
    handle: Handle,
    info: Arc<Handshake>,
}
pub struct Manager {
    nodes: Arc<DashMap<Arc<str>, Node>>,
}
impl Clone for Manager {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
        }
    }
}
impl Manager {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
        }
    }
    pub fn insert(&self, node_id: &str, handle: Handle, info: Handshake) {
        let key: Arc<str> = Arc::from(node_id);
        self.nodes.insert(
            key,
            Node {
                handle,
                info: Arc::new(info),
            },
        );
    }
    pub fn remove(&self, node_id: &str) {
        self.nodes.remove(node_id);
    }
    pub fn get(&self, node_id: &str) -> Option<(Handle, Arc<Handshake>)> {
        self.nodes
            .get(node_id)
            .map(|node| (node.handle.clone(), node.info.clone()))
    }
    pub fn exists(&self, node_id: &str) -> bool {
        self.nodes.get(node_id).is_some()
    }
}
