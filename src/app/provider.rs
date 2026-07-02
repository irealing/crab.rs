use super::Manager;
use crab::utils::crypto::{Config as TlsConfig, TLSProvider};
use std::sync::Arc;

struct Inner {
    local_node_id: String,
    tls_cfg: TLSProvider,
    manager: Manager,
}
pub struct ServiceProvider {
    inner: Arc<Inner>,
}
impl ServiceProvider {
    pub fn new(node_id: &String, cfg: TlsConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                local_node_id: node_id.to_string(),
                tls_cfg: TLSProvider::from_config(cfg),
                manager: Manager::new(),
            }),
        }
    }
    pub fn tls_provider(&self) -> TLSProvider {
        self.inner.tls_cfg.clone()
    }
    pub fn manager(&self) -> Manager {
        self.inner.manager.clone()
    }
    pub fn local_node_id(&self) -> &str {
        &self.inner.local_node_id
    }
}
impl Clone for ServiceProvider {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
