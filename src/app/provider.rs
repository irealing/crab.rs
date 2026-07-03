use super::Manager;
use crate::app::utils::http::HttpClient;
use crab::CrabError;
use crab::utils::crypto::{Config as TlsConfig, TLSProvider};
use std::sync::Arc;

struct Inner {
    local_node_id: String,
    tls_cfg: TLSProvider,
    manager: Manager,
    http_client: HttpClient,
}
pub struct ServiceProvider {
    inner: Arc<Inner>,
}
impl ServiceProvider {
    pub fn new(node_id: &String, cfg: TlsConfig) -> Result<Self, CrabError> {
        let tls_provider = TLSProvider::from_config(cfg);
        let http_client = HttpClient::create(&tls_provider)?;
        let ret = Self {
            inner: Arc::new(Inner {
                local_node_id: node_id.to_string(),
                tls_cfg: tls_provider,
                manager: Manager::new(),
                http_client,
            }),
        };
        Ok(ret)
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
    pub fn http_client(&self) -> &HttpClient {
        &self.inner.http_client
    }
}
impl Clone for ServiceProvider {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
