use super::Manager;
use crab::utils::crypto::{Config as TlsConfig, TLSProvider};
use hyper::body::Incoming;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::sync::Arc;

pub type HyperClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Incoming>;
fn create_hyper_client() -> HyperClient {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .unwrap()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new())
        .timer(TokioTimer::new())
        .build(https)
}
struct Inner {
    local_node_id: String,
    tls_cfg: TLSProvider,
    hyper_client: Arc<HyperClient>,
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
                hyper_client: Arc::new(create_hyper_client()),
                manager: Manager::new(),
            }),
        }
    }
    pub fn tls_provider(&self) -> TLSProvider {
        self.inner.tls_cfg.clone()
    }
    pub fn hyper_client(&self) -> Arc<HyperClient> {
        self.inner.hyper_client.clone()
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
