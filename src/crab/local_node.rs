use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, ServerConfig};

use super::utils::crypto::{Config as TLSConfig, TLSProvider};

use super::CrabError;

use super::node::Node;
const CRAB_TLS_ALPN: &[u8] = b"crab";
pub struct Config {
    id: String,
    listen: String,
}
struct LocalNode {
    node_id: String,
    endpoint: Arc<Endpoint>,
}
impl LocalNode {
    async fn new(cfg: Config, tls_cfg: TLSConfig) -> Result<Self, CrabError> {
        let addr = cfg.listen.parse::<SocketAddr>().map_err(|e| {
            log::warn!(
                "{}:{} bad addr {} error:{}",
                file!(),
                line!(),
                &cfg.listen,
                e
            );
            CrabError::ErrorCode(CrabError::BAD_ADDR)
        })?;
        let mut server_crypto_cfg = TLSProvider::from_config(tls_cfg).build_server_config()?;
        server_crypto_cfg.alpn_protocols = vec![CRAB_TLS_ALPN.to_vec()];
        let quic_cfg = ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server_crypto_cfg)
                .map_err(|_| CrabError::ErrorCode(CrabError::CRYPTO_ERROR))?,
        ));
        let endpoint = Endpoint::server(quic_cfg, addr).map_err(|e| {
            log::warn!(
                "{}:{}bind quic addr {} error {}",
                file!(),
                line!(),
                cfg.listen,
                e
            );
            e
        })?;
        Ok(LocalNode {
            node_id: cfg.listen,
            endpoint: Arc::new(endpoint),
        })
    }
}
#[async_trait::async_trait]
impl Node for LocalNode {
    fn id(&self) -> &str {
        return &self.node_id;
    }
}

mod tests {
    use super::{Config, LocalNode, TLSConfig};

    #[tokio::test]
    async fn test_create_local_node() {
        let tls_cfg = TLSConfig::load_default_config_file();
        LocalNode::new(
            Config {
                id: "12345".to_string(),
                listen: "127.0.0.1:65522".to_string(),
            },
            tls_cfg,
        )
        .await
        .inspect_err(|e| {
            panic!("create local node error {}", e);
        })
        .unwrap();
    }
}
