use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, ServerConfig};
use tokio::select;
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::crab::default_node_manager;
use crate::crab::node::Manager;
use crate::crab::utils::runit::Worker;

use super::utils::crypto::{Config as TLSConfig, TLSProvider};

use super::CrabError;

use super::node::Node;
const CRAB_TLS_ALPN: &[u8] = b"crab";
pub struct Config {
    pub id: String,
    pub listen: String,
}
pub trait LocalEndpoint: Node + Worker {}
struct LocalNodeInner {
    node_id: String,
    endpoint: Arc<Endpoint>,
    manager: Arc<dyn Manager>,
}
pub struct LocalNode {
    inner: Arc<LocalNodeInner>,
}
impl LocalNode {
    async fn accept(
        local_node: Arc<LocalNodeInner>,
        cancel: CancellationToken,
        tx: mpsc::Sender<Arc<dyn Node>>,
    ) -> Result<(), CrabError> {
        let mut tasks = JoinSet::new();
        loop {
            select! {
                _=cancel.cancelled()=>{
                    break;
                },
                accepted=local_node.endpoint.accept()=>{
                    if let Some(incoming)=accepted{
                        let manager=Arc::clone(&local_node.manager);
                        tasks.spawn(async move {
                            let conn=incoming.await.map_err(|e|{
                                log::warn!("quic connection handshake error {}",e);
                                CrabError::ErrorCode(CrabError::CONNECT_HANDSHKE_ERROR)})?;
                            manager.handshake(conn).await.inspect_err(|e|{
                                log::warn!("node handshake failed,error {}",e);
                            })
                        });
                    }else{
                        break;
                    }
                },
                Some(join_ret)=tasks.join_next()=>{
                    match join_ret {
                        Ok(ret)=>{
                            if let Ok(node)=ret{
                                log::info!("node {} handshake success",node.id());
                               let _= tx.send(node).await.inspect_err(|e|{log::warn!("send handshaked node ({}) failed",e)});
                            }
                        },
                        Err(e)=>log::warn!("quic connection handshake error {}",e),
                    }
                },
            }
        }
        tasks.shutdown().await;
        Ok(())
    }
    async fn start_origin_node(mut rx: Receiver<Arc<dyn Node>>) -> Result<(), CrabError> {
        Ok(())
    }
    async fn listen(&self, cancel: CancellationToken) -> Result<(), CrabError> {
        log::warn!("start listn");
        let (tx, mut rx) = mpsc::channel(32);
        let mut join_set = JoinSet::new();
        let accept_cancel = cancel.clone();
        let local_node = self.inner.clone();
        join_set.spawn(async move { Self::accept(local_node, accept_cancel, tx).await });
        join_set.spawn(async move { Self::start_origin_node(rx).await });
        while let Some(ret) = join_set.join_next().await {
            match ret {
                Ok(_) => continue,
                Err(e) => {
                    log::warn!("local node listen join error {}", e)
                }
            }
        }
        Ok(())
    }
}
impl LocalNode {
    async fn new(cfg: Config, tls_cfg: TLSConfig) -> Result<Self, CrabError> {
        let addr = cfg.listen.parse::<SocketAddr>().map_err(|e| {
            log::warn!("bad addr {} error:{}", &cfg.listen, e);
            CrabError::ErrorCode(CrabError::BAD_ADDR)
        })?;
        let mut server_crypto_cfg = TLSProvider::from_config(tls_cfg).build_server_config()?;
        server_crypto_cfg.alpn_protocols = vec![CRAB_TLS_ALPN.to_vec()];
        let quic_cfg = ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server_crypto_cfg)
                .map_err(|_| CrabError::ErrorCode(CrabError::CRYPTO_ERROR))?,
        ));
        let endpoint = Endpoint::server(quic_cfg, addr).map_err(|e| {
            log::warn!("bind quic addr {} error {}", cfg.listen, e);
            e
        })?;
        Ok(LocalNode {
            inner: Arc::new(LocalNodeInner {
                node_id: cfg.id,
                endpoint: Arc::new(endpoint),
                manager: Arc::new(default_node_manager()),
            }),
        })
    }
}
impl LocalEndpoint for LocalNode {}
#[async_trait]
impl Node for LocalNode {
    fn id(&self) -> &str {
        return &self.inner.node_id;
    }
}
#[async_trait]
impl Worker for LocalNode {
    async fn run(&self, cancel: CancellationToken) -> Option<CrabError> {
        log::warn!("local node start");
        None
    }
}
pub async fn create_local_node(
    c: Config,
    tls_cfg: TLSConfig,
) -> Result<impl LocalEndpoint, CrabError> {
    LocalNode::new(c, tls_cfg).await
}
mod tests {
    use std::sync::Arc;

    use crate::crab::default_node_manager;

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
