use std::sync::Arc;

use crate::crab::Node;
use crate::crab::utils::runit::Worker;

use super::remote_node::RemoteNode;
use super::{CrabError, utils::crypto::TLSProvider};
use quinn::{Endpoint, ServerConfig, crypto::rustls::QuicServerConfig};
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
#[derive(Deserialize, Debug)]
pub struct LocalNodeConfig {
    pub bind_address: String,
    pub node_id: String,
}
struct LocalNodeInner {
    tls: TLSProvider,
    cfg: LocalNodeConfig,
    endpoint: Endpoint,
}
impl LocalNodeInner {
    fn new(cfg: LocalNodeConfig, tls: TLSProvider) -> Result<LocalNodeInner, CrabError> {
        let server_config = ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(tls.build_server_config()?).map_err(|err| {
                log::error!("build quic server config error {}", err);
                CrabError::ErrorCode(CrabError::CRYPTO_ERROR)
            })?,
        ));
        let endpoint = Endpoint::server(
            server_config,
            cfg.bind_address.parse().map_err(|e| {
                log::error!("parse listen addr error {}", e);
                CrabError::ErrorCode(CrabError::PARSE_ERROR)
            })?,
        )
        .map_err(|err| {
            log::warn!("listen on {} error {}", cfg.bind_address, err);
            err
        })?;
        log::warn!("listen on {}", cfg.bind_address);
        Ok(LocalNodeInner { tls, cfg, endpoint })
    }
    async fn handshake(
        self: Arc<Self>,
        _: quinn::Connection,
        _: CancellationToken,
    ) -> Result<RemoteNode, CrabError> {
        Err(CrabError::ErrorCode(CrabError::HANDSHAKE_ERROR))
    }
    async fn listen(
        self: Arc<Self>,
        cancel: CancellationToken,
        tx: mpsc::Sender<RemoteNode>,
    ) -> Result<(), CrabError> {
        let mut join_set = JoinSet::new();
        loop {
            tokio::select! {
                ret=self.endpoint.accept()=>{
                    match ret{
                        Some(incoming)=>{
                            let self_clone=self.clone();
                            let cancel_clone=cancel.clone();
                            join_set.spawn(async move   {
                                match incoming.await{
                                    Ok(conn)=>self_clone.handshake(conn, cancel_clone).await,
                                    Err(err)=>{
                                        log::warn!("quic connection handshake error {}",err);
                                        Err(CrabError::ErrorCode(CrabError::HANDSHAKE_ERROR))
                                    }
                                }
                            });
                        },
                        None=>{
                            log::warn!("endpoint accept none,exit listen");
                            break
                        },
                    }
                }
                handshek_ret=join_set.join_next()=>{
                    if let Some(Ok(Ok(node)))=handshek_ret{
                       let _= tx.send(node).await.inspect_err(|e|{
                            log::error!("send remote node error {}",e);
                        });
                    }
                }
                _=cancel.cancelled()=>{
                    break;
                }
            }
        }
        join_set.shutdown().await;
        Ok(())
    }
    async fn serve_all_node(
        self: Arc<Self>,
        cancel: CancellationToken,
        rx: mpsc::Receiver<RemoteNode>,
    ) -> Result<(), CrabError> {
        let mut join_set = JoinSet::new();
        let mut rx = rx;
        loop {
            tokio::select! {
                msg=rx.recv()=>{
                    match msg{
                        Some(node)=>{
                            let cancel_copy = cancel.clone();
                            join_set.spawn(async move{if let Err(err)=node.serve(cancel_copy).await{
                                log::warn!("remote node {} serve error {}",node.id(),err);
                            }});
                        },
                        None=>break,
                    }
                }
                Some(Err(err))=join_set.join_next()=>{
                    log::error!("serve all node join error {}",err);
                }
            }
        }
        while let Some(ret) = join_set.join_next().await {
            if let Err(err) = ret {
                log::error!("serve all node shutdown join error {}", err);
            }
        }
        Ok(())
    }
    async fn start(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        let (tx, rx) = mpsc::channel::<RemoteNode>(10);
        let mut join_set = JoinSet::new();
        let (self_accept, self_serve) = (self.clone(), self.clone());
        let (accept_cancel, serve_cancel) = (cancel.clone(), cancel.clone());
        join_set.spawn(async move { self_accept.listen(accept_cancel, tx).await });
        join_set.spawn(async move { self_serve.serve_all_node(serve_cancel, rx).await });
        let mut r = None;
        while let Some(ret) = join_set.join_next().await {
            match ret {
                Err(join_err) => {
                    log::error!("local node start join error: {}", join_err);
                    r.get_or_insert(CrabError::ErrorCode(CrabError::ASYNC_RUNTIME_ERROR));
                }
                Ok(Err(e)) => {
                    r.get_or_insert(e);
                }
                _ => continue,
            }
        }
        if let Some(err) = r { Err(err) } else { Ok(()) }
    }
}

pub fn create_local_node(tls: TLSProvider, cfg: LocalNodeConfig) -> Result<impl Node, CrabError> {
    LocalNode::new(tls, cfg)
}
struct LocalNode {
    inner: Arc<LocalNodeInner>,
}
impl LocalNode {
    pub fn new(tls: TLSProvider, cfg: LocalNodeConfig) -> Result<Self, CrabError> {
        Ok(LocalNode {
            inner: Arc::new(LocalNodeInner::new(cfg, tls)?),
        })
    }
}
#[async_trait::async_trait]
impl Worker for LocalNode {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), CrabError> {
        self.inner.clone().start(cancel).await
    }
}
impl Node for LocalNode {
    fn id(&self) -> &str {
        return &self.inner.cfg.node_id;
    }
}
