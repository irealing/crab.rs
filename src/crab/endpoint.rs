use super::{Node, utils};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use super::utils::runit::Worker;

use super::nodes::RemoteNode;
use super::types::{Endpoint, Options};
use super::{CrabError, utils::crypto::TLSProvider};
use crate::crab::proto::{HandshakePacket, HandshakeRet, Hook, ProtoWrapper, Protocol};
use quinn::ClientConfig;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ServerConfig, crypto::rustls::QuicServerConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio::{sync::mpsc, task::JoinSet, time};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Debug)]
pub struct EndpointConfig {
    pub bind_address: String,
    pub node_id: String,
    #[serde(default)]
    pub listen: bool,
    #[serde(default)]
    pub remote_addr: Option<Vec<String>>,
    #[serde(default)]
    pub options: Options,
}

struct LocalEndpointInner {
    cfg: EndpointConfig,
    endpoint: quinn::Endpoint,
    local_addr: SocketAddr,
    hook: Arc<dyn Hook>,
}
impl LocalEndpointInner {
    const REMOTE_CONNECT_RETRY_DELAY: Duration = Duration::from_secs(10);
    fn new<S, H, P>(
        cfg: EndpointConfig,
        tls: TLSProvider,
        protocol: P,
    ) -> Result<LocalEndpointInner, CrabError>
    where
        P: Protocol<Handshake = S, Heartbeat = H> + 'static,
        S: HandshakePacket + 'static,
        H: DeserializeOwned + Serialize + Send + Sync + 'static,
    {
        let server_config = ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(tls.build_server_config()?).map_err(|err| {
                log::error!("build quic server config error {}", err);
                CrabError::ErrorCode(CrabError::CRYPTO_ERROR)
            })?,
        ));
        let client_crypto_cfg = tls.build_client_config()?;
        let client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(client_crypto_cfg).map_err(|e| {
                log::error!("build quic client config error{}", e);
                CrabError::ErrorCode(CrabError::CRYPTO_ERROR)
            })?,
        ));
        let endpoint = quinn::Endpoint::server(
            server_config,
            cfg.bind_address.parse().map_err(|e| {
                log::error!("parse listen addr error {}", e);
                CrabError::ErrorCode(CrabError::PARSE_ERROR)
            })?,
        )
        .map_err(|err| {
            log::info!("listen on {} error {}", cfg.bind_address, err);
            err
        })
        .map(|e| {
            let mut e = e;
            e.set_default_client_config(client_config);
            e
        })?;
        let local_addr = endpoint.local_addr()?;
        Ok(LocalEndpointInner {
            cfg,
            endpoint,
            local_addr,
            hook: Arc::new(ProtoWrapper::new(protocol)),
        })
    }
    async fn handshake(
        self: Arc<Self>,
        conn: &quinn::Connection,
        as_client: bool,
    ) -> Result<HandshakeRet, CrabError> {
        if as_client {
            log::debug!("handshake with connection from {}", conn.remote_address());
            self.hook.handshake_as_client(&conn).await
        } else {
            log::debug!(
                "handshake with connection from {} as client",
                conn.remote_address()
            );
            self.hook.handshake(conn).await
        }
    }
    async fn handshake_with_timeout(
        self: Arc<Self>,
        conn: quinn::Connection,
        cancel: CancellationToken,
        as_client: bool,
    ) -> Result<RemoteNode, CrabError> {
        let remote_addr = conn.remote_address();
        log::debug!("handshake with timeout remote address {}", remote_addr);
        let timeout = Duration::from_secs(self.cfg.options.handshake_timeout);
        let self_clone = self.clone();
        match tokio::select! {
            res=self_clone.handshake(&conn,as_client ) => res,
            _=time::sleep(timeout) => {
                log::debug!("handshake with timeout request {:?}", timeout);
                Err(CrabError::ErrorCode(CrabError::HANDSHAKE_TIMEOUT))
            }
            _=cancel.cancelled() => {
                Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
        } {
            Ok(ret) => Ok(RemoteNode::new(
                &ret,
                conn,
                as_client,
                self.hook.clone(),
                self.cfg.options,
            )),
            Err(err) => Err(err),
        }
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
                                    Ok(conn)=>self_clone.handshake_with_timeout(conn, cancel_clone,false).await,
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
                handshake_ret=join_set.join_next(),if !join_set.is_empty()=>{
                    if let Some(Ok(Ok(node)))=handshake_ret{
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
                            join_set.spawn(async move{
                                log::info!("start remote node {}({})",node.id(),node.addr());
                                if let Err(err)=node.serve(cancel_copy).await{
                                    log::warn!("remote node {}({}) exit with error {}",node.id(),node.addr(),err);
                                }else{
                                    log::warn!("remote node {}({}) exit",node.id(),node.addr());
                                }
                            });
                        },
                        None=>break,
                    }
                }
                Some(Err(err))=join_set.join_next(),if !join_set.is_empty()=>{
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
    async fn serve(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        let mut join_set = JoinSet::new();
        if self.cfg.listen {
            let self_copy = self.clone();
            let cancel_copy = cancel.clone();
            join_set.spawn(async move {
                let local_cancel = cancel_copy.clone();
                if let Err(e) = self_copy.start_listen(local_cancel).await {
                    log::error!("local node listen error {}", e);
                    cancel_copy.cancel();
                    Err(e)
                } else {
                    Ok(())
                }
            });
        }
        if self.cfg.remote_addr.is_some() {
            let cancel_copy = cancel.clone();
            let self_copy = self.clone();
            join_set.spawn(async move { self_copy.start_all_remote_node(cancel_copy).await });
        }
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Err(err)) => {
                    log::error!("remote node join error {}", err);
                }
                Err(err) => {
                    log::error!("local node worker join error {}", err);
                }
                _ => continue,
            }
        }
        log::info!("local node worker finished");
        Ok(())
    }
    async fn start_all_remote_node(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<(), CrabError> {
        let Some(remote_list) = self.cfg.remote_addr.clone().filter(|v| !v.is_empty()) else {
            return Ok(());
        };

        let mut join_set = JoinSet::new();
        for remote in remote_list {
            let self_copy = self.clone();
            let cancel_copy = cancel.clone();
            join_set.spawn(async move { self_copy.start_remote_node(&remote, cancel_copy).await });
        }
        while let Some(_) = join_set.join_next().await {}
        log::info!("remote node listen finished");
        Ok(())
    }
    async fn start_remote_node(
        self: Arc<Self>,
        remote_addr: &str,
        cancel: CancellationToken,
    ) -> Result<(), CrabError> {
        log::info!("start remote node  addr: {}", remote_addr);
        let retry_delay = Self::REMOTE_CONNECT_RETRY_DELAY;
        while !cancel.is_cancelled() {
            tokio::select! {
                _=time::sleep(retry_delay)=>{}
                _=cancel.cancelled()=>{
                    log::info!("remote node cancelled");
                    return Ok(());
                }
            }
            log::debug!("try connect to remote node {}", remote_addr);
            let self_copy = self.clone();
            let cancel_copy = cancel.clone();
            let node = tokio::select! {
                ret=self_copy.connect_remote_node(remote_addr, cancel.clone()) =>{
                    if let Ok(node) = ret {
                        node
                    }else{
                        continue;
                    }
                }
                _=cancel_copy.cancelled()=>{
                    return Ok(());
                }
            };
            log::info!("start remote node {}({})", node.id(), node.addr());
            if let Err(err) = node.serve(cancel_copy).await {
                log::error!(
                    "remote node {}({}) exit with error {}",
                    node.id(),
                    node.addr(),
                    err
                );
            } else {
                log::info!("remote node {}({}) exit", node.id(), node.addr());
            }
        }
        Ok(())
    }
    async fn connect_remote_node(
        self: Arc<Self>,
        addr: &str,
        cancel: CancellationToken,
    ) -> Result<RemoteNode, CrabError> {
        let (host, address_list) = utils::parse_remote_addr(addr).await?;
        let connect_timeout = Duration::from_secs(self.cfg.options.connect_timeout);
        for addr in address_list {
            log::debug!("try connect to remote node {} {}", host, addr);
            let conn_fut = match self.clone().endpoint.connect(addr, host) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("failed to init connection host{}({}) {}", host, addr, e);
                    continue;
                }
            };
            let conn = match timeout(connect_timeout, conn_fut).await {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    log::warn!("connect to remote node {} {} failed {}", host, addr, e);
                    continue;
                }
                Err(_) => {
                    log::warn!(
                        "connect to remote {} {} timeout after {} seconds",
                        host,
                        addr,
                        connect_timeout.as_secs(),
                    );
                    continue;
                }
            };
            match self
                .clone()
                .handshake_with_timeout(conn, cancel.clone(), true)
                .await
            {
                Ok(node) => {
                    return Ok(node);
                }
                Err(e) => {
                    log::warn!("handshake error {} host:{} remote addr:{}", e, host, addr);
                    continue;
                }
            }
        }
        Err(CrabError::ErrorCode(CrabError::CONNECT_ERROR))
    }
    async fn start_listen(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        log::info!("starting listen on {}", self.local_addr);
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

struct LocalEndpoint {
    inner: Arc<LocalEndpointInner>,
}
impl LocalEndpoint {
    pub fn new<S, H, P>(
        tls: TLSProvider,
        cfg: EndpointConfig,
        protocol: P,
    ) -> Result<Self, CrabError>
    where
        P: Protocol<Handshake = S, Heartbeat = H> + 'static,
        S: HandshakePacket + 'static,
        H: DeserializeOwned + Serialize + Send + Sync + 'static,
    {
        Ok(LocalEndpoint {
            inner: Arc::new(LocalEndpointInner::new(cfg, tls, protocol)?),
        })
    }
}
#[async_trait::async_trait]
impl Worker for LocalEndpoint {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), CrabError> {
        self.inner.clone().serve(cancel).await
    }
}
impl Endpoint for LocalEndpoint {
    fn id(&self) -> &str {
        &self.inner.cfg.node_id
    }
    fn addr(&self) -> SocketAddr {
        self.inner.local_addr.clone()
    }
}
pub fn create_local_endpoint<S, H, P>(
    tls: TLSProvider,
    cfg: EndpointConfig,
    protocol: P,
) -> Result<impl Endpoint, CrabError>
where
    P: Protocol<Handshake = S, Heartbeat = H> + 'static,
    S: HandshakePacket + 'static,
    H: DeserializeOwned + Serialize + Sync + Send + 'static,
{
    LocalEndpoint::new(tls, cfg, protocol)
}
