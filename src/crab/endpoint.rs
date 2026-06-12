use super::{Node, utils};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use super::utils::runit::Worker;

use super::Handle;
use super::nodes::RemoteNode;
use super::proto::{HandshakePacket, Hook, Protocol};
use super::types::{Endpoint, NodeMetadata, Options};
use super::wrapper::ProtoWrapper;
use super::{CrabError, utils::crypto::TLSProvider};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connecting, Connection};
use quinn::{ServerConfig, crypto::rustls::QuicServerConfig};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
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
type NodeTask = (RemoteNode, Option<oneshot::Sender<()>>);
type ConnTask = (Connection, bool, Option<oneshot::Sender<()>>);
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
                log::error!("build QUIC server config error {}", err);
                CrabError::ErrorCode(CrabError::CRYPTO_ERROR)
            })?,
        ));
        let client_crypto_cfg = tls.build_client_config()?;
        let client_config = ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(client_crypto_cfg).map_err(|e| {
                log::error!("build QUIC client config error{}", e);
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
    ) -> Result<NodeMetadata, CrabError> {
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
        conn: Connection,
        cancel: CancellationToken,
        as_client: bool,
    ) -> Result<(RemoteNode, Handle), CrabError> {
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
                ret,
                conn,
                self.hook.clone(),
                self.cfg.options,
            )),
            Err(err) => Err(err),
        }
    }
    async fn listen(
        self: Arc<Self>,
        cancel: CancellationToken,
        tx: mpsc::Sender<ConnTask>,
    ) -> Result<(), CrabError> {
        let mut join_set = JoinSet::new();
        loop {
            tokio::select! {
                ret=self.endpoint.accept()=>{
                    match ret{
                        Some(incoming)=>{
                            join_set.spawn(async move   {
                                incoming.await.map_err(|e|{
                                    log::warn!("QUIC connection handshake error {}",e);
                                    CrabError::ErrorCode(CrabError::CONN_HANDSHAKE_ERROR)
                                })
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
                       let _= tx.send((node,false,None)).await.inspect_err(|e|{
                            log::error!("send remote node error {}",e);
                        });
                    }
                }
                _=cancel.cancelled()=>{
                    log::debug!("endpoint connection accept connection cancel");
                    break;
                }
            }
        }
        join_set.shutdown().await;
        Ok(())
    }
    async fn serve_all_connection(
        self: Arc<Self>,
        cancel: CancellationToken,
        mut rx: mpsc::Receiver<ConnTask>,
    ) -> Result<(), CrabError> {
        let (node_tx, node_rx) = mpsc::channel(20);
        let mut join_set: JoinSet<Result<NodeTask, CrabError>> = JoinSet::new();
        let self_serve_node = self.clone();
        let cancel_serve_node = cancel.clone();
        let handle = tokio::spawn(async move {
            self_serve_node
                .serve_all_node(cancel_serve_node, node_rx)
                .await
        });
        loop {
            tokio::select! {
                ret=rx.recv()=>{
                    match ret{
                        Some((conn,as_client,notify))=>{
                            let self_clone = self.clone();
                            let cancel_clone=cancel.clone();
                            let hook=self.hook.clone();
                            join_set.spawn(async move {
                                hook.on_connection_accepted(&conn).await.map_err( |e|{
                                    log::warn!("connection from {} is blocked,error {}",conn.remote_address(),e);
                                    e
                                })?;
                                let (node,handle)=self_clone.handshake_with_timeout(conn, cancel_clone,as_client).await?;
                                hook.on_node_accepted(node.meta(),handle).await?;
                                Ok((node,notify))
                            });
                        }
                        None => {
                            log::debug!("no more connections available");
                            break;
                        }
                    }
                }
                handshake_ret=join_set.join_next(),if !join_set.is_empty()=>{
                    if let Some(Ok(Ok(task)))=handshake_ret{
                        let _=node_tx.send(task).await.inspect_err(|e|{
                            log::warn!("send node task error {}",e);
                        });
                    }
                },
                _=cancel.cancelled()=>{
                    log::debug!("endpoint connection accept connection cancel");
                    break;
                }
            }
        }
        join_set.shutdown().await;
        handle.await?
    }
    async fn serve_all_node(
        self: Arc<Self>,
        cancel: CancellationToken,
        rx: mpsc::Receiver<NodeTask>,
    ) -> Result<(), CrabError> {
        let mut join_set = JoinSet::new();
        let mut rx = rx;
        loop {
            let hook = self.hook.clone();
            tokio::select! {
                msg=rx.recv()=>{
                    match msg{
                        Some((node,callback))=>{
                            let node_cancel_token = cancel.child_token();
                            join_set.spawn(async move{
                                log::info!("start remote node {}({})",node.id(),node.addr());
                                if let Err(err)=node.serve(node_cancel_token).await{
                                    log::warn!("remote node {}({}) exit with error {}",node.id(),node.addr(),err);
                                }else{
                                    log::debug!("remote node {}({}) exit",node.id(),node.addr());
                                }
                                hook.on_node_exited(&node.meta()).await;
                                if let Some(sender)=callback{
                                    let _= sender.send(());
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
        let (tx, rx) = mpsc::channel(64);
        if self.cfg.listen {
            let tx_listen = tx.clone();
            let self_copy = self.clone();
            let cancel_copy = cancel.clone();
            join_set.spawn(async move {
                if let Err(e) = self_copy.listen(cancel_copy, tx_listen).await {
                    log::error!("endpoint listen finished with error {}", e);
                    Err(e)
                } else {
                    log::trace!("endpoint listen finished");
                    Ok(())
                }
            });
        }
        if self.cfg.remote_addr.is_some() {
            let cancel_copy = cancel.clone();
            let self_copy = self.clone();
            join_set.spawn(async move {
                let tx_remote = tx.clone();
                let res = self_copy
                    .start_all_remote_node(cancel_copy, tx_remote)
                    .await;
                log::trace!("endpoint all remote node loop exit");
                res
            });
        }
        if join_set.is_empty() {
            log::warn!("both listen and remote are disabled");
            return Ok(());
        }
        let self_nodes = self.clone();
        join_set.spawn(async move { self_nodes.serve_all_connection(cancel, rx).await });
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Err(err)) => {
                    log::error!("endpoint join error {}", err);
                }
                Err(err) => {
                    log::error!("local node worker join error {}", err);
                }
                _ => continue,
            }
        }
        log::trace!("endpoint worker finished");
        Ok(())
    }
    async fn start_all_remote_node(
        self: Arc<Self>,
        cancel: CancellationToken,
        rx: mpsc::Sender<ConnTask>,
    ) -> Result<(), CrabError> {
        let Some(remote_list) = self.cfg.remote_addr.clone().filter(|v| !v.is_empty()) else {
            return Ok(());
        };
        let mut join_set = JoinSet::new();
        for remote in remote_list {
            let self_copy = self.clone();
            let cancel_copy = cancel.clone();
            let rx_copy = rx.clone();
            join_set.spawn(async move {
                self_copy
                    .start_remote_node(&remote, cancel_copy, rx_copy)
                    .await
            });
        }
        while let Some(_) = join_set.join_next().await {}
        log::info!("remote node listen finished");
        Ok(())
    }
    async fn start_remote_node(
        &self,
        remote_addr: &str,
        cancel: CancellationToken,
        rx: mpsc::Sender<ConnTask>,
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
            if let Ok(conn) = self.connect_remote_node(remote_addr, cancel.clone()).await {
                log::debug!("connect to remote node {}", remote_addr);
                let (retry_tx, retry_rx) = oneshot::channel();
                rx.send((conn, true, Some(retry_tx))).await.map_err(|e| {
                    log::error!(
                        "remote {} worker exit with serve node channel error",
                        remote_addr,
                    );
                    CrabError::ErrorCode(CrabError::CANCELED_ERROR)
                })?;
                retry_rx.await.map_err(|_| {
                    log::error!(
                        "remote {} worker exit with retry channel error",
                        remote_addr
                    );
                    CrabError::ErrorCode(CrabError::CANCELED_ERROR)
                })?;
            }
        }
        Ok(())
    }
    async fn connect_remote_node(
        &self,
        addr: &str,
        cancel: CancellationToken,
    ) -> Result<Connection, CrabError> {
        let (host, address_list) = utils::parse_remote_addr(addr).await?;
        let connect_timeout = Duration::from_secs(self.cfg.options.connect_timeout);
        for addr in address_list {
            log::debug!("try connect to remote node {} {}", host, addr);
            let conn_fut = match self.endpoint.connect(addr, host) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("failed to init connection host{}({}) {}", host, addr, e);
                    continue;
                }
            };
            tokio::select! {
                ret=timeout(connect_timeout, conn_fut)=>{
                    if let Ok(Ok(ret))=ret{
                        return Ok(ret);
                    }
                }
                _=cancel.cancelled()=>{
                    return Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
                }
            }
        }
        Err(CrabError::ErrorCode(CrabError::CONNECT_ERROR))
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
