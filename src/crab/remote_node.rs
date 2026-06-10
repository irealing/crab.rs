use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::utils::runit::Worker;
use crate::crab::proto::{HandshakeRet, Hook, Stream};
use crate::crab::types::Options;
use crate::crab::{CrabError, Node, types::NodeStatus};

struct RemoteNodeInner {
    node_id: String,
    conn: quinn::Connection,
    local_addr: SocketAddr,
    status_tx: watch::Sender<NodeStatus>,
    status_rx: watch::Receiver<NodeStatus>,
    as_client: bool,
    hook: Arc<dyn Hook>,
    opts: Options,
}
impl RemoteNodeInner {
    pub async fn serve(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        let mut stream = if self.as_client {
            Stream::open(&self.conn).await
        } else {
            Stream::accept(&self.conn).await
        }?;
        if let Err(err) = if self.as_client {
            self.hook.heartbeat_as_client(&mut stream).await
        } else {
            self.hook.heartbeat(&mut stream).await
        } {
            log::error!("first heartbeat failed: {}", err);
            return Err(err);
        };
        log::trace!("first heartbeat finished");
        let cancel_clone = cancel.clone();
        let cancel_child = cancel.child_token();
        loop {
            tokio::select! {
                _=cancel_clone.cancelled() => {
                    break;
                },
                hb_res=self.clone().make_heartbeat(&mut stream, cancel.clone().clone()) => {
                    match hb_res {
                        Ok(()) => {
                            log::trace!("heartbeat success");
                        }
                        Err(err) => {
                            cancel_child.cancel();
                            return if let CrabError::ErrorCode(CrabError::CANCELED_ERROR) = err {
                                log::trace!("heartbeat error with cancel");
                                break;
                            }else{
                                log::error!("heartbeat failed: {}", err);
                                Err(err)
                            }
                        }
                    }
                }
                Ok(_)=Stream::accept(&self.conn)=>{
                    log::trace!("accepted new stream");
                }
            }
        }
        Ok(())
    }
    async fn handle_stream(
        cancel: CancellationToken,
        stream: &mut Stream,
    ) -> Result<(), CrabError> {
        todo!()
    }
    async fn make_heartbeat(
        self: Arc<Self>,
        stream: &mut Stream,
        cancel: CancellationToken,
    ) -> Result<(), CrabError> {
        if self.as_client {
            self.heartbeat_as_client(stream, cancel).await
        } else {
            self.heartbeat_as_server(stream, cancel).await
        }
    }
    async fn heartbeat_as_client(
        self: Arc<Self>,
        stream: &mut Stream,
        cancel: CancellationToken,
    ) -> Result<(), CrabError> {
        tokio::select! {
            _=time::sleep(Duration::from_secs(self.opts.heartbeat_interval)) =>{
                self.hook.heartbeat_as_client(stream).await
            }
            _ = cancel.cancelled() => {
                 Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
        }
    }
    async fn heartbeat_as_server(
        self: Arc<Self>,
        stream: &mut Stream,
        cancel: CancellationToken,
    ) -> Result<(), CrabError> {
        tokio::select! {
            _ = cancel.cancelled() => {
                Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
            res=timeout(Duration::from_secs(self.opts.heartbeat_timeout), self.hook.heartbeat(stream))=>{
               match res {
                    Ok(Ok(_))=>{
                        Ok(())
                    }
                    Ok(Err(err)) => {
                        return Err(err);
                    }
                    Err(_)=>{
                        Err(CrabError::ErrorCode(CrabError::HEARTBEAT_TIMEOUT))
                    }
                }
            }
        }
    }
}
pub(super) struct RemoteNode {
    inner: Arc<RemoteNodeInner>,
}
impl RemoteNode {
    pub(super) fn new(
        ret: &HandshakeRet,
        conn: quinn::Connection,
        as_client: bool,
        hook: Arc<dyn Hook>,
        opts: Options,
    ) -> Self {
        let (status_tx, status_rx) = watch::channel(NodeStatus::Ready);
        Self {
            inner: Arc::new(RemoteNodeInner {
                node_id: ret.node_id.clone(),
                local_addr: conn.remote_address(),
                conn,
                status_tx,
                status_rx,
                as_client,
                hook,
                opts,
            }),
        }
    }
}
#[async_trait::async_trait]
impl Worker for RemoteNode {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), CrabError> {
        self.inner.clone().serve(cancel).await
    }
}
impl Node for RemoteNode {
    fn id(&self) -> &str {
        &self.inner.node_id
    }
    fn status(&self) -> NodeStatus {
        *self.inner.status_rx.borrow()
    }
    fn addr(&self) -> SocketAddr {
        self.inner.local_addr
    }
    fn as_client(&self) -> bool {
        self.inner.as_client
    }
}
