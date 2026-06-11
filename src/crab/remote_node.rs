use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
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
    fn set_status(&self, status: NodeStatus) {
        let status_ref = &status;
        if self.status_tx.send_if_modified(|v| {
            return if *v != *status_ref {
                *v = *status_ref;
                true
            } else {
                false
            };
        }) {
            log::info!("node {} status {}", self.node_id, status_ref);
        }
    }
    pub async fn serve(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        let mut stream = if self.as_client {
            Stream::open(&self.conn).await?
        } else {
            Stream::accept(&self.conn).await?
        };
        if let Err(err) = if self.as_client {
            self.hook.heartbeat_as_client(&mut stream).await
        } else {
            self.hook.heartbeat(&mut stream).await
        } {
            log::error!("first heartbeat failed: {}", err);
            return Err(err);
        };
        self.set_status(NodeStatus::Running);
        log::trace!("first heartbeat finished");
        let (tx, rx) = mpsc::channel(32);
        let self_hs = self.clone();
        let cancel_clone = cancel.clone();
        let cancel_child = cancel.child_token();
        let cancel_child_clone = cancel_clone.clone();
        let hs_handle =
            tokio::spawn(async move { self_hs.handle_all_streams(cancel_child_clone, rx).await });

        loop {
            let self_hb = self.clone();
            tokio::select! {
                _=cancel_clone.cancelled() => {
                    break;
                },
                hb_res=self_hb.make_heartbeat(&mut stream, cancel.clone()) => {
                    match hb_res {
                        Ok(()) => {
                            log::trace!("heartbeat success");
                        }
                        Err(err) => {
                            cancel_child.cancel();
                            if let CrabError::ErrorCode(CrabError::CANCELED_ERROR) = err {
                                log::trace!("heartbeat error with cancel");
                            }else{
                                log::error!("heartbeat failed: {}", err);
                            }
                            break;
                        }
                    }
                }
                Ok(stream)=Stream::accept(&self.conn)=>{
                    log::trace!("accepted new stream");
                    if let Err(err)=tx.send(Some(stream)).await{
                        log::error!("failed to send new stream: {}", err);
                    }
                }
            }
        }
        let _ = tx.send(None).await;
        self.set_status(NodeStatus::Stopping);
        let ret = hs_handle.await?;
        self.set_status(NodeStatus::Stopped);
        ret
    }
    async fn handle_all_streams(
        self: Arc<Self>,
        cancel: CancellationToken,
        mut rx: mpsc::Receiver<Option<Stream>>,
    ) -> Result<(), CrabError> {
        let mut join_set: JoinSet<Result<(), CrabError>> = JoinSet::new();
        loop {
            let self_clone = self.clone();
            let cancel_clone = cancel.clone();
            tokio::select! {
                _=cancel.cancelled()=>{
                    rx.close();
                    break;
                }
                ret=rx.recv() => {
                    match ret{
                        None=>break,
                        Some(None)=>break,
                        Some(Some(mut stream))=> {
                            join_set.spawn(async move {
                                self_clone.handle_stream(cancel_clone,&mut stream).await
                            });
                        }
                    }
                }
                Some(join_ret)=join_set.join_next(),if !join_set.is_empty() => {
                    if let Err(err) = join_ret {
                        log::error!("join set: {:?}", err);
                    }
                }
            }
        }
        while let Some(join_ret) = join_set.join_next().await {
            if let Err(err) = join_ret {
                log::error!("handle all stream join error: {}", err);
            }
        }
        Ok(())
    }
    async fn handle_stream(
        self: Arc<Self>,
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
