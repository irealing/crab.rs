use super::proto::{AsyncTask, Hook, Stream};
use super::types::{NodeMetadata, Options};
use super::utils::runit::Worker;
use super::{CrabError, Node, types::NodeStatus};
use crate::crab::node_handle::Handle;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

struct RemoteNodeInner {
    pub(super) meta: Arc<NodeMetadata>,
    conn: quinn::Connection,
    status_tx: watch::Sender<NodeStatus>,
    status_rx: watch::Receiver<NodeStatus>,
    hook: Arc<dyn Hook>,
    opts: Options,
    cmd_rx: mpsc::Receiver<Box<dyn AsyncTask>>,
}
impl RemoteNodeInner {
    fn node_id(&self) -> &str {
        &self.meta.node_id
    }
    fn remote_addr(&self) -> SocketAddr {
        self.meta.remote_addr
    }
    fn as_client(&self) -> bool {
        self.meta.as_client
    }
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
            log::info!("node {} status {}", self.node_id(), status_ref);
        }
    }
    async fn accept(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        let (tx, rx) = mpsc::channel(32);
        let cancel_c = cancel.clone();
        let cancel_hs = cancel_c.clone();
        let self_hs = self.clone();
        let self_clone = self.clone();
        let hs_handle =
            tokio::spawn(async move { self_hs.handle_all_streams(cancel_hs, rx).await });
        loop {
            tokio::select! {
                _=cancel.cancelled() => {
                    break;
                }
                Ok(stream)=Stream::accept(&self_clone.conn)=>{
                    log::trace!("accepted new stream");
                    if let Err(err)=tx.send(Some(stream)).await{
                        log::error!("failed to send new stream: {}", err);
                    }
                }
            }
        }
        let _ = tx.send(None).await;
        hs_handle.await?
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
        self.hook.handle_stream(self.meta.deref(), cancel, stream).await
    }
    async fn make_heartbeat(
        self: Arc<Self>,
        stream: &mut Stream,
        cancel: CancellationToken,
    ) -> Result<(), CrabError> {
        if self.as_client() {
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
                self.hook.heartbeat_as_client(self.meta.deref(), stream).await
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
            res=timeout(Duration::from_secs(self.opts.heartbeat_timeout), self.hook.heartbeat(self.meta.deref(),stream))=>{
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
        ret: NodeMetadata,
        conn: quinn::Connection,
        hook: Arc<dyn Hook>,
        opts: Options,
    ) -> (Self, Handle) {
        let (status_tx, status_rx) = watch::channel(NodeStatus::Ready);
        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let inner = RemoteNodeInner {
            meta: Arc::new(ret),
            conn,
            status_tx,
            status_rx,
            hook,
            opts,
            cmd_rx,
        };
        let handle = Handle::new(inner.meta.clone(), inner.status_rx.clone(), cmd_tx.clone());
        (
            Self {
                inner: Arc::new(inner),
            },
            handle,
        )
    }

    async fn make_first_heartbeat(&self, cancel: CancellationToken) -> Result<Stream, CrabError> {
        let mut stream = if self.as_client() {
            Stream::open(&self.inner.conn).await?
        } else {
            Stream::accept(&self.inner.conn).await?
        };
        let co = if self.as_client() {
            self.inner
                .hook
                .heartbeat_as_client(self.meta(), &mut stream)
        } else {
            self.inner.hook.heartbeat(self.meta(), &mut stream)
        };
        tokio::select! {
            _=cancel.cancelled()=>{
                Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
            ret=co=>{
                ret?;
                Ok(stream)
            }
        }
    }
    pub(super) fn meta(&self) -> &NodeMetadata {
        self.inner.meta.deref()
    }
}
#[async_trait::async_trait]
impl Worker for RemoteNode {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), CrabError> {
        let mut hb_stream = timeout(
            Duration::from_secs(self.inner.opts.first_heartbeat),
            self.make_first_heartbeat(cancel.clone()),
        )
        .await
        .map_err(|_| CrabError::ErrorCode(CrabError::HEARTBEAT_TIMEOUT))??;
        log::trace!(
            "node {}({}) first heartbeat finished",
            self.inner.node_id(),
            self.inner.remote_addr()
        );
        self.inner.set_status(NodeStatus::Running);
        let hb_cancel = cancel.clone();
        let accept_cancel = cancel.clone();
        let accept_cancel_clone = accept_cancel.clone();
        let inner_hs = self.inner.clone();
        let accept_handle = tokio::spawn(async move { inner_hs.accept(accept_cancel_clone).await });
        loop {
            let inner_hb = self.inner.clone();
            if let Err(err) = tokio::select! {
                _=cancel.cancelled() => {
                    Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
                }
                hb_res=inner_hb.make_heartbeat(&mut hb_stream,hb_cancel.clone()) => {
                    if let Err(err) = hb_res {
                        Err(err)
                    }else{
                        log::trace!("node {}({}) heartbeat finished", self.inner.node_id(), self.inner.remote_addr());
                        Ok(())
                    }
                }
            } {
                self.inner.set_status(NodeStatus::Stopping);
                hb_cancel.cancel();
                if !matches!(err, CrabError::ErrorCode(CrabError::CANCELED_ERROR)) {
                    log::error!(
                        "node {}({})  heartbeat error :{}",
                        self.inner.node_id(),
                        self.inner.remote_addr(),
                        err
                    );
                }
                break;
            }
        }
        let ret = accept_handle.await?;
        self.inner.set_status(NodeStatus::Stopped);
        ret
    }
}
impl Node for RemoteNode {
    fn id(&self) -> &str {
        &self.inner.node_id()
    }
    fn status(&self) -> NodeStatus {
        *self.inner.status_rx.borrow()
    }
    fn addr(&self) -> SocketAddr {
        self.inner.remote_addr()
    }
    fn as_client(&self) -> bool {
        self.inner.as_client()
    }
}
