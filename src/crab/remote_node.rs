use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time;
use tokio_util::sync::CancellationToken;

use super::utils::runit::Worker;
use crate::crab::node::Options;
use crate::crab::proto::{HandshakeRet, Hook, Stream};
use crate::crab::{node::NodeStatus, CrabError, Node};

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
    pub async fn heartbeat(self: Arc<Self>, cancel: CancellationToken) -> Result<(), CrabError> {
        let mut stream = if self.as_client {
            Stream::open_from_connection(&self.conn).await
        } else {
            Stream::accept_from_connection(&self.conn).await
        }?;
        let heartbeat_interval = Duration::from_secs(self.opts.heartbeat_interval);
        let heartbeat_timeout = Duration::from_secs(self.opts.heartbeat_timeout);
        let mut interval = time::interval(heartbeat_interval);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let self_clone = self.clone();
        loop {
            tokio::select! {

                _ = cancel.cancelled() => {
                    return Ok(());
                },
                _ = interval.tick() =>{
                }
            }
            tokio::select! {
                _=time::sleep(heartbeat_timeout) => {
                    return Err(CrabError::ErrorCode(CrabError::HEARTBEAT_TIMEOUT));
                }
                _=cancel.cancelled() => {
                    return Ok(())
                }
                Err(err)=self_clone.make_heartbeat(&mut stream)=>{
                    return Err(err);
                }
            }
        }
    }
    async fn make_heartbeat(self: Arc<Self>, stream: &mut Stream) -> Result<(), CrabError> {
        if self.as_client {
            self.hook.heartbeat_as_client(stream).await
        } else {
            self.hook.heartbeat(stream).await
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
        self.inner.clone().heartbeat(cancel).await
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
}
