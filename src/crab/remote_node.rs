use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::crab::{CrabError, Node, node::NodeStatus};

use super::utils::runit::Worker;

struct RemoteNodeInner {
    node_id: String,
    conn: quinn::Connection,
    local_addr: SocketAddr,
    status_tx: watch::Sender<NodeStatus>,
    status_rx: watch::Receiver<NodeStatus>,
    client: bool,
}
pub(super) struct RemoteNode {
    inner: Arc<RemoteNodeInner>,
}
#[async_trait::async_trait]
impl Worker for RemoteNode {
    async fn serve(&self, _: CancellationToken) -> Result<(), CrabError> {
        todo!("RemoteNode serve")
    }
}
impl Node for RemoteNode {
    fn id(&self) -> &str {
        return &self.inner.node_id;
    }
    fn status(&self) -> NodeStatus {
        *self.inner.status_rx.borrow()
    }
    fn addr(&self) -> SocketAddr {
        self.inner.local_addr
    }
}
