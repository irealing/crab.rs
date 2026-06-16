use super::CrabError;
use super::Node;
use super::proto::Stream;
use super::proto::{AsyncJob, AsyncTask};
use super::types::{NodeMetadata, NodeStatus};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

struct HandleInner {
    meta: Arc<NodeMetadata>,
    status_rx: watch::Receiver<NodeStatus>,
    cmd_tx: mpsc::Sender<Box<dyn AsyncTask>>,
}
pub struct Handle {
    inner: Arc<HandleInner>,
}
impl Handle {
    pub(super) fn new(
        meta: Arc<NodeMetadata>,
        status_rx: watch::Receiver<NodeStatus>,
        cmd_tx: mpsc::Sender<Box<dyn AsyncTask>>,
    ) -> Self {
        Self {
            inner: Arc::new(HandleInner {
                meta,
                status_rx,
                cmd_tx,
            }),
        }
    }
    async fn exec<F, T, Fut>(&self, callback: F) -> Result<T, CrabError>
    where
        F: FnOnce(CancellationToken, &mut Stream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, CrabError>> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = AsyncJob { callback, tx };
        self.inner
            .cmd_tx
            .send(Box::new(job))
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))?;
        rx.await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))?
    }
}
impl Clone for Handle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl Node for Handle {
    fn id(&self) -> &str {
        &self.inner.meta.node_id
    }

    fn status(&self) -> NodeStatus {
        *self.inner.status_rx.borrow()
    }

    fn addr(&self) -> SocketAddr {
        self.inner.meta.remote_addr
    }

    fn as_client(&self) -> bool {
        self.inner.meta.as_client
    }
}
