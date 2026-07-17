use super::proto::{AsyncJob, AsyncTask, Executor, MultiStageTask};
use super::types::{NodeMetadata, NodeStatus};
use super::{CrabError, Node};
use crate::proto::BaseAsyncTask;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, oneshot, watch};

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
    #[inline]
    async fn send_task<T>(&self, task: T) -> Result<(), CrabError>
    where
        T: AsyncTask,
    {
        self.inner
            .cmd_tx
            .send(Box::new(task))
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))
    }
    /// 调用节点执行命令并通过oneshot等待返回结果
    pub async fn exec<C, CE, T>(&self, cmd: C, callback: CE) -> Result<T, CrabError>
    where
        C: Serialize + Send + Sync + 'static,
        CE: Executor<Output = T>,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = AsyncJob { cmd, callback, tx };
        self.send_task(job).await?;
        rx.await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))?
    }
    pub async fn exec_with_ack<C, I, E>(
        &self,
        cmd: C,
    ) -> Result<(Sender<Result<E, CrabError>>, I), CrabError>
    where
        C: Serialize + Sync + Send + 'static,
        I: DeserializeOwned + Send + 'static,
        E: Executor<Output = ()>,
    {
        let (initial_tx, initial_rx) = oneshot::channel();
        let task = MultiStageTask { initial_tx, cmd };
        self.send_task(task).await?;
        initial_rx
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))?
    }
    /// 发送命令但不等待结果
    pub async fn spawn<C, E>(&self, cmd: C, executor: E) -> Result<(), CrabError>
    where
        C: Serialize + Sync + Send + 'static,
        E: Executor<Output = ()>,
    {
        self.send_task(BaseAsyncTask::new(cmd, executor)).await
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
