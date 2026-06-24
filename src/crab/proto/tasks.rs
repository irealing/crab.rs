use crate::CrabError;
use crate::proto::{MessageHeader, Method, Stream};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[async_trait::async_trait]
pub trait AsyncTask: Send + 'static {
    async fn execute(self: Box<Self>, _: CancellationToken, _: Stream) -> Result<(), CrabError>;
}

#[async_trait::async_trait]
pub trait Executor: Send + 'static {
    type Output: Send + 'static;
    async fn execute(self, _: CancellationToken, _: Stream) -> Result<Self::Output, CrabError>;
}

pub struct AsyncJob<T, CE> {
    pub callback: CE,
    pub tx: oneshot::Sender<Result<T, CrabError>>,
}
#[async_trait::async_trait]
impl<T, CE> AsyncTask for AsyncJob<T, CE>
where
    T: Send + 'static,
    CE: Executor<Output = T>,
{
    async fn execute(
        self: Box<Self>,
        c: CancellationToken,
        stream: Stream,
    ) -> Result<(), CrabError> {
        let this = *self;
        let ret = this.callback.execute(c, stream).await;
        if this.tx.send(ret).is_err() {
            log::warn!("AsyncJob receiver dropped");
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl<F, Fut, T> Executor for F
where
    F: FnOnce(CancellationToken, Stream) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, CrabError>> + Send + 'static,
    T: Send + 'static,
{
    type Output = T;
    async fn execute(self, c: CancellationToken, stream: Stream) -> Result<T, CrabError> {
        self(c, stream).await
    }
}

/// 用于两阶段返回的异步任务
/// 第一次通过`oneshot`返回第一阶段数据，ack时（通过oneshot发送`Executor`用于回调后续数据）
/// 传入`Executor`的返回值会被忽略
pub struct MultiStageTask<C, I, E: Executor> {
    pub initial_tx: oneshot::Sender<Result<(oneshot::Sender<Result<E, CrabError>>, I), CrabError>>,
    pub cmd: C,
}
#[async_trait::async_trait]
impl<C, I, E> AsyncTask for MultiStageTask<C, I, E>
where
    C: Serialize + Sync + Send + 'static,
    I: DeserializeOwned + Send + 'static,
    E: Executor,
{
    async fn execute(
        self: Box<Self>,
        c: CancellationToken,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        if let Err(err) = stream
            .write_message(Method::Command, MessageHeader::OPTION_NONE, &self.cmd)
            .await
        {
            log::warn!("AsyncStreamTask write command error: {}", err);
            let _ = self.initial_tx.send(Err(err));
            return Ok(());
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        match stream.read_message::<I>().await {
            Ok((_, initial_ret)) => self
                .initial_tx
                .send(Ok((ack_tx, initial_ret)))
                .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?,
            Err(e) => {
                log::warn!("AsyncStreamTask read initial response error {}", e);
                self.initial_tx
                    .send(Err(e))
                    .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
                Err(CrabError::ErrorCode(CrabError::TASK_ACK_FAILED))?;
            }
        }
        match ack_rx
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?
        {
            Err(e) => Err(e),
            Ok(executor) => executor.execute(c, stream).await.map(|_| ()),
        }
    }
}
