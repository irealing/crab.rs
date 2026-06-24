mod hook;
mod hook_wrapper;
mod types;
mod util;

use super::{CrabError, Handle, NodeMetadata};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) use hook::Hook;
pub use hook::{HandshakePacket, Protocol};
pub(super) use hook_wrapper::ProtoWrapper;
pub use types::{AckMessage, MessageHeader, Method};
pub use util::Stream;
#[async_trait::async_trait]
pub(super) trait AsyncTask: Send + 'static {
    async fn execute(self: Box<Self>, _: CancellationToken, _: Stream) -> Result<(), CrabError>;
}

#[async_trait::async_trait]
pub trait Executor<T>: Send + 'static
where
    T: Send + 'static,
{
    async fn execute(self, _: CancellationToken, _: Stream) -> Result<T, CrabError>;
}

pub(super) struct AsyncJob<T, CE> {
    pub callback: CE,
    pub tx: oneshot::Sender<Result<T, CrabError>>,
}
#[async_trait::async_trait]
impl<T, CE> AsyncTask for AsyncJob<T, CE>
where
    T: Send + 'static,
    CE: Executor<T>,
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
impl<F, Fut, T> Executor<T> for F
where
    F: FnOnce(CancellationToken, Stream) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, CrabError>> + Send + 'static,
    T: Send + 'static,
{
    async fn execute(self, c: CancellationToken, stream: Stream) -> Result<T, CrabError> {
        self(c, stream).await
    }
}
