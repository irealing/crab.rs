use crate::app::types::Command;
use crab::proto::{Method, Stream};
use crab::{CrabError, Handle, Node};
use tokio_util::sync::CancellationToken;

#[async_trait::async_trait]
pub trait CommandExecutor {
    async fn ping(&self) -> Result<(), CrabError>;
}
#[async_trait::async_trait]
impl CommandExecutor for Handle {
    async fn ping(&self) -> Result<(), CrabError> {
        log::debug!("ping node {}", self.id());
        self.exec(async |_: CancellationToken, mut stream: Stream| {
            stream
                .write_message(Method::Command, 0, &Command::Ping)
                .await?;
            let (_, ret) = stream.read_message::<Command>().await?;
            if ret == Command::Pong {
                Ok(())
            } else {
                Err(CrabError::ErrorCode(CrabError::UNEXCEPTED_RESPONSE))
            }
        })
        .await
    }
}
