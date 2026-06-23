use super::types::{Command, CommandExecutor, DeleteCommand};
use crab::proto::{MessageHeader, Method, Stream};
use crab::{CrabError, Handle, Node};
use tokio_util::sync::CancellationToken;

#[async_trait::async_trait]
impl CommandExecutor for Handle {
    async fn ping(&self) -> Result<(), CrabError> {
        log::debug!("ping node {}", self.id());
        self.exec(async |_: CancellationToken, mut stream: Stream| {
            stream
                .write_message(Method::Command, MessageHeader::OPTION_NONE, &Command::Ping)
                .await?;
            let _ = stream.read_message::<Command>().await?;
            Ok(())
        })
        .await
    }
    async fn delete(&self, dir: String) -> Result<(), CrabError> {
        log::debug!("deleting node {} path {}", self.id(), dir);
        self.exec(async move |_: CancellationToken, mut stream: Stream| {
            stream
                .write_message(
                    Method::Command,
                    MessageHeader::OPTION_NONE,
                    &Command::Delete(DeleteCommand(dir)),
                )
                .await?;
            stream.read_ack().await
        })
        .await
    }
}
