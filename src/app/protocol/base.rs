use super::commands::{DeleteCommand, FileMetadata, ReadFile};
use super::types::{Command, CommandExecutor};
use crab::proto::{Executor, MessageHeader, Method, Stream};
use crab::{CrabError, Handle, Node};
use tokio::sync::oneshot;
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
    async fn delete(&self, p: String, dir: bool) -> Result<(), CrabError> {
        log::debug!("deleting node {} path {}", self.id(), dir);
        self.exec(async move |_: CancellationToken, mut stream: Stream| {
            stream
                .write_message(
                    Method::Command,
                    MessageHeader::OPTION_NONE,
                    &Command::Delete(DeleteCommand { path: p, dir }),
                )
                .await?;
            stream.read_ack().await
        })
        .await
    }
    async fn read_file<E>(
        &self,
        filename: String,
    ) -> Result<(oneshot::Sender<Result<E, CrabError>>, FileMetadata), CrabError>
    where
        E: Executor<Output = ()>,
    {
        self.exec_with_ack::<Command, FileMetadata, E>(Command::ReadFile(ReadFile {
            path: filename,
        }))
        .await
    }
}
