use super::commands::{DeleteCommand, FileMetadata, ReadFile};
use super::types::{Command, CommandExecutor};
use crate::app::protocol::WriteFile;
use crab::proto::{AckMessage, Executor, Stream, TaskHandle};
use crab::{CrabError, Handle, Node};
use tokio_util::sync::CancellationToken;
#[async_trait::async_trait]
impl CommandExecutor for Handle {
    async fn ping(&self) -> Result<(), CrabError> {
        log::debug!("ping node {}", self.id());
        self.exec(
            Command::Ping,
            async |_: CancellationToken, _: Stream| Ok(()),
        )
        .await
    }
    async fn delete(&self, p: String, dir: bool) -> Result<(), CrabError> {
        log::debug!("deleting node {} path {}", self.id(), dir);
        self.exec(
            Command::Delete(DeleteCommand { path: p, dir }),
            async move |_: CancellationToken, _: Stream| Ok(()),
        )
        .await
    }
    async fn read_file<E>(&self, filename: String) -> TaskHandle<E, FileMetadata>
    where
        E: Executor<Output = ()>,
    {
        self.exec_with_ack::<Command, FileMetadata, E>(Command::ReadFile(ReadFile {
            path: filename,
        }))
        .await
    }
    async fn write_file<E>(&self, cmd: WriteFile) -> TaskHandle<E, ()>
    where
        E: Executor<Output = ()>,
    {
        let (sender, _) = self
            .exec_with_ack::<Command, AckMessage, E>(Command::WriteFile(cmd))
            .await?;
        Ok((sender, ()))
    }
}
