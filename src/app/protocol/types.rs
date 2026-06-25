use super::commands::{DeleteCommand, FileMetadata, ReadFile};
use crab::CrabError;
use crab::proto::{Executor, MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
#[derive(Deserialize, Serialize)]
pub enum Command {
    Ping,
    Pong,
    Delete(DeleteCommand),
    ReadFile(ReadFile),
}
impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Command::Ping => {
                write!(f, "ping")
            }
            Command::Pong => {
                write!(f, "pong")
            }
            Command::Delete(ref delete) => {
                write!(f, "delete({})", delete.path)
            }
            Command::ReadFile(ref read) => {
                write!(f, "read_file({})", read.path)
            }
        }
    }
}
#[async_trait::async_trait]
pub trait CommandExecutor {
    async fn ping(&self) -> Result<(), CrabError>;
    async fn delete(&self, _: String, _: bool) -> Result<(), CrabError>;
    async fn read_file<E>(
        &self,
        _: String,
    ) -> Result<(oneshot::Sender<Result<E, CrabError>>, FileMetadata), CrabError>
    where
        E: Executor<Output = ()>;
}
#[async_trait::async_trait]
pub trait CommandHandler: Send {
    async fn handle(
        self: Box<Self>,
        _: CancellationToken,
        _: MessageHeader,
        _: Stream,
    ) -> Result<(), CrabError>;
}
#[async_trait::async_trait]
impl CommandHandler for Command {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let cmd = *self;
        let handler: Option<Box<dyn CommandHandler>> = match cmd {
            Command::Ping => {
                return stream
                    .write_message(header.method, header.option, &Command::Pong)
                    .await;
            }
            Command::Pong => {
                return stream
                    .write_message(header.method, header.option, &Command::Ping)
                    .await;
            }
            Command::Delete(delete) => Some(Box::new(delete)),
            Command::ReadFile(read) => Some(Box::new(read)),
        };
        if let Some(handler) = handler {
            handler
                .handle(cancel, header, stream)
                .await
                .inspect_err(|e| log::warn!("handle command error: {}", e))
        } else {
            stream
                .write_error(
                    header.method,
                    header.option,
                    &CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR),
                )
                .await
        }
    }
}
