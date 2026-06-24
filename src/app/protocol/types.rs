use super::commands::DeleteCommand;
use crab::CrabError;
use crab::proto::{MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tokio_util::sync::CancellationToken;
#[derive(Deserialize, Serialize)]
pub enum Command {
    Ping,
    Pong,
    Delete(DeleteCommand),
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
        }
    }
}
#[async_trait::async_trait]
pub trait CommandExecutor {
    async fn ping(&self) -> Result<(), CrabError>;
    async fn delete(&self, _: String, _: bool) -> Result<(), CrabError>;
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
