use crate::app::protocol::types::CommandHandler;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use std::fs;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
pub struct DeleteCommand(pub String);
#[async_trait::async_trait]
impl CommandHandler for DeleteCommand {
    async fn handle(
        self: Box<Self>,
        _: CancellationToken,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let ret = fs::remove_dir_all(&self.0).map_err(CrabError::from);
        stream
            .write_message(header.method, header.option, &AckMessage::from(ret))
            .await?;
        Ok(())
    }
}
