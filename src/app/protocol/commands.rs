use crate::app::protocol::types::CommandHandler;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use std::fs;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
pub struct DeleteCommand {
    pub path: String,
    pub dir: bool,
}
#[async_trait::async_trait]
impl CommandHandler for DeleteCommand {
    async fn handle(
        self: Box<Self>,
        _: CancellationToken,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let ret = if self.dir {
            fs::remove_dir_all(&self.path)
        } else {
            fs::remove_file(&self.path)
        }
        .map_err(CrabError::from);
        stream
            .write_message(header.method, header.option, &AckMessage::from(ret))
            .await?;
        Ok(())
    }
}
