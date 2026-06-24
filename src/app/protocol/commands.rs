use crate::app::protocol::types::CommandHandler;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::time::UNIX_EPOCH;
use tokio::{fs, io};
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
            fs::remove_dir_all(&self.path).await
        } else {
            fs::remove_file(&self.path).await
        }
        .map_err(CrabError::from);
        stream
            .write_message(header.method, header.option, &AckMessage::from(ret))
            .await?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
pub struct FileMetadata {
    pub filesize: u64,
    pub dir: bool,
    pub mtime: u64,
    pub atime: u64,
}
impl Into<FileMetadata> for Metadata {
    fn into(self) -> FileMetadata {
        FileMetadata {
            filesize: self.len(),
            dir: self.is_dir(),
            mtime: self
                .accessed()
                .map(|v| v.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64)
                .unwrap_or_default(),
            atime: self
                .modified()
                .map(|v| v.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64)
                .unwrap_or_default(),
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct ReadFile {
    pub path: String,
}
#[async_trait::async_trait]
impl CommandHandler for ReadFile {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        h: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        match fs::metadata(&self.path)
            .await
            .map(|m| m.into())
            .map_err(CrabError::from)
        {
            Ok(metadata) => {
                stream
                    .write_message::<FileMetadata>(h.method, h.option, &metadata)
                    .await
                    .inspect_err(|e| log::warn!("failed to write filemetadata: {}", e))?;
            }
            Err(e) => {
                stream.write_error(h.method, h.option, &e).await?;
                return Err(e);
            }
        }
        stream.read_ack().await?;
        let file = match fs::File::open(&self.path).await {
            Ok(f) => {
                stream
                    .write_message(h.method, h.option, &AckMessage::success())
                    .await?;
                Some(f)
            }
            Err(e) => {
                stream
                    .write_error(h.method, h.option, &CrabError::from(e))
                    .await?;
                None
            }
        };
        let Some(mut file) = file else {
            return Ok(());
        };
        tokio::select! {
            _ = cancel.cancelled() => {
                Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
            r=io::copy(&mut file, &mut stream.writer)=>{
                match r{
                    Ok(size)=>{
                        log::debug!("copy {} bytes", size);
                        Ok(())
                    }
                    Err(e)=>{
                        Err(e.into())
                    }
                }
            }
        }
    }
}
