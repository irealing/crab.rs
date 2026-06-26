use crate::app::protocol::types::CommandHandler;
use crate::app::protocol::util::generate_temp_path;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use std::fs::Metadata;
use std::path::PathBuf;
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
                    .inspect_err(|e| log::warn!("failed to write file metadata: {}", e))?;
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
                let _=stream.writer.finish();
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

#[derive(Serialize, Deserialize)]
pub struct WriteFile {
    pub path: String,
    #[serde(default)]
    pub overwrite: bool,
    #[serde(default)]
    pub mkdir: bool,
}
impl WriteFile {
    async fn create_temp_file(&self) -> Result<(PathBuf, fs::File), CrabError> {
        let temp_file_path = generate_temp_path(&self.path, self.mkdir).await?;
        let file = fs::File::create(&temp_file_path)
            .await
            .inspect_err(|e| log::warn!("WriteFile:failed to create temp file: {}", e))?;
        Ok((temp_file_path, file))
    }
}
#[async_trait::async_trait]
impl CommandHandler for WriteFile {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let (temp_file_path, mut output) = match self.create_temp_file().await {
            Ok(f) => {
                stream
                    .write_message(header.method, header.option, &AckMessage::success())
                    .await?;
                f
            }
            Err(err) => {
                stream
                    .write_error(header.method, header.option, &err)
                    .await?;
                return Err(err.into());
            }
        };
        let copy_file_ret = tokio::select! {
            _=cancel.cancelled() => {
                Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
            ret=io::copy(&mut stream.reader, &mut output) => {
                match ret {
                    Ok(size)=>{
                        log::debug!("WriteFile copy {} bytes", size);
                        Ok(())
                    },
                    Err(err)=>{
                        Err(err.into())
                    }
                }
            }
        };
        let write_ret = match copy_file_ret {
            Ok(_) => fs::try_exists(&self.path)
                .await
                .map_err(CrabError::from)
                .and_then(|exists| {
                    if !exists || !self.overwrite {
                        Ok(())
                    } else {
                        Err(
                            io::Error::new(io::ErrorKind::AlreadyExists, "File already exists")
                                .into(),
                        )
                    }
                }),
            Err(err) => Err(err),
        };
        drop(output);
        let ack = match write_ret {
            Ok(_) => {
                log::debug!(
                    "WriteFile successfully,rename file {}",
                    temp_file_path.display()
                );
                fs::rename(&temp_file_path, &self.path)
                    .await
                    .map(|_| AckMessage::success())
                    .unwrap_or_else(|e| AckMessage::from_error(&e.into()))
            }
            Err(err) => AckMessage::from_error(&err.into()),
        };
        if ack.code != CrabError::NO_ERROR {
            let _ = fs::remove_file(&temp_file_path)
                .await
                .inspect_err(|e| log::warn!("failed to remove file: {}", e));
        }
        stream
            .write_message(header.method, header.option, &ack)
            .await
    }
}
