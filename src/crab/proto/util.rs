use super::{AckMessage, MessageHeader, Method};
use crate::CrabError;
use bincode_next::config;
use bincode_next::serde::{decode_from_slice, encode_into_std_write};
use binrw::{BinRead, BinWrite};
use bytes::{BufMut, BytesMut};
use quinn::{Connection, RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::Cursor;
#[async_trait::async_trait]
pub trait MessageReader {
    async fn read_message<M: DeserializeOwned>(&mut self) -> Result<(MessageHeader, M), CrabError>;
    async fn read_ack(&mut self) -> Result<(), CrabError>;
}
const HEADER_SIZE: usize = 12;
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_BUF_SIZE: usize = 16 * 1024;

#[async_trait::async_trait]
impl MessageReader for RecvStream {
    async fn read_message<M: DeserializeOwned>(&mut self) -> Result<(MessageHeader, M), CrabError> {
        let mut header = [0u8; HEADER_SIZE];
        self.read_exact(&mut header[..]).await?;
        let msg_header = MessageHeader::read(&mut Cursor::new(&header))
            .map_err(|_| CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))?;
        if msg_header.length as usize > MAX_PAYLOAD_SIZE {
            return Err(CrabError::ErrorCode(CrabError::PAYLOAD_TOO_LARGE));
        }
        let mut buf = BytesMut::with_capacity(msg_header.length as usize);
        buf.resize(msg_header.length as usize, 0);
        self.read_exact(&mut buf[..]).await?;
        if !msg_header.ok() {
            let (msg, _): (AckMessage, usize) = decode_from_slice(&buf[..], config::standard())
                .map_err(|_| CrabError::ErrorCode(CrabError::DESERIALIZATION_ERROR))?;
            Err(CrabError::ErrorCode(msg.code))
        } else {
            let (message, _) = decode_from_slice(&buf[..], config::standard()).map_err(|e| {
                log::warn!("Failed to decode message: {}", e);
                CrabError::ErrorCode(CrabError::DESERIALIZATION_ERROR)
            })?;
            Ok((msg_header, message))
        }
    }
    async fn read_ack(&mut self) -> Result<(), CrabError> {
        let (_, ack) = self.read_message::<AckMessage>().await?;
        ack.into()
    }
}
#[async_trait::async_trait]
pub trait MessageWriter {
    async fn write_message<M: Serialize + Sync>(
        &mut self,
        _: Method,
        _: u8,
        msg: &M,
    ) -> Result<(), CrabError>;
    async fn write_error(&mut self, _: Method, _: u8, _: &CrabError) -> Result<(), CrabError>;
}
#[async_trait::async_trait]
impl MessageWriter for SendStream {
    async fn write_message<M: Serialize + Sync>(
        &mut self,
        method: Method,
        option: u8,
        msg: &M,
    ) -> Result<(), CrabError> {
        let mut buf = BytesMut::with_capacity(DEFAULT_BUF_SIZE);
        buf.resize(HEADER_SIZE, 0);
        let mut writer = (&mut buf).writer();
        encode_into_std_write(msg, &mut writer, config::standard())
            .map_err(|_| CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))?;
        let payload_len = buf.len() - HEADER_SIZE;
        let header = MessageHeader {
            version: 0,
            option,
            method,
            length: payload_len as u32,
        };
        header.write(&mut Cursor::new(&mut buf[..HEADER_SIZE]))?;
        self.write_all(buf.as_ref()).await?;
        Ok(())
    }

    async fn write_error(
        &mut self,
        method: Method,
        option: u8,
        err: &CrabError,
    ) -> Result<(), CrabError> {
        let msg = AckMessage::from_error(err);
        self.write_message(method, option, &msg).await
    }
}
pub struct Stream {
    pub writer: SendStream,
    pub reader: RecvStream,
}
impl Stream {
    pub fn split(self) -> (SendStream, RecvStream) {
        (self.writer, self.reader)
    }
    #[inline]
    pub async fn read_message<T: DeserializeOwned>(
        &mut self,
    ) -> Result<(MessageHeader, T), CrabError> {
        self.reader.read_message::<T>().await
    }
    #[inline]
    pub async fn read_ack(&mut self) -> Result<(), CrabError> {
        self.reader.read_ack().await
    }
    #[inline]
    pub async fn write_message<T: Serialize + Sync>(
        &mut self,
        method: Method,
        option: u8,
        msg: &T,
    ) -> Result<(), CrabError> {
        self.writer.write_message(method, option, msg).await
    }
    #[inline]
    pub async fn write_error(
        &mut self,
        method: Method,
        option: u8,
        error: &CrabError,
    ) -> Result<(), CrabError> {
        self.writer.write_error(method, option, error).await
    }
    pub async fn accept(conn: &Connection) -> Result<Self, CrabError> {
        let (writer, reader) = conn.accept_bi().await?;
        Ok(Self { writer, reader })
    }
    pub async fn open(conn: &Connection) -> Result<Self, CrabError> {
        let (writer, reader) = conn.open_bi().await?;
        Ok(Self { writer, reader })
    }
    pub async fn read_and_ack<T: DeserializeOwned, V>(
        &mut self,
        valid: Option<V>,
    ) -> Result<(MessageHeader, T), CrabError>
    where
        V: FnOnce(T) -> Result<T, CrabError>,
    {
        let (header, msg) = self.reader.read_message().await?;
        let ret = if let Some(val) = valid {
            val(msg)
        } else {
            Ok(msg)
        };
        match ret {
            Ok(msg) => {
                self.writer
                    .write_message(header.method, header.option, &AckMessage::success())
                    .await?;
                Ok((header, msg))
            }
            Err(err) => {
                self.writer
                    .write_error(header.method, header.option, &err)
                    .await?;
                Err(err)
            }
        }
    }
    pub async fn write_and_ack<T: Serialize + Sync>(
        &mut self,
        method: Method,
        option: u8,
        msg: T,
    ) -> Result<(), CrabError> {
        self.writer.write_message(method, option, &msg).await?;
        self.reader.read_ack().await
    }
}
