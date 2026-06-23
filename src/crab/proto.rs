use super::{CrabError, Handle, NodeMetadata};
use bincode_next::config;
use bincode_next::serde::{decode_from_slice, encode_into_std_write};
use binrw::{BinRead, BinWrite, binrw};
use bytes::BufMut;
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::any::Any;
use std::io::Cursor;
use tokio::sync::oneshot;
use tokio_util::bytes::BytesMut;
use tokio_util::sync::CancellationToken;

#[derive(BinRead, BinWrite, Debug, PartialEq, Eq, Clone, Copy)]
#[brw(repr = u16)]
#[brw(little)]
pub enum Method {
    Handshake = 0,
    Heartbeat = 1,
    Ack = 2,
    Error = 3,
    Command = 4,
}
#[binrw]
#[brw(little)]
#[brw(magic = b"CRAB")]
#[derive(Debug)]
pub struct MessageHeader {
    pub version: u8,
    pub option: u8,
    pub method: Method,
    pub length: u32,
}
impl MessageHeader {
    pub const OPTION_NONE: u8 = 0;
    pub const OPTION_ERROR: u8 = 1;
    pub fn ok(&self) -> bool {
        self.option & Self::OPTION_ERROR != Self::OPTION_ERROR
    }
}

#[derive(Serialize, Deserialize)]
pub struct AckMessage {
    pub code: u32,
    pub msg: Option<String>,
}
impl AckMessage {
    pub fn from_error(err: &CrabError) -> Self {
        match err {
            CrabError::ErrorCode(code) => Self {
                code: *code,
                msg: None,
            },
            _ => Self {
                code: CrabError::UNKNOWN_ERROR,
                msg: Some(err.to_string()),
            },
        }
    }
}
impl Into<Result<(), CrabError>> for AckMessage {
    fn into(self) -> Result<(), CrabError> {
        if self.code == CrabError::NO_ERROR {
            Ok(())
        } else {
            match self.msg {
                None => Err(CrabError::ErrorCode(self.code)),
                Some(msg) => Err(CrabError::ErrorCodeWithMessage(self.code, msg)),
            }
        }
    }
}
impl From<Result<(), CrabError>> for AckMessage {
    fn from(value: Result<(), CrabError>) -> Self {
        let err = match value {
            Ok(()) => CrabError::ErrorCode(CrabError::NO_ERROR),
            Err(e) => e,
        };
        Self::from_error(&err)
    }
}
const HEADER_SIZE: usize = 12;
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

pub struct Stream {
    pub writer: SendStream,
    pub reader: RecvStream,
}
impl Stream {
    const DEFAULT_BUF_SIZE: usize = 16 * 1024;
    pub async fn read_message<T: DeserializeOwned>(
        &mut self,
    ) -> Result<(MessageHeader, T), CrabError> {
        let mut header = [0u8; HEADER_SIZE];
        self.reader.read_exact(&mut header[..]).await?;
        let msg_header = MessageHeader::read(&mut Cursor::new(&header))
            .map_err(|_| CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))?;
        if msg_header.length as usize > MAX_PAYLOAD_SIZE {
            return Err(CrabError::ErrorCode(CrabError::PAYLOAD_TOO_LARGE));
        }
        let mut buf = BytesMut::with_capacity(msg_header.length as usize);
        buf.resize(msg_header.length as usize, 0);
        self.reader.read_exact(&mut buf[..]).await?;
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
    pub async fn read_ack(&mut self) -> Result<(), CrabError> {
        let (_, ack) = self.read_message::<AckMessage>().await?;
        ack.into()
    }
    pub async fn write_message<T: Serialize>(
        &mut self,
        method: Method,
        option: u8,
        msg: &T,
    ) -> Result<(), CrabError> {
        let mut buf = BytesMut::with_capacity(Self::DEFAULT_BUF_SIZE);
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
        self.writer.write_all(buf.as_ref()).await?;
        Ok(())
    }
    pub async fn write_error(
        &mut self,
        method: Method,
        option: u8,
        error: &CrabError,
    ) -> Result<(), CrabError> {
        let msg = AckMessage::from_error(error);
        self.write_message(method, option, &msg).await
    }
    pub async fn accept(conn: &Connection) -> Result<Self, CrabError> {
        let (writer, reader) = conn.accept_bi().await?;
        Ok(Self { writer, reader })
    }
    pub async fn open(conn: &Connection) -> Result<Self, CrabError> {
        let (writer, reader) = conn.open_bi().await?;
        Ok(Self { writer, reader })
    }
}

pub trait HandshakePacket: DeserializeOwned + Serialize + Send + Sync {
    fn node_id(&self) -> &str;
}
#[async_trait::async_trait]
pub trait Protocol: Send + Sync {
    type Handshake: HandshakePacket + 'static;
    type Heartbeat: DeserializeOwned + Serialize + Send + Sync + 'static;
    type Command: DeserializeOwned + Serialize + Send + Sync + 'static;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError>;
    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError>;
    async fn on_handshake(
        &self,
        _: &NodeMetadata,
        _: &Self::Handshake,
    ) -> Result<Self::Handshake, CrabError> {
        self.make_handshake()
    }
    async fn on_heartbeat(
        &self,
        _: &NodeMetadata,
        _: &Self::Heartbeat,
    ) -> Result<Self::Heartbeat, CrabError> {
        self.make_heartbeat()
    }
    async fn on_node_accepted(
        &self,
        _: &NodeMetadata,
        _: Handle,
        _: Self::Handshake,
    ) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_exited(&self, _: &NodeMetadata) {}
    async fn handle_command(
        &self,
        _: CancellationToken,
        _: &NodeMetadata,
        _: (MessageHeader, Self::Command),
        _: Stream,
    ) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNKNOWN_ERROR))
    }
}
#[async_trait::async_trait]
pub(super) trait Hook: Send + Sync {
    async fn handshake(
        &self,
        _: &Connection,
    ) -> Result<(NodeMetadata, Box<dyn Any + Send>), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn handshake_as_client(
        &self,
        _: &Connection,
    ) -> Result<(NodeMetadata, Box<dyn Any + Send>), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn heartbeat(&self, _: &NodeMetadata, _: &mut Stream) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn heartbeat_as_client(&self, _: &NodeMetadata, _: &mut Stream) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn on_connection_accepted(&self, _: &Connection) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_accepted(
        &self,
        _: &NodeMetadata,
        _: Handle,
        _: Box<dyn Any + Send>,
    ) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_exited(&self, meta: &NodeMetadata) {
        log::trace!("on_node_exited {}", meta.node_id);
    }
    async fn handle_stream(
        &self,
        _: &NodeMetadata,
        _: CancellationToken,
        _: Stream,
    ) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
}
#[async_trait::async_trait]
pub(super) trait AsyncTask: Send + 'static {
    async fn execute(self: Box<Self>, _: CancellationToken, _: Stream) -> Result<(), CrabError>;
}

#[async_trait::async_trait]
pub trait Executor<T>: Send + 'static
where
    T: Send + 'static,
{
    async fn execute(self, _: CancellationToken, _: Stream) -> Result<T, CrabError>;
}

pub(super) struct AsyncJob<T, CE> {
    pub callback: CE,
    pub tx: oneshot::Sender<Result<T, CrabError>>,
}
#[async_trait::async_trait]
impl<T, CE> AsyncTask for AsyncJob<T, CE>
where
    T: Send + 'static,
    CE: Executor<T>,
{
    async fn execute(
        self: Box<Self>,
        c: CancellationToken,
        stream: Stream,
    ) -> Result<(), CrabError> {
        let this = *self;
        let ret = this.callback.execute(c, stream).await;
        if this.tx.send(ret).is_err() {
            log::warn!("AsyncJob receiver dropped");
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl<F, Fut, T> Executor<T> for F
where
    F: FnOnce(CancellationToken, Stream) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, CrabError>> + Send + 'static,
    T: Send + 'static,
{
    async fn execute(self, c: CancellationToken, stream: Stream) -> Result<T, CrabError> {
        self(c, stream).await
    }
}
