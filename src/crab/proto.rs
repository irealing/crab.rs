use crate::crab::CrabError;
use crate::crab::types::NodeMetadata;
use bincode_next::config;
use bincode_next::serde::{decode_from_slice, encode_into_std_write};
use binrw::{BinRead, BinWrite, binrw};
use bytes::BufMut;
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::Cursor;
use tokio_util::bytes::BytesMut;

#[derive(BinRead, BinWrite, Debug, PartialEq, Eq, Clone, Copy)]
#[brw(repr = u16)]
#[brw(little)]
pub enum Method {
    Handshake = 0,
    Heartbeat = 1,
    Ack = 2,
    Error = 3,
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
pub(super) struct AckMessage {
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

const HEADER_SIZE: usize = 12;
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

pub trait HandshakePacket: DeserializeOwned + Serialize + Send + Sync {
    fn node_id(&self) -> &str;
}
pub trait Protocol: Send + Sync {
    type Handshake: HandshakePacket + 'static;
    type Heartbeat: DeserializeOwned + Serialize + Send + Sync + 'static;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError>;
    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError>;
    fn on_handshake(
        &self,
        _: &NodeMetadata,
        _: &Self::Handshake,
    ) -> Result<Self::Handshake, CrabError> {
        self.make_handshake()
    }
    fn on_heartbeat(
        &self,
        _: &NodeMetadata,
        _: &Self::Heartbeat,
    ) -> Result<Self::Heartbeat, CrabError> {
        self.make_heartbeat()
    }
}
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

#[async_trait::async_trait]
pub(super) trait Hook: Send + Sync {
    async fn handshake(&self, _: &Connection) -> Result<NodeMetadata, CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn handshake_as_client(&self, _: &Connection) -> Result<NodeMetadata, CrabError> {
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
    async fn on_node_accepted(&self, _: &NodeMetadata) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_exited(&self, meta: &NodeMetadata) {
        log::trace!("on_node_exited {}", meta.node_id);
    }
}
pub(super) struct ProtoWrapper<S, H, P: Protocol<Handshake = S, Heartbeat = H>> {
    protocol: P,
}
impl<S, H, P> ProtoWrapper<S, H, P>
where
    P: Protocol<Handshake = S, Heartbeat = H>,
{
    pub fn new(protocol: P) -> Self {
        Self { protocol }
    }
}
#[async_trait::async_trait]
impl<S, H, P> Hook for ProtoWrapper<S, H, P>
where
    P: Protocol<Handshake = S, Heartbeat = H>,
    S: HandshakePacket + 'static,
    H: DeserializeOwned + Serialize + Sync + Send + 'static,
{
    async fn handshake(&self, conn: &Connection) -> Result<NodeMetadata, CrabError> {
        log::trace!("handshake with connection from {}", conn.remote_address());
        let mut session = Stream::accept(conn).await?;
        let (header, handshake) = session.read_message::<P::Handshake>().await.map_err(|e| {
            log::warn!("handshake failed,read header {:?}", e);
            e
        })?;
        if header.method != Method::Handshake {
            log::warn!(
                "invalid message method,accept {:?} receive {:?} ",
                Method::Handshake,
                header.method
            );
            return Err(CrabError::ErrorCode(CrabError::BAD_MESSAGE_HEADER));
        }
        let meta = NodeMetadata {
            node_id: handshake.node_id().to_string(),
            remote_addr: conn.remote_address(),
            as_client: true,
        };
        match self.protocol.on_handshake(&meta, &handshake) {
            Err(err) => {
                session
                    .write_error(Method::Handshake, MessageHeader::OPTION_ERROR, &err)
                    .await?;
                Err(err)
            }
            Ok(ret) => {
                if let Err(err) = session
                    .write_message(Method::Handshake, header.option, &ret)
                    .await
                {
                    log::error!("write handshake message failed {:}", err);
                    Err(CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))
                } else {
                    Ok(meta)
                }
            }
        }
    }
    async fn handshake_as_client(&self, conn: &Connection) -> Result<NodeMetadata, CrabError> {
        log::trace!(
            "handshake_as_client with connection {}",
            conn.remote_address()
        );
        let handshake = self.protocol.make_handshake()?;
        let mut session = Stream::open(conn).await?;
        session
            .write_message(Method::Handshake, 0, &handshake)
            .await?;
        let (_, body) = session.read_message::<P::Handshake>().await?;
        log::trace!(
            "remote {} node id {}",
            conn.remote_address(),
            body.node_id()
        );
        let meta = NodeMetadata {
            node_id: body.node_id().to_string(),
            remote_addr: conn.remote_address(),
            as_client: false,
        };
        Ok(meta)
    }
    async fn heartbeat(&self, meta: &NodeMetadata, stream: &mut Stream) -> Result<(), CrabError> {
        match stream
            .read_message::<P::Heartbeat>()
            .await
            .and_then(|(_, body)| {
                self.protocol.on_heartbeat(&meta, &body)?;
                self.protocol.make_heartbeat()
            }) {
            Err(err) => {
                stream
                    .write_error(Method::Heartbeat, MessageHeader::OPTION_ERROR, &err)
                    .await
            }
            Ok(ret) => {
                stream
                    .write_message(Method::Heartbeat, MessageHeader::OPTION_NONE, &ret)
                    .await?;
                let (_, ack) = stream.read_message::<AckMessage>().await?;
                if ack.code != CrabError::NO_ERROR {
                    Err(CrabError::ErrorCode(ack.code))
                } else {
                    Ok(())
                }
            }
        }
    }
    async fn heartbeat_as_client(
        &self,
        meta: &NodeMetadata,
        stream: &mut Stream,
    ) -> Result<(), CrabError> {
        match self.protocol.make_heartbeat() {
            Err(err) => {
                log::warn!("make heartbeat failed {},write ack with error", err);
                stream
                    .write_error(Method::Heartbeat, MessageHeader::OPTION_ERROR, &err)
                    .await?
            }
            Ok(ret) => {
                stream
                    .write_message(Method::Heartbeat, MessageHeader::OPTION_NONE, &ret)
                    .await?
            }
        }
        let handshake_ret = stream
            .read_message::<P::Heartbeat>()
            .await
            .and_then(|(_, body)| self.protocol.on_heartbeat(meta, &body));
        if let Err(err) = handshake_ret {
            stream
                .write_error(Method::Heartbeat, MessageHeader::OPTION_ERROR, &err)
                .await?;
        } else {
            stream
                .write_message(
                    Method::Heartbeat,
                    MessageHeader::OPTION_NONE,
                    &AckMessage {
                        code: CrabError::NO_ERROR,
                        msg: None,
                    },
                )
                .await?;
        }
        Ok(())
    }
}
