use crate::crab::{CrabError, Node};
use bincode_next::config;
use bincode_next::serde::decode_from_slice;
use binrw::{BinRead, BinWrite, binrw};
use serde::{Serialize, de::DeserializeOwned};
use std::{io::Cursor, marker::PhantomData};
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;
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
pub struct MessageHeader {
    #[br(default)]
    pub version: u8,
    pub option: u8,
    pub method: Method,
    pub length: u32,
}
pub struct MessageCodec<T>(PhantomData<T>);
const HEADER_SIZE: usize = 12;

impl<T> Decoder for MessageCodec<T>
where
    T: DeserializeOwned,
{
    type Item = (MessageHeader, T);
    type Error = CrabError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = MessageHeader::read(&mut Cursor::new(&src[..HEADER_SIZE])).map_err(|e| {
            log::warn!("read message header error:{}", e);
            CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE)
        })?;
        if header.length == 0 {
            return Err(CrabError::ErrorCode(CrabError::NO_PAYLOAD));
        }
        let frame_size = header.length as usize + HEADER_SIZE;
        if frame_size > src.len() {
            return Ok(None);
        }
        let mut b = src.split_to(frame_size);
        b.advance(HEADER_SIZE);
        let (ret, read_bytes): (T, usize) = decode_from_slice(&b, config::standard())
            .map_err(|_| CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))?;
        src.advance(HEADER_SIZE + read_bytes);
        Ok(Some((header, ret)))
    }
}

pub struct HandshakeRet {
    pub node_id: String,
}

pub trait HandshakePacket: DeserializeOwned {
    fn node_id(&self) -> &str;
}
pub trait Protocol: Send + Sync {
    type Handshake: HandshakePacket + 'static;
    type Heartbeat: DeserializeOwned + Serialize + 'static;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError>;
    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError>;
    fn on_handshake(&self, packet: &Self::Handshake) -> Result<HandshakeRet, CrabError> {
        Ok(HandshakeRet {
            node_id: packet.node_id().to_string(),
        })
    }
    fn on_heartbeat(
        &self,
        _: &dyn Node,
        _: &Self::Heartbeat,
    ) -> Result<Self::Heartbeat, CrabError> {
        self.make_heartbeat()
    }
}
