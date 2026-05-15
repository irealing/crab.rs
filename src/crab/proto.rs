use std::marker::PhantomData;

use crate::crab::CrabError;
use bincode_next::config;
use binrw::{BinRead, BinWrite, binrw};
use serde::de::DeserializeOwned;
use std::io::Cursor;
use tokio_util::{bytes::Buf, codec::Decoder};
#[derive(BinRead, BinWrite, Debug, PartialEq, Eq, Clone, Copy)]
#[brw(repr = u16)]
#[brw(little)]
pub enum Method {
    Handshake = 0,
    Heartbeat = 1,
    Ack = 2,
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
    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
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
        if header.length as usize + HEADER_SIZE > src.len() {
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let (ret, read_bytes): (T, usize) = bincode_next::serde::decode_from_slice(
            &src[..header.length as usize],
            config::standard(),
        )
        .map_err(|_| CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))?;
        src.advance(HEADER_SIZE + read_bytes);
        Ok(Some((header, ret)))
    }
}
