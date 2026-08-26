use super::types::{Address, PacketHeader};
use super::udp_util::copy_udp_stream;
use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use binrw::BinWrite;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use quinn::{RecvStream, SendStream};
use std::io::Cursor;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
pub const UDP_FORWARD_MTU: usize = 1500;
const OVER_PACKET_TAG: u16 = 1 << 15;
const PACKET_LEN_MASK: u16 = 0xffff ^ OVER_PACKET_TAG;
#[async_trait::async_trait]
pub trait UdpPacketReader {
    async fn read_packet<'a>(
        &mut self,
        buf: &'a mut [u8],
    ) -> Result<(Address, &'a [u8]), CrabError>;
}
#[async_trait::async_trait]
impl UdpPacketReader for RecvStream {
    async fn read_packet<'a>(
        &mut self,
        buf: &'a mut [u8],
    ) -> Result<(Address, &'a [u8]), CrabError> {
        let flag = self.read_u16().await?;
        let size = (flag & PACKET_LEN_MASK) as usize;
        if size == 0 {
            log::error!("recv packet is empty");
            return Err(CrabError::ErrorCode(CrabError::BAD_MESSAGE_HEADER));
        }
        if buf.len() < size {
            return Err(CrabError::ErrorCode(CrabError::NO_ENOUGH_SPACE));
        }
        self.read_exact(&mut buf[..size]).await?;
        let (header, position) = PacketHeader::unpack(&buf[..size])?;
        Ok((header.try_into()?, &buf[position..]))
    }
}
#[async_trait::async_trait]
impl UdpPacketReader for Arc<UdpSocket> {
    async fn read_packet<'a>(
        &mut self,
        buf: &'a mut [u8],
    ) -> Result<(Address, &'a [u8]), CrabError> {
        {
            let (size, addr) = self.recv_from(buf).await?;
            Ok((addr.into(), buf[..size].as_ref()))
        }
    }
}
#[async_trait::async_trait]
pub trait UdpPacketWriter: Send {
    async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, CrabError>;
}
#[async_trait::async_trait]
impl UdpPacketWriter for Arc<UdpSocket> {
    async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, CrabError> {
        let size = UdpSocket::send_to(self, buf, addr).await?;
        Ok(size)
    }
}
#[async_trait::async_trait]
impl UdpPacketWriter for Mutex<SendStream> {
    async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, CrabError> {
        let header: PacketHeader = addr.into();
        let mut header_buf = [0u8; 24];
        let header_len = {
            let mut cursor = Cursor::new(&mut header_buf[2..]);
            header.write(&mut cursor)?;
            cursor.position() as usize
        };
        let total_len = buf.len() as u32 + header_len as u32;
        header_buf[0] = (total_len >> 8) as u8;
        header_buf[1] = total_len as u8;
        let mut stream = self.lock().await;
        stream.write_all(&header_buf[..2 + header_len]).await?;
        stream.write_all(buf).await?;
        Ok(buf.len())
    }
}

pub struct UdpForwardHandler {
    via: Option<SocketAddr>,
}
impl UdpForwardHandler {
    pub fn new(params: Option<SocketAddr>) -> Self {
        Self { via: params }
    }
    async fn prepare_socket(&self) -> Result<(UdpSocket, SocketAddr), CrabError> {
        let sock = if let Some(addr) = self.via {
            UdpSocket::bind(addr).await?
        } else {
            match UdpSocket::bind(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::UNSPECIFIED,
                0,
                0,
                0,
            )))
            .await
            {
                Ok(s) => s,
                Err(err) => {
                    log::error!("ipv6 bind error {}", err);
                    UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)))
                        .await?
                }
            }
        };
        let addr = sock.local_addr()?;
        Ok((sock, addr))
    }
}

#[async_trait::async_trait]
impl CommandHandler for UdpForwardHandler {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        _: ServiceProvider,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let this = *self;
        stream
            .write_message(header.method, header.option, &AckMessage::success())
            .await?;
        let sock = match this.prepare_socket().await {
            Ok((sock, local_addr)) => {
                log::debug!("UdpForwardHandler use local address: {}", local_addr);

                stream
                    .write_message(header.method, header.option, &local_addr)
                    .await?;
                sock
            }
            Err(err) => {
                stream
                    .write_error(header.method, header.option, &err)
                    .await?;
                return Err(err);
            }
        };
        stream.read_ack().await?;
        copy_udp_stream(cancel, stream, sock).await
    }
}
