use super::types::{Address, PacketHeader};
use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use binrw::BinWrite;
use bytes::BytesMut;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use crab::utils::runit::InvokeWithCancel;
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
pub async fn copy_udp_packet<R, W>(
    cancel: CancellationToken,
    mut reader: R,
    writer: W,
) -> Result<(), CrabError>
where
    R: UdpPacketReader + Send,
    W: UdpPacketWriter + Send,
{
    let copy = async move || -> Result<(), CrabError> {
        let mut buf = BytesMut::with_capacity(UDP_FORWARD_MTU);
        buf.resize(UDP_FORWARD_MTU, 0);
        loop {
            let (address, data) = reader.read_packet(&mut buf).await?;
            let addr = match address.resolve().await {
                Ok(addr) => addr,
                Err(err) => {
                    log::warn!("failed to resolve address: {}", err);
                    continue;
                }
            };
            writer.send_to(data, addr).await?;
        }
    };
    copy.invoke(cancel).await
}

pub struct UdpForwardHandler {
    params: Address,
}
impl UdpForwardHandler {
    pub fn new(params: Address) -> Self {
        Self { params }
    }
    async fn prepare_socket(&self) -> Result<(UdpSocket, SocketAddr), CrabError> {
        let (local_addr, remote_addr) = match self.params.resolve().await? {
            SocketAddr::V4(params) => {
                let local_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
                (local_addr, SocketAddr::V4(params))
            }
            SocketAddr::V6(params) => {
                let local_addr = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0));
                (local_addr, SocketAddr::V6(params))
            }
        };
        let sock = UdpSocket::bind(local_addr).await?;
        sock.connect(remote_addr).await?;
        let local_addr = sock.local_addr()?;
        Ok((sock, local_addr))
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
                Arc::new(sock)
            }
            Err(err) => {
                stream
                    .write_error(header.method, header.option, &err)
                    .await?;
                return Err(err);
            }
        };
        stream.read_ack().await?;
        let (writer, reader) = stream.split();
        match tokio::try_join!(
            copy_udp_packet(cancel.clone(), reader, sock.clone()),
            copy_udp_packet(cancel.clone(), sock.clone(), Mutex::new(writer))
        ) {
            Ok(_) => Ok(()),
            Err(err) => {
                log::error!("udp forward handle error: {}", err);
                Err(err)
            }
        }
    }
}
