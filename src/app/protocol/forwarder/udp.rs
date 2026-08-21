use super::types::Address;
use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use bytes::BytesMut;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use crab::utils::runit::InvokeWithCancel;
use quinn::{RecvStream, SendStream};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
pub const UDP_FORWARD_MTU: usize = 1500;
pub const UDP_FORWARD_MAX_PACKET_SIZE: usize = UDP_FORWARD_MTU + 2;
const OVER_PACKET_TAG: u16 = 1 << 15;
const PACKET_LEN_MASK: u16 = 0xffff ^ OVER_PACKET_TAG;
#[async_trait::async_trait]
pub trait UdpPacketReader {
    async fn read_packet(&mut self, buf: &mut [u8]) -> Result<(usize, bool), CrabError>;
}
#[async_trait::async_trait]
impl UdpPacketReader for RecvStream {
    async fn read_packet(&mut self, buf: &mut [u8]) -> Result<(usize, bool), CrabError> {
        let flag = self.read_u16().await?;
        let size = (flag & PACKET_LEN_MASK) as usize;
        let over_tag = (flag & OVER_PACKET_TAG) > 0;
        if size == 0 {
            return Ok((0, over_tag));
        }
        if buf.len() < size {
            return Err(CrabError::ErrorCode(CrabError::NO_ENOUGH_SPACE));
        }
        self.read_exact(&mut buf[..size]).await?;
        Ok((size, over_tag))
    }
}
#[async_trait::async_trait]
impl UdpPacketReader for Arc<UdpSocket> {
    async fn read_packet(&mut self, buf: &mut [u8]) -> Result<(usize, bool), CrabError> {
        match self.recv(buf).await {
            Ok(size) => Ok((size, true)),
            Err(err) => Err(CrabError::IO(err)),
        }
    }
}
pub struct UdpForwardHandler {
    params: Address,
}
impl UdpForwardHandler {
    const BUF_SIZE: usize = UDP_FORWARD_MAX_PACKET_SIZE;
    const OVER_PACKET_TAG: u16 = 1 << 15;
    const PACKET_LEN_MASK: u16 = 0xffff ^ Self::OVER_PACKET_TAG;
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
    async fn forward_send(
        cancel: CancellationToken,
        mut reader: RecvStream,
        sock: Arc<UdpSocket>,
    ) -> Result<usize, CrabError> {
        let mut ret = 0;
        let mut buf = BytesMut::with_capacity(Self::BUF_SIZE);
        buf.resize(Self::BUF_SIZE, 0);
        loop {
            tokio::select! {
                _=cancel.cancelled() => {return Ok(ret)},
                read_ret=reader.read_packet(&mut buf)=>{
                    let (size,over_tag) = read_ret?;
                    if size>0{
                        ret += size;
                        sock.send(&buf[..size]).await?;
                    }
                    if over_tag {
                        cancel.cancel();
                        return Ok(ret);
                    }
                }
            }
        }
    }
    async fn forward_recv(
        cancel: CancellationToken,
        mut writer: SendStream,
        sock: Arc<UdpSocket>,
    ) -> Result<usize, CrabError> {
        let mut ret = 0;
        let mut buf = BytesMut::with_capacity(Self::BUF_SIZE);
        buf.resize(Self::BUF_SIZE, 0);
        loop {
            tokio::select! {
                _=cancel.cancelled() => {
                    return Ok(ret);
                }
                recv_ret=sock.recv(&mut buf[2..]) => {
                    let size=recv_ret?;
                    buf[1]=(size & 0xff) as u8;
                    buf[0]=(size >> 8) as u8;
                    ret+=size;
                    writer.write_all(&buf[..2+size]).await?;
                }
            }
        }
    }
    async fn forward(
        cancel: CancellationToken,
        stream: Stream,
        sock: UdpSocket,
    ) -> Result<(), CrabError> {
        let (writer, reader) = stream.split();
        let sock_arc = Arc::new(sock);
        let forward_cancel = cancel.child_token();
        let handle_send = Self::forward_send(forward_cancel.clone(), reader, sock_arc.clone());
        let handle_recv = Self::forward_recv(forward_cancel.clone(), writer, sock_arc.clone());
        let (send_size, recv_size) = tokio::try_join!(handle_send, handle_recv)?;
        log::warn!(
            "UdpForwarder forwarded {}({}/{}) bytes ",
            send_size + recv_size,
            recv_size,
            send_size
        );
        Ok(())
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
    async fn send_to(&self, buf: &[u8], _: SocketAddr) -> Result<usize, CrabError> {
        let mut stream = self.lock().await;
        stream.write_u32(buf.len() as u32).await?;
        stream.write_all(buf).await?;
        Ok(buf.len())
    }
}
pub async fn copy_udp_packet<R, W>(
    cancel: CancellationToken,
    mut reader: R,
    writer: W,
    addr: SocketAddr,
) -> Result<(), CrabError>
where
    R: UdpPacketReader + Send,
    W: UdpPacketWriter + Send,
{
    let copy = async move || -> Result<(), CrabError> {
        let mut buf = BytesMut::with_capacity(UDP_FORWARD_MTU);
        buf.resize(UDP_FORWARD_MTU, 0);
        loop {
            let (len, over) = reader.read_packet(&mut buf).await?;
            if len > 0 {
                writer.send_to(&buf[..len], addr).await?;
            }
            if over {
                break;
            }
        }
        Ok(())
    };
    copy.invoke(cancel).await
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
        Self::forward(cancel, stream, sock).await
    }
}
