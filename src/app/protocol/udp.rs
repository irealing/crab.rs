use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use bytes::BytesMut;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct IPv4Forward {
    pub target_address: SocketAddrV4,
    pub via: Option<Ipv4Addr>,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct IPv6Forward {
    pub target_address: SocketAddrV6,
    pub via: Option<Ipv6Addr>,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum UdpForwardParams {
    IPv4(IPv4Forward),
    IPv6(IPv6Forward),
}
pub struct UdpForwardHandler {
    params: UdpForwardParams,
}
impl UdpForwardHandler {
    const MTU: usize = 1500;
    const BUF_SIZE: usize = Self::MTU + 2;
    const OVER_PACKET_TAG: u16 = 1 << 15;
    const PACKET_LEN_MASK: u16 = 0xffff ^ Self::OVER_PACKET_TAG;
    pub fn new(params: UdpForwardParams) -> Self {
        Self { params }
    }
    async fn prepare_socket(&self) -> Result<UdpSocket, CrabError> {
        let (local_addr, remote_addr) = match self.params {
            UdpForwardParams::IPv4(params) => {
                let local_addr = if let Some(addr) = params.via {
                    SocketAddr::V4(SocketAddrV4::new(addr, 0))
                } else {
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
                };
                (local_addr, SocketAddr::V4(params.target_address))
            }
            UdpForwardParams::IPv6(params) => {
                let local_addr = if let Some(addr) = params.via {
                    SocketAddr::V6(SocketAddrV6::new(addr, 0, 0, 0))
                } else {
                    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0))
                };
                (local_addr, SocketAddr::V6(params.target_address))
            }
        };
        let sock = UdpSocket::bind(local_addr).await?;
        sock.connect(remote_addr).await?;
        Ok(sock)
    }
    async fn read_packet(
        stream: &mut RecvStream,
        buf: &mut [u8],
    ) -> Result<(usize, bool), CrabError> {
        let flag = stream.read_u16().await?;
        let size = (flag & Self::PACKET_LEN_MASK) as usize;
        let over_tag = (flag & Self::OVER_PACKET_TAG) > 0;
        if size == 0 {
            return Ok((0, over_tag));
        }
        if buf.len() < size {
            return Err(CrabError::ErrorCode(CrabError::NO_ENOUGH_SPACE));
        }
        stream.read_exact(&mut buf[..size]).await?;
        Ok((size, over_tag))
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
                read_ret=Self::read_packet(&mut reader,&mut buf)=>{
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
impl CommandHandler for UdpForwardHandler {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        _: ServiceProvider,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let this = *self;
        let sock = match this.prepare_socket().await {
            Ok(sock) => {
                stream
                    .write_message(header.method, header.option, &AckMessage::success())
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
        Self::forward(cancel, stream, sock).await
    }
}
