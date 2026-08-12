use crate::app::ServiceProvider;
use crate::app::protocol::types::{Command, CommandHandler};
use bytes::{Bytes, BytesMut};
use crab::proto::{Executor, MessageHeader, Stream};
use crab::{CrabError, Handle};
use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
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
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionOption {
    pub idle_timeout_sec: u8,
    pub params: UdpForwardParams,
}
pub const UDP_FORWARD_MTU: usize = 1500;
pub const UDP_FORWARD_MAX_PACKET_SIZE: usize = UDP_FORWARD_MTU + 2;
const OVER_PACKET_TAG: u16 = 1 << 15;
const PACKET_LEN_MASK: u16 = 0xffff ^ OVER_PACKET_TAG;
#[async_trait::async_trait]
trait PacketReader {
    async fn read_packet(&mut self, buf: &mut [u8]) -> Result<(usize, bool), CrabError>;
}
#[async_trait::async_trait]
impl PacketReader for RecvStream {
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
pub struct UdpForwardHandler {
    params: UdpForwardParams,
}
impl UdpForwardHandler {
    const MTU: usize = UDP_FORWARD_MTU;
    const BUF_SIZE: usize = UDP_FORWARD_MAX_PACKET_SIZE;
    const OVER_PACKET_TAG: u16 = 1 << 15;
    const PACKET_LEN_MASK: u16 = 0xffff ^ Self::OVER_PACKET_TAG;
    pub fn new(params: UdpForwardParams) -> Self {
        Self { params }
    }
    async fn prepare_socket(&self) -> Result<(UdpSocket, SocketAddr), CrabError> {
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
        Self::forward(cancel, stream, sock).await
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
pub struct UdpForwarderHandle {
    bytes_tx: mpsc::Sender<Bytes>,
    via: SocketAddr,
}
impl UdpForwarderHandle {
    pub async fn send(&self, data: Bytes) -> Result<(), CrabError> {
        self.bytes_tx
            .send(data)
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
        Ok(())
    }
    pub fn close(self) {
        drop(self.bytes_tx);
    }
    pub fn is_closed(&self) -> bool {
        self.bytes_tx.is_closed()
    }
    pub fn relay_address(&self) -> SocketAddr {
        self.via
    }
}
#[async_trait::async_trait]
pub trait UdpForwarder {
    async fn udp_forward<T>(
        &self,
        _: CancellationToken,
        _: SessionOption,
        _: SocketAddr,
        _: T,
    ) -> Result<UdpForwarderHandle, CrabError>
    where
        T: UdpPacketWriter + Send + Sync + 'static;
}
#[async_trait::async_trait]
impl UdpForwarder for Handle {
    async fn udp_forward<T>(
        &self,
        _: CancellationToken,
        opt: SessionOption,
        src: SocketAddr,
        udp_writer: T,
    ) -> Result<UdpForwarderHandle, CrabError>
    where
        T: UdpPacketWriter + Send + Sync + 'static,
    {
        let (handle_tx, handle_rx) = oneshot::channel::<UdpForwarderHandle>();
        self.spawn(
            Command::UdpForward(opt.params),
            async move |cancel: CancellationToken, stream: Stream| {
                let (bytes_tx, bytes_rx) = mpsc::channel(10);
                let session = UdpForwardSession {
                    udp_writer,
                    src,
                    bytes_rx,
                };
                let handle = UdpForwarderHandle { bytes_tx, via: src };
                handle_tx
                    .send(handle)
                    .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
                session.execute(cancel, stream).await
            },
        )
        .await?;
        Ok(handle_rx
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?)
    }
}
struct UdpForwardSession<T>
where
    T: UdpPacketWriter + Send + Sync + 'static,
{
    udp_writer: T,
    src: SocketAddr,
    bytes_rx: mpsc::Receiver<Bytes>,
}
impl<T> UdpForwardSession<T>
where
    T: UdpPacketWriter + Send + Sync + 'static,
{
    async fn stream_to_udp(
        cancel: CancellationToken,
        mut stream: RecvStream,
        udp_writer: T,
        addr: SocketAddr,
    ) -> Result<(), CrabError> {
        let mut buf = BytesMut::with_capacity(UDP_FORWARD_MTU);
        loop {
            tokio::select! {
                _=cancel.cancelled() => {
                    return Ok(());
                }
                recv_ret=stream.read_packet(&mut buf) => {
                    let (size,over)=recv_ret?;
                    udp_writer.send_to(&buf[..size],addr).await?;
                    if over {
                        return Ok(());
                    }
                }
            }
        }
    }
    async fn udp_to_stream(
        cancel: CancellationToken,
        mut stream: SendStream,
        mut bytes_rx: mpsc::Receiver<Bytes>,
    ) -> Result<(), CrabError> {
        loop {
            tokio::select! {
                _=cancel.cancelled() => {
                    return Ok(());
                }
                packet_ret=bytes_rx.recv() => {
                    match packet_ret {
                        None=>{return Ok(())}
                        Some(data) => {
                            stream.write_all(&data).await?;
                        }
                    }
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl<T> Executor for UdpForwardSession<T>
where
    T: UdpPacketWriter + Send + Sync + 'static,
{
    type Output = ();
    async fn execute(
        self,
        cancel: CancellationToken,
        stream: Stream,
    ) -> Result<Self::Output, CrabError> {
        let (bytes_rx, udp_writer, addr) = (self.bytes_rx, self.udp_writer, self.src);
        let (reader, writer) = stream.split();
        let _ = tokio::try_join!(
            Self::stream_to_udp(cancel.clone(), writer, udp_writer, addr),
            Self::udp_to_stream(cancel.clone(), reader, bytes_rx)
        )?;
        Ok(())
    }
}
