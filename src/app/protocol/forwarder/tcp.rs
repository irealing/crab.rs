use super::types::Address;
use crate::app::protocol::types::{Command, CommandHandler};
use crate::app::ServiceProvider;
use crab::proto::{AckMessage, MessageHeader, Stream};
use crab::{CrabError, Handle};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TcpForwardParams {
    /// 目标连接地址
    pub target_address: Address,
    /// TCP连接超时时间
    pub connect_timeout: u8,
    pub keepalive_timeout: u8,
    pub keepalive_interval: u8,
    pub keepalive_retries: u8,
}
impl From<&TcpForwardParams> for TcpKeepalive {
    fn from(value: &TcpForwardParams) -> Self {
        Self::new()
            .with_time(Duration::from_secs(value.keepalive_timeout as u64))
            .with_interval(Duration::from_secs(value.keepalive_interval as u64))
            .with_retries(value.keepalive_retries as u32)
    }
}
pub struct TcpForwardHandler {
    req: TcpForwardParams,
}
impl TcpForwardHandler {
    pub fn new(req: TcpForwardParams) -> Self {
        Self { req }
    }
    async fn connect(&self) -> Result<(TcpStream, SocketAddr), CrabError> {
        let socket = timeout(
            Duration::from_secs(self.req.connect_timeout as u64),
            TcpStream::connect(self.req.target_address.resolve().await?),
        )
        .await
        .map_err(|_| CrabError::ErrorCode(CrabError::TIMEOUT_ERROR))??;
        let keepalive = TcpKeepalive::from(&self.req);
        let socket_ref = SockRef::from(&socket);
        socket_ref.set_tcp_keepalive(&keepalive)?;
        let local_addr = socket.local_addr()?;
        Ok((socket, local_addr))
    }
}

#[cfg(any(feature = "tcp_forward", feature = "socks5"))]
#[async_trait::async_trait]
pub trait TcpForwarder {
    async fn tcp_forward(
        &self,
        _: CancellationToken,
        _: TcpForwardParams,
        _: TcpStream,
    ) -> Result<(), CrabError>;
}
#[cfg(any(feature = "tcp_forward", feature = "socks5"))]
#[async_trait::async_trait]
impl TcpForwarder for Handle {
    async fn tcp_forward(
        &self,
        _: CancellationToken,
        param: TcpForwardParams,
        conn: TcpStream,
    ) -> Result<(), CrabError> {
        self.exec(
            Command::TcpForward(param),
            async move |cancel: CancellationToken, mut stream: Stream| -> Result<(), CrabError> {
                let (_, addr) = stream.read_message::<SocketAddr>().await?;
                log::info!("tcp forward request via: {}", addr);
                tcp_forward(cancel, stream, conn).await
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl CommandHandler for TcpForwardHandler {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        _: ServiceProvider,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let sock = match self.connect().await {
            Ok((sock, addr)) => {
                stream
                    .write_message(header.method, header.option, &AckMessage::success())
                    .await?;
                stream
                    .write_message(header.method, header.option, &addr)
                    .await?;
                sock
            }
            Err(err) => {
                let e = err.into();
                stream.write_error(header.method, header.option, &e).await?;
                return Err(e);
            }
        };
        tcp_forward(cancel, stream, sock).await
    }
}
pub async fn tcp_forward(
    cancel: CancellationToken,
    stream: Stream,
    mut conn: TcpStream,
) -> Result<(), CrabError> {
    let (mut quic_writer, mut quic_reader) = stream.split();
    let (mut tcp_reader, mut tcp_writer) = conn.split();
    tokio::select! {
        _=cancel.cancelled()=>{
            log::error!("TCP Forwarder task cancelled.");
        }
        write_ret=tokio::io::copy(&mut quic_reader,&mut tcp_writer) => {
            match write_ret {
                Err(e) => {
                    log::error!("TCP Forwarder task write error. {}", e);
                }
                Ok(size)=>{
                    log::trace!("TCP Forwarder task write request size: {}", size);
                }
            }
        }
        read_ret=tokio::io::copy(&mut tcp_reader, &mut quic_writer) => {
            match read_ret {
                Err(e) => {
                    log::error!("TCP Forwarder task read error. {}", e);
                }
                Ok(size)=>{
                    log::trace!("TCP Forwarder task read request size: {}", size);
                }
            }
        }
    }
    Ok(())
}
