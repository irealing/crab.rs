use super::tcp_util::tcp_forward;
use super::types::Address;
use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, Stream};
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
        socket.set_nodelay(true)?;
        let local_addr = socket.local_addr()?;
        Ok((socket, local_addr))
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
        stream
            .write_message(header.method, header.option, &AckMessage::success())
            .await?;
        let sock = match self.connect().await {
            Ok((sock, addr)) => {
                log::debug!("tcp forward via {}", addr);
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
        stream.read_ack().await?;
        tcp_forward(cancel, stream, sock)
            .await
            .inspect_err(|e| log::error!("tcp forward error: {}", e))
    }
}
