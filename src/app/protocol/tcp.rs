use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use crab::CrabError;
use crab::proto::{MessageHeader, Stream};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize, Deserialize)]
pub struct TCPForwardRequest {
    pub target_address: SocketAddr,
    pub keepalive_timeout: u8,
    pub keepalive_interval: u8,
    pub keepalive_retries: u8,
}
pub struct TCPForwarder {
    req: TCPForwardRequest,
}
impl TCPForwarder {
    pub fn new(req: TCPForwardRequest) -> Self {
        Self { req }
    }
    async fn connect(&self) -> Result<TcpStream, CrabError> {
        let socket = TcpStream::connect(self.req.target_address).await?;
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(self.req.keepalive_timeout as u64))
            .with_interval(Duration::from_secs(self.req.keepalive_interval as u64))
            .with_retries(self.req.keepalive_retries as u32);
        let socket_ref = SockRef::from(&socket);
        socket_ref.set_tcp_keepalive(&keepalive)?;
        Ok(socket)
    }
}
#[async_trait::async_trait]
impl CommandHandler for TCPForwarder {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        _: ServiceProvider,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let mut sock = match self.connect().await {
            Ok(sock) => sock,
            Err(err) => {
                let e = err.into();
                stream.write_error(header.method, header.option, &e).await?;
                return Err(e);
            }
        };
        let (mut quic_writer, mut quic_reader) = stream.split();
        let (mut tcp_reader, mut tcp_writer) = sock.split();
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
}
