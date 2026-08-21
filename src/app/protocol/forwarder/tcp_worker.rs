use super::tcp::TcpForwardParams;
use crate::app::Manager;
use crate::app::protocol::forwarder::tcp_util::tcp_forward;
use crate::app::protocol::types::Command;
use crab::proto::Stream;
use crab::utils::runit::OnceWorker;
use crab::{CrabError, Handle};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "tcp_forward")]
#[async_trait::async_trait]
pub trait TcpForwarder {
    async fn tcp_forward(
        &self,
        _: CancellationToken,
        _: TcpForwardParams,
        _: TcpStream,
    ) -> Result<(), CrabError>;
}
#[cfg(feature = "tcp_forward")]
#[async_trait::async_trait]
impl TcpForwarder for Handle {
    async fn tcp_forward(
        &self,
        _: CancellationToken,
        param: TcpForwardParams,
        conn: TcpStream,
    ) -> Result<(), CrabError> {
        let (handle, addr) = self
            .exec_ack::<_, SocketAddr, _>(Command::TcpForward(param))
            .await?;
        log::debug!("tcp forward tcp addr {}", addr);
        let executor = async move |cancel: CancellationToken, stream: Stream| {
            tcp_forward(cancel, stream, conn).await
        };
        handle
            .send(Ok(executor))
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct TcpForwardOption {
    /// 本地监听地址
    pub listen: SocketAddr,
    /// 目标代理节点
    pub target: String,
    /// TCP转发参数
    pub params: TcpForwardParams,
}
pub struct TcpForwarderWorker {
    options: TcpForwardOption,
    manager: Manager,
}
impl TcpForwarderWorker {
    pub fn new(options: TcpForwardOption, manager: Manager) -> Self {
        Self { options, manager }
    }
}
#[async_trait::async_trait]
impl OnceWorker for TcpForwarderWorker {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        let listener = TcpListener::bind(self.options.listen).await?;
        let keepalive = TcpKeepalive::from(&self.options.params);
        let (tx, rx) = mpsc::channel(10);
        let workers_handle = tokio::spawn(rx.serve(token.clone()));
        loop {
            tokio::select! {
                _=token.cancelled() => {
                    break;
                }
                accept_ret = listener.accept() => {
                    match accept_ret{
                        Err(err)=>{
                            log::warn!("tcp-forwarder accept error: {}", err);
                        }
                        Ok((conn, _)) => {
                            let Some((handle,_))= self.manager.get(&self.options.target)else{
                                drop(conn);
                                continue
                            };
                            let socket_ref=SockRef::from(&conn);
                            if let Err(err)=socket_ref.set_tcp_keepalive(&keepalive){
                                log::warn!("tcp-forwarder set_tcp_keepalive error: {}", err);
                                continue;
                            }
                            let params=self.options.params.clone();
                            let worker=
                                async move |cancel:CancellationToken| {
                                handle.tcp_forward(cancel,params,conn).await?;
                                    Ok(())
                            };
                            if tx.send(worker).await.is_err(){
                                break;
                            }
                        }
                    }
                }
            }
        }
        workers_handle.await?
    }
}
