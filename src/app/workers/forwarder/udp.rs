use crate::app::ServiceProvider;
use crate::app::protocol::SessionOption;
use crab::CrabError;
use crab::utils::runit::Worker;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug)]
pub struct UdpForwarderOption {
    /// 本地监听地址
    pub listen: SocketAddr,
    /// 目标代理节点
    pub target: String,
    /// UDP转发参数
    pub params: SessionOption,
}

pub struct UdpForwarderWorker {
    options: UdpForwarderOption,
    provider: ServiceProvider,
}
impl UdpForwarderWorker {
    pub fn new(options: UdpForwarderOption, provider: ServiceProvider) -> Self {
        Self { options, provider }
    }
}
#[async_trait::async_trait]
impl Worker for UdpForwarderWorker {
    async fn serve(&self, cancel: CancellationToken) -> Result<(), CrabError> {
        let _ = UdpSocket::bind(self.options.listen)
            .await
            .inspect_err(|err| log::error!("failed to bind udp address {}", err))?;
        loop {
            tokio::select! {
                _=cancel.cancelled()=>{
                   break;
                }
            }
        }
        Ok(())
    }
}
