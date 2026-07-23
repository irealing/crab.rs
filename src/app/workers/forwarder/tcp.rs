use crate::app::ServiceProvider;
use crate::app::protocol::TcpForwardParams;
use crate::app::protocol::TcpForwarder;
use crab::CrabError;
use crab::utils::runit::{OnceRunnerWorker, Worker, serve_all_workers};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
#[derive(Serialize, Deserialize, Debug)]
pub struct TcpForwarderOption {
    pub listen: SocketAddr,
    pub target: String,
    pub params: TcpForwardParams,
}
pub struct TcpForwarderWorker {
    options: TcpForwarderOption,
    provider: ServiceProvider,
}
impl TcpForwarderWorker {
    pub fn new(options: TcpForwarderOption, provider: ServiceProvider) -> Self {
        Self { options, provider }
    }
}
#[async_trait::async_trait]
impl Worker for TcpForwarderWorker {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        let listener = TcpListener::bind(self.options.listen).await?;
        let keepalive = TcpKeepalive::from(&self.options.params);
        let (tx, rx) = mpsc::channel(10);
        let workers_cancel = token.clone();
        let workers_handle =
            tokio::spawn(async move { serve_all_workers(workers_cancel, rx).await });
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
                        Ok((stream, _)) => {
                            let Some((handle,_))= self.provider.manager().get(&self.options.target)else{
                                drop(stream);
                                continue
                            };
                            let socket_ref=SockRef::from(&stream);
                            if let Err(err)=socket_ref.set_tcp_keepalive(&keepalive){
                                log::warn!("tcp-forwarder set_tcp_keepalive error: {}", err);
                                continue;
                            }
                            let params=self.options.params;
                            let worker=OnceRunnerWorker::from(
                                async move |cancel:CancellationToken| {
                                handle.tcp_forward(cancel,params,stream).await
                            });
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
