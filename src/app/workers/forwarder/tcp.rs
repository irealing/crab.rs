use crate::app::ServiceProvider;
use crate::app::protocol::TcpForwardParams;
use crate::app::protocol::TcpForwarder;
use crab::utils::runit::{Worker, serve_all_workers};
use crab::{CrabError, Handle};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
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
                            let w=TcpForwardSessionWorker{
                                session:Mutex::new(Some(TcpForwardSession{
                                    params:self.options.params,
                                    stream,
                                    handle
                                }))
                            };
                            if tx.send(w).await.is_err(){
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

struct TcpForwardSession {
    params: TcpForwardParams,
    stream: TcpStream,
    handle: Handle,
}
impl TcpForwardSession {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        self.handle
            .tcp_forward(token, self.params, self.stream)
            .await
    }
}
struct TcpForwardSessionWorker {
    session: Mutex<Option<TcpForwardSession>>,
}
#[async_trait::async_trait]
impl Worker for TcpForwardSessionWorker {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        let mut guard = self.session.lock().await;
        let inner = guard.take();
        drop(guard);
        match inner {
            Some(session) => session.serve(token).await,
            None => Ok(()),
        }
    }
}
