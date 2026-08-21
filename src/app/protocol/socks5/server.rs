use super::session::{Session, TcpSession, UdpSession};
use super::types::{AuthConfig, Config};
use crate::app::ServiceProvider;
use crab::utils::runit::OnceWorker;
use crab::{CrabError, Handle};
use socks5_server::auth::{NoAuth, Password};
use socks5_server::connection::state::NeedAuthenticate;
use socks5_server::proto::{Address, Reply};
use socks5_server::{Command, IncomingConnection, Server};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct Socks5Server {
    config: Config,
    provider: ServiceProvider,
}
impl Socks5Server {
    pub fn new(config: Config, provider: ServiceProvider) -> Self {
        Self { config, provider }
    }
}
impl Socks5Server {
    async fn start<T>(&self, server: Server<T>, cancel: CancellationToken) -> Result<(), CrabError>
    where
        T: 'static,
    {
        let (tx, rx) = mpsc::channel::<Handshake<T>>(10);
        let (sess_tx, sess_rx) = mpsc::channel(10);
        let handle = tokio::spawn(rx.serve(cancel.clone()));
        let sess_handle = tokio::spawn(sess_rx.serve(cancel.clone()));
        loop {
            tokio::select! {
                _=cancel.cancelled()=>break,
                accept_ret=server.accept() => {
                    match accept_ret {
                        Err(err) => {
                            log::error!("Socks5Server Accept error: {}", err);
                            break;
                        }
                        Ok((stream, addr)) => {
                            log::info!("Accept socks5 client ok result {:?}", addr);
                            let Some((handle,_))=self.provider.manager().get(&self.config.target)else{
                                log::warn!("Socks5Server Accept error target node {} not exists",&self.config.target);
                                continue;
                            };
                            if let Err(_)=tx.send(Handshake{handle,stream,sender:sess_tx.clone()}).await{
                                log::error!("Socks5Server send handshake task error");
                            }
                        }
                    }
                }
            }
        }
        let _ = tokio::try_join!(handle, sess_handle)?;
        Ok(())
    }
}
struct Handshake<T> {
    handle: Handle,
    stream: IncomingConnection<T, NeedAuthenticate>,
    sender: mpsc::Sender<Session>,
}
#[async_trait::async_trait]
impl<T> OnceWorker for Handshake<T> {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        let conn = tokio::select! {
            _=token.cancelled()=>{
                return Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            },
            handshake_ret=self.stream.authenticate()=>{
                match handshake_ret {
                    Err((err,_)) => {
                        log::error!("Socks5Server Handshake error: {}", err);
                        return Ok(());
                    }
                    Ok((stream, _)) => {
                        stream
                    }
                }
            }
        };
        let cmd = match conn.wait().await {
            Ok(cmd) => cmd,
            Err((err, _)) => {
                log::error!("receive socks5 command error {}", err);
                return Ok(());
            }
        };
        let sess = match cmd {
            Command::Associate(associate, address) => Session::Udp(UdpSession {
                associate,
                address,
            }),
            Command::Connect(conn, address) => Session::Tcp(TcpSession {
                handle: self.handle,
                conn,
                address,
            }),
            Command::Bind(bind, _) => {
                let _ = bind
                    .reply(Reply::CommandNotSupported, Address::unspecified())
                    .await;
                return Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR));
            }
        };
        self.sender
            .send(sess)
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))
    }
}
#[async_trait::async_trait]
impl OnceWorker for Socks5Server {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        let listener = TcpListener::bind(self.config.listen).await?;
        match self.config.auth {
            AuthConfig::NoAuth => {
                self.start(Server::new(listener, Arc::new(NoAuth)), token)
                    .await
            }
            AuthConfig::Password {
                ref username,
                ref password,
            } => {
                let password =
                    Password::new(username.clone().into_bytes(), password.clone().into_bytes());
                self.start(Server::new(listener, Arc::new(password)), token)
                    .await
            }
        }
    }
}
