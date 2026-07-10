use axum::Router;
use crab::CrabError;
use crab::utils::runit::Worker;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub trait ApiWorker: Worker {
    fn routers(&self) -> Router;
    fn tag(&self) -> &str;
}
struct HttpWorker {
    app: Router,
    bind_address: SocketAddr,
}
#[async_trait::async_trait]
impl Worker for HttpWorker {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        let listener = TcpListener::bind(self.bind_address).await?;
        axum::serve(listener, self.app.clone())
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
        Ok(())
    }
}

pub struct BaseApiWorker(pub SocketAddr, pub Vec<Arc<dyn ApiWorker>>);
#[async_trait::async_trait]
impl Worker for BaseApiWorker {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        let mut router = Router::new();
        for worker in &self.1 {
            let child_router = worker.routers();
            router = router.nest(&format!("/{}", worker.tag()), child_router);
        }
        let mut workers = self
            .1
            .iter()
            .cloned()
            .map(|a| a.clone() as Arc<dyn Worker>)
            .collect::<Vec<Arc<dyn Worker>>>();
        let api_worker = HttpWorker {
            app: router,
            bind_address: self.0,
        };
        workers.push(Arc::new(api_worker));
        workers.serve(token).await
    }
}
