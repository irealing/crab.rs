use crate::app::workers::ApiWorker;
use axum::{Router, routing};
use crab::CrabError;
use crab::utils::runit::Worker;
use tokio_util::sync::CancellationToken;

pub struct EndpointApiWorker;
impl EndpointApiWorker {
    pub fn new() -> Self {
        Self {}
    }
}
impl ApiWorker for EndpointApiWorker {
    fn routers(&self) -> Router {
        Router::new().route("/", routing::get(|| async { "Hello, Crab.rs!" }))
    }
    fn tag(&self) -> &str {
        "endpoint"
    }
}
#[async_trait::async_trait]
impl Worker for EndpointApiWorker {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        Ok(())
    }
}
