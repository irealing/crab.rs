mod app;
use std::{process::ExitCode, sync::Arc};

use tokio_util::sync::CancellationToken;

use app::ServiceProvider;
#[cfg(feature = "api")]
use app::workers::{BaseApiWorker, CtrlWorker};
use app::{config, protocol};
use crab::{
    CrabError, create_local_endpoint,
    utils::runit::{WaitExitWorker, Worker},
};

const DEFAULT_CONFIG_FILE: &str = "@config.toml";
#[tokio::main]
async fn main() -> ExitCode {
    logforth::starter_log::stderr().apply();
    let cfg = match DEFAULT_CONFIG_FILE.parse::<config::Config>() {
        Ok(c) => c,
        Err(err) => {
            log::error!("parse config file {} error {}", DEFAULT_CONFIG_FILE, err);
            return ExitCode::FAILURE;
        }
    };
    if let Err(err) = start(cfg).await {
        log::error!("exit with error {}", err);
        return ExitCode::FAILURE;
    } else {
        log::info!("exit without error");
    }
    ExitCode::SUCCESS
}
async fn start(cfg: config::Config) -> Result<(), CrabError> {
    let provider = ServiceProvider::new(&cfg.node_id, cfg.tls)?;
    let mut worker: Vec<Arc<dyn Worker>> = Vec::new();
    #[cfg(feature = "api")]
    {
        let api_worker = BaseApiWorker(
            cfg.endpoint.bind_address,
            vec![Arc::new(CtrlWorker::new(provider.clone()))],
        );
        worker.push(Arc::new(api_worker));
    }
    let proto = protocol::AppProtocol::new(provider.clone());
    let local_node = create_local_endpoint(provider.tls_provider(), cfg.endpoint, proto)?;
    worker.push(Arc::new(local_node));
    WaitExitWorker::new(Box::new(worker))
        .serve(CancellationToken::new())
        .await
}
