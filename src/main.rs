mod app;
use std::{process::ExitCode, sync::Arc};

use tokio_util::sync::CancellationToken;

use app::{config, protocol};
use crab::utils::crypto::TLSProvider;
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
    let proto = protocol::AppProtocol::new(&cfg.node_id);
    let local_node = Arc::new(create_local_endpoint(
        TLSProvider::from_config(cfg.tls),
        cfg.endpoint,
        proto,
    )?);
    let workers: Vec<Arc<dyn Worker>> = vec![local_node as Arc<dyn Worker>];
    WaitExitWorker::new(workers)
        .serve(CancellationToken::new())
        .await
}
