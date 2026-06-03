use std::{process::ExitCode, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::crab::{
    CrabError, create_local_node,
    utils::{
        crypto::TLSProvider,
        runit::{WaitExitWorker, Worker, worker_group},
    },
};
mod config;
mod crab;
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
    }
    ExitCode::SUCCESS
}
async fn start(cfg: config::Config) -> Result<(), CrabError> {
    let loca_node = Arc::new(create_local_node(
        TLSProvider::from_config(cfg.tls),
        cfg.node,
    )?);
    let workers: Vec<Arc<dyn Worker>> = vec![
        Arc::new(WaitExitWorker::new()),
        loca_node as Arc<dyn Worker>,
    ];
    worker_group(workers).serve(CancellationToken::new()).await
}
