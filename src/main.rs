use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
mod crab;

use crab::{
    CrabError, create_local_node,
    utils::runit::{Worker, wait_exit, worker_group},
};

use crate::crab::Config as CrabConfig;
mod config;
struct WaitExitWorker;
impl WaitExitWorker {
    fn new() -> Self {
        Self {}
    }
}
#[async_trait]
impl Worker for WaitExitWorker {
    async fn run(&self, token: CancellationToken) -> Option<CrabError> {
        if let Err(e) = wait_exit(token).await {
            log::warn!("listen exit signal failed");
            Some(e)
        } else {
            None
        }
    }
}
const DEFAULT_CONFIG_FILE: &str = "@config.toml";
#[tokio::main]
async fn main() {
    logforth::starter_log::stderr().apply();

    let app_cfg = match DEFAULT_CONFIG_FILE.parse::<config::Config>() {
        Ok(cfg) => cfg,
        Err(e) => {
            log::error!("failed to parse config file, err:{}", e);
            return;
        }
    };
    let crab_cfg = CrabConfig {
        id: app_cfg.id,
        listen: app_cfg.listen,
    };
    let local_node = match create_local_node(crab_cfg, app_cfg.tls).await {
        Ok(local_node) => local_node,
        Err(e) => {
            log::error!("failed to create local node,err: {}", e);
            return;
        }
    };
    if let Some(err) = worker_group(vec![Arc::new(WaitExitWorker::new()), Arc::new(local_node)])
        .run(CancellationToken::new())
        .await
    {
        log::warn!("app exit with error {}", err);
    } else {
        log::warn!("app exit completed");
    }
}
