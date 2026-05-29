use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::crab::utils::runit::{WaitExitWorker, Worker, worker_group};
mod crab;

#[tokio::main]
async fn main() {
    logforth::starter_log::stderr().apply();
    worker_group(vec![Arc::new(WaitExitWorker::new())])
        .serve(CancellationToken::new())
        .await
        .unwrap();
}
