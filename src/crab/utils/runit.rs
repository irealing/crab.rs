use super::super::errors::CrabError;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError>;
}

struct WorkerGroup {
    workers: Vec<Arc<dyn Worker>>,
}
impl WorkerGroup {
    fn new(workers: Vec<Arc<dyn Worker>>) -> Self {
        Self { workers }
    }
}
pub fn worker_group(workers: Vec<Arc<dyn Worker>>) -> impl Worker {
    WorkerGroup::new(workers)
}

#[async_trait::async_trait]
impl Worker for WorkerGroup {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        let mut join_set = JoinSet::new();
        for worker in &self.workers {
            let worker = worker.clone();
            let token = token.clone();
            join_set.spawn(async move { worker.serve(token).await });
        }
        let mut first_err = None;
        while let Some(res) = join_set.join_next().await {
            match res {
                Err(e) => {
                    log::warn!("Worker task failed: {}", e);
                    token.cancel();
                    first_err.get_or_insert(CrabError::ErrorCode(CrabError::ASYNC_RUNTIME_ERROR));
                }
                Ok(Err(e)) => {
                    token.cancel();
                    first_err.get_or_insert(e);
                }
                _ => {
                    continue;
                }
            }
        }
        if let Some(err) = first_err {
            Err(err)
        } else {
            Ok(())
        }
    }
}

pub struct WaitExitWorker;
impl WaitExitWorker {
    pub fn new() -> Self {
        Self
    }
}
#[async_trait::async_trait]
impl Worker for WaitExitWorker {
    async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
        wait_exit(token).await
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::select;
    use tokio_util::sync::CancellationToken;

    use crate::crab::CrabError;

    use super::{Worker, worker_group};
    struct LocalWorker(u64);
    #[async_trait::async_trait]
    impl Worker for LocalWorker {
        async fn serve(&self, token: CancellationToken) -> Result<(), CrabError> {
            eprintln!("{}:{} LocalWorker:run {}", file!(), line!(), self.0);
            if self.0 % 2 != 0 {
                tokio::time::sleep(Duration::from_secs(self.0)).await;
                return Err(CrabError::ErrorCode(CrabError::UNKNOWN_ERROR));
            }
            select! {
                _=tokio::time::sleep(Duration::from_secs(self.0))=>{
                    eprintln!("{}:{} LocalWorker({})::run completed",file!(),line!(),self.0);
                }
                _=token.cancelled()=>{
                    eprintln!("{}:{} LocalWorker({})::run cancelled",file!(),line!(),self.0);
                }
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn test_worker_error() {
        worker_group(vec![
            Arc::new(LocalWorker(3)),
            Arc::new(LocalWorker(10)),
            Arc::new(LocalWorker(20)),
        ])
        .serve(CancellationToken::new())
        .await
        .unwrap_err();
    }
}
#[cfg(windows)]
mod win;
#[cfg(windows)]
pub use win::wait_exit;
