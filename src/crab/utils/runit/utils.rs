use super::super::CrabError;
use super::Worker;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn serve_all_workers<W>(
    cancel: CancellationToken,
    mut workers: mpsc::Receiver<W>,
) -> Result<(), CrabError>
where
    W: Worker + 'static,
{
    let mut join_set: JoinSet<Result<(), CrabError>> = JoinSet::new();
    loop {
        tokio::select! {
            _=cancel.cancelled()=>{
                break;
            }
            worker_ret = workers.recv() => {
                match worker_ret {
                    Some(worker) => {
                        let worker_cancel=cancel.clone();
                        join_set.spawn(async move {
                            worker.serve(worker_cancel).await
                        });
                    }
                    None => {
                        break;
                    }
                }
            }
            Some(join_ret)=join_set.join_next(),if !join_set.is_empty()=>{
                match join_ret {
                        Ok(Err(err)) => {
                        log::warn!("worker exited with error: {}", err);
                    }
                    Err(err) => {
                        log::warn!("worker exited with join error: {}", err);
                    }
                    _ => {}
                }
            }
        }
    }
    drop(workers);
    while let Some(value) = join_set.join_next().await {
        match value {
            Ok(Err(err)) => {
                log::warn!("worker exited with error: {}", err);
            }
            Err(err) => {
                log::warn!("worker exited with join error: {}", err);
            }
            _ => {}
        }
    }
    Ok(())
}
