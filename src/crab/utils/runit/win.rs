use tokio::{
    select,
    signal::windows::{ctrl_c, ctrl_close},
};
use tokio_util::sync::CancellationToken;

use crate::crab::CrabError;

pub async fn wait_exit(token: CancellationToken) -> Result<(), CrabError> {
    let mut ctrl_c = ctrl_c()?;
    let mut ctrl_close = ctrl_close()?;
    select! {
        _=ctrl_c.recv()=>{
            log::warn!("receive ctrl+c ");
        },
        _=ctrl_close.recv()=>{
            log::warn!("receive ctrl+c ");
        }
        _=token.cancelled()=>{
            log::warn!("cancellation token cancelled");
        }
    }
    token.cancel();
    Ok(())
}
