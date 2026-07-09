use tokio::signal::{
    ctrl_c,
    unix::{SignalKind, signal},
};
use tokio_util::sync::CancellationToken;

use crate::crab::CrabError;

pub async fn wait_exit(token: CancellationToken) -> Result<(), CrabError> {
    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_term = signal(SignalKind::terminate())?;
    let ctrl_c = ctrl_c();
    tokio::select! {
        _=sig_int.recv()=>{
            log::warn!("receive siganl iterrupt");
        },
        _=sig_term.recv()=>{
            log::warn!("receive signal terminate");
        },
        _=ctrl_c=>{
            log::warn!("receive ctrl+c");
        }
    }
    token.cancel();
    Ok(())
}
