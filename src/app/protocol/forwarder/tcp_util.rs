use crab::CrabError;
use crab::proto::Stream;
use tokio::io::{AsyncWriteExt, copy};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
pub async fn tcp_forward(
    cancel: CancellationToken,
    stream: Stream,
    mut conn: TcpStream,
) -> Result<(), CrabError> {
    conn.set_nodelay(true)?;
    let (mut quic_writer, mut quic_reader) = stream.split();
    let (mut tcp_reader, mut tcp_writer) = conn.split();
    let forward_fut = async move {
        let s_to_t = async move {
            let ret = copy(&mut quic_reader, &mut tcp_writer).await;
            let _ = tcp_writer.shutdown().await;
            ret
        };
        let t_to_s = async move {
            let ret = copy(&mut tcp_reader, &mut quic_writer).await;
            let _ = quic_writer.shutdown().await;
            ret
        };
        tokio::join!(s_to_t, t_to_s)
    };
    tokio::select! {
        _=cancel.cancelled()=>{
            log::error!("TCP Forwarder task cancelled.");
        }
        (write_ret, read_ret) = forward_fut => {
            if let Err(e) = write_ret {
                log::error!("QUIC -> TCP write error: {}", e);
            }
            if let Err(e) = read_ret {
                log::error!("TCP -> QUIC read error: {}", e);
            }
        }
    }
    Ok(())
}
