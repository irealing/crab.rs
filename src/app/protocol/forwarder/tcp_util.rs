use crab::CrabError;
use crab::proto::Stream;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

pub async fn tcp_forward(
    cancel: CancellationToken,
    stream: Stream,
    mut conn: TcpStream,
) -> Result<(), CrabError> {
    let (mut quic_writer, mut quic_reader) = stream.split();
    let (mut tcp_reader, mut tcp_writer) = conn.split();
    tokio::select! {
        _=cancel.cancelled()=>{
            log::error!("TCP Forwarder task cancelled.");
        }
        write_ret=tokio::io::copy(&mut quic_reader,&mut tcp_writer) => {
            match write_ret {
                Err(e) => {
                    log::error!("TCP Forwarder task write error. {}", e);
                }
                Ok(size)=>{
                    log::trace!("TCP Forwarder task write request size: {}", size);
                }
            }
        }
        read_ret=tokio::io::copy(&mut tcp_reader, &mut quic_writer) => {
            match read_ret {
                Err(e) => {
                    log::error!("TCP Forwarder task read error. {}", e);
                }
                Ok(size)=>{
                    log::trace!("TCP Forwarder task read request size: {}", size);
                }
            }
        }
    }
    Ok(())
}
