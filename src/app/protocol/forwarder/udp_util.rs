use super::udp::{UDP_FORWARD_MTU, UdpPacketReader, UdpPacketWriter};
use bytes::BytesMut;
use crab::CrabError;
use crab::proto::Stream;
use crab::utils::runit::InvokeWithCancel;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub async fn copy_udp_packet<R, W>(
    cancel: CancellationToken,
    mut reader: R,
    writer: W,
) -> Result<(), CrabError>
where
    R: UdpPacketReader + Send,
    W: UdpPacketWriter + Send,
{
    let copy = async move || -> Result<(), CrabError> {
        let mut buf = BytesMut::with_capacity(UDP_FORWARD_MTU);
        buf.resize(UDP_FORWARD_MTU, 0);
        loop {
            let (address, data) = reader.read_packet(&mut buf).await?;
            let addr = match address.resolve().await {
                Ok(addr) => addr,
                Err(err) => {
                    log::warn!("failed to resolve address: {}", err);
                    continue;
                }
            };
            writer.send_to(data, addr).await?;
        }
    };
    copy.invoke(cancel).await
}
pub async fn copy_udp_stream(
    cancel: CancellationToken,
    stream: Stream,
    sock: UdpSocket,
) -> Result<(), CrabError> {
    let (writer, reader) = stream.split();
    let sock = Arc::new(sock);
    match tokio::try_join!(
        copy_udp_packet(cancel.clone(), reader, sock.clone()),
        copy_udp_packet(cancel.clone(), sock.clone(), Mutex::new(writer))
    ) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("udp forward handle error: {}", err);
            Err(err)
        }
    }
}
