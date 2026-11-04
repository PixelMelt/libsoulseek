use std::net::SocketAddr;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};

use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::{ConnControl, ConnEvent, FRAME_QUEUE_CAPACITY, connect, write_all};
use crate::network::codec::{MAX_SERVER_MESSAGE_SIZE, read_payload};
use crate::protocol::{ProtocolError, ServerResponse};

const KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2);
const KEEPALIVE_RETRIES: u32 = 10;
const USER_TIMEOUT: Duration = Duration::from_secs(30);

fn enable_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let socket = SockRef::from(stream);
    socket.set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(KEEPALIVE_IDLE)
            .with_interval(KEEPALIVE_INTERVAL)
            .with_retries(KEEPALIVE_RETRIES),
    )?;
    socket.set_tcp_user_timeout(Some(USER_TIMEOUT))
}

pub async fn run_server_conn(
    address: SocketAddr,
    events: mpsc::Sender<ConnEvent>,
    mut control: mpsc::Receiver<ConnControl>,
) {
    let stream = match connect(address).await.and_then(|stream| {
        enable_keepalive(&stream).map_err(|error| format!("cannot enable keepalive: {error}"))?;
        Ok(stream)
    }) {
        Ok(stream) => stream,
        Err(error) => {
            let _ = events
                .send(ConnEvent::ServerClosed { error: Some(error) })
                .await;
            return;
        }
    };
    let (read_half, write_half) = stream.into_split();
    let mut writer = BufWriter::new(write_half);

    let (frames_tx, mut frames) = mpsc::channel(FRAME_QUEUE_CAPACITY);
    let reader_task = tokio::spawn(read_server_frames(BufReader::new(read_half), frames_tx));

    let mut pending = None;
    let error = loop {
        tokio::select! {
            frame = frames.recv(), if pending.is_none() => {
                match frame {
                    Some(Ok(message)) => pending = Some(ConnEvent::ServerMessage(message)),
                    Some(Err(error)) => break Some(error),
                    None => break Some("connection closed".into()),
                }
            }
            reserved = events.reserve(), if pending.is_some() => {
                match reserved {
                    Ok(permit) => permit.send(pending.take().expect("reserved without a pending event")),
                    Err(_) => break None,
                }
            }
            ctrl = control.recv() => {
                match ctrl {
                    Some(ConnControl::Send(bytes)) => {
                        if write_all(&mut writer, &bytes).await.is_err() {
                            break Some("write failed".into());
                        }
                    }
                    Some(ConnControl::Close) | None => break None,
                    Some(other) => unreachable!("invalid server control {other:?}"),
                }
            }
        }
    };
    reader_task.abort();
    if let Some(event) = pending {
        let _ = events.send(event).await;
    }
    let _ = events.send(ConnEvent::ServerClosed { error }).await;
}

async fn read_server_frames(
    mut reader: BufReader<OwnedReadHalf>,
    frames: mpsc::Sender<Result<ServerResponse, String>>,
) {
    let error = loop {
        let size = match reader.read_u32_le().await {
            Ok(size) => size as usize,
            Err(error) => break error.to_string(),
        };
        if !(4..=MAX_SERVER_MESSAGE_SIZE).contains(&size) {
            break format!("invalid server message size {size}");
        }
        let code = match reader.read_u32_le().await {
            Ok(code) => code,
            Err(error) => break error.to_string(),
        };
        let payload = match read_payload(&mut reader, size - 4).await {
            Ok(payload) => payload,
            Err(error) => break error.to_string(),
        };
        match ServerResponse::parse(code, &payload) {
            Ok(message) => {
                if frames.send(Ok(message)).await.is_err() {
                    return;
                }
            }
            Err(ProtocolError::UnknownMessageCode { .. }) => {
                debug!(code, "ignoring unsupported server message");
            }
            Err(error) => {
                warn!(code, %error, "ignoring malformed server message");
            }
        }
    };
    let _ = frames.send(Err(error)).await;
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    use super::*;

    fn ping() -> Vec<u8> {
        let mut frame = 4u32.to_le_bytes().to_vec();
        frame.extend(32u32.to_le_bytes());
        frame
    }

    #[tokio::test]
    async fn writes_continue_while_an_event_waits_for_room() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (events, mut received) = mpsc::channel(1);
        let (control, control_rx) = mpsc::channel(8);
        let conn = tokio::spawn(run_server_conn(address, events, control_rx));
        let (mut server, _) = listener.accept().await.unwrap();

        for _ in 0..3 {
            server.write_all(&ping()).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        control
            .send(ConnControl::Send(b"hello".to_vec()))
            .await
            .unwrap();

        let mut written = [0u8; 5];
        timeout(Duration::from_secs(5), server.read_exact(&mut written))
            .await
            .expect("the write waited behind the undelivered event")
            .unwrap();
        assert_eq!(&written, b"hello");

        for _ in 0..3 {
            assert!(matches!(
                received.recv().await,
                Some(ConnEvent::ServerMessage(ServerResponse::ServerPing))
            ));
        }
        control.send(ConnControl::Close).await.unwrap();
        assert!(matches!(
            received.recv().await,
            Some(ConnEvent::ServerClosed { error: None })
        ));
        conn.await.unwrap();
    }
}
