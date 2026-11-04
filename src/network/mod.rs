mod actor;
mod api;
mod codec;
mod conn;
mod file_limit;

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::outbox::Outbox;

pub use api::{AllowedResponse, ConnId, NetworkCommand, NetworkEvent, OpenDownload, OpenUpload};

const COMMAND_QUEUE_CAPACITY: usize = 1024;
const EVENT_QUEUE_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct NetworkHandle {
    room: mpsc::Sender<NetworkCommand>,
    commands: Arc<Mutex<Outbox<NetworkCommand>>>,
}

impl NetworkHandle {
    fn new(commands: mpsc::Sender<NetworkCommand>) -> Self {
        Self {
            room: commands.clone(),
            commands: Arc::new(Mutex::new(Outbox::new(commands))),
        }
    }

    pub fn send(&self, command: NetworkCommand) {
        assert!(!self.room.is_closed(), "network actor terminated");
        let mut commands = self.commands.lock().unwrap();
        commands.flush();
        commands.send(command);
    }

    pub(crate) fn is_backlogged(&self) -> bool {
        !self.commands.lock().unwrap().is_empty()
    }

    pub(crate) async fn ready(&self) {
        let _ = self.room.reserve().await;
    }

    pub(crate) fn flush(&self) {
        self.commands.lock().unwrap().flush();
    }

    pub fn allow(&self, response: AllowedResponse) {
        self.send(NetworkCommand::Allow(response));
    }

    pub fn disallow(&self, response: AllowedResponse) {
        self.send(NetworkCommand::Disallow(response));
    }

    pub fn server(&self, request: crate::protocol::ServerRequest) {
        self.send(NetworkCommand::SendServerMessage(request));
    }

    pub fn peer(&self, username: impl Into<String>, message: crate::protocol::PeerMessage) {
        self.send(NetworkCommand::SendPeerMessage {
            username: username.into(),
            message,
        });
    }

    pub fn peer_frame(&self, username: impl Into<String>, bytes: Vec<u8>) {
        self.send(NetworkCommand::SendPeerFrame {
            username: username.into(),
            bytes,
        });
    }
}

#[cfg(test)]
pub(crate) fn test_channel() -> (NetworkHandle, mpsc::Receiver<NetworkCommand>) {
    let (commands, rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
    (NetworkHandle::new(commands), rx)
}

pub fn spawn() -> (NetworkHandle, mpsc::Receiver<NetworkEvent>) {
    let (commands_tx, commands_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
    let (events_tx, events_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let max_sockets = file_limit::raise_file_limit();
    tracing::debug!(max_sockets, "maximum concurrent connections");
    tokio::spawn(actor::Actor::new(commands_rx, events_tx, max_sockets).run());
    (NetworkHandle::new(commands_tx), events_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(token: u32) -> NetworkCommand {
        NetworkCommand::Allow(AllowedResponse::Search(token))
    }

    #[tokio::test]
    async fn commands_past_the_queue_wait_in_order() {
        let (net, mut rx) = test_channel();
        let total = COMMAND_QUEUE_CAPACITY as u32 + 500;
        for token in 0..total {
            net.send(allow(token));
        }
        assert!(net.is_backlogged());

        let mut seen = Vec::new();
        while seen.len() < total as usize {
            while let Ok(NetworkCommand::Allow(AllowedResponse::Search(token))) = rx.try_recv() {
                seen.push(token);
            }
            net.ready().await;
            net.flush();
        }
        assert_eq!(seen, (0..total).collect::<Vec<_>>());
        assert!(!net.is_backlogged());
    }
}
