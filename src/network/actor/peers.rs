use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use tokio::sync::mpsc;
use tracing::warn;

use super::indirect::InitId;
use super::{Actor, CONN_CONTROL_QUEUE_CAPACITY};
use crate::network::ConnId;
use crate::network::NetworkEvent;
use crate::network::conn::{
    ConnControl, PeerTask, SharedTraffic, run_incoming_peer, run_outgoing_peer,
};
use crate::protocol::{PeerInitMessage, PeerMessage, ServerRequest};
use crate::types::{ConnectionType, TransferDirection, ip_matches};

#[derive(Clone)]
pub(super) struct PeerIdentity {
    pub(super) username: String,
    pub(super) conn_type: ConnectionType,
}

pub(super) struct Conn {
    pub(super) control: mpsc::Sender<ConnControl>,
    pub(super) identity: Option<PeerIdentity>,
    pub(super) init_id: Option<InitId>,
    pub(super) established: bool,
    pub(super) file_transfer: Option<(u32, TransferDirection)>,
    pub(super) pierce_token: Option<u32>,
    pub(super) ip: Option<Ipv4Addr>,
    traffic: SharedTraffic,
    sends_pushed: u64,
    pub(super) received_through: u64,
}

pub(super) struct Outgoing {
    pub(super) addr: SocketAddrV4,
    pub(super) username: String,
    pub(super) first_message: PeerInitMessage,
    pub(super) conn_type: ConnectionType,
    pub(super) init_id: Option<InitId>,
}

impl Conn {
    #[cfg(test)]
    pub(super) fn established(control: mpsc::Sender<ConnControl>, identity: PeerIdentity) -> Self {
        Self {
            control,
            identity: Some(identity),
            init_id: None,
            established: true,
            file_transfer: None,
            pierce_token: None,
            ip: None,
            traffic: SharedTraffic::default(),
            sends_pushed: 0,
            received_through: 0,
        }
    }

    pub(super) fn username(&self) -> &str {
        &self
            .identity
            .as_ref()
            .expect("connection used before it was identified")
            .username
    }

    fn is_quiescent(&self) -> bool {
        self.traffic.received() == self.received_through
            && self.traffic.sends_written() == self.sends_pushed
    }

    fn is_critical(&self, own_username: Option<&str>) -> bool {
        self.identity.as_ref().is_some_and(|identity| {
            identity.conn_type != ConnectionType::Peer
                || Some(identity.username.as_str()) == own_username
        })
    }
}

pub(super) struct Peers {
    conns: HashMap<ConnId, Conn>,
    next_conn_id: ConnId,
}

impl Peers {
    pub(super) fn new() -> Self {
        Self {
            conns: HashMap::new(),
            next_conn_id: 1,
        }
    }

    pub(super) fn add(&mut self, conn: Conn) -> ConnId {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        self.conns.insert(conn_id, conn);
        conn_id
    }

    pub(super) fn get(&self, conn_id: ConnId) -> Option<&Conn> {
        self.conns.get(&conn_id)
    }

    pub(super) fn get_mut(&mut self, conn_id: ConnId) -> Option<&mut Conn> {
        self.conns.get_mut(&conn_id)
    }

    pub(super) fn remove(&mut self, conn_id: ConnId) -> Option<Conn> {
        self.conns.remove(&conn_id)
    }

    pub(super) fn contains(&self, conn_id: ConnId) -> bool {
        self.conns.contains_key(&conn_id)
    }

    pub(super) fn matching_addresses(&self, patterns: &[String]) -> Vec<ConnId> {
        self.conns
            .iter()
            .filter(|(_, conn)| {
                conn.ip
                    .is_some_and(|ip| patterns.iter().any(|pattern| ip_matches(pattern, ip)))
            })
            .map(|(conn_id, _)| *conn_id)
            .collect()
    }

    pub(super) fn count(&self) -> usize {
        self.conns.len()
    }

    fn inactive(&self, own_username: Option<&str>) -> Vec<ConnId> {
        self.conns
            .iter()
            .filter(|(_, conn)| {
                conn.established && !conn.is_critical(own_username) && conn.is_quiescent()
            })
            .map(|(conn_id, _)| *conn_id)
            .collect()
    }

    pub(super) fn close_all(&mut self) {
        for conn in self.conns.values() {
            let _ = conn.control.try_send(ConnControl::Close);
        }
        self.conns.clear();
    }
}

impl Actor {
    pub(super) fn push_conn(&mut self, conn_id: ConnId, control: ConnControl) {
        let Some(conn) = self.peers.get_mut(conn_id) else {
            return;
        };
        let is_send = matches!(control, ConnControl::Send(_) | ConnControl::SendPeer(_));
        let (rejected, overflowed) = match conn.control.try_send(control) {
            Ok(()) => {
                if is_send {
                    conn.sends_pushed += 1;
                }
                return;
            }
            Err(mpsc::error::TrySendError::Full(rejected)) => (rejected, true),
            Err(mpsc::error::TrySendError::Closed(rejected)) => (rejected, false),
        };
        if let ConnControl::SendPeer(messages) = rejected {
            let username = conn.username().to_owned();
            self.emit_unsent(username, messages);
        }
        if overflowed {
            warn!(conn_id, "connection outbound queue overflowed, dropping");
            self.handle_conn_closed(conn_id, Some("outbound queue overflowed".into()));
        } else {
            self.detach_conn_routing(conn_id);
        }
    }

    fn detach_conn_routing(&mut self, conn_id: ConnId) {
        if let Some(init_id) = self
            .peers
            .get_mut(conn_id)
            .and_then(|conn| conn.init_id.take())
        {
            self.detach_init_conn(init_id, conn_id, None);
        }
    }

    pub(super) fn emit_unsent(&mut self, username: String, unsent: Vec<PeerMessage>) {
        self.emit(NetworkEvent::PeerConnectionError {
            username,
            unsent,
            is_offline: false,
        });
    }

    pub(super) fn close_conn(&mut self, conn_id: ConnId) {
        self.detach_conn_routing(conn_id);
        self.push_conn(conn_id, ConnControl::Close);
    }

    pub(super) fn discard_conn(&mut self, conn_id: ConnId) {
        self.close_conn(conn_id);
        self.handle_conn_closed(conn_id, None);
    }

    fn open_conn(
        &mut self,
        identity: Option<PeerIdentity>,
        init_id: Option<InitId>,
        pierce_token: Option<u32>,
        ip: Option<Ipv4Addr>,
        established: bool,
    ) -> PeerTask {
        let (control_tx, control) = mpsc::channel(CONN_CONTROL_QUEUE_CAPACITY);
        let traffic = SharedTraffic::default();
        let conn_id = self.peers.add(Conn {
            control: control_tx,
            identity,
            init_id,
            established,
            file_transfer: None,
            pierce_token,
            ip,
            traffic: traffic.clone(),
            sends_pushed: 0,
            received_through: 0,
        });
        PeerTask {
            conn_id,
            events: self.conn_events_tx.clone(),
            control,
            allowed: self.allowed.clone(),
            limits: self.limits.clone(),
            traffic,
        }
    }

    pub(super) fn handle_accepted(&mut self, stream: tokio::net::TcpStream, addr: SocketAddr) {
        let ip = match addr {
            SocketAddr::V4(addr) => Some(*addr.ip()),
            SocketAddr::V6(_) => None,
        };
        if ip.is_some_and(|ip| {
            self.banned_addresses
                .iter()
                .any(|pattern| ip_matches(pattern, ip))
        }) {
            return;
        }
        let task = self.open_conn(None, None, None, ip, true);
        super::spawn_conn_task("incoming peer", run_incoming_peer(task, stream, addr));
    }

    fn sockets(&self) -> usize {
        self.peers.count()
            + usize::from(self.server.listener.is_some())
            + usize::from(self.server.is_connected())
    }

    pub(super) fn report_connection_count(&mut self) {
        let count = self.peers.count();
        if count != self.reported_connections {
            self.reported_connections = count;
            self.emit(NetworkEvent::ConnectionCount(count));
        }
    }

    pub(super) fn has_free_socket(&self) -> bool {
        self.sockets() < self.max_sockets
    }

    pub(super) fn check_socket_limit(&mut self) {
        if !self.has_free_socket() {
            for conn_id in self.peers.inactive(self.server.username()) {
                self.close_conn(conn_id);
            }
        }
        self.connect_pending();
    }

    pub(super) fn connect_pending(&mut self) {
        for outgoing in std::mem::take(&mut self.pending_outgoing) {
            let wanted = outgoing.init_id.is_none_or(|init_id| {
                self.indirect
                    .get(init_id)
                    .is_some_and(|init| init.conn_id.is_none())
            });
            if wanted {
                self.connect_peer(outgoing);
            }
        }
    }

    pub(super) fn connect_peer(&mut self, outgoing: Outgoing) {
        if !self.has_free_socket() {
            self.pending_outgoing.push(outgoing);
            return;
        }
        let Outgoing {
            addr,
            username,
            first_message,
            conn_type,
            init_id,
        } = outgoing;
        let pierce_token = match first_message {
            PeerInitMessage::PierceFireWall { token } => Some(token),
            _ => None,
        };
        let identity = PeerIdentity {
            username: username.clone(),
            conn_type,
        };
        let task = self.open_conn(
            Some(identity),
            init_id,
            pierce_token,
            Some(*addr.ip()),
            false,
        );
        let conn_id = task.conn_id;
        super::spawn_conn_task(
            "outgoing peer",
            run_outgoing_peer(
                task,
                SocketAddr::V4(addr),
                username,
                first_message,
                conn_type,
            ),
        );
        if let Some(init_id) = init_id {
            self.indirect.set_conn(init_id, conn_id);
        }
    }

    pub(super) fn handle_established(&mut self, conn_id: ConnId) {
        let Some(conn) = self.peers.get_mut(conn_id) else {
            return;
        };
        conn.established = true;
        let PeerIdentity {
            username,
            conn_type,
        } = conn
            .identity
            .clone()
            .expect("outgoing connection established without identity");
        let ip = conn.ip;
        let init_id = conn.init_id;
        let pierced = conn.pierce_token.is_some();
        if let Some(init_id) = init_id
            && !self.indirect.mark_established(init_id, conn_id)
        {
            return;
        }
        self.emit(NetworkEvent::PeerConnected {
            username: username.clone(),
            conn_type,
            conn_id,
            ip,
        });
        if let Some(init_id) = init_id {
            self.flush_init_queue(init_id);
        }
        if pierced && conn_type == ConnectionType::Distributed {
            self.accept_child_peer(conn_id, &username);
        }
    }

    pub(super) fn handle_peer_message(
        &mut self,
        conn_id: ConnId,
        message: PeerMessage,
        received_through: u64,
    ) {
        let Some(conn) = self.peers.get_mut(conn_id) else {
            return;
        };
        conn.received_through = received_through;
        let username = conn.username().to_owned();
        let close_after = matches!(message, PeerMessage::FileSearchResponse { .. })
            && self.server.username() != Some(username.as_str())
            && conn.is_quiescent();
        self.emit(NetworkEvent::PeerMessage { username, message });
        if close_after {
            self.close_conn(conn_id);
        }
    }

    pub(super) fn handle_consumed(&mut self, conn_id: ConnId, received_through: u64) {
        if let Some(conn) = self.peers.get_mut(conn_id) {
            conn.received_through = received_through;
        }
    }

    pub(super) fn handle_conn_closed(&mut self, conn_id: ConnId, error: Option<String>) {
        let Some(conn) = self.peers.remove(conn_id) else {
            return;
        };

        if !conn.established
            && let (Some(token), Some(identity)) = (conn.pierce_token, &conn.identity)
        {
            self.send_to_server(ServerRequest::CantConnectToPeer {
                token,
                user: identity.username.clone(),
            });
        }

        if let Some(init_id) = conn.init_id {
            self.detach_init_conn(init_id, conn_id, error.as_deref());
        }

        let Some(PeerIdentity {
            username,
            conn_type,
        }) = conn.identity
        else {
            return;
        };
        match conn_type {
            ConnectionType::File => {
                if let Some((token, direction)) = conn.file_transfer {
                    self.emit(NetworkEvent::FileConnectionClosed {
                        username,
                        token,
                        conn_id,
                        direction,
                    });
                }
            }
            ConnectionType::Distributed => {
                self.handle_distributed_conn_closed(&username, conn_id);
            }
            ConnectionType::Peer => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::conn::ConnEvent;

    fn actor(max_sockets: usize) -> (Actor, mpsc::Receiver<NetworkEvent>) {
        let (_commands_tx, commands) = mpsc::channel(8);
        let (events_tx, events) = mpsc::channel(64);
        (Actor::new(commands, events_tx, max_sockets), events)
    }

    fn conn(
        actor: &mut Actor,
        username: &str,
        conn_type: ConnectionType,
    ) -> (ConnId, mpsc::Receiver<ConnControl>) {
        let (control, rx) = mpsc::channel(8);
        let conn_id = actor.peers.add(Conn::established(
            control,
            PeerIdentity {
                username: username.to_owned(),
                conn_type,
            },
        ));
        (conn_id, rx)
    }

    fn closed(rx: &mut mpsc::Receiver<ConnControl>) -> bool {
        std::iter::from_fn(|| rx.try_recv().ok())
            .any(|control| matches!(control, ConnControl::Close))
    }

    fn outgoing(init_id: Option<InitId>) -> Outgoing {
        Outgoing {
            addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9),
            username: "peer".into(),
            first_message: PeerInitMessage::PierceFireWall { token: 1 },
            conn_type: ConnectionType::File,
            init_id,
        }
    }

    #[test]
    fn at_the_socket_limit_idle_peer_connections_close() {
        let (mut actor, _events) = actor(3);
        let (_, mut idle) = conn(&mut actor, "idle", ConnectionType::Peer);
        let (_, mut file) = conn(&mut actor, "file", ConnectionType::File);
        let (busy_id, mut busy) = conn(&mut actor, "busy", ConnectionType::Peer);
        actor.peers.get_mut(busy_id).unwrap().sends_pushed = 1;

        actor.check_socket_limit();

        assert!(closed(&mut idle));
        assert!(!closed(&mut file));
        assert!(!closed(&mut busy));
    }

    #[test]
    fn the_connection_count_is_reported_only_when_it_changes() {
        let (mut actor, mut events) = actor(8);
        actor.report_connection_count();
        let (_conn, _rx) = conn(&mut actor, "peer", ConnectionType::Peer);
        actor.report_connection_count();
        actor.report_connection_count();
        actor.peers.close_all();
        actor.report_connection_count();

        let counts: Vec<usize> = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event {
                NetworkEvent::ConnectionCount(count) => Some(count),
                _ => None,
            })
            .collect();
        assert_eq!(counts, vec![1, 0]);
    }

    #[test]
    fn below_the_socket_limit_idle_connections_stay_open() {
        let (mut actor, _events) = actor(2);
        let (_, mut idle) = conn(&mut actor, "idle", ConnectionType::Peer);

        actor.check_socket_limit();

        assert!(!closed(&mut idle));
    }

    #[tokio::test]
    async fn connections_wait_for_a_free_socket() {
        let (mut actor, _events) = actor(1);
        let (busy_id, _busy) = conn(&mut actor, "busy", ConnectionType::File);

        actor.connect_peer(outgoing(None));
        actor.connect_peer(outgoing(Some(99)));
        assert_eq!(actor.pending_outgoing.len(), 2);
        assert_eq!(actor.peers.count(), 1);

        actor.handle_conn_event(ConnEvent::Closed {
            conn_id: busy_id,
            error: None,
        });

        assert!(actor.pending_outgoing.is_empty());
        assert_eq!(actor.peers.count(), 1);
        assert!(!actor.peers.contains(busy_id));
    }
}
