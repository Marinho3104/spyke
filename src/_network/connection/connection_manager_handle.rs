use thiserror::Error;
use tokio::sync::mpsc;

use crate::{network::{connection::connection::ConnectionId, packet::packet::Packet, protocols::protocol_dispatcher::ProtocolDispatcher}, utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, OnSendErrorPanic}};

#[derive(Debug, Clone)]
pub(crate) enum ConnectionManagerCommand {
    Protocol {
        protocol: ProtocolDispatcher, 
        connection_id: ConnectionId
    },
    RemoveConnection {
        connection_id: ConnectionId,
    },
    SendPackets {
        connection_id: ConnectionId,
        packets: Vec<Packet>,
    },
    Authenticated {
        connection_id: ConnectionId
    }
}

pub(crate) type ConnectionManagerHandle = BackgroundServiceHandle<ConnectionManagerCommand, OnSendErrorPanic>;

impl BackgroundServiceHandle<ConnectionManagerCommand, OnSendErrorPanic> {

    pub(crate) async fn protocol(&mut self, protocol: ProtocolDispatcher, connection_id: ConnectionId) {
        let command = ConnectionManagerCommand::Protocol { protocol, connection_id };
        self.send(command).await;
    }

    pub(crate) async fn remove_connection(&mut self, connection_id: ConnectionId) {
        let command = ConnectionManagerCommand::RemoveConnection { connection_id };
        self.send(command).await;
    }

    pub(crate) async fn send_packets(&mut self, connection_id: ConnectionId, packets: Vec<Packet>) {
        let command = ConnectionManagerCommand::SendPackets { connection_id, packets };
        self.send(command).await;
    }

    pub(crate) async fn authenticated(&mut self, connection_id: ConnectionId) {
        let command = ConnectionManagerCommand::Authenticated { connection_id };
        self.send(command).await;
    }

}
