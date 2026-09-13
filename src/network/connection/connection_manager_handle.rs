use thiserror::Error;
use tokio::sync::mpsc;

use crate::{network::{connection::connection::ConnectionId, protocols::protocol_dispatcher::ProtocolDispatcher}, utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, OnErrorPanic}};

#[derive(Debug)]
pub(crate) enum ConnectionManagerCommand {
    Protocol {
        protocol: ProtocolDispatcher, 
        connection_id: ConnectionId
    },
    RemoveConnection {
        connection_id: ConnectionId,
        notify_connection: bool
    }
}

pub(crate) type ConnectionManagerHandle = BackgroundServiceHandle<ConnectionManagerCommand, OnErrorPanic>;

impl BackgroundServiceHandle<ConnectionManagerCommand, OnErrorPanic> {

    pub(crate) async fn protocol(&mut self, protocol: ProtocolDispatcher, connection_id: ConnectionId) {
        let command = ConnectionManagerCommand::Protocol { protocol, connection_id };
        self.send(command).await;
    }

    pub(crate) async fn remove_connection(&mut self, connection_id: ConnectionId, notify_connection: bool) {
        let command = ConnectionManagerCommand::RemoveConnection { connection_id, notify_connection };
        self.send(command).await;
    }

}
