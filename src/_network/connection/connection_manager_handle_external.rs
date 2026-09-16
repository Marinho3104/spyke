use thiserror::Error;
use tokio::sync::mpsc;

use crate::{network::{connection::connection::ConnectionId, packet::packet::Packet, protocols::protocol_dispatcher::ProtocolDispatcher}, utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, OnSendErrorPanic}};

#[derive(Debug, Clone)]
pub(crate) enum ConnectionManagerCommandExternal {
    Protocol {
        protocol: ProtocolDispatcher, 
        connection_id: ConnectionId
    },
}

pub(crate) type ConnectionManagerHandleExternal = BackgroundServiceHandle<ConnectionManagerCommandExternal, OnSendErrorPanic>;

impl BackgroundServiceHandle<ConnectionManagerCommandExternal, OnSendErrorPanic> {

    pub(crate) async fn protocol(&mut self, protocol: ProtocolDispatcher, connection_id: ConnectionId) {
        let command = ConnectionManagerCommandExternal::Protocol { protocol, connection_id };
        self.send(command).await;
    }
}
