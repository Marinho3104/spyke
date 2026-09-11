use tokio::sync::mpsc;

use crate::{network::{connection::connection::ConnectionId, protocols::protocol_dispatcher::ProtocolDispatcher}, utils::background_service_handle::BackgroundServiceHandler};


pub(crate) enum ConnectionManagerCommand {
    HandleProtocol {
        protocol: ProtocolDispatcher, 
        connection_id: ConnectionId
    },
}

pub(crate) type ConnectionManagerHandle = BackgroundServiceHandler<ConnectionManagerCommand>;

impl BackgroundServiceHandler<ConnectionManagerCommand> {

    pub(crate) fn handle_protocol(protocol: ProtocolDispatcher, connection_id: ConnectionId) {

    }

}
