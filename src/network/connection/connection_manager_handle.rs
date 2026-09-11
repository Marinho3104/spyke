use thiserror::Error;
use tokio::sync::mpsc;

use crate::{network::{connection::connection::ConnectionId, protocols::protocol_dispatcher::ProtocolDispatcher}, utils::background_service_handle::BackgroundServiceHandler};

#[derive(Debug, Error)]
pub(crate) enum ConnectionManagerHandleError {

    #[error("Packet sequence index must be less than sequence total: {sequence_index} >= {sequence_total}")]
    SequenceIndexShouldBeLessThanSequenceTotal {
        sequence_index: u16,
        sequence_total: u16
    },
}


pub(crate) enum ConnectionManagerCommand {
    HandleProtocol {
        protocol: ProtocolDispatcher, 
        connection_id: ConnectionId
    },
}

pub(crate) type ConnectionManagerHandle = BackgroundServiceHandler<ConnectionManagerCommand>;

impl BackgroundServiceHandler<ConnectionManagerCommand> {

    pub(crate) async fn handle_protocol(protocol: ProtocolDispatcher, connection_id: ConnectionId) {

    }

}
