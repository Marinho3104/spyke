use crate::{network::{connection::connection::ConnectionId, protocol::dispatcher::Dispatcher}, utils::background_service_handle::{BackgroundServiceHandle, OnSendErrorPanic}};


#[derive(Debug)]
pub(crate) enum ConnectionManagerCommand {
    Protocol {
        dispatcher: Dispatcher,
        connection_id: ConnectionId
    }
}

pub(crate) type ConnectionManagerHandle = BackgroundServiceHandle<ConnectionManagerCommand, OnSendErrorPanic>;
