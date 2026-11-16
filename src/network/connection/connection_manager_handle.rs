use crate::{
    network::{
        connection::connection::ConnectionId,
        protocol::dispatcher::{self, Dispatcher},
    },
    utils::background_service_handle::{BackgroundServiceHandle, OnSendErrorPanic},
};

#[derive(Debug)]
pub(crate) enum ConnectionManagerCommand {
    Protocol {
        dispatcher: Dispatcher,
        connection_id: ConnectionId,
    },
}

pub(crate) type ConnectionManagerHandle =
    BackgroundServiceHandle<ConnectionManagerCommand, OnSendErrorPanic>;

impl BackgroundServiceHandle<ConnectionManagerCommand, OnSendErrorPanic> {
    pub(crate) async fn protocol(&mut self, dispatcher: Dispatcher, connection_id: ConnectionId) {
        let command = ConnectionManagerCommand::Protocol {
            dispatcher,
            connection_id,
        };
        self.send(command).await;
    }
}
