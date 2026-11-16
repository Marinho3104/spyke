use crate::{
    network::protocol::dispatcher::Dispatcher,
    utils::background_service_handle::{
        BackgroundServiceHandle, BackgroundServiceHandleError, OnSendErrorResult,
    },
};

#[derive(Debug)]
pub(crate) enum ConnectionCommand {
    Protocol(Dispatcher),
}

pub(crate) type ConnectionHandle = BackgroundServiceHandle<ConnectionCommand, OnSendErrorResult>;

impl BackgroundServiceHandle<ConnectionCommand, OnSendErrorResult> {
    pub(crate) async fn protocol(
        &self,
        dispatcher: Dispatcher,
    ) -> Result<(), BackgroundServiceHandleError<ConnectionCommand>> {
        let command = ConnectionCommand::Protocol(dispatcher);
        self.send(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::{
        conversation_id::ConversationId, disconnect::Disconnect, dispatcher::Headers,
    };
    use tokio::sync::mpsc;

    fn get_handle_and_rx() -> (ConnectionHandle, mpsc::Receiver<ConnectionCommand>) {
        ConnectionHandle::new(16)
    }

    #[tokio::test]
    async fn send_commands() {
        let (handle, mut rx) = get_handle_and_rx();

        assert!(
            handle
                .protocol(Dispatcher::Disconnect {
                    protocol: Disconnect::new(),
                    headers: Headers::new(ConversationId::new(0))
                })
                .await
                .is_ok()
        );
        assert!(rx.recv().await.is_some())
    }
}
