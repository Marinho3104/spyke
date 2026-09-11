use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub(crate) enum BackgroundServiceHandlerError {
    #[error("Not able to send command all receivers were dropped!")]
    AllReceiversWereDrop,
}

pub(crate) struct BackgroundServiceHandler<C> {
    tx: mpsc::Sender<C>,
}

impl<C> BackgroundServiceHandler<C> {

    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self {
            tx
        }
    }
    
    pub(crate) async fn send(&mut self, command: C) -> Result<(), BackgroundServiceHandlerError> {
        self.tx.send(command).await
            .map_err(|_| BackgroundServiceHandlerError::AllReceiversWereDrop)
    }

}

#[cfg(debug_assertions)]
mod tests {
    use tokio::sync::mpsc;

use crate::utils::background_service_handle::{BackgroundServiceHandler, BackgroundServiceHandlerError};

    #[tokio::test]
    async fn test_send_command_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let mut handler = BackgroundServiceHandler::new(tx);

        let result = handler.send(1).await;
        assert!(matches!(result, Ok(())))
    }

    #[tokio::test]
    async fn test_send_command_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handler = BackgroundServiceHandler::new(tx);
        drop(rx);

        let result = handler.send(1).await;
        assert!(matches!(result, Err(BackgroundServiceHandlerError::AllReceiversWereDrop)))
    }
}
