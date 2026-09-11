use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub(crate) enum BackgroundServiceHandleError {
    #[error("Not able to send command all receivers were dropped!")]
    AllReceiversWereDrop,
}

pub(crate) struct BackgroundServiceHandle<C> {
    tx: mpsc::Sender<C>,
}

impl<C> BackgroundServiceHandle<C> {

    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self {
            tx
        }
    }
    
    pub(crate) async fn send(&mut self, command: C) -> Result<(), BackgroundServiceHandleError> {
        self.tx.send(command).await
            .map_err(|_| BackgroundServiceHandleError::AllReceiversWereDrop)
    }

}

#[cfg(debug_assertions)]
mod tests {
    use tokio::sync::mpsc;

use crate::utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError};

    #[tokio::test]
    async fn test_send_command_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::new(tx);

        let result = handle.send(1).await;
        assert!(matches!(result, Ok(())))
    }

    #[tokio::test]
    async fn test_send_command_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::new(tx);
        drop(rx);

        let result = handle.send(1).await;
        assert!(matches!(result, Err(BackgroundServiceHandleError::AllReceiversWereDrop)))
    }
}
