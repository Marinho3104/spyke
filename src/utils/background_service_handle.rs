use std::marker::PhantomData;

use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub(crate) enum BackgroundServiceHandleError {
    #[error("Not able to send command all receivers were dropped!")]
    AllReceiversWereDrop,
}

pub(crate) struct OnErrorPanic {}
pub(crate) struct OnErrorResult {}

pub(crate) trait OnError {}
impl OnError for OnErrorPanic {}
impl OnError for OnErrorResult {}

#[derive(Clone)]
pub(crate) struct BackgroundServiceHandle<C, O: OnError> {
    tx: mpsc::Sender<C>,
    _on_error: PhantomData<O>
}

impl<C, O: OnError> BackgroundServiceHandle<C, O> {

    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self {
            tx,
            _on_error: PhantomData
        }
    }

}

impl<C> BackgroundServiceHandle<C, OnErrorPanic> {   
    pub(crate) async fn send(&mut self, command: C) {
        self.tx.send(command).await
            .expect("This send should not return error, never!");
    }
}

impl<C> BackgroundServiceHandle<C, OnErrorResult> {
    pub(crate) async fn send(&mut self, command: C) -> Result<(), BackgroundServiceHandleError> {
        self.tx.send(command).await
            .map_err(|_| BackgroundServiceHandleError::AllReceiversWereDrop)
    }
}

impl<C, O: OnError> From<mpsc::Sender<C>> for BackgroundServiceHandle<C, O> {
    fn from(value: mpsc::Sender<C>) -> Self {
        Self::new(value)
    }
}

#[cfg(debug_assertions)]
mod tests {
    use tokio::sync::mpsc;

use crate::utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, OnErrorPanic, OnErrorResult};

    #[tokio::test]
    async fn test_send_command_on_error_result_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnErrorResult>::from(tx);

        let result = handle.send(1).await;
        assert!(matches!(result, Ok(())))
    }

    #[tokio::test]
    async fn test_send_command_on_error_result_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnErrorResult>::from(tx);
        drop(rx);

        let result = handle.send(1).await;
        assert!(matches!(result, Err(BackgroundServiceHandleError::AllReceiversWereDrop)))
    }

    #[tokio::test]
    async fn test_send_command_on_error_panic_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnErrorPanic>::new(tx);

        handle.send(1).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_send_command_on_error_panic_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnErrorPanic>::new(tx);
        drop(rx);

        handle.send(1).await;
    }
}
