use std::marker::PhantomData;

use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub(crate) enum BackgroundServiceHandleError {
    #[error("Not able to send command all receivers were dropped!")]
    AllReceiversWereDrop,
}

#[derive(Clone)]
pub(crate) struct OnSendErrorPanic {}

#[derive(Clone)]
pub(crate) struct OnSendErrorResult {}

#[derive(Clone)]
pub(crate) struct OnSendErrorCallback {}

pub(crate) trait OnSendError {}
impl OnSendError for OnSendErrorPanic {}
impl OnSendError for OnSendErrorResult {}
impl OnSendError for OnSendErrorCallback {}

#[derive(Clone)]
pub(crate) struct BackgroundServiceHandle<C, O: OnSendError> {
    tx: mpsc::Sender<C>,
    on_error: PhantomData<O>
}

impl<C, O: OnSendError> BackgroundServiceHandle<C, O> {
    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self {
            tx,
            on_error: PhantomData
        }
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorPanic> {   
    pub(crate) async fn send(&mut self, command: C) {
        self.tx.send(command).await
            .expect("This send should not return error, never!");
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorResult> {
    pub(crate) async fn send(&mut self, command: C) -> Result<(), BackgroundServiceHandleError> {
        self.tx.send(command).await
            .map_err(|_| BackgroundServiceHandleError::AllReceiversWereDrop)
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorCallback> {
    pub(crate) async fn send<F: FnMut()>(&mut self, command: C, mut callback: F) {
        if self.tx.send(command).await.is_err() {
            callback();
        }
    }
}

impl<C, O: OnSendError> From<mpsc::Sender<C>> for BackgroundServiceHandle<C, O> {
    fn from(value: mpsc::Sender<C>) -> Self {
        Self::new(value)
    }
}

#[cfg(debug_assertions)]
mod tests {
    use tokio::sync::mpsc;

use crate::utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, OnSendErrorCallback, OnSendErrorPanic, OnSendErrorResult};

    #[tokio::test]
    async fn test_clone_handle_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let handle = BackgroundServiceHandle::<u32, OnSendErrorResult>::from(tx);
        let _handle_clone = handle.clone();
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_result_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorResult>::from(tx);

        let result = handle.send(1).await;
        assert!(matches!(result, Ok(())))
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_result_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorResult>::from(tx);
        drop(rx);

        let result = handle.send(1).await;
        assert!(matches!(result, Err(BackgroundServiceHandleError::AllReceiversWereDrop)))
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_panic_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorPanic>::new(tx);

        handle.send(1).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn test_send_command_on_send_error_panic_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorPanic>::new(tx);
        drop(rx);

        handle.send(1).await;
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_callback_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);
        let mut value = 0;

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorCallback>::from(tx);

        handle.send(1, || value += 1).await;
        assert_eq!(value, 0);
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_callback_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);
        let mut value = 0;

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorCallback>::from(tx);
        drop(rx);

        handle.send(1, || value += 1).await;
        assert_eq!(value, 1);
    }
}
