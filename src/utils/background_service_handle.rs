use std::marker::PhantomData;

use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub(crate) enum BackgroundServiceHandleError<C> {
    #[error("mpsc Send error: {0}")]
    SendError(#[from] mpsc::error::SendError<C>),
}

#[derive(Clone)]
pub(crate) struct OnSendErrorPanic {}

#[derive(Clone)]
pub(crate) struct OnSendErrorResult {}

pub(crate) trait OnSendError {}
impl OnSendError for OnSendErrorPanic {}
impl OnSendError for OnSendErrorResult {}

#[derive(Debug, Clone)]
pub(crate) struct BackgroundServiceHandle<C, O: OnSendError> {
    tx: mpsc::Sender<C>,
    on_error: PhantomData<O>
}

impl<C, O: OnSendError> BackgroundServiceHandle<C, O> {
    pub(crate) fn new(channel_size: usize) -> (Self, mpsc::Receiver<C>) {
        let (tx, rx) = mpsc::channel::<C>(channel_size);
        (
            Self {
                tx,
                on_error: PhantomData
            },
            rx
        )
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorPanic> {   
    pub(crate) async fn send(&mut self, command: C) {
        self.tx.send(command).await
            .expect("This send should not return error, never!");
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorResult> {
    pub(crate) async fn send(&mut self, command: C) -> Result<(), BackgroundServiceHandleError<C>> {
        self.tx.send(command).await?;
        Ok(())
    }
}

impl<C, O: OnSendError> From<mpsc::Sender<C>> for BackgroundServiceHandle<C, O> {
    fn from(value: mpsc::Sender<C>) -> Self {
        Self {
            tx: value,
            on_error: PhantomData
        }
    }
}

#[cfg(debug_assertions)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_command_on_send_error_result() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorResult>::from(tx);

        let result = handle.send(1).await;
        assert!(result.is_ok());

        drop(rx);

        let result = handle.send(1).await;
        assert!(result.is_err())
    }

    #[tokio::test]
    #[should_panic]
    async fn test_send_command_on_send_error_panic_success() {
        let (mut handle, rx) = BackgroundServiceHandle::<u32, OnSendErrorPanic>::new(10);

        handle.send(1).await;

        drop(rx);
        handle.send(1).await;
    }
}
