use std::{fmt, marker::PhantomData};

use thiserror::Error;
use tokio::sync::mpsc;

// Debug is implemented by hand, since a derive would require C: Debug
#[derive(Error)]
pub(crate) enum BackgroundServiceHandleError<C> {
    #[error("mpsc Send error: {0}")]
    SendError(#[from] mpsc::error::SendError<C>),
}

impl<C> fmt::Debug for BackgroundServiceHandleError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SendError(error) => f.debug_tuple("SendError").field(error).finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OnSendErrorPanic {}

#[derive(Debug, Clone)]
pub(crate) struct OnSendErrorResult {}

pub(crate) trait OnSendError {}
impl OnSendError for OnSendErrorPanic {}
impl OnSendError for OnSendErrorResult {}

// Clone and Debug are implemented by hand, since a derive would require C and O to implement
// them, while mpsc::Sender<C> implements both for any C
pub(crate) struct BackgroundServiceHandle<C, O: OnSendError> {
    tx: mpsc::Sender<C>,
    on_error: PhantomData<O>,
}

impl<C, O: OnSendError> Clone for BackgroundServiceHandle<C, O> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            on_error: PhantomData,
        }
    }
}

impl<C, O: OnSendError> fmt::Debug for BackgroundServiceHandle<C, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackgroundServiceHandle")
            .field("tx", &self.tx)
            .finish_non_exhaustive()
    }
}

impl<C, O: OnSendError> BackgroundServiceHandle<C, O> {
    pub(crate) fn new(channel_size: usize) -> (Self, mpsc::Receiver<C>) {
        let (tx, rx) = mpsc::channel::<C>(channel_size);
        (
            Self {
                tx,
                on_error: PhantomData,
            },
            rx,
        )
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorPanic> {
    pub(crate) async fn send(&self, command: C) {
        self.tx
            .send(command)
            .await
            .expect("background service receiver was dropped");
    }
}

impl<C> BackgroundServiceHandle<C, OnSendErrorResult> {
    pub(crate) async fn send(&self, command: C) -> Result<(), BackgroundServiceHandleError<C>> {
        self.tx.send(command).await?;
        Ok(())
    }
}

impl<C, O: OnSendError> From<mpsc::Sender<C>> for BackgroundServiceHandle<C, O> {
    fn from(value: mpsc::Sender<C>) -> Self {
        Self {
            tx: value,
            on_error: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_command_on_send_error_result() {
        let (tx, rx) = mpsc::channel::<u32>(10);

        let handle = BackgroundServiceHandle::<u32, OnSendErrorResult>::from(tx);

        let result = handle.send(1).await;
        assert!(result.is_ok());

        drop(rx);

        let result = handle.send(1).await;
        assert!(result.is_err())
    }

    // Neither Clone nor Debug, like a request carrying a reply channel
    struct Request {
        _reply: tokio::sync::oneshot::Sender<()>,
    }

    #[tokio::test]
    async fn handle_with_non_clone_non_debug_command() {
        let (handle, rx) = BackgroundServiceHandle::<Request, OnSendErrorResult>::new(10);

        // Only compiles if Clone and Debug don't require them from the command
        let cloned = handle.clone();
        assert!(format!("{cloned:?}").starts_with("BackgroundServiceHandle"));

        drop(rx);
        let (reply, _) = tokio::sync::oneshot::channel();
        let error = cloned.send(Request { _reply: reply }).await.unwrap_err();
        assert!(format!("{error:?}").starts_with("SendError"));
    }

    #[tokio::test]
    #[should_panic]
    async fn test_send_command_on_send_error_panic_success() {
        let (handle, rx) = BackgroundServiceHandle::<u32, OnSendErrorPanic>::new(10);

        handle.send(1).await;

        drop(rx);
        handle.send(1).await;
    }
}
