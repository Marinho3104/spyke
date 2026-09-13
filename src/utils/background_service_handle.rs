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

pub(crate) trait CallBack {
    type Input<'a>;

    fn callback(input: Self::Input<'_>);
}

#[derive(Clone)]
pub(crate) struct OnSendErrorCallback<CB> 
    where CB: Clone + CallBack {
        _callback: PhantomData<CB>,
}

pub(crate) trait OnSendError {}
impl OnSendError for OnSendErrorPanic {}
impl OnSendError for OnSendErrorResult {}
impl<CB: Clone + CallBack> OnSendError for OnSendErrorCallback<CB> {}

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

impl<C, CB: CallBack + Clone> BackgroundServiceHandle<C, OnSendErrorCallback<CB>> {
    pub(crate) async fn send(&mut self, command: C, input: CB::Input<'_>) {
        if self.tx.send(command).await.is_err() {
            CB::callback(input);
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

use crate::utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, CallBack, OnSendErrorCallback, OnSendErrorPanic, OnSendErrorResult};

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

    #[derive(Clone)]
    struct TestCallBack {}

    impl CallBack for TestCallBack {
        type Input<'a> = &'a mut u32;

        fn callback(input: Self::Input<'_>) {
            *input += 1;
        }
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_callback_success() {
        let (tx, _rx) = mpsc::channel::<u32>(10);
        let mut value = 0;

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorCallback<TestCallBack>>::from(tx);

        handle.send(1, &mut value).await;
        assert_eq!(value, 0);
    }

    #[tokio::test]
    async fn test_send_command_on_send_error_callback_error() {
        let (tx, rx) = mpsc::channel::<u32>(10);
        let mut value = 0;

        let mut handle = BackgroundServiceHandle::<u32, OnSendErrorCallback<TestCallBack>>::from(tx);
        drop(rx);

        handle.send(1, &mut value).await;
        assert_eq!(value, 1);
    }
}
