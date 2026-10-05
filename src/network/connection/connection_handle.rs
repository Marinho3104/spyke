use crate::utils::background_service_handle::{BackgroundServiceHandle, OnSendErrorResult};


#[derive(Debug)]
pub(crate) enum ConnectionCommand {}

pub(crate) type ConnectionHandle = BackgroundServiceHandle<ConnectionCommand, OnSendErrorResult>;
