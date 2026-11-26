use crate::utils::background_service_handle::{BackgroundServiceHandle, OnSendErrorResult};

#[derive(Debug)]
pub(crate) enum PeerHandleCommand {}

pub(crate) type PeerHandle = BackgroundServiceHandle<PeerHandleCommand, OnSendErrorResult>;
