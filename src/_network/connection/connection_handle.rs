use std::collections::HashMap;

use thiserror::Error;

use crate::{network::{connection::{connection::ConnectionId, connection_manager::ConnectionEntry}, packet::packet::Packet}, utils::background_service_handle::{BackgroundServiceHandle, BackgroundServiceHandleError, OnSendErrorResult}};

#[derive(Debug, Error)]
pub(crate) enum ConnectionHandleError {
    #[error("Background Service Handle error: {0}")]
    BackgroundServerHandle(#[from] BackgroundServiceHandleError),
}

#[derive(Debug)]
pub(crate) enum ConnectionCommand {
    SendPacket(Vec<Packet>),
}

pub(crate) type ConnectionHandle = BackgroundServiceHandle<ConnectionCommand, OnSendErrorResult>;

impl BackgroundServiceHandle<ConnectionCommand, OnSendErrorResult> {

    pub(crate) async fn send_packets(&mut self, packets: Vec<Packet>) -> Result<(), ConnectionHandleError> {
        let command = ConnectionCommand::SendPacket(packets);
        self.send(command).await?;
        Ok(())
    }
}
