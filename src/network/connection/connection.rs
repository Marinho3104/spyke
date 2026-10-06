use std::{net::SocketAddr, time::Duration};

use futures_util::{StreamExt, TryStreamExt};
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc, time::{error::Elapsed, timeout}};
use tokio_util::codec::Framed;

use crate::network::{connection::{connection_handle::{ConnectionCommand, ConnectionHandle}, connection_manager_handle::ConnectionManagerHandle}, framing::{codec::{Codec, DecoderError}, frame::Frame, protocol_reassembler::{Config as ProtocolReassemblerConfig, ProtocolReassembler, ProtocolReassemblerError}}, protocol::dispatcher::Dispatcher};

#[derive(Debug)]
pub(crate) struct ConnectionId(u64);

#[derive(Debug)]
pub(crate) struct Config {
    id: ConnectionId,
    ping_protocol_timeout: Duration
}

#[derive(Debug)]
pub(crate) struct Connection {
    config: Config,
    framed: Framed<TcpStream, Codec>,
    protocol_reassembler: ProtocolReassembler,
    rx: mpsc::Receiver<ConnectionCommand>,
    connection_manager_handle: ConnectionManagerHandle, 
}

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    #[error("TcpStream error: {0}")]
    TcpStreamError(#[from] std::io::Error),

    #[error("Protocol reassembler error: {0}")]
    ProtocolReassemblerError(#[from] ProtocolReassemblerError),

    #[error("Socket was closed")]
    SocketClosed,

    #[error("Invalid frame received with error: {0}")]
    InvalidFrame(String),

    #[error("Manager closed communication channel")]
    ManagerChannel,
} 

impl ConnectionId {
    pub(crate) fn new(connection_id: u64) -> Self {
        Self(connection_id)
    }
}

impl Config {
    pub(crate) fn new(id: ConnectionId, ping_protocol_timeout: Duration) -> Self {
        Self {
            id,
            ping_protocol_timeout
        }
    }
}

impl Connection {
    pub(crate) fn new(
        stream: TcpStream, 
        connection_manager_handle: ConnectionManagerHandle, 
        config: Config, 
        connection_handle_size: usize, 
        protocol_reassembler_config: ProtocolReassemblerConfig
    ) -> (Self, ConnectionHandle) {
        let (handle, rx) = ConnectionHandle::new(connection_handle_size);
        (
            Self {
                config,
                framed: Framed::new(stream, Codec::new()),
                protocol_reassembler: ProtocolReassembler::new(protocol_reassembler_config),
                rx
            },
            handle
        )
    }

    pub(crate) async fn from_addr(
        addr: SocketAddr, 
        config: Config,
        connection_handle_size: usize, 
        protocol_reassembler_config: ProtocolReassemblerConfig
    ) -> Result<(Self, ConnectionHandle), ConnectionError> {
        Ok(Self::new(TcpStream::connect(addr).await?, config, connection_handle_size, protocol_reassembler_config))
    }

    pub(crate) async fn run(mut self) {
        loop {
            let result = tokio::select! {
                frame = timeout(self.config.ping_protocol_timeout, self.framed.try_next()) => self.handle_frame(frame),
                command = self.rx.recv() => self.handle_command(command),
            };

            if result.is_err() {
                break;
            }
        }
    }

    fn handle_frame(&mut self, frame: Result<Result<Option<Frame>, DecoderError>, Elapsed>) -> Result<(), ConnectionError> {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return self.send_ping_protocol(),
        };

        match frame {
            Ok(Some(frame)) => match self.protocol_reassembler.push(frame)? {
                Some(dispatcher) => self.handle_protocol(dispatcher),
                None => Ok(()),
            },

            Ok(None) => Err(ConnectionError::SocketClosed),
            Err(err) => Err(ConnectionError::InvalidFrame(err.to_string())),
        }
    }

    fn handle_command(&mut self, command: Option<ConnectionCommand>) -> Result<(), ConnectionError> {
        let command = match command {
            Some(command) => command,
            None => Err(ConnectionError::ManagerChannel)?,
        };

        match command {
            
        }
    }

    fn handle_protocol(&mut self, dispatcher: Dispatcher) -> Result<(), ConnectionError> {
        todo!()
    }

    fn send_ping_protocol(&mut self) -> Result<(), ConnectionError> {
        todo!()
    }
}
