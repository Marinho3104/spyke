use std::{net::SocketAddr};

use futures_util::StreamExt;
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::network::{connection::connection_handle::{ConnectionCommand, ConnectionHandle}, framing::{codec::Codec, protocol_reassembler::{Config as ProtocolReassemblerConfig, ProtocolReassembler}}};

#[derive(Debug)]
pub(crate) struct ConnectionId(u64);

#[derive(Debug)]
pub(crate) struct Config {
    id: ConnectionId,
}

#[derive(Debug)]
pub(crate) struct Connection {
    config: Config,
    stream: Framed<TcpStream, Codec>,
    protocol_reassembler: ProtocolReassembler,
    rx: mpsc::Receiver<ConnectionCommand>,
}

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {
    #[error("TcpStream error: {0}")]
    TcpStreamError(#[from] std::io::Error),
} 

impl ConnectionId {
    pub(crate) fn new(connection_id: u64) -> Self {
        Self(connection_id)
    }
}

impl Config {
    pub(crate) fn new(id: ConnectionId) -> Self {
        Self {
            id
        }
    }
}

impl Connection {
    pub(crate) fn new(stream: TcpStream, config: Config, connection_handle_size: usize, protocol_reassembler_config: ProtocolReassemblerConfig) -> (Self, ConnectionHandle) {
        let (handle, rx) = ConnectionHandle::new(connection_handle_size);
        (
            Self {
                config,
                stream: Framed::new(stream, Codec::new()),
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
            tokio::select! {
                frame = self.stream.next() => {},
                command = self.rx.recv() => {}
            }
        }
    }
}
