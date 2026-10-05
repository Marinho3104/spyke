use std::{net::SocketAddr, sync::mpsc};

use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::network::{connection::connection_handle::ConnectionHandle, framing::{codec::Codec, protocol_reassembler::ProtocolReassembler}};


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
    protocol_reassembler: ProtocolReassembler
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
    pub(crate) fn new(stream: TcpStream, config: Config, connection_handle_size: usize) -> (Self, ConnectionHandle) {
        (
            Self {
                config,
                stream: Framed::new(stream, Codec::new())
            },
        )
    }
}
