use std::net::SocketAddr;

use thiserror::Error;
use tokio::net::TcpStream;

use crate::network::packet::{packet::Packet, protocol_reassembler::ProtocolReassembler};


#[derive(Debug, Error)]
pub(crate) enum ConnectionError {

    #[error("Not able to establish connection with given address: {addr} with error: {error_message}")]
    NotAbleToEstablishConnectionWithGivenAddress {
        addr: SocketAddr,
        error_message: String,
    },

}


pub(crate) struct Connection {
    stream: TcpStream, 
    addr: SocketAddr,
    protocol_reassembler: ProtocolReassembler
}

impl Connection {

    pub(crate) fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            protocol_reassembler: ProtocolReassembler::new()
        }
    }

    pub(crate) async fn from(addr: SocketAddr) -> Result<Self, ConnectionError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| ConnectionError::NotAbleToEstablishConnectionWithGivenAddress { addr, error_message: err.to_string() })?;
        Ok(Self::new(stream, addr))
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                _packet = Packet::read_packet(&mut self.stream, None) => {}
            }
        }
    }

}
