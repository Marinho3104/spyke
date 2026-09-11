use std::net::SocketAddr;

use thiserror::Error;
use tokio::net::TcpStream;

use crate::network::{connection::connection_manager_handle::ConnectionManagerHandle, packet::{packet::{self, Packet, ReadPacketTimeouts}, protocol_reassembler::ProtocolReassembler}};

pub(crate) type ConnectionId = u64;

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {

    #[error("Not able to establish connection with given address: {addr} with error: {error_message}")]
    NotAbleToEstablishConnectionWithGivenAddress {
        addr: SocketAddr,
        error_message: String,
    },

}

pub(crate) struct ConnectionConfig {
    read_packet_timeout: Option<ReadPacketTimeouts>,
    addr: SocketAddr,
    id: ConnectionId,
}

impl ConnectionConfig {

    pub(crate) fn new(read_packet_timeout: Option<ReadPacketTimeouts>, addr: SocketAddr, id: ConnectionId) -> Self {
        Self {
            read_packet_timeout,
            addr,
            id
        }
    }

    pub(crate) fn read_packet_timeout(&self) -> &Option<ReadPacketTimeouts> {
        &self.read_packet_timeout
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
    
    pub(crate) fn id(&self) -> ConnectionId {
        self.id
    }
}

pub(crate) struct Connection {
    config: ConnectionConfig,
    stream: TcpStream, 
    protocol_reassembler: ProtocolReassembler,
    connection_manager_handle: ConnectionManagerHandle,
}

impl Connection {

    pub(crate) fn new(config: ConnectionConfig, stream: TcpStream, connection_manager_handle: ConnectionManagerHandle) -> Self {
        Self {
            config,
            stream,
            protocol_reassembler: ProtocolReassembler::new(),
            connection_manager_handle
        }
    }

    pub(crate) async fn from(config: ConnectionConfig, connection_manager_handle: ConnectionManagerHandle) -> Result<Self, ConnectionError> {
        let stream = TcpStream::connect(config.addr())
            .await
            .map_err(|err| ConnectionError::NotAbleToEstablishConnectionWithGivenAddress { addr: config.addr(), error_message: err.to_string() })?;
        Ok(Self::new(config, stream, connection_manager_handle))
    }

    pub(crate) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.stream.local_addr()
    }

    pub(crate) fn peer_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.stream.peer_addr()
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                _packet = Packet::read_packet(&mut self.stream, None) => {}
            }
        }
    }

}

#[cfg(debug_assertions)]
mod tests {
    use std::{net::{IpAddr, Ipv4Addr, SocketAddr}};

use tokio::sync::mpsc;

use crate::network::connection::{connection::{Connection, ConnectionConfig}, connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}, server::Server};

    #[tokio::test]
    async fn test_create_connection_success() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let mut server = Server::from(addr).await
            .expect("Expected Server Ok");
        let server_addr = server.local_addr()
            .expect("Expected server local addr Ok");

        let mut addr1 = addr.clone();
        addr1.set_port(server_addr.port());

        tokio::spawn(async move {
            let _ = server.accept_connection(None).await;
        });

        let (tx, _rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(None, addr1, 0);
        let connection = Connection::from(connection_config, connection_manager_handle).await;
        assert!(matches!(connection, Ok(Connection { .. })));
    }

    #[tokio::test]
    async fn test_create_connection_error() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let (tx, rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(None, addr, 0);

        let connection = Connection::from(connection_config, connection_manager_handle).await;
        assert!(matches!(connection, Err(..)));
    }
}
