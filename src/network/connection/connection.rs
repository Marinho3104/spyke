use std::net::SocketAddr;

use futures_util::StreamExt;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::network::{connection::{connection_handle::ConnectionCommand, connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}}, packet::{packet::{self, Packet, PacketError, PacketHeader}, protocol_reassembler::ProtocolReassembler}, protocols::{disconnect_protocol, protocol_dispatcher::ProtocolDispatcher, protocol_id::ProtocolId::DisconnectProtocol}};

pub(crate) type ConnectionId = u64;

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {

    #[error("Packet error: {0}")]
    Packet(#[from] PacketError),

    #[error("Not able to establish connection with given address: {addr} with error: {error_message}")]
    NotAbleToEstablishConnectionWithGivenAddress {
        addr: SocketAddr,
        error_message: String,
    },

    #[error("Connection was closed")]
    ConnectionWasClosed,

    #[error("Frame next call fail with error: {error_message}")]
    FrameNextCall {
        error_message: String
    },

}

pub(crate) struct ConnectionConfig {
    addr: SocketAddr,
    id: ConnectionId,
}

impl ConnectionConfig {

    pub(crate) fn new(addr: SocketAddr, id: ConnectionId) -> Self {
        Self {
            addr,
            id
        }
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
    framed_stream: Framed<TcpStream, LengthDelimitedCodec>,
    protocol_reassembler: ProtocolReassembler,
    connection_manager_handle: ConnectionManagerHandle,
}

impl Connection {

    pub(crate) fn new(config: ConnectionConfig, stream: TcpStream, connection_manager_handle: ConnectionManagerHandle) -> Self {
        Self {
            config,
            framed_stream: Framed::new(stream, Self::codec()),
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
        self.framed_stream.get_ref().local_addr()
    }

    pub(crate) fn peer_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.framed_stream.get_ref().peer_addr()
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                packet = Connection::read_packet(&mut self.framed_stream) => match packet {
                    Ok(packet) => Self::handle_packet(packet, &mut self.protocol_reassembler, &mut self.connection_manager_handle).await,
                    Err(..) => {
                        self.connection_manager_handle.remove_connection(self.config.id(), false).await
                            .expect("The connection manager should not be dropped if connection is still alive");
                        break;
                    }
                }
            }
        }
    }

    async fn read_packet(framed_stream: &mut Framed<TcpStream, LengthDelimitedCodec>) -> Result<Packet, ConnectionError> {
        let encoded_packet = framed_stream.next().await
            .ok_or(ConnectionError::ConnectionWasClosed)?
            .map_err(|err| ConnectionError::FrameNextCall { error_message: err.to_string() })?;
        let decoded_packet = Packet::decode(&encoded_packet)?;

        Ok(decoded_packet)
    }

    async fn handle_packet(packet: Packet, protocol_reassembler: &mut ProtocolReassembler, connection_manager_handle: &mut ConnectionManagerHandle) {
        // let protocol = protocol_reassembler.push
    }

    fn codec() -> LengthDelimitedCodec {
        LengthDelimitedCodec::builder()
            .length_field_type::<u16>()
            .little_endian()
            .max_frame_length(Packet::MAX_PAYLOAD_SIZE + PacketHeader::SERIALIZED_SIZE)
            .new_codec()
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

        let connection_config = ConnectionConfig::new(addr1, 0);
        let connection = Connection::from(connection_config, connection_manager_handle).await;
        assert!(matches!(connection, Ok(Connection { .. })));
    }

    #[tokio::test]
    async fn test_create_connection_error() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let (tx, _rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(addr, 0);

        let connection = Connection::from(connection_config, connection_manager_handle).await;
        assert!(matches!(connection, Err(..)));
    }
}
