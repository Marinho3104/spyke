use std::{net::SocketAddr};

use futures_util::{ StreamExt, SinkExt };
use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{diagnostics::logs::{log_error, log_info, log_success}, network::{connection::{connection_handle::{ConnectionCommand, ConnectionHandle}, connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}}, packet::{packet::{self, Packet, PacketError, PacketHeader}, protocol_reassembler::{self, ProtocolReassembler, ProtocolReassemblerError}}, protocols::{disconnect_protocol::{self, DisconnectProtocol}, protocol_dispatcher::ProtocolDispatcher}}};

pub(crate) type ConnectionId = u64;

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {

    #[error("Packet error: {0}")]
    Packet(#[from] PacketError),

    #[error("Protocol Reassembler error: {0}")]
    ProtocolReassembler(#[from] ProtocolReassemblerError),

    #[error("Not able to establish connection with given address: {addr} with error: {error_message}")]
    NotAbleToEstablishConnectionWithGivenAddress {
        addr: SocketAddr,
        error_message: String,
    },

    #[error("Socket was closed")]
    SocketClosed,

    #[error("Frame next call fail with error: {error_message}")]
    FrameNextCall {
        error_message: String
    },

    #[error("Send packet call failed with error: {error_message}")]
    SendPacket {
        error_message: String
    },

    // This is not an actual error, but is being implemented as so
    // to reuse alright existing logic to close the run loop correctly
    #[error("Connection is shutting down")]
    Shutdown,

}

pub(crate) struct ConnectionConfig {
    addr: SocketAddr,
    id: ConnectionId,
    communication_queue_size: usize
}

impl ConnectionConfig {

    pub(crate) fn new(addr: SocketAddr, id: ConnectionId, communication_queue_size: usize) -> Self {
        Self {
            addr,
            id,
            communication_queue_size
        }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
    
    pub(crate) fn id(&self) -> ConnectionId {
        self.id
    }
 
    pub(crate) fn communication_queue_size(&self) -> usize {
        self.communication_queue_size
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

    async fn shutdown(mut self, notify_connection_manager: bool) {
        if notify_connection_manager {
            self.connection_manager_handle.remove_connection(self.config.id(), false).await;
        }
    }

    pub(crate) fn spawn(self) -> ConnectionHandle {
        let (tx, rx) = mpsc::channel::<ConnectionCommand>(self.config.communication_queue_size());

        tokio::spawn(self.run(rx));
        
        ConnectionHandle::new(tx)
    }

    async fn run(mut self, mut communication_queue: mpsc::Receiver<ConnectionCommand>) {
        loop {
            let result: Result<(), ConnectionError> = tokio::select! {
                packet = Connection::read_packet(&mut self.framed_stream) => self.handle_packet(packet).await,
                command = communication_queue.recv() => self.handle_command(command).await,
            };

            if let Err(err) = result {
                log_error!("Connection run loop reached a error, exiting! Error: {err}");
                self.shutdown(!matches!(err, ConnectionError::Shutdown)).await;
                break;
            }
        }
    }

    async fn read_packet(framed_stream: &mut Framed<TcpStream, LengthDelimitedCodec>) -> Result<Packet, ConnectionError> {
        let encoded_packet = framed_stream.next().await
            .ok_or(ConnectionError::SocketClosed)?
            .map_err(|err| ConnectionError::FrameNextCall { error_message: err.to_string() })?;
        log_info!("Read packet: {:?}", encoded_packet);
        let decoded_packet = Packet::decode(&encoded_packet)?;

        log_success!("Decoded packet: {:?}", decoded_packet);
        Ok(decoded_packet)
    }

    async fn send_packets(&mut self, packets: &Vec<Packet>) -> Result<(), ConnectionError> {
        log_info!("Sending packet: {:?}!", packets);
        for packet in packets {
            let packet_encoded = packet.encode();
            self.framed_stream.send(packet_encoded.into()).await
                .map_err(|err| ConnectionError::SendPacket { error_message: err.to_string() })?;
        }
        
        log_success!("Packets were sent!");
        Ok(())
    }

    async fn handle_packet(
        &mut self,
        packet: Result<Packet, ConnectionError>, 
    ) -> Result<(), ConnectionError> {
        log_info!("Handling packet: {:?}", packet);
        let packet = packet?;

        log_info!("Pushing packet {:?} to reassembler!", packet);
        let protocol = self.protocol_reassembler.push(packet)?;
        log_info!("Reassembler returned {:?}", protocol.is_some());

        if let Some(protocol) = protocol {
            log_info!("Notifying connection manager about new protocol: {:?}", protocol);
            self.connection_manager_handle.protocol(protocol, self.config.id()).await;
        }

        log_success!("Packet handled!");
        Ok(())
    }

    async fn handle_command(&mut self, command: Option<ConnectionCommand>) -> Result<(), ConnectionError> {
        log_info!("Handling command: {:?}", command);
        let command = command
            .expect("Connection manager should never drop the channel of communication while connection is still alive!");

        match command {
            ConnectionCommand::SendPacket(packets) => self.send_packets(&packets).await?,
            ConnectionCommand::Shutdown(notify_peer) => { 
                if notify_peer {
                    // We are shutting down the connection, 
                    // doesn't really matter if this is sent or not
                    let _ = self.send_packets(&DisconnectProtocol.into()).await;
                }
                Err(ConnectionError::Shutdown)? 
            }
        }

        log_success!("Command handled!");
        Ok(())
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
    use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, time::Duration};
    use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::mpsc, time::sleep};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
    use crate::network::{connection::{connection::{Connection, ConnectionConfig}, connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}, server::Server}, packet::packet::{Packet, PacketHeader}, protocols::{disconnect_protocol::DisconnectProtocol, protocol_dispatcher::ProtocolDispatcher}};

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

        let connection_config = ConnectionConfig::new(addr1, 0, 32);
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

        let connection_config = ConnectionConfig::new(addr, 0, 32);

        let connection = Connection::from(connection_config, connection_manager_handle).await;
        assert!(matches!(connection, Err(..)));
    }

    #[tokio::test]
    async fn test_connection_local_addr_and_peer_addr_success() {
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

        let connection_config = ConnectionConfig::new(addr1, 0, 32);
        let connection = Connection::from(connection_config, connection_manager_handle).await
            .expect("Expected Connection Ok");
        let _local_addr = connection.local_addr();
        let _peer_addr = connection.peer_addr();
    }

    #[tokio::test]
    async fn test_read_packet_and_report_to_connection_manager_success() {
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

        let task = tokio::spawn(async move {
            let (stream, _) = server.accept_connection(None).await
                .expect("Expected accepted connection Ok");
            let codec = LengthDelimitedCodec::builder()
                .length_field_type::<u16>()
                .little_endian()
                .max_frame_length(Packet::MAX_PAYLOAD_SIZE + PacketHeader::SERIALIZED_SIZE)
                .new_codec();
            let mut framed_stream: Framed<TcpStream, LengthDelimitedCodec> = Framed::new(stream, codec);

            let disconnect_protocol = DisconnectProtocol;
            let packets: Vec<Packet> = disconnect_protocol.into();
            let packet = packets.first()
                .expect("Expected first packet to exists");

            let encoded_packet = packet.encode();
            let _ = framed_stream.send(encoded_packet.into()).await;
            std::future::pending::<()>().await;
        });

        let (tx, mut rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(addr1, 0, 32);
        let connection = Connection::from(connection_config, connection_manager_handle).await
            .expect("Expected Connection Ok");

        let _connection_handle = connection.spawn();

        let command = rx.recv().await
            .expect("Expected Ok Command");
        assert!(matches!(command, ConnectionManagerCommand::Protocol { 
            protocol: ProtocolDispatcher::DisconnectProtocol(..), 
            connection_id: 0 
        }));

        task.abort();
    }

    #[tokio::test]
    async fn test_command_send_packet_success() {
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

        let task = tokio::spawn(async move {
            let (stream, _) = server.accept_connection(None).await
                .expect("Expected accepted connection Ok");
            let codec = LengthDelimitedCodec::builder()
                .length_field_type::<u16>()
                .little_endian()
                .max_frame_length(Packet::MAX_PAYLOAD_SIZE + PacketHeader::SERIALIZED_SIZE)
                .new_codec();
            let mut framed_stream: Framed<TcpStream, LengthDelimitedCodec> = Framed::new(stream, codec);

            let encoded_packet = framed_stream.next().await
                .expect("Expected Ok Connection")
                .expect("Expected Ok next call");
            let decoded_packet = Packet::decode(&encoded_packet)
                .expect("Expected Decoded packet Ok");

            let packets = vec![decoded_packet];

            let protocol: ProtocolDispatcher = packets.try_into()
                .expect("Expected ok try into");

            assert!(matches!(protocol, ProtocolDispatcher::DisconnectProtocol(DisconnectProtocol { .. })))
        });

        let (tx, _rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(addr1, 0, 32);
        let connection = Connection::from(connection_config, connection_manager_handle).await
            .expect("Expected Connection Ok");

        let mut connection_handle = connection.spawn();

        connection_handle.send_packets(DisconnectProtocol.into()).await
            .expect("Expected Send packet command sent OK");

        task.await
            .expect("Expected task to not fail");
    }

    #[tokio::test]
    async fn test_command_shutdown_success() {
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

        let task = tokio::spawn(async move {
            let (stream, _) = server.accept_connection(None).await
                .expect("Expected accepted connection Ok");
            let codec = LengthDelimitedCodec::builder()
                .length_field_type::<u16>()
                .little_endian()
                .max_frame_length(Packet::MAX_PAYLOAD_SIZE + PacketHeader::SERIALIZED_SIZE)
                .new_codec();
            let mut framed_stream: Framed<TcpStream, LengthDelimitedCodec> = Framed::new(stream, codec);

            let encoded_packet = framed_stream.next().await
                .expect("Expected Ok Connection")
                .expect("Expected Ok next call");
            let decoded_packet = Packet::decode(&encoded_packet)
                .expect("Expected Decoded packet Ok");

            let packets = vec![decoded_packet];

            let protocol: ProtocolDispatcher = packets.try_into()
                .expect("Expected ok try into");

            assert!(matches!(protocol, ProtocolDispatcher::DisconnectProtocol(DisconnectProtocol { .. })))
        });

        let (tx, _rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(addr1, 0, 32);
        let connection = Connection::from(connection_config, connection_manager_handle).await
            .expect("Expected Connection Ok");

        let mut connection_handle = connection.spawn();

        connection_handle.shutdown(true).await
            .expect("Expected shutdown command sent OK");

        task.await
            .expect("Expected task to not fail");
    }

    #[tokio::test]
    async fn test_notify_connection_manager_when_shutdown_success() {
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

        let task = tokio::spawn(async move {
            let (stream, _) = server.accept_connection(None).await
                .expect("Expected accepted connection Ok");

            drop(stream);
            std::future::pending::<()>().await;
        });

        let (tx, mut rx) = mpsc::channel::<ConnectionManagerCommand>(32);
        let connection_manager_handle = ConnectionManagerHandle::new(tx);

        let connection_config = ConnectionConfig::new(addr1, 0, 32);
        let connection = Connection::from(connection_config, connection_manager_handle).await
            .expect("Expected Connection Ok");

        let mut _connection_handle = connection.spawn();

        let command = rx.recv().await
            .expect("Expected Command Ok");

        assert!(matches!(command, ConnectionManagerCommand::RemoveConnection { connection_id: 0, notify_connection: false }));
        task.abort();
    }
}
