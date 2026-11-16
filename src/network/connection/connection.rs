use std::{net::SocketAddr, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt, TryStreamExt};
use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{error::Elapsed, timeout},
};
use tokio_util::codec::Framed;

use crate::network::{
    connection::{
        connection_authenticator::ConnectionAuthenticator,
        connection_handle::{ConnectionCommand, ConnectionHandle},
        connection_manager_handle::ConnectionManagerHandle,
    },
    framing::{
        codec::{Codec, DecoderError, EncoderError},
        frame::Frame,
        protocol_reassembler::{
            Config as ProtocolReassemblerConfig, ProtocolReassembler, ProtocolReassemblerError,
        },
    },
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionId(u64);

#[derive(Debug)]
pub(crate) struct Config {
    id: ConnectionId,
    ping_protocol_timeout: Duration,
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

    #[error("Decoder error: {0}")]
    Decoder(#[from] DecoderError),

    #[error("Encoder error: {0}")]
    Encoder(#[from] EncoderError),

    #[error("Socket was closed")]
    SocketClosed,

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
            ping_protocol_timeout,
        }
    }
}

impl Connection {
    pub(crate) fn new(
        stream: TcpStream,
        connection_manager_handle: ConnectionManagerHandle,
        config: Config,
        connection_handle_size: usize,
        protocol_reassembler_config: ProtocolReassemblerConfig,
    ) -> (Self, ConnectionHandle) {
        let (handle, rx) = ConnectionHandle::new(connection_handle_size);
        (
            Self {
                config,
                framed: Framed::new(stream, Codec::new()),
                protocol_reassembler: ProtocolReassembler::new(protocol_reassembler_config),
                rx,
                connection_manager_handle,
            },
            handle,
        )
    }

    pub(crate) async fn from_addr(
        addr: SocketAddr,
        connection_manager_handle: ConnectionManagerHandle,
        config: Config,
        connection_handle_size: usize,
        protocol_reassembler_config: ProtocolReassemblerConfig,
    ) -> Result<(Self, ConnectionHandle), ConnectionError> {
        Ok(Self::new(
            TcpStream::connect(addr).await?,
            connection_manager_handle,
            config,
            connection_handle_size,
            protocol_reassembler_config,
        ))
    }

    pub(crate) async fn run<CN: ConnectionAuthenticator>(
        mut self,
        connection_authenticator: Arc<CN>,
    ) {
        if !connection_authenticator.authenticate(&mut self.framed) {
            return;
        }

        loop {
            let result = tokio::select! {
                frame = timeout(self.config.ping_protocol_timeout, self.framed.try_next()) => self.handle_frame(frame).await,
                command = self.rx.recv() => self.handle_command(command).await,
            };

            match result {
                Ok(..) => {}
                Err(ConnectionError::SocketClosed) | Err(ConnectionError::ManagerChannel) => break,
                _ => unreachable!(),
            }
        }
    }

    async fn handle_frame(
        &mut self,
        frame: Result<Result<Option<Frame>, DecoderError>, Elapsed>,
    ) -> Result<(), ConnectionError> {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return self.send_ping_protocol(),
        }?;

        match frame {
            Some(frame) => {
                if let Some(dispatcher) = self.protocol_reassembler.push(frame)? {
                    self.connection_manager_handle
                        .protocol(dispatcher, self.config.id)
                        .await;
                }
                Ok(())
            }

            None => Err(ConnectionError::SocketClosed),
        }
    }

    async fn handle_command(
        &mut self,
        command: Option<ConnectionCommand>,
    ) -> Result<(), ConnectionError> {
        let command = match command {
            Some(command) => command,
            None => Err(ConnectionError::ManagerChannel)?,
        };

        match command {
            ConnectionCommand::Protocol(dispatcher) => self.framed.send(dispatcher).await?,
        };

        Ok(())
    }

    fn send_ping_protocol(&mut self) -> Result<(), ConnectionError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use tokio::{net::TcpListener, task::JoinHandle};

    use super::*;
    use crate::network::{
        connection::{
            connection::Connection,
            connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle},
        },
        protocol::{
            conversation_id::ConversationId,
            disconnect::Disconnect,
            dispatcher::{Dispatcher, Headers},
        },
    };
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    struct AuthenticatorTest;
    impl ConnectionAuthenticator for AuthenticatorTest {
        fn authenticate(&self, _framed: &mut Framed<TcpStream, Codec>) -> bool {
            return true;
        }
    }

    struct FalsyAuthenticatorTest;
    impl ConnectionAuthenticator for FalsyAuthenticatorTest {
        fn authenticate(&self, _framed: &mut Framed<TcpStream, Codec>) -> bool {
            return false;
        }
    }

    async fn launch_connection_run(
        port: u16,
    ) -> (
        mpsc::Receiver<ConnectionManagerCommand>,
        ConnectionHandle,
        JoinHandle<()>,
    ) {
        let (connection_manager_handle, connection_manager_rx) = ConnectionManagerHandle::new(16);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let config = Config::new(ConnectionId::new(1), Duration::from_secs(2));
        let protocol_reassembler_config = ProtocolReassemblerConfig::new(16);
        let authenticator_test = Arc::new(AuthenticatorTest);

        let (connection, handle) = Connection::from_addr(
            addr,
            connection_manager_handle,
            config,
            16,
            protocol_reassembler_config,
        )
        .await
        .unwrap();
        let connection_task = tokio::spawn(connection.run(authenticator_test.clone()));

        (connection_manager_rx, handle, connection_task)
    }

    async fn launch_server() -> TcpListener {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        TcpListener::bind(addr).await.unwrap()
    }

    #[tokio::test]
    async fn authentication_fail() {
        let listener = launch_server().await;

        let (connection_manager_handle, connection_manager_rx) = ConnectionManagerHandle::new(16);
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            listener.local_addr().unwrap().port(),
        );
        let config = Config::new(ConnectionId::new(1), Duration::from_secs(2));
        let protocol_reassembler_config = ProtocolReassemblerConfig::new(16);
        let authenticator_test = Arc::new(FalsyAuthenticatorTest);

        let (connection, handle) = Connection::from_addr(
            addr,
            connection_manager_handle,
            config,
            16,
            protocol_reassembler_config,
        )
        .await
        .unwrap();
        let connection_task = tokio::spawn(connection.run(authenticator_test.clone()));
        assert!(connection_task.await.is_ok())
    }

    #[tokio::test]
    async fn exit_paths() {
        let listener = launch_server().await;
        let (connection_manager_rx, handle, connection_task) =
            launch_connection_run(listener.local_addr().unwrap().port()).await;
        drop(handle);
        assert!(connection_task.await.is_ok());

        let listener = launch_server().await;
        let (_, _, connection_task) =
            launch_connection_run(listener.local_addr().unwrap().port()).await;

        tokio::spawn(async move {
            listener.accept().await.unwrap();
        });
        assert!(connection_task.await.is_ok());
    }

    #[tokio::test]
    async fn handle_protocol_command() {
        let listener = launch_server().await;
        let (connection_manager_rx, handle, connection_task) =
            launch_connection_run(listener.local_addr().unwrap().port()).await;

        let server_task = tokio::spawn(async move {
            let (stream, _addr) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, Codec::new());

            let frame = framed.next().await.unwrap().unwrap();
            let protocol = borsh::from_slice(frame.payload.as_bytes()).unwrap();

            match protocol {
                Dispatcher::Disconnect { .. } => {}
                _ => panic!(),
            }
        });

        let result = handle
            .protocol(Dispatcher::Disconnect {
                protocol: Disconnect::new(),
                headers: Headers::new(ConversationId::new(0)),
            })
            .await;
        assert!(result.is_ok());
        assert!(server_task.await.is_ok());
    }

    #[tokio::test]
    async fn receiving_protocol() {
        let listener = launch_server().await;
        let (mut connection_manager_rx, handle, connection_task) =
            launch_connection_run(listener.local_addr().unwrap().port()).await;

        let server_task = tokio::spawn(async move {
            let (stream, _addr) = listener.accept().await.unwrap();
            let mut framed = Framed::new(stream, Codec::new());

            framed
                .send(Dispatcher::Disconnect {
                    protocol: Disconnect::new(),
                    headers: Headers::new(ConversationId::new(0)),
                })
                .await
                .unwrap();
        });

        let command_received = connection_manager_rx.recv().await.unwrap();
        match command_received {
            ConnectionManagerCommand::Protocol { .. } => {}
            _ => panic!(),
        };

        assert!(server_task.await.is_ok());
    }
}
