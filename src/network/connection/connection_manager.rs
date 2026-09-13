use std::{collections::HashMap, net::SocketAddr, time::Duration};

use thiserror::Error;
use tokio::{net::{TcpListener, TcpStream}, sync::mpsc};

use crate::network::connection::{connection::{Connection, ConnectionConfig, ConnectionId}, connection_handle::ConnectionHandle, connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}, server::{Server, ServerError}};
use crate::diagnostics::logs::log_info;

#[derive(Debug, Error)]
pub(crate) enum ConnectionManagerError {

    #[error("Server error: {0}")]
    Server(#[from] ServerError),

    #[error("All communication queue senders were dropped")]
    CommunicationQueueSender,
}

struct ConnectionEntry {
    handle: ConnectionHandle,
}

impl ConnectionEntry {

    pub(crate) fn new(handle: ConnectionHandle) -> Self {
        Self {
            handle
        }
    }

    pub(crate) fn handle(&mut self) -> &mut ConnectionHandle {
        &mut self.handle
    }

}

pub(crate) struct ConnectionManagerConfig {
    max_connections: u16,
    accept_connection_timeout_duration: Option<Duration>,
    max_queue_commands: usize,
    max_connection_queue_commands: usize,
}

impl ConnectionManagerConfig {

    pub(crate) fn new(
        max_connections: u16, 
        accept_connection_timeout_duration: Option<Duration>, 
        max_queue_commands: usize, 
        max_connection_queue_commands: usize
    ) -> Self {
        Self {
            max_connections,
            accept_connection_timeout_duration,
            max_queue_commands,
            max_connection_queue_commands
        }
    }

    pub(crate) fn max_connections(&self) -> u16 {
        self.max_connections
    }

    pub(crate) fn accept_connection_timeout_duration(&self) -> Option<Duration> {
        self.accept_connection_timeout_duration
    }

    pub(crate) fn max_queue_commands(&self) -> usize {
        self.max_queue_commands
    }

    pub(crate) fn max_connection_queue_commands(&self) -> usize {
        self.max_connection_queue_commands
    }

}

pub(crate) struct ConnectionManager {
    config: ConnectionManagerConfig,
    server: Option<Server>,
    connection_manager_handle: ConnectionManagerHandle,
    communication_queue: mpsc::Receiver<ConnectionManagerCommand>,
    connections: HashMap<ConnectionId, ConnectionEntry>,
}

impl ConnectionManager {

    pub(crate) fn new(config: ConnectionManagerConfig, server: Option<Server>) -> Self {
        let (tx, communication_queue) = mpsc::channel::<ConnectionManagerCommand>(config.max_queue_commands() as usize);
        Self {
            config,
            server,
            connection_manager_handle: ConnectionManagerHandle::new(tx),
            communication_queue,
            connections: HashMap::new(),
        }
    }

    fn new_connection_id(&self) -> ConnectionId {
        rand::random()
    }

    fn add_connection(&mut self, connection_id: ConnectionId, connection: ConnectionEntry) {
        self.connections.insert(connection_id, connection);
    }

    fn remove_connection(&mut self, connection_id: ConnectionId) {
        self.connections.remove(&connection_id);
    }

    pub(crate) fn spawn(self) -> ConnectionManagerHandle {
        let connection_manager_handle = self.connection_manager_handle.clone();

        tokio::spawn(self.run());

        connection_manager_handle
    }

    async fn run(mut self) {
        let accept_timeout_duration = self.config.accept_connection_timeout_duration();
        loop {
            let result = tokio::select! {
                accepted_connection = self.server.as_mut().unwrap().accept_connection(accept_timeout_duration), 
                    if self.server.is_some() && self.connections.len() < self.config.max_connections() as usize => 
                        self.handle_accepted_connection(accepted_connection),
                command = self.communication_queue.recv() => self.handle_command(command).await
            };

            if let Err(_) = result {
                break;
            }
        }
    }
   
    fn handle_accepted_connection(&mut self, accepted_connection: Result<(TcpStream, SocketAddr), ServerError>) -> Result<(), ConnectionManagerError> {

        match accepted_connection {
            
            Ok((stream, addr)) => {
                let connection_id = self.new_connection_id();
                let new_connection_config = ConnectionConfig::new(addr, connection_id, self.config.max_connection_queue_commands());
                let new_connection = Connection::new(new_connection_config, stream, self.connection_manager_handle.clone());
                let connection_handle = new_connection.spawn();
                self.add_connection(connection_id, ConnectionEntry::new(connection_handle));

                Ok(()) 
            },

            Err(ServerError::TimeoutReachedOnAccept) | Err(ServerError::NonFatalAcceptError { .. }) => Ok(()),
            Err(err) => Err(err)?
        }
    }

    async fn handle_command(&mut self, command: Option<ConnectionManagerCommand>) -> Result<(), ConnectionManagerError> {
        let command = command
            .ok_or(ConnectionManagerError::CommunicationQueueSender)?;

        match command {
            ConnectionManagerCommand::Protocol { protocol, connection_id } => {},
            ConnectionManagerCommand::RemoveConnection { connection_id } => self.remove_connection(connection_id),
            ConnectionManagerCommand::SendPackets { connection_id, packets } => 
                if let Some(connection_entry) = self.connections.get_mut(&connection_id) {
                    connection_entry.handle().send_packets(packets).await;
                }
        }

        Ok(())
    }
}









