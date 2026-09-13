use std::{net::SocketAddr, time::Duration};

use thiserror::Error;
use tokio::{net::{TcpListener, TcpStream}, sync::mpsc};

use crate::network::connection::{connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}, server::{Server, ServerError}};
use crate::diagnostics::logs::log_info;

#[derive(Debug, Error)]
pub(crate) enum ConnectionManagerError {

    #[error("Server error: {0}")]
    Server(#[from] ServerError),

}

pub(crate) struct ConnectionManagerConfig {
    max_connections: u16,
    accept_connection_timeout_duration: Option<Duration>,
    max_queue_commands: usize
}

impl ConnectionManagerConfig {

    pub(crate) fn new(max_connections: u16, accept_connection_timeout_duration: Option<Duration>, max_queue_commands: usize) -> Self {
        Self {
            max_connections,
            accept_connection_timeout_duration,
            max_queue_commands
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

}

pub(crate) struct ConnectionManager {
    config: ConnectionManagerConfig,
    server: Option<Server>,
    connection_manager_handle: ConnectionManagerHandle,
    communication_queue: mpsc::Receiver<ConnectionManagerCommand>,
}

impl ConnectionManager {

    pub(crate) fn new(config: ConnectionManagerConfig, server: Option<Server>) -> Self {
        let (tx, communication_queue) = mpsc::channel::<ConnectionManagerCommand>(self.config.max_queue_commands() as usize);
        Self {
            config,
            server,
            connection_manager_handle: ConnectionManagerHandle::new(tx),
            communication_queue
        }
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
                    if self.server.is_some() => self.handle_accepted_connection(accepted_connection),

                // command = rx.recv() => {
                //     match command {
                //         Some(command) => self.handle_command(command),
                //         None => panic!("Connection manager communication channel was closed, exiting from the run loop!")
                //     }
                // }
            };

            if let Err(_) = result {
                break;
            }
        }
    }
   
    fn handle_accepted_connection(&mut self, accepted_connection: Result<(TcpStream, SocketAddr), ServerError>) -> Result<(), ConnectionManagerError> {

        match accepted_connection {
            
            Ok((stream, addr)) => { Ok(()) },

            Err(ServerError::TimeoutReachedOnAccept) | Err(ServerError::NonFatalAcceptError { .. }) => Ok(()),
            Err(err) => Err(err)?
        }
    }

    fn handle_command(&mut self, command: ConnectionManagerCommand) {}
}









