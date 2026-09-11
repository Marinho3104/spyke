use std::{net::SocketAddr, time::Duration};

use tokio::{net::TcpStream, sync::mpsc};

use crate::network::connection::{connection_manager_handle::{ConnectionManagerCommand, ConnectionManagerHandle}, server::{Server, ServerError}};
use crate::diagnostics::logs::log_info;

pub(crate) struct ConnectionManagerConfig {
    max_connections: u16,
    accept_connection_timeout_duration: Option<Duration>,
    max_queue_commands: u16
}

impl ConnectionManagerConfig {

    pub(crate) fn new(max_connections: u16, accept_connection_timeout_duration: Option<Duration>, max_queue_commands: u16) -> Self {
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

    pub(crate) fn max_queue_commands(&self) -> u16 {
        self.max_queue_commands
    }

}

pub(crate) struct ConnectionManager {
    config: ConnectionManagerConfig,
    server: Option<Server>,
}

impl ConnectionManager {

    pub(crate) fn new(config: ConnectionManagerConfig, server: Option<Server>) -> Self {
        Self {
            config,
            server
        }
    }

    pub(crate) fn spawn(self) -> ConnectionManagerHandle {
        let (tx, rx) = mpsc::channel::<ConnectionManagerCommand>(self.config.max_queue_commands() as usize);

        tokio::spawn(self.run(rx));

        ConnectionManagerHandle::new(tx)
    }

    async fn run(mut self, mut rx: mpsc::Receiver<ConnectionManagerCommand>) {
        let accept_timeout_duration = self.config.accept_connection_timeout_duration();
        loop {
            tokio::select! {
                accepted_connection = self.server.as_mut().unwrap().accept_connection(accept_timeout_duration), 
                    if self.server.is_some() => {
                        match accepted_connection {
                            Ok(accepted_connection) => self.handle_accepted_connection(accepted_connection),
                            Err(ServerError::TimeoutReachedOnAccept) => {} // Timeout reached just loop again
                            Err(ServerError::NonFatalAcceptError { .. }) => {} // No fatal error occur just ignore
                            Err(err) => {
                                log_info!("A fatal error occur on the server object, exiting from run loop! Error: {err}");
                                break;
                            }
                        }
                }

                command = rx.recv() => {
                    match command {
                        Some(command) => self.handle_command(command),
                        None => {
                            log_info!("Connection manager communication channel was closed, exiting from the run loop!");
                            break;
                        }
                    }
                }
            }
        }
    }

    fn handle_command(&mut self, command: ConnectionManagerCommand) {}

    fn handle_accepted_connection(&mut self, accepted_connection: (TcpStream, SocketAddr)) {}
}









