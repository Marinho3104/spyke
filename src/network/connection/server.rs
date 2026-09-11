use std::{net::SocketAddr, time::Duration};

use thiserror::Error;
use tokio::{net::{TcpListener, TcpStream}, time::timeout};

#[derive(Debug, Error)]
pub(crate) enum ServerError {

    #[error("Not able to bind with given address: {addr} with error: {error_message}")]
    NotAbleToBindServerWithGivenAddress {
        addr: SocketAddr,
        error_message: String,
    },

    #[error("Timeout reached on accept")]
    TimeoutReachedOnAccept,

    #[error("Non-fatal accept error: {error_message}")]
    NonFatalAcceptError {
        error_message: String
    },

    #[error("Fatal accept error: {error_message}")]
    FatalAcceptError {
        error_message: String
    }

}

pub(crate) struct Server {
    stream: TcpListener, 
    addr: SocketAddr,
}

impl Server {
 
    pub(crate) fn new(stream: TcpListener, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
        }
    }

    pub(crate) async fn from(addr: SocketAddr) -> Result<Self, ServerError> {
        let stream = TcpListener::bind(addr).await
            .map_err(|err| ServerError::NotAbleToBindServerWithGivenAddress { addr, error_message: err.to_string() })?;
        Ok(Self::new(stream, addr))
    }

    pub(crate) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.stream.local_addr()
    }

    pub(crate) async fn accept_connection(&mut self, timeout_duration: Option<Duration>) -> Result<(TcpStream, SocketAddr), ServerError> {
        let accept_fn = self.stream.accept();

        let accepted_data = match timeout_duration {
            Some(timeout_duration) => timeout(timeout_duration, accept_fn).await
                .map_err(|_| ServerError::TimeoutReachedOnAccept)?,
            None => accept_fn.await
        };


        match accepted_data {
            Ok(new_connection) => Ok(new_connection),
            Err(e) => match e.kind() {
                std::io::ErrorKind::Interrupted |
                    std::io::ErrorKind::ConnectionAborted => Err(ServerError::NonFatalAcceptError { error_message: e.to_string() }),
                _ => return Err(ServerError::FatalAcceptError { error_message: e.to_string() })
            }
        }
    }
   
}

#[cfg(debug_assertions)]
mod tests {
    use std::{net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream}, time::Duration};

use crate::network::connection::server::{Server, ServerError};

    
    #[tokio::test]
    async fn test_create_server_success() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let server = Server::from(addr).await;
        assert!(server.is_ok());
    }

    #[tokio::test]
    async fn test_create_server_error() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let server = Server::from(addr.clone()).await
            .expect("Expected server Ok");
        let server_local_addr = server.local_addr()
            .expect("Expected server local addr Ok");

        let mut addr1 = addr.clone();
        addr1.set_port(server_local_addr.port());
        let server1 = Server::from(addr1).await;
        assert!(matches!(server1, Err(ServerError::NotAbleToBindServerWithGivenAddress { .. })));
    }

    #[tokio::test]
    async fn test_accept_connection_success() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let mut server = Server::from(addr.clone()).await
            .expect("Expected ok server");
        let server_local_addr = server.local_addr()
            .expect("Expected server local addr Ok");

        tokio::spawn(async move {
            let mut addr1 = addr.clone();
            addr1.set_port(server_local_addr.port());

            let _ = TcpStream::connect(addr1);
        });

        let accepted_connection = server.accept_connection(None).await;
        assert!(accepted_connection.is_ok());
    }

    #[tokio::test]
    async fn test_accept_connection_timeout_reached_error() {
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            0,
        );

        let mut server = Server::from(addr.clone()).await
            .expect("Expected ok server");

        let accepted_connection = server.accept_connection(Some(Duration::from_millis(100))).await;
        assert!(matches!(accepted_connection, Err(ServerError::TimeoutReachedOnAccept)));
    }
}
