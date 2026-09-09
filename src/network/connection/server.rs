use std::net::SocketAddr;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Error)]
pub(crate) enum ServerError {

    #[error("Not able to bind with given address: {addr} with error: {error_message}")]
    NotAbleToBindServerWithGivenAddress {
        addr: SocketAddr,
        error_message: String,
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
        let stream = TcpListener::bind(addr)
            .await
            .map_err(|err| ServerError::NotAbleToBindServerWithGivenAddress { addr, error_message: err.to_string() })?;
        Ok(Self::new(stream, addr))
    }

    pub(crate) async fn accept_connection(&mut self) -> Result<Option<(TcpStream, SocketAddr)>, ServerError> {
        let accepted_data: Option<(TcpStream, SocketAddr)> = match self.stream.accept().await {
            Ok(new_connection) => Some(new_connection),
            Err(e) => match e.kind() {
                std::io::ErrorKind::Interrupted |
                    std::io::ErrorKind::ConnectionAborted => None,
                _ => return Err(ServerError::FatalAcceptError { error_message: e.to_string() })
            }
        };
        Ok(accepted_data)
    }
   
}
