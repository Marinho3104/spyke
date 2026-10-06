use std::{net::SocketAddr, time::Duration};

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};


#[derive(Debug)]
pub(crate) struct Server {
    stream: TcpListener,
    addr: SocketAddr
}

#[derive(Debug, Error)]
pub(crate) enum ServerError {
    #[error("TcpListenner error: {0}")]
    TcpListener(#[from] std::io::Error),
}


impl Server {
    pub(crate) fn new(stream: TcpListener, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr
        }
    }

    pub(crate) async fn from_addr(addr: SocketAddr) -> Result<Self, ServerError> {
        Ok(Self::new(TcpListener::bind(addr).await?, addr))
    }

    pub(crate) async fn accept_connection(&mut self) -> Result<(TcpStream, SocketAddr), ServerError> {
        Ok(self.stream.accept().await?)
    }

    pub(crate) fn is_fatal_error(error: ServerError) -> bool {
        let ServerError::TcpListener(error) = error;
        error.kind() != std::io::ErrorKind::ConnectionAborted && error.kind() != std::io::ErrorKind::Interrupted
    }
    
}
