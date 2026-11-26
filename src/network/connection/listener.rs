use std::net::SocketAddr;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
pub(crate) struct Listener {
    listener: TcpListener,
}

impl Listener {
    pub(crate) fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    pub(crate) async fn bind(addr: SocketAddr) -> Result<Listener, ListenerError> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self::new(listener))
    }

    pub(crate) async fn accept(&self) -> Result<Option<TcpStream>, ListenerError> {
        let result = match self.listener.accept().await {
            Ok((stream, _)) => Some(stream),
            Err(error) => match error.kind() {
                std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::Interrupted => None,
                _ => return Err(error.into()),
            },
        };
        Ok(result)
    }
}

#[derive(Debug, Error)]
pub(crate) enum ListenerError {
    #[error("TcpListener error: {0}")]
    TcpListener(#[from] std::io::Error),
}
