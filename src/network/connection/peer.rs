use std::net::SocketAddr;

use thiserror::Error;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::network::{
    connection::{
        peer_handle::{PeerHandle, PeerHandleCommand},
        peers_manager_handle::PeersManagerHandle,
    },
    framing::codec::Codec,
};

const PEER_HANDLE_CHANNEL_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PeerId(pub(crate) u16);

pub(crate) struct Peer {
    peer_id: PeerId,
    rx: mpsc::Receiver<PeerHandleCommand>,
    peers_manager_handle: PeersManagerHandle,
    framed: Framed<TcpStream, Codec>,
}

impl PeerId {
    pub(crate) fn new(peer_id: u16) -> Self {
        Self(peer_id)
    }
}

impl Peer {
    pub(crate) fn create(
        peer_id: PeerId,
        peers_manager_handle: PeersManagerHandle,
        stream: TcpStream,
    ) -> (Self, PeerHandle) {
        let (handle, rx) = PeerHandle::new(PEER_HANDLE_CHANNEL_SIZE);
        (
            Self {
                peer_id,
                framed: Framed::new(stream, Codec::new()),
                rx,
                peers_manager_handle,
            },
            handle,
        )
    }

    pub(crate) async fn connect(
        addr: SocketAddr,
        peer_id: PeerId,
        peers_manager_handle: PeersManagerHandle,
    ) -> Result<(Self, PeerHandle), PeerError> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::create(peer_id, peers_manager_handle, stream))
    }
}

#[derive(Debug, Error)]
pub(crate) enum PeerError {
    #[error("TcpStream error: {0}")]
    TcpStream(#[from] std::io::Error),
}
