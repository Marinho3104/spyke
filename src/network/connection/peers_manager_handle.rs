use crate::{
    network::connection::peer::PeerId,
    utils::background_service_handle::{BackgroundServiceHandle, OnSendErrorPanic},
};

#[derive(Debug)]
pub(crate) enum PeersManagerHandleCommand {
    PeerAuthentication {
        authenticated: bool,
        peer_id: PeerId,
    },
}

pub(crate) type PeersManagerHandle =
    BackgroundServiceHandle<PeersManagerHandleCommand, OnSendErrorPanic>;

impl PeersManagerHandle {
    pub(crate) async fn peer_authentication(&self, authenticated: bool, peer_id: PeerId) {
        let command = PeersManagerHandleCommand::PeerAuthentication {
            authenticated,
            peer_id,
        };
        self.send(command).await
    }
}
