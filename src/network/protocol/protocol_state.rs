use thiserror::Error;

use crate::network::protocol::{
    authentication::State as AuthenticationState,
    disconnect::State as DisconnectState,
    flow::Flow,
    message::Protocol,
    ping::State as PingState,
    specs::{StateSpec, StateSpecError},
};

pub(crate) enum ProtocolState {
    Disconnect(DisconnectState),
    Ping(PingState),
    Authentication(AuthenticationState),
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum ProtocolStateError {
    #[error("State spec error: {0}")]
    StateSpec(#[from] StateSpecError),

    #[error("Received unexpected protocol")]
    UnexpectedProtocol,
}

impl ProtocolState {
    pub(crate) fn try_new(
        protocol: &Protocol,
        flow: Flow,
    ) -> Result<Option<Self>, ProtocolStateError> {
        let result = match protocol {
            Protocol::Disconnect(protocol) => {
                DisconnectState::try_new(protocol, flow)?.map(ProtocolState::Disconnect)
            }
            Protocol::Ping(protocol) => {
                PingState::try_new(protocol, flow)?.map(ProtocolState::Ping)
            }
            Protocol::Authentication(protocol) => {
                AuthenticationState::try_new(protocol, flow)?.map(ProtocolState::Authentication)
            }
        };
        Ok(result)
    }

    pub(crate) fn update(
        &self,
        protocol: &Protocol,
        flow: Flow,
    ) -> Result<Option<Self>, ProtocolStateError> {
        let result = match (protocol, self) {
            (Protocol::Disconnect(protocol), ProtocolState::Disconnect(state)) => {
                state.update(protocol, flow)?.map(ProtocolState::Disconnect)
            }
            (Protocol::Ping(protocol), ProtocolState::Ping(state)) => {
                state.update(protocol, flow)?.map(ProtocolState::Ping)
            }
            (Protocol::Authentication(protocol), ProtocolState::Authentication(state)) => state
                .update(protocol, flow)?
                .map(ProtocolState::Authentication),
            _ => return Err(ProtocolStateError::UnexpectedProtocol),
        };
        Ok(result)
    }
}
