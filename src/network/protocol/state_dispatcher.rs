use thiserror::Error;

use crate::network::protocol::{
    disconnect::State as DisconnectState,
    dispatcher::Dispatcher,
    flow::Flow,
    ping::State as PingState,
    specs::{StateSpec, StateSpecError},
};

pub(crate) enum StateDispatcher {
    Disconnect(DisconnectState),
    Ping(PingState),
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum StateDispatcherError {
    #[error("State spec error: {0}")]
    StateSpec(#[from] StateSpecError),

    #[error("Received unexpected protocol")]
    UnexpectedProtocol,
}

impl StateDispatcher {
    pub(crate) fn try_new(
        dispatcher: &Dispatcher,
        flow: Flow,
    ) -> Result<Option<Self>, StateDispatcherError> {
        let result = match dispatcher {
            Dispatcher::Disconnect { protocol, .. } => {
                DisconnectState::try_new(protocol, flow)?.map(StateDispatcher::Disconnect)
            }
            Dispatcher::Ping { protocol, .. } => {
                PingState::try_new(protocol, flow)?.map(StateDispatcher::Ping)
            }
        };
        Ok(result)
    }

    pub(crate) fn update(
        &self,
        dispatcher: &Dispatcher,
        flow: Flow,
    ) -> Result<Option<Self>, StateDispatcherError> {
        let result = match (dispatcher, self) {
            (Dispatcher::Disconnect { protocol, .. }, StateDispatcher::Disconnect(state)) => state
                .update(protocol, flow)?
                .map(StateDispatcher::Disconnect),
            (Dispatcher::Ping { protocol, .. }, StateDispatcher::Ping(state)) => {
                state.update(protocol, flow)?.map(StateDispatcher::Ping)
            }
            _ => Err(StateDispatcherError::UnexpectedProtocol)?,
        };
        Ok(result)
    }
}
