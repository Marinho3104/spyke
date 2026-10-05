use thiserror::Error;

use crate::network::{framing::frame::{Frame, Payload}, protocol::{disconnect::{Disconnect, DisconnectTryFromError}, protocol_id::{ProtocolId, ProtocolIdTryFromError}}};


pub(crate) enum Dispatcher {
    Disconnect(Disconnect),
}

#[derive(Debug, Error)]
pub(crate) enum DispatcherTryFromPayloadsError {
    #[error("Disconnect try from error: {0}")]
    DisconnectTryFrom(#[from] DisconnectTryFromError),

    #[error("Protocol id try from error: {0}")]
    ProtocolIdTryFrom(#[from] ProtocolIdTryFromError),

    #[error("Cannot convert a empty vec of payloads")]
    Payloads,
}


impl Dispatcher {

    pub(crate) fn try_from_payloads(payloads: Vec<Payload>, protocol_id: ProtocolId) -> Result<Self, DispatcherTryFromPayloadsError> {
        if payloads.is_empty() {
            return Err(DispatcherTryFromPayloadsError::Payloads)
        }

        Ok(
            match protocol_id {
                ProtocolId::Disconnect => Disconnect::try_from(payloads).map(Dispatcher::Disconnect)?,
            }
        )
    }

}

impl From<Dispatcher> for Vec<Frame> {
    fn from(value: Dispatcher) -> Self {
        match value {
            Dispatcher::Disconnect(disconnect) => disconnect.into(),
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::network::framing::frame_headers::{FrameHeaders, RawProtocolId};

use super::*;

    #[test]
    fn valid_try_from_vec_payload() {
        let list = [
            (ProtocolId::Disconnect, vec![Payload::try_new(vec![0; 0]).unwrap()])
        ];

        for (protocol_id, payloads) in list {
            let dispatcher: Result<Dispatcher, DispatcherTryFromPayloadsError> = Dispatcher::try_from_payloads(payloads, protocol_id);
            assert!(dispatcher.is_ok())
        }
    }

    #[test]
    fn invalid_try_from_vec_frame() {
        let list = [
            // Empty
            (ProtocolId::Disconnect, vec![]),

            // Invalid raw protocol id
            // (ProtocolId::Disconnect, vec![Payload::try_new(vec![0; 0]).unwrap()])

            // Disconnect Protocol - Non empty payload
            (ProtocolId::Disconnect, vec![Payload::try_new(vec![0; 10]).unwrap()]),

            // Disconnect Protocol - Unexpected number of frames in vec
            (ProtocolId::Disconnect, vec![Payload::try_new(vec![0; 0]).unwrap(), Payload::try_new(vec![0; 0]).unwrap()]),
        ];

        for (protocol_id, payloads) in list {
            let dispatcher: Result<Dispatcher, DispatcherTryFromPayloadsError> = Dispatcher::try_from_payloads(payloads, protocol_id);
            assert!(dispatcher.is_err())
        }
    }

    #[test]
    fn valid_from_dispatcher() {
        let list = [
            Dispatcher::Disconnect(Disconnect)
        ];

        for dispatcher in list {
            let _: Vec<Frame> = dispatcher.into();
        }
    }
}
