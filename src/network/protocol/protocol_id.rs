use thiserror::Error;

use crate::network::framing::frame_headers::RawProtocolId;


#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ProtocolId {
    Disconnect
}

#[derive(Debug, Error)]
pub(crate) enum ProtocolIdTryFromError {
    #[error("Unknown protocol id: {0}")]
    ProtocolId(u8),
}


impl TryFrom<RawProtocolId> for ProtocolId {
    type Error = ProtocolIdTryFromError;

    fn try_from(value: RawProtocolId) -> Result<Self, Self::Error> {
        match value {
            RawProtocolId(0) => Ok(ProtocolId::Disconnect),
            RawProtocolId(unknown) => Err(ProtocolIdTryFromError::ProtocolId(unknown))
        }       
    }
}


