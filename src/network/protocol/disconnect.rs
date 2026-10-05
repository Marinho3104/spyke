use thiserror::Error;

use crate::network::{framing::{frame::{Frame, Payload}, frame_headers::FrameHeaders}, protocol::{protocol_id::{ProtocolId, ProtocolIdTryFromError}, spec::Spec}};

#[derive(Debug)]
pub(crate) struct Disconnect;

#[derive(Debug, Error)]
pub(crate) enum DisconnectTryFromError {
    #[error("Protocol id try from error: {0}")]
    ProtocolIdTryFrom(#[from] ProtocolIdTryFromError),

    #[error("Unexpected protocol id: {0}")]
    ProtocolId(u8),
    
    #[error("Unexpected payload lenght: {0}")]
    PayloadLength(usize),
    
    #[error("Unexpected number of frames received: {0}")]
    FramesCount(usize),
}


impl Spec for Disconnect {
    const PROTOCOL_ID: ProtocolId = ProtocolId::Disconnect;
}

impl TryFrom<Vec<Payload>> for Disconnect {
    type Error = DisconnectTryFromError;

    fn try_from(value: Vec<Payload>) -> Result<Self, Self::Error> {
        match value.as_slice() {
            [frame] => match frame.0.len() {
                0 => Ok(Disconnect),
                unexpected => Err(DisconnectTryFromError::FramesCount(unexpected))
            },
            slice => Err(DisconnectTryFromError::FramesCount(slice.len()))
        }
    }

}

impl From<Disconnect> for Vec<Frame> {
    fn from(_: Disconnect) -> Self {
        let headers = FrameHeaders::new(Disconnect::PROTOCOL_ID.into(), None);

        // Unwrap here is safe as all the values used are hardcoded
        vec![Frame::new(headers, Payload::try_new(vec![0; 0]).unwrap())]
    }
}
