use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;

use crate::network::framing::frame_headers::FrameHeaders;

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct Payload(pub(crate) Vec<u8>);

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct Frame {
    pub(crate) headers: FrameHeaders,
    pub(crate) payload: Payload,
}

#[derive(Debug, Error)]
pub(crate) enum PayloadError {
    #[error("Payload is too big: {actual} > {max}")]
    Size {
        actual: usize,
        max: usize
    },
}


impl Payload {
    pub(crate) const MAX_SIZE: usize = 512;
    pub(crate) const MAX_ENCODED_SIZE: usize = Payload::MAX_SIZE + 4;

    pub(crate) fn try_new(payload: Vec<u8>) -> Result<Self, PayloadError> {
        if payload.len() <= Payload::MAX_SIZE {
            Ok(Self(payload))
        }
        else {
            Err(
                PayloadError::Size { 
                    actual: payload.len(), 
                    max: Payload::MAX_SIZE 
                }
            )
        }
        
    }

}

impl Frame {
    pub(crate) const MAX_ENCODED_SIZE: usize = Payload::MAX_ENCODED_SIZE + FrameHeaders::MAX_ENCODED_SIZE;

    pub(crate) fn new(headers: FrameHeaders, payload: Payload) -> Self {
        Self {
            headers,
            payload
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::framing::frame_headers::{FragmentInfo, FrameHeaders, MessageId, RawProtocolId};


    fn frame_headers() -> FrameHeaders {
        FrameHeaders::new(RawProtocolId::new(0), Some(FragmentInfo::try_new(MessageId::new(0), 0, 1).unwrap()))
    }

    #[test]
    fn validate_encoded_size() {
        let frame = Frame::new(frame_headers(), Payload::try_new(vec![0; Payload::MAX_SIZE]).unwrap());
        let encoded = borsh::to_vec(&frame).unwrap();
        assert_eq!(encoded.len(), Frame::MAX_ENCODED_SIZE);
    }

    #[test]
    fn invalid_payload_size() {
        let result = Payload::try_new(vec![0; Payload::MAX_SIZE + 1]);
        assert!(result.is_err())
    }
}

