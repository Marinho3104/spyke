use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;
use crate::network::protocol::protocol_id::ProtocolId;


#[derive(Debug, Clone, Copy, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct RawProtocolId(pub(crate) u8);

#[derive(Debug, Clone, Copy, PartialEq, BorshSerialize, BorshDeserialize, Eq, Hash)]
pub(crate) struct MessageId(pub(crate) u16);

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct FragmentInfo {
    pub(crate) message_id: MessageId,
    pub(crate) index: u16,
    pub(crate) total: u16, 
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct FrameHeaders {
    pub(crate) raw_protocol_id: RawProtocolId,
    pub(crate) fragment_info: Option<FragmentInfo>,
}

#[derive(Debug, Error)]
pub(crate) enum FragmentInfoError {
    #[error("Fragment sequence index must be less than sequence total: {index} >= {total}")]
    InvalidSequence {
        index: u16,
        total: u16
    },
}


impl RawProtocolId {
    pub(crate) fn new(protocol_id: u8) -> Self {
        Self(protocol_id)
    }
}

impl MessageId {
    pub(crate) fn new(message_id: u16) -> Self {
        Self(message_id)
    }
}

impl From<ProtocolId> for RawProtocolId {
    fn from(value: ProtocolId) -> Self {
        Self::new(value as u8)
    }
}

impl FragmentInfo {
        pub(crate) fn try_new(message_id: MessageId, index: u16, total: u16) -> Result<Self, FragmentInfoError> {
        if index >= total {
            Err(FragmentInfoError::InvalidSequence { index, total })?
        }

        Ok(
            Self {
                message_id,
                index,
                total
            }
        )
    }
}

impl FrameHeaders {
    pub(crate) const MAX_ENCODED_SIZE: usize = 
        size_of::<RawProtocolId>() +
        1 + // Additional byte added from the option type
        size_of::<FragmentInfo>();

    pub(crate) fn new(raw_protocol_id: RawProtocolId, fragment_info: Option<FragmentInfo>) -> Self {
        Self {
            raw_protocol_id,
            fragment_info
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_encoded_size() {
        let headers = FrameHeaders::new(RawProtocolId::new(0), Some(FragmentInfo::try_new(MessageId::new(0), 0, 1).unwrap()));
        let encoded_headers = borsh::to_vec(&headers).unwrap();
        assert_eq!(encoded_headers.len(), FrameHeaders::MAX_ENCODED_SIZE);

        let headers = FrameHeaders::new(RawProtocolId::new(0), None);
        let encoded_headers = borsh::to_vec(&headers).unwrap();
        assert_eq!(encoded_headers.len(), 2);
    }

    #[test]
    fn fragment_info_error() {
        let fragment_info = FragmentInfo::try_new(MessageId::new(0), 1, 1);
        assert!(fragment_info.is_err());
    }

}
