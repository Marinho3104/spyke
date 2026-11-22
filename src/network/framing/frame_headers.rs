use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, BorshSerialize, BorshDeserialize, Eq, Hash)]
pub(crate) struct MessageId(pub(crate) u16);

#[derive(Debug, Clone, PartialEq, BorshSerialize)]
pub(crate) struct FragmentInfo {
    message_id: MessageId,
    index: u16,
    total: u16,
}

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct FrameHeaders(pub(crate) Option<FragmentInfo>);

#[derive(Debug, Error)]
pub(crate) enum FragmentInfoError {
    #[error("Fragment sequence index must be less than sequence total: {index} >= {total}")]
    InvalidSequence { index: u16, total: u16 },

    #[error("Fragment total must be greater than 1: {0} <= 1")]
    InvalidTotal(u16),

    #[error("Fragment total is too large: {total} > {max}")]
    TotalTooLarge { total: u16, max: u16 },
}

impl MessageId {
    pub(crate) fn new(message_id: u16) -> Self {
        Self(message_id)
    }
}

impl FragmentInfo {
    /// Max fragments per message, so the largest message is MAX_TOTAL * Payload::MAX_SIZE (64 KiB)
    pub(crate) const MAX_TOTAL: u16 = 128;

    pub(crate) fn try_new(
        message_id: MessageId,
        index: u16,
        total: u16,
    ) -> Result<Self, FragmentInfoError> {
        let slf = Self {
            message_id,
            index,
            total,
        };
        slf.validate()?;
        Ok(slf)
    }

    pub(crate) fn message_id(&self) -> MessageId {
        self.message_id
    }

    pub(crate) fn index(&self) -> u16 {
        self.index
    }

    pub(crate) fn total(&self) -> u16 {
        self.total
    }

    fn validate(&self) -> Result<(), FragmentInfoError> {
        let index = self.index;
        let total = self.total;
        if index >= total {
            return Err(FragmentInfoError::InvalidSequence { index, total });
        }
        if total <= 1 {
            return Err(FragmentInfoError::InvalidTotal(total));
        }
        if total > Self::MAX_TOTAL {
            return Err(FragmentInfoError::TotalTooLarge {
                total,
                max: Self::MAX_TOTAL,
            });
        }
        Ok(())
    }
}

impl BorshDeserialize for FragmentInfo {
    fn deserialize_reader<R: std::io::prelude::Read>(reader: &mut R) -> std::io::Result<Self> {
        let fragment_info = FragmentInfo::try_new(
            MessageId::deserialize_reader(reader)?,
            u16::deserialize_reader(reader)?,
            u16::deserialize_reader(reader)?,
        )
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string()))?;

        Ok(fragment_info)
    }
}

impl FrameHeaders {
    pub(crate) const MAX_ENCODED_SIZE: usize = 1 + // Additional byte added from the option type
        size_of::<u16>() + // message_id
        size_of::<u16>() + // index
        size_of::<u16>(); // total

    pub(crate) fn new(fragment_info: Option<FragmentInfo>) -> Self {
        Self(fragment_info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_encoded_size() {
        let headers = FrameHeaders::new(Some(
            FragmentInfo::try_new(MessageId::new(0), 0, 2).unwrap(),
        ));
        let encoded_headers = borsh::to_vec(&headers).unwrap();
        assert_eq!(encoded_headers.len(), FrameHeaders::MAX_ENCODED_SIZE);

        let headers = FrameHeaders::new(None);
        let encoded_headers = borsh::to_vec(&headers).unwrap();
        assert_eq!(encoded_headers.len(), 1);
    }

    #[test]
    fn fragment_info_error() {
        let fragment_info = FragmentInfo::try_new(MessageId::new(0), 1, 1);
        assert!(matches!(
            fragment_info,
            Err(FragmentInfoError::InvalidSequence { index: 1, total: 1 })
        ));
    }

    #[test]
    fn fragment_total_limit() {
        let max = FragmentInfo::MAX_TOTAL;
        FragmentInfo::try_new(MessageId::new(0), 0, max).unwrap();

        let result = FragmentInfo::try_new(MessageId::new(0), 0, max + 1);
        assert!(matches!(
            result,
            Err(FragmentInfoError::TotalTooLarge { total, max: m }) if total == max + 1 && m == max
        ));
    }
}
