use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;

use crate::network::framing::frame_headers::{FragmentInfo, FrameHeaders, MessageId};

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct Payload(Vec<u8>);

#[derive(Debug, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
pub(crate) struct Frame {
    pub(crate) headers: FrameHeaders,
    pub(crate) payload: Payload,
}

#[derive(Debug, Error)]
pub(crate) enum PayloadError {
    #[error("Payload is too big: {actual} > {max}")]
    Size { actual: usize, max: usize },
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum FrameError {
    #[error("Message is too large: {actual} bytes > {max} bytes")]
    MessageTooLarge { actual: usize, max: usize },
}

impl Payload {
    pub(crate) const MAX_SIZE: usize = 512;
    pub(crate) const MAX_ENCODED_SIZE: usize = Payload::MAX_SIZE + 4;

    pub(crate) fn try_new(payload: Vec<u8>) -> Result<Self, PayloadError> {
        if payload.len() <= Payload::MAX_SIZE {
            Ok(Self(payload))
        } else {
            Err(PayloadError::Size {
                actual: payload.len(),
                max: Payload::MAX_SIZE,
            })
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl Frame {
    pub(crate) const MAX_ENCODED_SIZE: usize =
        Payload::MAX_ENCODED_SIZE + FrameHeaders::MAX_ENCODED_SIZE;

    pub(crate) fn new(headers: FrameHeaders, payload: Payload) -> Self {
        Self { headers, payload }
    }

    /// Largest message that can be split into frames
    pub(crate) const MAX_MESSAGE_SIZE: usize = FragmentInfo::MAX_TOTAL as usize * Payload::MAX_SIZE;

    pub(crate) fn split_into_frames(
        data: Vec<u8>,
        message_id: MessageId,
    ) -> Result<Vec<Frame>, FrameError> {
        if data.len() <= Payload::MAX_SIZE {
            // Unwrap safe, data is less or equal to max size
            return Ok(vec![Frame::new(
                FrameHeaders::new(None),
                Payload::try_new(data).unwrap(),
            )]);
        }

        if data.len() > Self::MAX_MESSAGE_SIZE {
            return Err(FrameError::MessageTooLarge {
                actual: data.len(),
                max: Self::MAX_MESSAGE_SIZE,
            });
        }

        // Fits in a u16, it is bound to FragmentInfo::MAX_TOTAL by the check above
        let total = data.len().div_ceil(Payload::MAX_SIZE) as u16;
        let frames = (0..total)
            .zip(data.chunks(Payload::MAX_SIZE))
            .map(|(index, chunk)| {
                // Unwrap safe, 1 < total <= MAX_TOTAL and index < total
                let headers = FrameHeaders::new(Some(
                    FragmentInfo::try_new(message_id, index, total).unwrap(),
                ));
                // Unwrap safe, the chunk is bound to the max payload size
                Frame::new(headers, Payload::try_new(chunk.to_vec()).unwrap())
            })
            .collect();

        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::framing::frame_headers::{FragmentInfo, FrameHeaders, MessageId};

    fn frame_headers() -> FrameHeaders {
        FrameHeaders::new(Some(
            FragmentInfo::try_new(MessageId::new(0), 0, 2).unwrap(),
        ))
    }

    #[test]
    fn validate_encoded_size() {
        let frame = Frame::new(
            frame_headers(),
            Payload::try_new(vec![0; Payload::MAX_SIZE]).unwrap(),
        );
        let encoded = borsh::to_vec(&frame).unwrap();
        assert_eq!(encoded.len(), Frame::MAX_ENCODED_SIZE);
    }

    #[test]
    fn invalid_payload_size() {
        let result = Payload::try_new(vec![0; Payload::MAX_SIZE + 1]);
        assert!(matches!(
            result,
            Err(PayloadError::Size { actual, max })
                if actual == Payload::MAX_SIZE + 1 && max == Payload::MAX_SIZE
        ));
    }

    #[test]
    fn split_into_multiple_frames() {
        // Distinct bytes, so lost or reordered data is detected
        let data: Vec<u8> = (0..=Payload::MAX_SIZE).map(|i| i as u8).collect();
        let message_id = MessageId::new(7);
        let frames = Frame::split_into_frames(data.clone(), message_id).unwrap();
        assert_eq!(frames.len(), 2);

        for (index, frame) in frames.iter().enumerate() {
            let fragment_info = frame.headers.0.as_ref().unwrap();
            assert_eq!(fragment_info.message_id(), message_id);
            assert_eq!(fragment_info.index() as usize, index);
            assert_eq!(fragment_info.total() as usize, frames.len());
        }

        let joined: Vec<u8> = frames
            .into_iter()
            .flat_map(|frame| frame.payload.into_bytes())
            .collect();
        assert_eq!(joined, data);
    }

    #[test]
    fn message_size_limit() {
        let frames =
            Frame::split_into_frames(vec![0; Frame::MAX_MESSAGE_SIZE], MessageId::new(0)).unwrap();
        assert_eq!(frames.len(), FragmentInfo::MAX_TOTAL as usize);

        let result =
            Frame::split_into_frames(vec![0; Frame::MAX_MESSAGE_SIZE + 1], MessageId::new(0));
        assert_eq!(
            result,
            Err(FrameError::MessageTooLarge {
                actual: Frame::MAX_MESSAGE_SIZE + 1,
                max: Frame::MAX_MESSAGE_SIZE,
            })
        );
    }
}
