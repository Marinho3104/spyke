use thiserror::Error;
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Encoder, LengthDelimitedCodec},
};

use crate::network::{
    framing::{
        frame::{Frame, FrameError},
        frame_headers::MessageId,
    },
    protocol::dispatcher::Dispatcher,
};

#[derive(Debug)]
pub(crate) struct Codec {
    inner: LengthDelimitedCodec,
    next_message_id: u16,
}

#[derive(Debug, Error)]
pub(crate) enum DecoderError {
    /// Reading from the underlying socket failed. Never produced by the codec itself: the
    /// Decoder trait requires `From<io::Error>` so `FramedRead` can report read errors with it
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The length prefix is invalid, e.g. a frame bigger than `Frame::MAX_ENCODED_SIZE`.
    /// The peer is sending a malformed stream
    #[error("LengthDelimitedCodec error: {0}")]
    LengthDelimitedCodecError(std::io::Error),

    /// The length-delimited bytes are not a valid `Frame`
    #[error("Deserialization failed with error: {0}")]
    Deserialize(std::io::Error),
}

#[derive(Debug, Error)]
pub(crate) enum EncoderError {
    /// Writing to the underlying socket failed. Never produced by the codec itself: the
    /// Encoder trait requires `From<io::Error>` so `FramedWrite` can report write errors with it
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The inner length-delimited encoding failed, e.g. a frame bigger than
    /// `Frame::MAX_ENCODED_SIZE`. Not expected, since frames are bound to that size
    #[error("LengthDelimitedCodec error: {0}")]
    LengthDelimitedCodecError(std::io::Error),

    /// A `Frame` or `Dispatcher` could not be serialized with borsh
    #[error("Serialization failed with error: {0}")]
    Serialize(std::io::Error),

    /// The message could not be split into frames, e.g. it is bigger than
    /// `Frame::MAX_MESSAGE_SIZE`
    #[error("Frame error: {0}")]
    Frame(#[from] FrameError),
}

impl Codec {
    pub(crate) fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .length_field_type::<u32>()
                .max_frame_length(Frame::MAX_ENCODED_SIZE)
                .new_codec(),
            next_message_id: 0,
        }
    }
}

impl Decoder for Codec {
    type Item = Frame;
    type Error = DecoderError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        let decoded_frame = self
            .inner
            .decode(src)
            .map_err(DecoderError::LengthDelimitedCodecError)?;

        let result: Option<Frame> = match decoded_frame {
            Some(decoded_frame) => Some(
                borsh::from_slice::<Frame>(&decoded_frame).map_err(DecoderError::Deserialize)?,
            ),
            None => None,
        };

        Ok(result)
    }
}

impl Encoder<Frame> for Codec {
    type Error = EncoderError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_frame = borsh::to_vec(&item).map_err(EncoderError::Serialize)?;

        self.inner
            .encode(encoded_frame.into(), dst)
            .map_err(EncoderError::LengthDelimitedCodecError)?;

        Ok(())
    }
}

impl Encoder<Vec<Frame>> for Codec {
    type Error = EncoderError;

    fn encode(&mut self, item: Vec<Frame>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        for frame in item {
            self.encode(frame, dst)?;
        }

        Ok(())
    }
}

impl Encoder<Dispatcher> for Codec {
    type Error = EncoderError;

    fn encode(&mut self, item: Dispatcher, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_dispatcher = borsh::to_vec(&item).map_err(EncoderError::Serialize)?;

        // Sequential ids per connection, so a message id is only reused after wrapping around
        let message_id = MessageId::new(self.next_message_id);
        let frames = Frame::split_into_frames(encoded_dispatcher, message_id)?;
        // Only consume the id once the message is accepted
        self.next_message_id = self.next_message_id.wrapping_add(1);

        self.encode(frames, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{
        framing::frame_headers::FragmentInfo,
        protocol::{conversation_id::ConversationId, disconnect::Disconnect, dispatcher::Headers},
    };
    use tokio_util::bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let dispatcher = Dispatcher::Disconnect {
            protocol: Disconnect::new(),
            headers: Headers::new(ConversationId::new(0)),
        };

        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();

        codec.encode(dispatcher, &mut buffer).unwrap();

        codec.decode(&mut buffer).unwrap().unwrap();
    }

    #[test]
    fn encode_decode_max_size_message() {
        // Distinct bytes, so lost or reordered data is detected
        let data: Vec<u8> = (0..Frame::MAX_MESSAGE_SIZE).map(|i| i as u8).collect();
        let frames = Frame::split_into_frames(data.clone(), MessageId::new(3)).unwrap();
        assert_eq!(frames.len(), FragmentInfo::MAX_TOTAL as usize);

        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();
        codec.encode(frames.clone(), &mut buffer).unwrap();

        let mut decoded = Vec::new();
        while let Some(frame) = codec.decode(&mut buffer).unwrap() {
            decoded.push(frame);
        }
        assert!(buffer.is_empty());
        assert_eq!(decoded, frames);
    }

    #[test]
    fn decode_errors() {
        // Length prefix bigger than the max frame size
        let mut buffer = BytesMut::from(&[0xFF, 0xFF, 0xFF, 0xFF][..]);
        assert!(matches!(
            Codec::new().decode(&mut buffer),
            Err(DecoderError::LengthDelimitedCodecError(_))
        ));

        // Valid length, but the bytes are not a Frame (invalid Option tag)
        let mut buffer = BytesMut::from(&[0, 0, 0, 1, 0xFF][..]);
        assert!(matches!(
            Codec::new().decode(&mut buffer),
            Err(DecoderError::Deserialize(_))
        ));
    }

    #[test]
    fn invalid_fragment_info() {
        // (index, total, valid): the valid pair proves the raw layout below matches a Frame
        let max = FragmentInfo::MAX_TOTAL;
        let fragments: [(u16, u16, bool); 8] = [
            (0, 2, true),
            (0, max, true),
            (0, 0, false),
            (0, 1, false),
            (1, 1, false),
            (2, 1, false),
            (2, 2, false),
            (0, max + 1, false),
        ];

        for (index, total, valid) in fragments {
            // Raw Frame bytes, as a peer could send them: Some tag, message_id, index, total, payload
            let frame_bytes = borsh::to_vec(&(1u8, 0u16, index, total, vec![0u8; 10])).unwrap();

            let mut codec = Codec::new();
            let mut buffer = BytesMut::new();
            codec.inner.encode(frame_bytes.into(), &mut buffer).unwrap();

            let result = codec.decode(&mut buffer);
            if valid {
                assert!(result.unwrap().is_some());
            } else {
                assert!(matches!(result, Err(DecoderError::Deserialize(_))));
            }
        }
    }
}
