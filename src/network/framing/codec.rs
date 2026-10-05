use thiserror::Error;
use tokio_util::{bytes::BytesMut, codec::{Decoder, Encoder, LengthDelimitedCodec, LengthDelimitedCodecError}};

use crate::network::framing::frame::Frame;


#[derive(Debug)]
pub(crate) struct Codec {
    inner: LengthDelimitedCodec,
}


#[derive(Debug, Error)]
pub(crate) enum DecoderError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("LengthDelimitedCodec error: {0}")]
    LengthDelimitedCodecError(#[from] LengthDelimitedCodecError),

    #[error("Deserialization failed with error: {0}")]
    Deserialize(String),
}

#[derive(Debug, Error)]
pub(crate) enum EncoderError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("LengthDelimitedCodec error: {0}")]
    LengthDelimitedCodecError(#[from] LengthDelimitedCodecError),

    #[error("Serialization failed with error: {0}")]
    Serialize(String),
}


impl Codec {
    pub(crate) fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .length_field_type::<u32>()
                .max_frame_length(Frame::MAX_ENCODED_SIZE)
                .new_codec(),
        }
    }
}

impl Decoder for Codec {
    type Item = Frame;
    type Error = DecoderError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        let decoded_frame = self.inner.decode(src)?;
    
        let result: Option<Frame> = match decoded_frame {
            Some(decoded_frame) => Some(
                borsh::from_slice(&decoded_frame)
                    .map_err(|err| DecoderError::Deserialize(err.to_string()))?
                ),
            None => None
        };

        Ok(result)
    }
}

impl Encoder<Frame> for Codec {
    type Error = EncoderError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_frame = borsh::to_vec(&item)
            .map_err(|err| EncoderError::Serialize(err.to_string()))?;

        self.inner.encode(encoded_frame.into(), dst)?;

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


#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::BytesMut;
    use crate::network::framing::{frame::{Frame, Payload}, frame_headers::{FragmentInfo, FrameHeaders, MessageId, RawProtocolId}};


    fn frame(protocol_id: RawProtocolId) -> Frame {
        let frame_headers = FrameHeaders::new(protocol_id, Some(FragmentInfo::try_new(MessageId::new(0), 0, 1).unwrap()));
        Frame::new(frame_headers, Payload::try_new(vec![0; Payload::MAX_SIZE]).unwrap())
    }

    #[test]
    fn encode_decode() {
        let frames = vec![frame(RawProtocolId::new(0)), frame(RawProtocolId::new(1))];
        let frame1 = frames.first().unwrap().clone();
        let frame2 = frames.get(1).unwrap().clone();

        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();

        codec.encode(frames, &mut buffer).unwrap();

        let decoded_frame1 = codec.decode(&mut buffer).unwrap().unwrap();
        let decoded_frame2 = codec.decode(&mut buffer).unwrap().unwrap();

        assert_eq!(decoded_frame1, frame1);
        assert_eq!(decoded_frame2, frame2);
    }
}
