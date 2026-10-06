use thiserror::Error;
use tokio_util::{bytes::BytesMut, codec::{Decoder, Encoder, LengthDelimitedCodec, LengthDelimitedCodecError}};

use crate::network::{framing::frame::Frame, protocol::dispatcher::{Dispatcher, DispatcherTryFromPayloadsError}};


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

impl Encoder<Dispatcher> for Codec {
    type Error = EncoderError;

    fn encode(&mut self, item: Dispatcher, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let frames: Vec<Frame> = item.into();
        self.encode(frames, dst)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::BytesMut;
    use crate::network::{framing::{frame::{Frame, Payload}, frame_headers::{FragmentInfo, FrameHeaders, MessageId, RawProtocolId}}, protocol::{disconnect::Disconnect, protocol_id::ProtocolId, spec::Spec}};

    #[test]
    fn encode_decode() {
        let dispatcher = Dispatcher::Disconnect(Disconnect);

        let mut codec = Codec::new();
        let mut buffer = BytesMut::new();

        codec.encode(dispatcher, &mut buffer).unwrap();

        let decoded_frame = codec.decode(&mut buffer).unwrap().unwrap();
        let raw_protocol_id = decoded_frame.headers.raw_protocol_id;
        let protocol_id: ProtocolId = raw_protocol_id.try_into().unwrap();

        assert_eq!(protocol_id, Disconnect::PROTOCOL_ID);
        assert!(decoded_frame.payload.0.len() == 0)
    }
}
