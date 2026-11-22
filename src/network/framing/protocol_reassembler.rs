use crate::network::{
    framing::{
        frame::{Frame, Payload},
        frame_headers::{FragmentInfo, MessageId},
    },
    protocol::dispatcher::Dispatcher,
};
use std::{
    collections::{
        HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
    mem,
};
use thiserror::Error;

#[derive(Debug)]
struct ReassembledData {
    data: Vec<u8>,
    next_index: u16,
    total: u16,
}

/// Every fragment except the last must be full and the fragment count is capped, so the
/// worst-case memory per connection is `max_concurrent_reassemblies * Frame::MAX_MESSAGE_SIZE`
/// (e.g. 16 * 64 KiB = 1 MiB)
#[derive(Debug)]
pub(crate) struct Config {
    max_concurrent_reassemblies: u16,
}

#[derive(Debug)]
pub(crate) struct ProtocolReassembler {
    config: Config,
    data: HashMap<MessageId, ReassembledData>,
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum ReassembledDataError {
    #[error("Unexpected fragment index: {0}")]
    Index(u16),

    #[error("Unexpected fragment total: {0}")]
    Total(u16),

    #[error("Unexpected fragment size: fragment {index} has {size} bytes")]
    Size { index: u16, size: usize },
}

#[derive(Debug, Error)]
pub(crate) enum ProtocolReassemblerError {
    #[error("Borsh error: {0}")]
    Borsh(#[from] std::io::Error),

    #[error("Reassembled data error: {0}")]
    ReassembledData(#[from] ReassembledDataError),

    #[error("Max number of concurrent reassemblies reached")]
    ConcurrentReassemblies,
}

impl ReassembledData {
    fn try_new(
        fragment_info: FragmentInfo,
        payload: Payload,
    ) -> Result<Self, ReassembledDataError> {
        let mut slf = Self {
            data: Vec::new(),
            next_index: 0,
            total: fragment_info.total(),
        };
        // A fragment can never complete a new reassembly, since the total is always > 1
        slf.push(fragment_info, payload)?;
        Ok(slf)
    }

    /// Returns the reassembled bytes once the last fragment is pushed
    fn push(
        &mut self,
        fragment_info: FragmentInfo,
        payload: Payload,
    ) -> Result<Option<Vec<u8>>, ReassembledDataError> {
        let index = fragment_info.index();

        if self.total != fragment_info.total() {
            return Err(ReassembledDataError::Total(fragment_info.total()));
        }

        if self.next_index != index {
            return Err(ReassembledDataError::Index(index));
        }

        // Every fragment but the last must be full, and the last one can't be empty
        let size = payload.as_bytes().len();
        let is_last = index + 1 == self.total;
        let valid_size = if is_last {
            size > 0
        } else {
            size == Payload::MAX_SIZE
        };
        if !valid_size {
            return Err(ReassembledDataError::Size { index, size });
        }

        self.data.extend_from_slice(payload.as_bytes());
        self.next_index += 1;

        Ok(is_last.then(|| mem::take(&mut self.data)))
    }
}

impl Config {
    pub(crate) fn new(max_concurrent_reassemblies: u16) -> Self {
        Self {
            max_concurrent_reassemblies,
        }
    }
}

impl ProtocolReassembler {
    pub(crate) fn new(config: Config) -> Self {
        Self {
            config,
            data: HashMap::new(),
        }
    }

    fn handle_fragmented_frame(
        &mut self,
        fragment_info: FragmentInfo,
        payload: Payload,
    ) -> Result<Option<Dispatcher>, ProtocolReassemblerError> {
        let current_concurrent_reassemblies = self.data.len();

        let result = match self.data.entry(fragment_info.message_id()) {
            Occupied(mut entry) => match entry.get_mut().push(fragment_info, payload) {
                Ok(None) => None,
                // Finished or failed, either way the reassembly is over
                result => {
                    entry.remove();
                    match result? {
                        Some(bytes) => Some(borsh::from_slice(&bytes)?),
                        None => None,
                    }
                }
            },
            Vacant(entry) => {
                if current_concurrent_reassemblies
                    == self.config.max_concurrent_reassemblies as usize
                {
                    return Err(ProtocolReassemblerError::ConcurrentReassemblies);
                }
                entry.insert(ReassembledData::try_new(fragment_info, payload)?);
                None
            }
        };

        Ok(result)
    }

    fn handle_unique_frame(
        payload: Payload,
    ) -> Result<Option<Dispatcher>, ProtocolReassemblerError> {
        Ok(Some(borsh::from_slice(payload.as_bytes())?))
    }

    pub(crate) fn push(
        &mut self,
        frame: Frame,
    ) -> Result<Option<Dispatcher>, ProtocolReassemblerError> {
        match frame.headers.0 {
            Some(fragment_info) => self.handle_fragmented_frame(fragment_info, frame.payload),
            None => Self::handle_unique_frame(frame.payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{
        framing::frame_headers::FrameHeaders,
        protocol::{
            conversation_id::ConversationId,
            disconnect::Disconnect,
            dispatcher::Headers,
            ping::{Content as PingContent, Ping},
        },
    };

    fn config() -> Config {
        Config::new(16)
    }

    fn reassembler() -> ProtocolReassembler {
        ProtocolReassembler::new(config())
    }

    fn fragment_info(message_id: u16, index: u16, total: u16) -> FragmentInfo {
        FragmentInfo::try_new(MessageId::new(message_id), index, total).unwrap()
    }

    fn payload(size: usize) -> Payload {
        Payload::try_new(vec![0; size]).unwrap()
    }

    /// Size that passes the size check for the fragment position
    fn valid_size(index: u16, total: u16) -> usize {
        if index + 1 == total {
            1
        } else {
            Payload::MAX_SIZE
        }
    }

    fn fragment_with_size(message_id: u16, index: u16, total: u16, size: usize) -> Frame {
        let headers = FrameHeaders::new(Some(fragment_info(message_id, index, total)));
        Frame::new(headers, payload(size))
    }

    fn fragment(message_id: u16, index: u16, total: u16) -> Frame {
        fragment_with_size(message_id, index, total, valid_size(index, total))
    }

    fn reassembled_data_error(
        result: Result<Option<Dispatcher>, ProtocolReassemblerError>,
    ) -> ReassembledDataError {
        match result {
            Err(ProtocolReassemblerError::ReassembledData(error)) => error,
            other => panic!("expected a reassembled data error, got {other:?}"),
        }
    }

    // No Dispatcher spans more than one frame yet, so a completed fragmented message is never a
    // valid Dispatcher. The slot is freed the same way whether the decoding succeeds or fails.
    fn assert_completes_with_decode_error(reassembler: &mut ProtocolReassembler, frame: Frame) {
        assert!(matches!(
            reassembler.push(frame),
            Err(ProtocolReassemblerError::Borsh(_))
        ));
    }

    #[test]
    fn reassembled_data_joins_fragments() {
        // Distinct bytes, so lost or reordered data is detected
        let data: Vec<u8> = (0..2 * Payload::MAX_SIZE + 10).map(|i| i as u8).collect();
        let mut frames = Frame::split_into_frames(data.clone(), MessageId::new(0))
            .unwrap()
            .into_iter();
        assert_eq!(frames.len(), 3);

        let first = frames.next().unwrap();
        let mut reassembled =
            ReassembledData::try_new(first.headers.0.unwrap(), first.payload).unwrap();

        let second = frames.next().unwrap();
        let result = reassembled.push(second.headers.0.unwrap(), second.payload);
        assert_eq!(result, Ok(None));

        let last = frames.next().unwrap();
        let result = reassembled.push(last.headers.0.unwrap(), last.payload);
        assert_eq!(result, Ok(Some(data)));
    }

    #[test]
    fn last_fragment_sizes() {
        // The last fragment may be anything from 1 byte to a full payload
        for size in [1, Payload::MAX_SIZE] {
            let mut reassembled =
                ReassembledData::try_new(fragment_info(0, 0, 2), payload(Payload::MAX_SIZE))
                    .unwrap();

            let result = reassembled.push(fragment_info(0, 1, 2), payload(size));
            assert_eq!(result.unwrap().unwrap().len(), Payload::MAX_SIZE + size);
        }
    }

    #[test]
    fn completed_reassembly_frees_slot() {
        let mut reassembler = reassembler();

        // More messages than the concurrent limit, so completed ones must free their slot
        for message_id in 0..32 {
            assert!(
                reassembler
                    .push(fragment(message_id, 0, 2))
                    .unwrap()
                    .is_none()
            );
            assert_completes_with_decode_error(&mut reassembler, fragment(message_id, 1, 2));
        }

        assert!(reassembler.data.is_empty());
    }

    #[test]
    fn concurrent_reassemblies() {
        let mut reassembler = reassembler();
        let limit = config().max_concurrent_reassemblies;

        for message_id in 0..limit {
            assert!(
                reassembler
                    .push(fragment(message_id, 0, 2))
                    .unwrap()
                    .is_none()
            );
        }

        let result = reassembler.push(fragment(limit, 0, 2));
        assert!(matches!(
            result,
            Err(ProtocolReassemblerError::ConcurrentReassemblies)
        ));

        // Open reassemblies can still finish while full
        for message_id in 0..limit {
            assert_completes_with_decode_error(&mut reassembler, fragment(message_id, 1, 2));
        }
        assert!(reassembler.data.is_empty());

        // And the freed slots accept new reassemblies
        assert!(reassembler.push(fragment(limit, 0, 2)).unwrap().is_none());
    }

    #[test]
    fn fragment_total_mismatch() {
        let mut reassembler = reassembler();

        assert!(reassembler.push(fragment(0, 0, 2)).unwrap().is_none());

        let result = reassembler.push(fragment(0, 1, 3));
        assert_eq!(
            reassembled_data_error(result),
            ReassembledDataError::Total(3)
        );
        assert!(reassembler.data.is_empty());
    }

    #[test]
    fn wrong_fragment_order() {
        // Every fragment must succeed except the last one, which has an unexpected index
        let sequences = [vec![1], vec![0, 2], vec![0, 0], vec![0, 1, 1]];

        for mut sequence in sequences {
            let mut reassembler = reassembler();
            let last_index = sequence.pop().unwrap();

            for index in sequence {
                assert!(reassembler.push(fragment(0, index, 3)).unwrap().is_none());
            }

            let result = reassembler.push(fragment(0, last_index, 3));
            assert_eq!(
                reassembled_data_error(result),
                ReassembledDataError::Index(last_index)
            );
            assert!(reassembler.data.is_empty());
        }
    }

    #[test]
    fn wrong_fragment_size() {
        let full = Payload::MAX_SIZE;
        // (index, size) steps of a 3 fragment message, every step must succeed except the last
        let sequences = [
            // Short first fragment
            vec![(0, full - 1)],
            // Empty first fragment
            vec![(0, 0)],
            // Short middle fragment
            vec![(0, full), (1, 1)],
            // Empty last fragment
            vec![(0, full), (1, full), (2, 0)],
        ];

        for mut sequence in sequences {
            let mut reassembler = reassembler();
            let (last_index, last_size) = sequence.pop().unwrap();

            for (index, size) in sequence {
                let frame = fragment_with_size(0, index, 3, size);
                assert!(reassembler.push(frame).unwrap().is_none());
            }

            let result = reassembler.push(fragment_with_size(0, last_index, 3, last_size));
            assert_eq!(
                reassembled_data_error(result),
                ReassembledDataError::Size {
                    index: last_index,
                    size: last_size
                }
            );
            assert!(reassembler.data.is_empty());
        }
    }

    #[test]
    fn non_fragmented_protocol() {
        let protocols = [
            Dispatcher::Disconnect {
                protocol: Disconnect::new(),
                headers: Headers::new(ConversationId::new(0)),
            },
            Dispatcher::Ping {
                protocol: Ping::new(PingContent::Ping),
                headers: Headers::new(ConversationId::new(0)),
            },
            Dispatcher::Ping {
                protocol: Ping::new(PingContent::Pong),
                headers: Headers::new(ConversationId::new(0)),
            },
        ];

        for protocol in protocols {
            let encoded = borsh::to_vec(&protocol).unwrap();
            let mut frames = Frame::split_into_frames(encoded.clone(), MessageId::new(0)).unwrap();
            let frame: Frame = frames.pop().unwrap();
            assert!(frames.is_empty());

            let mut reassembler = reassembler();
            let dispatcher = reassembler.push(frame).unwrap().unwrap();

            // Dispatcher has no PartialEq, so compare its encoding instead
            assert_eq!(borsh::to_vec(&dispatcher).unwrap(), encoded);
        }
    }

    #[test]
    fn invalid_unique_frame_payload() {
        let headers = FrameHeaders::new(None);
        let frame = Frame::new(headers, Payload::try_new(vec![]).unwrap());

        let mut reassembler = reassembler();
        let dispatcher = reassembler.push(frame);

        assert!(matches!(
            dispatcher,
            Err(ProtocolReassemblerError::Borsh(_))
        ));
    }
}
