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
    data: Vec<Payload>,
    total: u16,
}

#[derive(Debug)]
pub(crate) struct Config {
    max_concurrent_reassemblies: u16,
}

#[derive(Debug)]
pub(crate) struct ProtocolReassembler {
    config: Config,
    data: HashMap<MessageId, ReassembledData>,
}

#[derive(Debug, Error)]
pub(crate) enum ReassembledDataError {
    #[error("Borsh error: {0}")]
    Borsh(#[from] std::io::Error),

    #[error("Unexpected fragment index: {0}")]
    FragmentIndex(u16),

    #[error("Unexpected fragment total: {0}")]
    FragmentTotal(u16),
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
    pub(crate) fn try_new(
        fragment_info: FragmentInfo,
        payload: Payload,
    ) -> Result<Self, ReassembledDataError> {
        match fragment_info.index() {
            0 => Ok(Self {
                data: vec![payload],
                total: fragment_info.total(),
            }),
            unexpected => Err(ReassembledDataError::FragmentIndex(unexpected)),
        }
    }

    pub(crate) fn push(
        &mut self,
        fragment_info: FragmentInfo,
        payload: Payload,
    ) -> Result<Option<Dispatcher>, ReassembledDataError> {
        if self.total != fragment_info.total() {
            return Err(ReassembledDataError::FragmentTotal(fragment_info.total()));
        }

        if self.data.len() != fragment_info.index() as usize {
            return Err(ReassembledDataError::FragmentIndex(fragment_info.index()));
        }

        self.data.push(payload);

        let result = if self.data.len() == self.total as usize {
            let payloads = mem::take(&mut self.data);
            Some(borsh::from_slice(
                &payloads
                    .into_iter()
                    .map(Payload::into_bytes)
                    .collect::<Vec<Vec<u8>>>()
                    .concat(),
            )?)
        } else {
            None
        };

        Ok(result)
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
                result => {
                    entry.remove();
                    result?
                }
            },
            Vacant(entry) => {
                if current_concurrent_reassemblies
                    == self.config.max_concurrent_reassemblies as usize
                {
                    Err(ProtocolReassemblerError::ConcurrentReassemblies)?;
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
        framing::frame_headers::{FragmentInfo, FrameHeaders},
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

    fn protocol_reassembler() -> ProtocolReassembler {
        ProtocolReassembler::new(config())
    }

    fn fragment(message_id: u16, index: u16, total: u16, payload: &[u8]) -> Frame {
        let headers = FrameHeaders::new(Some(
            FragmentInfo::try_new(MessageId::new(message_id), index, total).unwrap(),
        ));
        Frame::new(headers, Payload::try_new(payload.to_vec()).unwrap())
    }

    #[test]
    fn fragmented_protocol() {
        let encoded = borsh::to_vec(&Dispatcher::Disconnect {
            protocol: Disconnect::new(),
            headers: Headers::new(ConversationId::new(0)),
        })
        .unwrap();
        let (first, second) = encoded.split_at(1);

        let mut reassembler = protocol_reassembler();

        // More messages than the concurrent limit, so completed ones must free their slot
        for message_id in 0..32 {
            assert!(
                reassembler
                    .push(fragment(message_id, 0, 2, first))
                    .unwrap()
                    .is_none()
            );
            assert!(
                reassembler
                    .push(fragment(message_id, 1, 2, second))
                    .unwrap()
                    .is_some()
            );
        }

        assert!(reassembler.data.is_empty());
    }

    #[test]
    fn fragment_total_mismatch() {
        let mut reassembler = protocol_reassembler();

        assert!(reassembler.push(fragment(0, 0, 2, &[0])).is_ok());

        let result = reassembler.push(fragment(0, 1, 3, &[0]));
        assert!(matches!(
            result,
            Err(ProtocolReassemblerError::ReassembledData(
                ReassembledDataError::FragmentTotal(3)
            ))
        ));
        assert!(reassembler.data.is_empty());
    }

    #[test]
    fn concurrent_reassemblies() {
        let encoded = borsh::to_vec(&Dispatcher::Disconnect {
            protocol: Disconnect::new(),
            headers: Headers::new(ConversationId::new(0)),
        })
        .unwrap();
        let (first, second) = encoded.split_at(1);

        let mut reassembler = protocol_reassembler();
        let limit = config().max_concurrent_reassemblies;

        for message_id in 0..limit {
            assert!(
                reassembler
                    .push(fragment(message_id, 0, 2, first))
                    .unwrap()
                    .is_none()
            );
        }

        let result = reassembler.push(fragment(limit, 0, 2, first));
        assert!(matches!(
            result,
            Err(ProtocolReassemblerError::ConcurrentReassemblies)
        ));

        // Open reassemblies can still finish while full
        for message_id in 0..limit {
            assert!(
                reassembler
                    .push(fragment(message_id, 1, 2, second))
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn wrong_fragment_order() {
        // Every fragment must succeed except the last one, which has an unexpected index
        let sequences = [vec![1], vec![0, 2], vec![0, 0], vec![0, 1, 1]];

        for mut sequence in sequences {
            let mut reassembler = protocol_reassembler();
            let last_index = sequence.pop().unwrap();

            for index in sequence {
                assert!(
                    reassembler
                        .push(fragment(0, index, 3, &[0]))
                        .unwrap()
                        .is_none()
                );
            }

            let result = reassembler.push(fragment(0, last_index, 3, &[0]));
            assert!(
                matches!(result, Err(ProtocolReassemblerError::ReassembledData(ReassembledDataError::FragmentIndex(index))) if index == last_index)
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
            let mut frames = Frame::split_into_frames(encoded, MessageId::new(0));
            let frame: Frame = frames.pop().unwrap();

            let mut reassembler = protocol_reassembler();
            let dispatcher = reassembler.push(frame);

            assert!(dispatcher.is_ok());
            assert!(dispatcher.unwrap().is_some());
        }
    }

    #[test]
    fn invalid_unique_frame_payload() {
        let headers = FrameHeaders::new(None);
        let frame = Frame::new(headers, Payload::try_new(vec![]).unwrap());

        let mut reassembler = protocol_reassembler();
        let dispatcher = reassembler.push(frame);

        assert!(dispatcher.is_err());
    }
}
