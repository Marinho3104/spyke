use std::{collections::{HashMap, hash_map::Entry::{Occupied, Vacant}}, mem};

use thiserror::Error;

use crate::network::{framing::{frame::{Frame, Payload}, frame_headers::MessageId}, protocol::{dispatcher::{Dispatcher, DispatcherTryFromPayloadsError}, protocol_id::{ProtocolId, ProtocolIdTryFromError}}};

#[derive(Debug)]
struct ReassembledData {
    data: Vec<Payload>,
    protocol_id: ProtocolId,
    total: u16,
}

#[derive(Debug)]
pub(crate) struct ProtocolReassemblerConfig {
    max_concurrent_reassemblies: u16,
}

#[derive(Debug)]
pub(crate) struct ProtocolReassembler {
    config: ProtocolReassemblerConfig,
    data: HashMap<MessageId, ReassembledData>   
}

#[derive(Debug, Error)]
pub(crate) enum ReassembledDataError {
    #[error("Protocol id error: {0}")]
    ProtocolIdTryFromError(#[from] ProtocolIdTryFromError),

    #[error("Dispatcher try from payloads error: {0}")]
    DispatcherTryFromPayloadsError(#[from] DispatcherTryFromPayloadsError),

    #[error("Unexpected fragment index: {0}")]
    FragmentIndex(u16),
}

#[derive(Debug, Error)]
pub(crate) enum ProtocolReassemblerError {
    #[error("Reassembled data error: {0}")]
    ReassembledData(#[from] ReassembledDataError),

    #[error("Protocol id error: {0}")]
    ProtocolIdTryFromError(#[from] ProtocolIdTryFromError),

    #[error("Dispatcher try from payloads error: {0}")]
    DispatcherTryFromPayloadsError(#[from] DispatcherTryFromPayloadsError),

    #[error("Max number of concurrent reassemblies reached")]
    ConcurrentReassemblies,
}


impl ReassembledData {
    pub(crate) fn try_new(frame: Frame) -> Result<Self, ReassembledDataError> {
        // This is safe because only fragmented frames can reach this point
        let fragment_info = frame.headers.fragment_info.unwrap();

        match fragment_info.index {
            0 => Ok(
                Self {
                    data: vec![frame.payload],
                    protocol_id: frame.headers.raw_protocol_id.try_into()?,
                    total: fragment_info.total
                }
            ),
            unexpected => Err(ReassembledDataError::FragmentIndex(unexpected))
        }
    }

    pub(crate) fn push(&mut self, frame: Frame) -> Result<Option<Dispatcher>, ReassembledDataError> {
        // This is safe because only fragmented frames can reach this point
        let fragment_info = frame.headers.fragment_info.unwrap();

        if self.data.len() != fragment_info.index as usize {
            return Err(ReassembledDataError::FragmentIndex(fragment_info.index))
        }

        self.data.push(frame.payload);

        if self.data.len() == self.total as usize {
            let payloads = mem::take(&mut self.data);
            Ok(Some(Dispatcher::try_from_payloads(payloads, self.protocol_id)?))
        } else {
            Ok(None)
        }
    }
}

impl ProtocolReassemblerConfig {
    pub(crate) fn new(max_concurrent_reassemblies: u16) -> Self {
        Self {
            max_concurrent_reassemblies
        }
    }
}

impl ProtocolReassembler {
    pub(crate) fn new(config: ProtocolReassemblerConfig) -> Self {
        Self {
            config,
            data: HashMap::new(),
        }
    }

    fn handle_fragmented_frame(&mut self, frame: Frame) -> Result<Option<Dispatcher>, ProtocolReassemblerError> {
        // This is safe because only fragmented frames can reach this point
        let message_id = frame.headers.fragment_info.as_ref().unwrap().message_id;

        let current_concurrent_reassemblies = self.data.len();

        let result = match self.data.entry(message_id) {
            Occupied(mut entry) => entry.get_mut().push(frame).inspect_err(|_| { entry.remove(); })?,
            Vacant(entry) => {
                if current_concurrent_reassemblies == self.config.max_concurrent_reassemblies as usize {
                    Err(ProtocolReassemblerError::ConcurrentReassemblies)?;
                }
                entry.insert(ReassembledData::try_new(frame)?);
                None
            }
        };
       
        Ok(result)
    }

    fn handle_unique_frame(&mut self, frame: Frame) -> Result<Option<Dispatcher>, ProtocolReassemblerError> {
        let protocol_id = ProtocolId::try_from(frame.headers.raw_protocol_id)?;
        Ok(Some(Dispatcher::try_from_payloads(vec![frame.payload], protocol_id)?))
    }

    pub(crate) fn push(&mut self, frame: Frame) -> Result<Option<Dispatcher>, ProtocolReassemblerError> {
        match frame.headers.fragment_info {
            Some(_) => self.handle_fragmented_frame(frame),
             None => self.handle_unique_frame(frame),
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::network::{framing::frame_headers::{FrameHeaders, RawProtocolId}, protocol::disconnect::Disconnect};

use super::*;

    fn config() -> ProtocolReassemblerConfig {
        ProtocolReassemblerConfig::new(16)
    }

    fn protocol_reassembler() -> ProtocolReassembler {
        ProtocolReassembler::new(config())
    }

    #[test]
    fn non_fragmented_protocol() {
        let protocol = Dispatcher::Disconnect(Disconnect);
        let mut frames: Vec<Frame> = protocol.into();
        let frame: Frame = frames.pop().unwrap();

        let mut reassembler = protocol_reassembler();
        let dispatcher = reassembler.push(frame);

        assert!(dispatcher.is_ok());
        assert!(dispatcher.unwrap().is_some());
    }

    #[test]
    fn invalid_raw_protocol_id() {
        let headers = FrameHeaders::new(RawProtocolId(100), None);
        let frame = Frame::new(headers, Payload(vec![]));

        let mut reassembler = protocol_reassembler();
        let dispatcher = reassembler.push(frame);

        assert!(dispatcher.is_err());
    }
}
