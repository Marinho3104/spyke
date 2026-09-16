use std::collections::{HashMap, hash_map::Entry};

use crate::network::{packet::packet::{Packet, PacketId}, protocols::protocol_dispatcher::ProtocolDispatcher};
use thiserror::Error;


pub(crate) const MAX_CONCURRENT_REASSEMBLIES: usize = 16;

#[derive(Debug, Error)]
pub(crate) enum ProtocolReassemblerError {

    #[error("Max number of concurrent reassemblies reached!")]
    MaxNumberOfConcurrentReassembliesReached {},

    #[error("The given packet dont match the sequence total with the ones already stored with the same id: {existing_sequence_total} != {given_sequence_total}")]
    SequenceTotalDontMatchExistingSequenceTotalForSameId {
        given_sequence_total: u16,
        existing_sequence_total: u16,
    },

    #[error("Given packet sequence index dont match the expected one for same id")]
    UnexpectedSequenceIndex {
        given_index: u16,
        expected_index: u16
    },

    #[error("Sequence total provided by the packet is greater than the max defined for this protocol: {given} > {defined}")]
    SequenceTotalProvidedIsGreaterThanMaxDefined {
        given: u16,
        defined: u16,
    },


    #[error("Reassemble protocol issue: {error_message}")]
    ReassembleProtocolIssue {
        error_message: String
    },



}

pub(crate) struct ReassemblyProtocolData {
    packets: Vec<Packet>,
    sequence_total: u16,
}

impl ReassemblyProtocolData {

    pub(crate) fn new(packet: Packet) -> Self {
        Self {
            sequence_total: packet.headers().sequence_total(),
            packets: vec![packet],
        }
    }
    
    pub(crate) fn packets(&self) -> &Vec<Packet> {
        &self.packets
    }

    pub(crate) fn sequence_total(&self) -> u16 {
        self.sequence_total
    }

    pub(crate) fn push_packet(&mut self, packet: Packet) {
        self.packets.push(packet);
    }

    pub(crate) fn take_packets(self) -> Vec<Packet> {
        self.packets
    }

}

pub(crate) struct ProtocolReassembler {
    data: HashMap<PacketId, ReassemblyProtocolData>,
}

impl ProtocolReassembler {

    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new()
        }
    }

    pub(crate) fn push(&mut self, packet: Packet) -> Result<Option<ProtocolDispatcher>, ProtocolReassemblerError> {

        if self.data.len() == MAX_CONCURRENT_REASSEMBLIES {
            return Err(
                ProtocolReassemblerError::MaxNumberOfConcurrentReassembliesReached {  }
            );
        }

        let reassembly_protocol_data_full: Option<ReassemblyProtocolData> = match self.data.entry(packet.headers().id()) {
            Entry::Occupied(mut entry) => {
                let reassembly_protocol_data = entry.get_mut();

                if reassembly_protocol_data.sequence_total() != packet.headers().sequence_total() {
                    return Err(
                        ProtocolReassemblerError::SequenceTotalDontMatchExistingSequenceTotalForSameId { 
                            given_sequence_total: packet.headers().sequence_total(), 
                            existing_sequence_total: reassembly_protocol_data.sequence_total() 
                        }
                    )
                }

                if reassembly_protocol_data.packets().len() != packet.headers().sequence_index() as usize {
                    return Err(
                        ProtocolReassemblerError::UnexpectedSequenceIndex { 
                            expected_index: reassembly_protocol_data.packets().len() as u16, 
                            given_index: packet.headers().sequence_index() 
                        }
                    )
                }

                reassembly_protocol_data.push_packet(packet);

                if reassembly_protocol_data.packets().len() == reassembly_protocol_data.sequence_total() as usize {
                    Some( entry.remove() )
                } else {
                    None
                }
            },
            Entry::Vacant(entry) => {
                let sequence_total = packet.headers().sequence_total(); 
                
                if sequence_total > ProtocolDispatcher::max_sequence_total(packet.headers().protocol_id()) {
                    return Err(
                        ProtocolReassemblerError::SequenceTotalProvidedIsGreaterThanMaxDefined { 
                            given: sequence_total, 
                            defined: ProtocolDispatcher::max_sequence_total(packet.headers().protocol_id()) 
                        }
                    )
                }

                let reassembly_protocol_data = ReassemblyProtocolData::new(packet);
                
                if sequence_total == 1 {
                    Some(reassembly_protocol_data)
                } else {
                    entry.insert(reassembly_protocol_data);
                    None
                }
            }
        };

        match reassembly_protocol_data_full {
            None => Ok( None ),
            Some( reassembly_protocol_data_full ) => {
                let reassembled_protocol = ProtocolDispatcher::try_from(reassembly_protocol_data_full.take_packets())
                    .map_err(|err| ProtocolReassemblerError::ReassembleProtocolIssue { error_message: err.to_string() })?;
                Ok( Some( reassembled_protocol ) )
            }
        }
    }
}

#[cfg(debug_assertions)]
mod tests {

use crate::network::{packet::packet::PacketHeader, protocols::{disconnect_protocol::DisconnectProtocol, protocol_id::ProtocolId}};

use super::*;

    #[test]
    fn test_push_packet_get_protocol_success() {
        let mut reassembler = ProtocolReassembler::new();

        let disconnect_protocol = DisconnectProtocol {};
        let mut packets: Vec<Packet> = disconnect_protocol.into();

        let result = reassembler.push(packets.pop().expect("Expected Ok packet"));
        assert!(matches!(result, Ok(Some(ProtocolDispatcher::DisconnectProtocol(..)))));
    }

    #[test]
    fn test_packet_wrong_sequence_total_error() {
        let mut reassembler = ProtocolReassembler::new();

        let headers = PacketHeader::new(rand::random(), ProtocolId::DisconnectProtocol, 0, 2).expect("Expected Ok packet headers");
	let payload = vec![0; 0];
	let packet = Packet::try_new(headers, payload).expect("Expected Ok packet");

        let result = reassembler.push(packet);
        assert!(matches!(result, Err(ProtocolReassemblerError::SequenceTotalProvidedIsGreaterThanMaxDefined { .. })));
    }
}
