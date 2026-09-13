use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;

use crate::network::protocols::protocol_id::ProtocolId;

pub(crate) type PacketId = u64;

#[derive(Debug, Error)]
pub(crate) enum PacketError {

    #[error("Packet headers error: {0}")]
    PacketHeaders(#[from] PacketHeadersError),
    
    #[error("Payload is too big: {actual_payload_size} > {max_payload_size}")]
    PayloadTooBig {
        actual_payload_size: usize,
        max_payload_size: usize
    },

    #[error("Failed to serialize with error: {error_message}")]
    Serialize {
        error_message: String
    },

    #[error("Failed to deserialize with error: {error_message}")]
    Deserialize {
        error_message: String
    },
}

#[derive(Debug, Error)]
pub(crate) enum PacketHeadersError {

    #[error("Packet sequence index must be less than sequence total: {sequence_index} >= {sequence_total}")]
    SequenceIndexShouldBeLessThanSequenceTotal {
        sequence_index: u16,
        sequence_total: u16
    },

}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq)]
pub(crate) struct PacketHeader {
    id: PacketId,
    protocol_id: ProtocolId,
    sequence_index: u16,
    sequence_total: u16, 
}

impl PacketHeader {

    pub(crate) const SERIALIZED_SIZE: usize = 
        size_of::<PacketId>() + // id
        size_of::<ProtocolId>() + // protocol_id
        size_of::<u16>() + // sequence_index
        size_of::<u16>(); // sequence_total

    pub(crate) fn new(id: PacketId, protocol_id: ProtocolId, sequence_index: u16, sequence_total: u16) -> Result<PacketHeader, PacketHeadersError> {
        if sequence_index >= sequence_total {
            return Err(
                PacketHeadersError::SequenceIndexShouldBeLessThanSequenceTotal { sequence_index, sequence_total }
            );
        }

        Ok(
            Self {
                id,
                protocol_id,
                sequence_index, 
                sequence_total
            }
        )
    }
    
    pub(crate) fn id(&self) -> PacketId {
        self.id
    }

    pub(crate) fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }
    
    pub(crate) fn sequence_index(&self) -> u16 {
        self.sequence_index
    }

    pub(crate) fn sequence_total(&self) -> u16 {
        self.sequence_total
    }

}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub(crate) struct Packet {
    headers: PacketHeader,
    payload: Vec<u8>
}

impl Packet {
    pub(crate) const MAX_PAYLOAD_SIZE: usize = 512;

    pub(crate) fn try_new(headers: PacketHeader, payload: Vec<u8>) -> Result<Self, PacketError> {
        if payload.len() > Packet::MAX_PAYLOAD_SIZE {
            return Err(
                PacketError::PayloadTooBig { 
                    actual_payload_size: payload.len(),
                    max_payload_size: Packet::MAX_PAYLOAD_SIZE
                }
            )
        }

	Ok(
            Self {
	        headers,
	        payload
	    }
        )
    }

    pub(crate) fn headers(&self) -> &PacketHeader {
	&self.headers
    }

    pub(crate) fn payload(&self) -> &Vec<u8> {
	&self.payload
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        borsh::to_vec(self)
            .expect("Encoding should never fail under normal circumstances, the packet is validated when first created!")
    }

    pub(crate) fn decode(data: &[u8]) -> Result<Packet, PacketError> {
        borsh::from_slice(data)
            .map_err(|err| PacketError::Deserialize { error_message: err.to_string() } )
    }
}

#[cfg(debug_assertions)]
mod tests {
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use super::*;

    #[test]
    fn test_packet_headers_serialized_size_validation_success() {
        let packet_id = rand::random();
        let protocol_id = ProtocolId::DisconnectProtocol;
        let sequence_index = 0;
        let sequence_total = 1;
        let headers = PacketHeader::new(packet_id, protocol_id, sequence_index, sequence_total).expect("Expected Packet Header Ok");
        let headers_encoded = borsh::to_vec(&headers).expect("Expected Encode Ok");
        assert_eq!(headers_encoded.len(), PacketHeader::SERIALIZED_SIZE)
    }

    #[test]
    fn test_packet_headers_creation_success() {
        let packet_id = rand::random();
        let protocol_id = ProtocolId::DisconnectProtocol;
        let sequence_index = 0;
        let sequence_total = 1;
        let headers_result = PacketHeader::new(packet_id, protocol_id, sequence_index, sequence_total);
        assert!(headers_result.is_ok());
        let headers = headers_result.unwrap();
        assert_eq!(headers.id(), packet_id);
        assert_eq!(headers.protocol_id(), protocol_id);
        assert_eq!(headers.sequence_total(), sequence_total);
        assert_eq!(headers.sequence_index(), sequence_index);
    }

    #[test]
    fn test_packet_headers_creation_error() {
        let headers = PacketHeader::new(rand::random(), ProtocolId::DisconnectProtocol, 1, 1);
        assert!(headers.is_err());
    }

    #[test]
    fn test_packet_creation_success() {
        let headers = PacketHeader::new(rand::random(), ProtocolId::DisconnectProtocol, 0, 1).expect("Expected Ok packet headers");
        let payload = vec![0];
        let headers_copy = headers.clone();
        let payload_copy = payload.clone();
	let packet = Packet::try_new(headers, payload).expect("Expected Ok packet");
	assert_eq!(*packet.headers(), headers_copy);
	assert_eq!(*packet.payload(), payload_copy);
    }

    #[test]
    fn test_packet_creation_payload_too_big_error() {
        let headers = PacketHeader::new(rand::random(), ProtocolId::DisconnectProtocol, 0, 1).expect("Expected Ok packet headers");
        let payload = vec![0; Packet::MAX_PAYLOAD_SIZE + 1];
	let packet = Packet::try_new(headers, payload);
        assert!(packet.is_err());
    }

    #[test]
    fn test_packet_encode_success() {
        let headers = PacketHeader::new(rand::random(), ProtocolId::DisconnectProtocol, 0, 1).expect("Expected Ok packet headers");
        let payload = vec![0];
	let packet = Packet::try_new(headers, payload).expect("Expected Ok packet");
        packet.encode();
    }

    #[test]
    fn test_packet_decode_success() {
        let headers = PacketHeader::new(rand::random(), ProtocolId::DisconnectProtocol, 0, 1).expect("Expected Ok packet headers");
        let payload = vec![0];
	let packet = Packet::try_new(headers, payload).expect("Expected Ok packet");
        let encoded_data = packet.encode();

        let decoded_packet = Packet::decode(&encoded_data);
        assert!(decoded_packet.is_ok())
    }

}
