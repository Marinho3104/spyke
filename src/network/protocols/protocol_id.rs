use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, BorshSerialize, BorshDeserialize, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum ProtocolId {
    DisconnectProtocol,
}

impl fmt::Display for ProtocolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolId::DisconnectProtocol => write!(f, "Disconnect Protocol"),
        }
    }
}

mod tests {
    use crate::network::protocols::protocol_id::ProtocolId;


    #[test]
    fn test_protocol_id_display() {
        let protocol_id = ProtocolId::DisconnectProtocol;
        assert_eq!(format!("{}", protocol_id), "Disconnect Protocol");
    }

}
