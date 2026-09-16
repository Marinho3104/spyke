use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, BorshSerialize, BorshDeserialize, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum ProtocolId {
    DisconnectProtocol,
    AuthenticationProtocol,
}

impl fmt::Display for ProtocolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolId::DisconnectProtocol => write!(f, "Disconnect Protocol"),
            ProtocolId::AuthenticationProtocol => write!(f, "Authentication Protocol"),
        }
    }
}

mod tests {
    use crate::network::protocols::protocol_id::ProtocolId;


    #[test]
    fn test_protocol_id_display() {
        let protocol_ids = [ ProtocolId::DisconnectProtocol, ProtocolId::AuthenticationProtocol ];

        for protocol_id in protocol_ids {
            assert_eq!(
                format!("{}", protocol_id), 
                match protocol_id {
                    ProtocolId::DisconnectProtocol => "Disconnect Protocol",
                    ProtocolId::AuthenticationProtocol => "Authentication Protocol"
                }
            );
        }
    }

}
