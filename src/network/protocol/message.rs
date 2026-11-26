use crate::network::protocol::{
    authentication::Authentication, conversation_id::ConversationId, disconnect::Disconnect,
    ping::Ping,
};
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) struct Headers {
    pub(crate) conversation_id: ConversationId,
}

/// What is sent over the wire: the protocol plus the headers to route it
#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) struct Message {
    pub(crate) headers: Headers,
    pub(crate) protocol: Protocol,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) enum Protocol {
    Disconnect(Disconnect),
    Ping(Ping),
    Authentication(Authentication),
}

impl Headers {
    pub(crate) fn new(conversation_id: ConversationId) -> Self {
        Self { conversation_id }
    }
}

impl Message {
    pub(crate) fn new(headers: Headers, protocol: Protocol) -> Self {
        Self { headers, protocol }
    }
}
