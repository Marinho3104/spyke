use crate::network::protocol::{
    conversation_id::ConversationId, disconnect::Disconnect, ping::Ping,
};
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) struct Headers {
    pub(crate) conversation_id: ConversationId,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) enum Dispatcher {
    Disconnect {
        headers: Headers,
        protocol: Disconnect,
    },
    Ping {
        headers: Headers,
        protocol: Ping,
    },
}

impl Headers {
    pub(crate) fn new(conversation_id: ConversationId) -> Self {
        Self { conversation_id }
    }
}

impl Dispatcher {
    pub(crate) fn headers(&self) -> &Headers {
        match self {
            Self::Disconnect { headers, .. } | Self::Ping { headers, .. } => headers,
        }
    }
}
