use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, Copy, PartialEq, BorshSerialize, BorshDeserialize, Eq, Hash)]
pub(crate) struct ConversationId(pub(crate) u16);

/// Which half of the id range a side uses to start conversations
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ConversationIdParity {
    Even,
    Odd,
}

impl ConversationId {
    pub(crate) fn new(conversation_id: u16) -> Self {
        Self(conversation_id)
    }

    pub(crate) fn first(parity: ConversationIdParity) -> Self {
        Self(match parity {
            ConversationIdParity::Even => 0,
            ConversationIdParity::Odd => 1,
        })
    }

    pub(crate) fn parity(&self) -> ConversationIdParity {
        if self.0.is_multiple_of(2) {
            ConversationIdParity::Even
        } else {
            ConversationIdParity::Odd
        }
    }

    pub(crate) fn advance(&mut self) {
        self.0 = self.0.wrapping_add(2);
    }
}

impl ConversationIdParity {
    pub(crate) fn opposite(&self) -> Self {
        match self {
            Self::Even => Self::Odd,
            Self::Odd => Self::Even,
        }
    }
}
