use borsh::{BorshDeserialize, BorshSerialize};

use crate::network::protocol::{
    flow::Flow,
    specs::{ContentSpec, Spec, StateSpec, StateSpecError},
};

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) enum Content {
    Ping,
    Pong,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub(crate) struct Ping {
    pub(crate) content: Content,
}

#[derive(Debug)]
pub(crate) enum State {
    Ping(Flow),
}

impl ContentSpec for Content {}

impl Spec for Ping {
    type Content = Content;

    fn content(&self) -> &Self::Content {
        &self.content
    }
}

impl Ping {
    pub(crate) fn new(content: Content) -> Self {
        Self { content }
    }
}

impl StateSpec for State {
    type Protocol = Ping;

    fn try_new(protocol: &Self::Protocol, flow: Flow) -> Result<Option<Self>, StateSpecError> {
        let result = match protocol.content {
            Content::Ping => State::Ping(flow),
            Content::Pong => Err(StateSpecError::InvalidState)?,
        };
        Ok(Some(result))
    }

    fn update(
        &self,
        protocol: &Self::Protocol,
        flow: Flow,
    ) -> Result<Option<Self>, StateSpecError> {
        let Self::Ping(state_flow) = self;
        let result = match &protocol.content {
            Content::Pong if *state_flow != flow => None,
            Content::Pong => Err(StateSpecError::InvalidFlow)?,
            Content::Ping => Err(StateSpecError::InvalidState)?,
        };
        Ok(result)
    }
}
