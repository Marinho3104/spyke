use borsh::{BorshDeserialize, BorshSerialize};

use crate::network::protocol::{
    flow::Flow,
    specs::{ContentSpec, Spec, StateSpec, StateSpecError},
};

#[derive(Debug, BorshDeserialize, BorshSerialize)]
pub(crate) struct Content;

#[derive(Debug, BorshDeserialize, BorshSerialize)]
pub(crate) struct Disconnect {
    pub(crate) content: Content,
}

#[derive(Debug)]
pub(crate) enum State {}

impl ContentSpec for Content {}

impl Spec for Disconnect {
    type Content = Content;
}

impl Disconnect {
    pub(crate) fn new() -> Self {
        Self { content: Content }
    }
}

impl StateSpec for State {
    type Protocol = Disconnect;

    fn try_new(_: &Self::Protocol, _: Flow) -> Result<Option<Self>, StateSpecError> {
        Ok(None)
    }

    fn update(&self, _: &Self::Protocol, _: Flow) -> Result<Option<Self>, StateSpecError> {
        match *self {}
    }
}
