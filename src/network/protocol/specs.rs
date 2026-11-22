use crate::network::protocol::flow::Flow;
use borsh::{BorshDeserialize, BorshSerialize};
use thiserror::Error;

pub(crate) trait ContentSpec: BorshSerialize + BorshDeserialize {}

pub(crate) trait StateSpec: Sized {
    type Protocol: Spec;

    fn try_new(protocol: &Self::Protocol, flow: Flow) -> Result<Option<Self>, StateSpecError>;

    fn update(&self, protocol: &Self::Protocol, flow: Flow)
    -> Result<Option<Self>, StateSpecError>;
}

pub(crate) trait Spec: BorshSerialize + BorshDeserialize {
    type Content: ContentSpec;
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum StateSpecError {
    #[error("Invalid state")]
    InvalidState,

    #[error("Invalid flow")]
    InvalidFlow,
}
