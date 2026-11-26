use borsh::{BorshDeserialize, BorshSerialize};

use crate::network::protocol::{
    flow::Flow,
    specs::{ContentSpec, Spec, StateSpec, StateSpecError},
};

#[derive(Debug, BorshDeserialize, BorshSerialize)]
pub(crate) enum RejectionReason {
    InvalidCredentials,
    UnsupportedMethod,
}

#[derive(Debug, BorshDeserialize, BorshSerialize)]
pub(crate) enum Content {
    // NO AUTHENTICATION
    NoAuthentication,

    // GENERAL
    Accepted,
    Rejected(RejectionReason),
}

#[derive(Debug, BorshDeserialize, BorshSerialize)]
pub(crate) struct Authentication {
    pub(crate) content: Content,
}

#[derive(Debug)]
pub(crate) enum State {
    // NO AUTHENTICATION
    NoAuthentication(Flow),
}

impl ContentSpec for Content {}

impl Spec for Authentication {
    type Content = Content;
}

impl Authentication {
    pub(crate) fn new(content: Content) -> Self {
        Self { content }
    }
}

impl StateSpec for State {
    type Protocol = Authentication;

    fn try_new(protocol: &Self::Protocol, flow: Flow) -> Result<Option<Self>, StateSpecError> {
        match protocol.content {
            Content::NoAuthentication => Ok(Some(Self::NoAuthentication(flow))),
            Content::Accepted | Content::Rejected(_) => Err(StateSpecError::InvalidState),
        }
    }

    fn update(
        &self,
        protocol: &Self::Protocol,
        flow: Flow,
    ) -> Result<Option<Self>, StateSpecError> {
        match (self, &protocol.content) {
            (
                Self::NoAuthentication(state_flow),
                Content::Accepted | Content::Rejected(RejectionReason::UnsupportedMethod),
            ) => {
                if *state_flow != flow {
                    Ok(None)
                } else {
                    Err(StateSpecError::InvalidFlow)
                }
            }
            _ => Err(StateSpecError::InvalidState),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::flow::Flow::{Inbound, Outbound};

    const NO_AUTH: Content = Content::NoAuthentication;
    const UNSUPPORTED: Content = Content::Rejected(RejectionReason::UnsupportedMethod);
    const INVALID: Content = Content::Rejected(RejectionReason::InvalidCredentials);

    /// State after a NoAuthentication message sent in `flow`
    fn opened(flow: Flow) -> State {
        let protocol = Authentication::new(NO_AUTH);
        State::try_new(&protocol, flow).unwrap().unwrap()
    }

    fn opposite(flow: Flow) -> Flow {
        match flow {
            Inbound => Outbound,
            Outbound => Inbound,
        }
    }

    #[test]
    fn valid_flows() {
        // Opened from either side, a reply from the other side ends the conversation
        for flow in [Inbound, Outbound] {
            for reply in [Content::Accepted, UNSUPPORTED] {
                let result = opened(flow).update(&Authentication::new(reply), opposite(flow));
                assert!(result.unwrap().is_none());
            }
        }
    }

    #[test]
    fn wrong_opening() {
        for content in [Content::Accepted, UNSUPPORTED, INVALID] {
            let result = State::try_new(&Authentication::new(content), Inbound);
            assert!(matches!(result, Err(StateSpecError::InvalidState)));
        }
    }

    #[test]
    fn wrong_replies() {
        // (reply, reply flow, expected error), after opening Inbound
        let replies = [
            // Reply from the same side that opened
            (Content::Accepted, Inbound, StateSpecError::InvalidFlow),
            (UNSUPPORTED, Inbound, StateSpecError::InvalidFlow),
            // Not a valid reply to NoAuthentication
            (NO_AUTH, Outbound, StateSpecError::InvalidState),
            // No credentials were sent, so they cannot be invalid
            (INVALID, Outbound, StateSpecError::InvalidState),
        ];

        for (reply, flow, expected) in replies {
            let result = opened(Inbound).update(&Authentication::new(reply), flow);
            assert!(matches!(result, Err(error) if error == expected));
        }
    }
}
