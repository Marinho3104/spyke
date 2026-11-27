use std::collections::{
    HashMap,
    hash_map::Entry::{Occupied, Vacant},
};

use thiserror::Error;

use crate::network::protocol::{
    conversation_id::{ConversationId, ConversationIdParity},
    flow::Flow,
    message::{Message, Protocol},
    protocol_state::{ProtocolState, ProtocolStateError},
};

pub(crate) struct Config {
    pub(crate) max_conversations_per_side: u8,
    pub(crate) local_ids: ConversationIdParity,
}

pub(crate) struct StateValidator {
    config: Config,
    local: HashMap<ConversationId, ProtocolState>,
    remote: HashMap<ConversationId, ProtocolState>,
    next_conversation_id: ConversationId,
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum StateValidatorError {
    #[error("Protocol state error: {0}")]
    ProtocolState(#[from] ProtocolStateError),

    #[error("the other side exceeded the limit of open conversations")]
    RemoteLimitExceeded,

    #[error("this side reached its limit of open conversations")]
    LocalLimitReached,

    #[error("the conversation id belongs to the other side")]
    InvalidConversationId,
}

impl Config {
    pub(crate) fn new(max_conversations_per_side: u8, local_ids: ConversationIdParity) -> Self {
        Self {
            max_conversations_per_side,
            local_ids,
        }
    }
}

impl StateValidator {
    pub(crate) fn new(config: Config) -> Self {
        Self {
            next_conversation_id: ConversationId::first(config.local_ids),
            config,
            local: HashMap::new(),
            remote: HashMap::new(),
        }
    }

    pub(crate) fn update(
        &mut self,
        message: &Message,
        flow: Flow,
    ) -> Result<(), StateValidatorError> {
        let conversation_id = message.headers.conversation_id;

        // Whoever starts a conversation must use an id from its own range
        let starter_ids = match flow {
            Flow::Outbound => self.config.local_ids,
            Flow::Inbound => self.config.local_ids.opposite(),
        };

        let limit = self.config.max_conversations_per_side as usize;
        let (states, limit_error) = if conversation_id.parity() == self.config.local_ids {
            (&mut self.local, StateValidatorError::LocalLimitReached)
        } else {
            (&mut self.remote, StateValidatorError::RemoteLimitExceeded)
        };
        let available_space = states.len() < limit;

        match states.entry(conversation_id) {
            Occupied(mut protocol_state) => {
                match protocol_state.get().update(&message.protocol, flow) {
                    Ok(Some(new_state)) => *protocol_state.get_mut() = new_state,
                    // Finished or failed, either way the conversation is over
                    result => {
                        protocol_state.remove();
                        result?;
                    }
                }
            }
            Vacant(_) if conversation_id.parity() != starter_ids => {
                return Err(StateValidatorError::InvalidConversationId);
            }
            Vacant(entry) => match ProtocolState::try_new(&message.protocol, flow)? {
                Some(new_state) if available_space => {
                    entry.insert(new_state);
                }
                Some(_) => return Err(limit_error),
                None => {}
            },
        }

        Ok(())
    }

    /// Starts a new outbound conversation: picks a free id from this side's range and records the
    /// state of its first message. Doing both in one call means no inbound message can take the
    /// last free slot between choosing the id and opening the conversation.
    ///
    /// The first message must not also go through `update`, it is already recorded here
    pub(crate) fn open(
        &mut self,
        protocol: &Protocol,
    ) -> Result<ConversationId, StateValidatorError> {
        let state = ProtocolState::try_new(protocol, Flow::Outbound)?;

        // A protocol that opens no state takes no slot, the same as in `update`
        if state.is_some() && !self.local_space() {
            return Err(StateValidatorError::LocalLimitReached);
        }

        let conversation_id = self.new_conversation_id();
        if let Some(state) = state {
            self.local.insert(conversation_id, state);
        }
        Ok(conversation_id)
    }

    /// Next id from this side's range that is not in use. Always ends, since at most
    /// `u8::MAX` conversations are open and this side's range has 32768 ids
    fn new_conversation_id(&mut self) -> ConversationId {
        loop {
            let candidate = self.next_conversation_id;
            self.next_conversation_id.advance();
            if !self.local.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    fn local_space(&self) -> bool {
        self.local.len() < self.config.max_conversations_per_side as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::{
        authentication::{Authentication, Content as AuthenticationContent},
        disconnect::Disconnect,
        flow::Flow::{Inbound, Outbound},
        message::Headers,
        ping::{Content as PingContent, Ping},
        specs::StateSpecError,
    };

    // This side uses even ids, so the peer uses odd ones
    const LOCAL_ID: u16 = 0;
    const REMOTE_ID: u16 = 1;

    fn config(max_conversations_per_side: u8) -> Config {
        Config::new(max_conversations_per_side, ConversationIdParity::Even)
    }

    fn open_conversations(validator: &StateValidator) -> usize {
        validator.local.len() + validator.remote.len()
    }

    fn state_validator() -> StateValidator {
        StateValidator::new(config(16))
    }

    fn message(conversation_id: u16, protocol: Protocol) -> Message {
        Message::new(Headers::new(ConversationId::new(conversation_id)), protocol)
    }

    fn ping(conversation_id: u16) -> Message {
        message(
            conversation_id,
            Protocol::Ping(Ping::new(PingContent::Ping)),
        )
    }

    fn pong(conversation_id: u16) -> Message {
        message(
            conversation_id,
            Protocol::Ping(Ping::new(PingContent::Pong)),
        )
    }

    fn spec_error(error: StateSpecError) -> StateValidatorError {
        StateValidatorError::ProtocolState(ProtocolStateError::StateSpec(error))
    }

    fn no_authentication(conversation_id: u16) -> Message {
        message(
            conversation_id,
            Protocol::Authentication(Authentication::new(AuthenticationContent::NoAuthentication)),
        )
    }

    fn accepted(conversation_id: u16) -> Message {
        message(
            conversation_id,
            Protocol::Authentication(Authentication::new(AuthenticationContent::Accepted)),
        )
    }

    fn disconnect(conversation_id: u16) -> Message {
        message(conversation_id, Protocol::Disconnect(Disconnect::new()))
    }

    #[test]
    fn valid_flows() {
        // Each step is (message, flow, expected open states after the step)
        let flows = vec![
            vec![
                (ping(REMOTE_ID), Inbound, 1),
                (pong(REMOTE_ID), Outbound, 0),
            ],
            vec![(ping(LOCAL_ID), Outbound, 1), (pong(LOCAL_ID), Inbound, 0)],
            vec![(disconnect(REMOTE_ID), Inbound, 0)],
            vec![(disconnect(LOCAL_ID), Outbound, 0)],
            vec![
                (no_authentication(REMOTE_ID), Inbound, 1),
                (accepted(REMOTE_ID), Outbound, 0),
            ],
        ];

        for flow in flows {
            let mut validator = state_validator();

            for (message, direction, expected_states) in flow {
                validator.update(&message, direction).unwrap();
                assert_eq!(open_conversations(&validator), expected_states);
            }
        }
    }

    #[test]
    fn wrong_flows() {
        // Each group is (expected error, flows); every step must succeed except the last one
        let groups = vec![
            // Conversation started with an id from the other side's range
            (
                StateValidatorError::InvalidConversationId,
                vec![
                    vec![(ping(LOCAL_ID), Inbound)],
                    vec![(ping(REMOTE_ID), Outbound)],
                    vec![(disconnect(LOCAL_ID), Inbound)],
                    vec![(disconnect(REMOTE_ID), Outbound)],
                ],
            ),
            // Pong without a ping
            (
                spec_error(StateSpecError::InvalidState),
                vec![
                    vec![(pong(REMOTE_ID), Inbound)],
                    vec![(pong(LOCAL_ID), Outbound)],
                ],
            ),
            // Pong in the same direction as the ping
            (
                spec_error(StateSpecError::InvalidFlow),
                vec![
                    vec![(ping(REMOTE_ID), Inbound), (pong(REMOTE_ID), Inbound)],
                    vec![(ping(LOCAL_ID), Outbound), (pong(LOCAL_ID), Outbound)],
                ],
            ),
            // Second ping on an open conversation
            (
                spec_error(StateSpecError::InvalidState),
                vec![
                    vec![(ping(REMOTE_ID), Inbound), (ping(REMOTE_ID), Inbound)],
                    vec![(ping(REMOTE_ID), Inbound), (ping(REMOTE_ID), Outbound)],
                    vec![(ping(LOCAL_ID), Outbound), (ping(LOCAL_ID), Inbound)],
                    vec![(ping(LOCAL_ID), Outbound), (ping(LOCAL_ID), Outbound)],
                ],
            ),
            // Other protocol on an open conversation
            (
                StateValidatorError::ProtocolState(ProtocolStateError::UnexpectedProtocol),
                vec![
                    vec![(ping(REMOTE_ID), Inbound), (disconnect(REMOTE_ID), Inbound)],
                    vec![
                        (ping(REMOTE_ID), Inbound),
                        (disconnect(REMOTE_ID), Outbound),
                    ],
                    vec![(ping(LOCAL_ID), Outbound), (disconnect(LOCAL_ID), Inbound)],
                    vec![(ping(LOCAL_ID), Outbound), (disconnect(LOCAL_ID), Outbound)],
                ],
            ),
        ];

        for (expected_error, flows) in groups {
            for mut flow in flows {
                let mut validator = state_validator();
                let (last_message, last_direction) = flow.pop().unwrap();

                for (message, direction) in flow {
                    validator.update(&message, direction).unwrap();
                }

                let result = validator.update(&last_message, last_direction);
                assert_eq!(result.as_ref().err(), Some(&expected_error));
            }
        }
    }

    #[test]
    fn remote_limit() {
        let mut validator = StateValidator::new(config(2));

        validator.update(&ping(1), Inbound).unwrap();
        validator.update(&ping(3), Inbound).unwrap();

        let result = validator.update(&ping(5), Inbound);
        assert_eq!(result, Err(StateValidatorError::RemoteLimitExceeded));

        // A protocol that opens no state is still accepted while full
        validator.update(&disconnect(7), Inbound).unwrap();

        // An open conversation can still finish while full, freeing a slot
        validator.update(&pong(1), Outbound).unwrap();
        validator.update(&ping(5), Inbound).unwrap();
    }

    #[test]
    fn limits_are_per_side() {
        let mut validator = StateValidator::new(config(1));

        let local_id = validator.open(&ping(0).protocol).unwrap();
        validator.update(&ping(1), Inbound).unwrap();

        let result = validator.open(&ping(0).protocol);
        assert_eq!(result, Err(StateValidatorError::LocalLimitReached));
        let result = validator.update(&ping(2), Outbound);
        assert_eq!(result, Err(StateValidatorError::LocalLimitReached));
        let result = validator.update(&ping(3), Inbound);
        assert_eq!(result, Err(StateValidatorError::RemoteLimitExceeded));

        validator.update(&pong(local_id.0), Inbound).unwrap();
        validator.open(&ping(0).protocol).unwrap();
        let result = validator.update(&ping(3), Inbound);
        assert_eq!(result, Err(StateValidatorError::RemoteLimitExceeded));
    }

    #[test]
    fn open_allocates_ids_by_parity() {
        for (parity, expected) in [
            (ConversationIdParity::Even, [0, 2, 4]),
            (ConversationIdParity::Odd, [1, 3, 5]),
        ] {
            let mut validator = StateValidator::new(Config::new(16, parity));

            for id in expected {
                let conversation_id = validator.open(&ping(0).protocol).unwrap();
                assert_eq!(conversation_id, ConversationId::new(id));
            }
            assert_eq!(validator.local.len(), expected.len());
        }
    }

    #[test]
    fn open_records_the_first_message() {
        let mut validator = state_validator();

        let conversation_id = validator.open(&ping(0).protocol).unwrap();
        assert_eq!(validator.local.len(), 1);

        // The reply ends the conversation opened above
        validator.update(&pong(conversation_id.0), Inbound).unwrap();
        assert!(validator.local.is_empty());
    }

    #[test]
    fn open_skips_ids_in_use_and_wraps() {
        let mut validator = state_validator();

        // Conversation 0 is open, so the next id is 2
        validator.update(&ping(LOCAL_ID), Outbound).unwrap();
        assert_eq!(
            validator.open(&ping(0).protocol).unwrap(),
            ConversationId::new(2)
        );

        // Wrapping around keeps the parity and still skips 0 and 2, which are open
        validator.next_conversation_id = ConversationId::new(u16::MAX - 1);
        assert_eq!(
            validator.open(&ping(0).protocol).unwrap(),
            ConversationId::new(u16::MAX - 1)
        );
        assert_eq!(
            validator.open(&ping(0).protocol).unwrap(),
            ConversationId::new(4)
        );
    }

    #[test]
    fn open_when_full() {
        let mut validator = StateValidator::new(config(1));
        validator.open(&ping(0).protocol).unwrap();

        let result = validator.open(&ping(0).protocol);
        assert_eq!(result, Err(StateValidatorError::LocalLimitReached));
        // The failed open did not use up an id
        assert_eq!(validator.next_conversation_id, ConversationId::new(2));

        // A protocol that opens no state still gets an id while full
        assert_eq!(
            validator.open(&disconnect(0).protocol).unwrap(),
            ConversationId::new(2)
        );
    }

    #[test]
    fn open_with_invalid_first_message() {
        let mut validator = state_validator();

        let result = validator.open(&pong(0).protocol);
        assert_eq!(result, Err(spec_error(StateSpecError::InvalidState)));
        assert!(validator.local.is_empty());
        assert_eq!(validator.next_conversation_id, ConversationId::new(0));
    }
}
