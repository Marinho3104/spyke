use std::collections::{
    HashMap,
    hash_map::Entry::{Occupied, Vacant},
};

use thiserror::Error;

use crate::network::protocol::{
    conversation_id::{ConversationId, ConversationIdParity},
    dispatcher::Dispatcher,
    flow::Flow,
    state_dispatcher::{StateDispatcher, StateDispatcherError},
};

pub(crate) struct Config {
    pub(crate) max_concurrent_protocols: u16,
    pub(crate) local_ids: ConversationIdParity,
}

pub(crate) struct StateValidator {
    config: Config,
    states: HashMap<ConversationId, StateDispatcher>,
}

#[derive(Debug, Error)]
pub(crate) enum StateValidatorError {
    #[error("State dispatcher error: {0}")]
    StateDispatcher(#[from] StateDispatcherError),

    #[error("the max number of concurrent protocols was reached")]
    MaxConcurrentProtocols,

    #[error("the conversation id belongs to the other side")]
    InvalidConversationId,
}

impl Config {
    pub(crate) fn new(max_concurrent_protocols: u16, local_ids: ConversationIdParity) -> Self {
        Self {
            max_concurrent_protocols,
            local_ids,
        }
    }
}

impl StateValidator {
    pub(crate) fn new(config: Config) -> Self {
        Self {
            config,
            states: HashMap::new(),
        }
    }

    pub(crate) fn update(
        &mut self,
        dispatcher: &Dispatcher,
        flow: Flow,
    ) -> Result<(), StateValidatorError> {
        let conversation_id = dispatcher.headers().conversation_id;
        let available_space = self.available_space();

        // Whoever starts a conversation must use an id from its own range
        let starter_ids = match flow {
            Flow::Outbound => self.config.local_ids,
            Flow::Inbound => self.config.local_ids.opposite(),
        };

        match self.states.entry(conversation_id) {
            Occupied(mut state_dispatcher) => match state_dispatcher.get().update(dispatcher, flow)
            {
                Ok(Some(new_state)) => *state_dispatcher.get_mut() = new_state,
                // Finished or failed, either way the conversation is over
                result => {
                    state_dispatcher.remove();
                    result?;
                }
            },
            Vacant(_) if conversation_id.parity() != starter_ids => {
                Err(StateValidatorError::InvalidConversationId)?
            }
            Vacant(entry) => match StateDispatcher::try_new(dispatcher, flow)? {
                Some(new_state) if available_space => {
                    entry.insert(new_state);
                }
                Some(_) => Err(StateValidatorError::MaxConcurrentProtocols)?,
                None => {}
            },
        }

        Ok(())
    }

    fn available_space(&self) -> bool {
        self.states.len() < self.config.max_concurrent_protocols as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::{
        disconnect::Disconnect,
        dispatcher::Headers,
        flow::Flow::{Inbound, Outbound},
        ping::{Content as PingContent, Ping},
    };

    // This side uses even ids, so the peer uses odd ones
    const LOCAL_ID: u16 = 0;
    const REMOTE_ID: u16 = 1;

    fn config(max_concurrent_protocols: u16) -> Config {
        Config::new(max_concurrent_protocols, ConversationIdParity::Even)
    }

    fn state_validator() -> StateValidator {
        StateValidator::new(config(16))
    }

    fn headers(conversation_id: u16) -> Headers {
        Headers::new(ConversationId::new(conversation_id))
    }

    fn ping(conversation_id: u16) -> Dispatcher {
        Dispatcher::Ping {
            protocol: Ping::new(PingContent::Ping),
            headers: headers(conversation_id),
        }
    }

    fn pong(conversation_id: u16) -> Dispatcher {
        Dispatcher::Ping {
            protocol: Ping::new(PingContent::Pong),
            headers: headers(conversation_id),
        }
    }

    fn disconnect(conversation_id: u16) -> Dispatcher {
        Dispatcher::Disconnect {
            protocol: Disconnect::new(),
            headers: headers(conversation_id),
        }
    }

    #[test]
    fn valid_flows() {
        // Each step is (dispatcher, flow, expected open states after the step)
        let flows = vec![
            vec![
                (ping(REMOTE_ID), Inbound, 1),
                (pong(REMOTE_ID), Outbound, 0),
            ],
            vec![(ping(LOCAL_ID), Outbound, 1), (pong(LOCAL_ID), Inbound, 0)],
            vec![(disconnect(REMOTE_ID), Inbound, 0)],
            vec![(disconnect(LOCAL_ID), Outbound, 0)],
        ];

        for flow in flows {
            let mut validator = state_validator();

            for (dispatcher, direction, expected_states) in flow {
                assert!(validator.update(&dispatcher, direction).is_ok());
                assert_eq!(validator.states.len(), expected_states);
            }
        }
    }

    #[test]
    fn wrong_flows() {
        // Every step must succeed except the last one
        let flows = vec![
            // Conversation started with an id from the other side's range
            vec![(ping(LOCAL_ID), Inbound)],
            vec![(ping(REMOTE_ID), Outbound)],
            vec![(disconnect(LOCAL_ID), Inbound)],
            vec![(disconnect(REMOTE_ID), Outbound)],
            // Pong without a ping
            vec![(pong(REMOTE_ID), Inbound)],
            vec![(pong(LOCAL_ID), Outbound)],
            // Pong in the same direction as the ping
            vec![(ping(REMOTE_ID), Inbound), (pong(REMOTE_ID), Inbound)],
            vec![(ping(LOCAL_ID), Outbound), (pong(LOCAL_ID), Outbound)],
            // Second ping on an open conversation
            vec![(ping(REMOTE_ID), Inbound), (ping(REMOTE_ID), Inbound)],
            vec![(ping(REMOTE_ID), Inbound), (ping(REMOTE_ID), Outbound)],
            vec![(ping(LOCAL_ID), Outbound), (ping(LOCAL_ID), Inbound)],
            vec![(ping(LOCAL_ID), Outbound), (ping(LOCAL_ID), Outbound)],
            // Other protocol on an open conversation
            vec![(ping(REMOTE_ID), Inbound), (disconnect(REMOTE_ID), Inbound)],
            vec![
                (ping(REMOTE_ID), Inbound),
                (disconnect(REMOTE_ID), Outbound),
            ],
            vec![(ping(LOCAL_ID), Outbound), (disconnect(LOCAL_ID), Inbound)],
            vec![(ping(LOCAL_ID), Outbound), (disconnect(LOCAL_ID), Outbound)],
        ];

        for mut flow in flows {
            let mut validator = state_validator();
            let (last_dispatcher, last_direction) = flow.pop().unwrap();

            for (dispatcher, direction) in flow {
                assert!(validator.update(&dispatcher, direction).is_ok());
            }

            assert!(validator.update(&last_dispatcher, last_direction).is_err());
        }
    }

    #[test]
    fn max_concurrent_protocols() {
        let mut validator = StateValidator::new(config(2));

        assert!(validator.update(&ping(1), Inbound).is_ok());
        assert!(validator.update(&ping(3), Inbound).is_ok());

        let result = validator.update(&ping(5), Inbound);
        assert!(matches!(
            result,
            Err(StateValidatorError::MaxConcurrentProtocols)
        ));

        // A protocol that opens no state is still accepted while full
        assert!(validator.update(&disconnect(7), Inbound).is_ok());

        // An open conversation can still finish while full, freeing a slot
        assert!(validator.update(&pong(1), Outbound).is_ok());
        assert!(validator.update(&ping(5), Inbound).is_ok());
    }
}
