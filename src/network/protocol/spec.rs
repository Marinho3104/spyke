use crate::network::{framing::frame::{Frame, Payload}, protocol::protocol_id::ProtocolId};

pub(crate) trait Spec where 
    Self: TryFrom<Vec<Payload>>,
    Vec<Frame>: From<Self> {
        const PROTOCOL_ID: ProtocolId;
}
