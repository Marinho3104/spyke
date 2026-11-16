/// Direction of a message relative to this side of the connection
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Flow {
    Inbound,
    Outbound,
}
