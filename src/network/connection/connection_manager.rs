use crate::network::connection::connection_manager_handle::ConnectionManagerHandle;

pub(crate) struct Config {}

pub(crate) struct ConnectionManager {
    config: Config,
    handle: ConnectionManagerHandle,
}
