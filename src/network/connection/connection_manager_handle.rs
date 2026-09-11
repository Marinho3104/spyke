use tokio::sync::mpsc;

use crate::utils::background_service_handle::BackgroundServiceHandler;


pub(crate) enum ConnectionManagerCommand {
    Protocol(),   
}

pub(crate) type ConnectionManagerHandle = BackgroundServiceHandler<ConnectionManagerCommand>;

impl BackgroundServiceHandler<ConnectionManagerCommand> {
}
