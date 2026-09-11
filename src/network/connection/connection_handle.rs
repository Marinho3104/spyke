use crate::utils::background_service_handle::BackgroundServiceHandler;

pub(crate) enum ConnectionCommand {
    
}


pub(crate) type ConnectionHandle = BackgroundServiceHandler<ConnectionCommand>;

impl BackgroundServiceHandler<ConnectionCommand> {}
