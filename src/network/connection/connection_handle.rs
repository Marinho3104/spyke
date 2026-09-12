use crate::utils::background_service_handle::BackgroundServiceHandle;

pub(crate) enum ConnectionCommand {
    
}


pub(crate) type ConnectionHandle = BackgroundServiceHandle<ConnectionCommand>;

impl BackgroundServiceHandle<ConnectionCommand> {}
