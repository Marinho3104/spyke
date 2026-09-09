use tokio::sync::mpsc;


pub(crate) enum ConnectionManagerCommand {
    
}

pub(crate) struct ConnectionManagerHandle {
    connection_manager_tx: mpsc::Sender<ConnectionManagerCommand>,
}

impl ConnectionManagerHandle {
    
    pub(crate) fn new(connection_manager_tx: mpsc::Sender<ConnectionManagerCommand>) -> Self {
        Self {
            connection_manager_tx
        }
    }
}
