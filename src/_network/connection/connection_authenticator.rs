use crate::network::connection::connection::Connection;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ConnectionAuthenticatorError {

    #[error("Connection did not provide authentication before timeout")]
    Timeout,

    #[error("The authentication provided by the connection failed")]
    Authentication,
}

#[async_trait]
pub(crate) trait ConnectionAuthenticator: Send + Clone + 'static {
    fn authenticate(&self, connection: &mut Connection) -> Result<(), ConnectionAuthenticatorError>;
}
