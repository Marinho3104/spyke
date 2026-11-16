use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::network::framing::codec::Codec;

pub(crate) trait ConnectionAuthenticator {
    fn authenticate(&self, framed: &mut Framed<TcpStream, Codec>) -> bool;
}
