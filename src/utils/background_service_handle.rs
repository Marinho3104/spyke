use std::sync::mpsc;


pub(crate) struct BackgroundServiceHandler<C> {
    tx: mpsc::Sender<C>,
}
