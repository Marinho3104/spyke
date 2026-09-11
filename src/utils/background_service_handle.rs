use std::sync::mpsc;


pub(crate) struct BackgroundServiceHandler<C> {
    tx: mpsc::Sender<C>,
}

impl<C> BackgroundServiceHandler<C> {

    pub(crate) fn new(tx: mpsc::Sender<C>) -> Self {
        Self {
            tx
        }
    }

}
