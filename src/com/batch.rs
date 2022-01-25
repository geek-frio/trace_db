use super::fsm::Fsm;
use std::sync::mpsc::Receiver;
pub struct BatchSystem<N> {
    receiver: Receiver<N>,
    pool_size: usize,
    max_batch_size: usize,
}

impl<N> BatchSystem<N> where N: Fsm + Send + 'static {}
