use super::fsm::Fsm;
use crossbeam_channel::Receiver;
use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
    thread::{current, ThreadId},
    time::Instant,
};

pub struct Batch<N> {
    normals: Vec<Option<NormalFsm<N>>>,
}

pub struct NormalFsm<N> {
    fsm: Box<N>,
    timer: Instant,
}

pub struct BatchSystem<N> {
    receiver: Receiver<N>,
    pool_size: usize,
    max_batch_size: usize,
}

impl<N> BatchSystem<N>
where
    N: Fsm + Send + 'static,
{
    // fn start_poller(&mut self, name: String) {}
}
pub struct Poller<N, H> {
    pub fsm_receiver: Receiver<N>,
    pub handler: H,
    pub max_batch_size: usize,
    pub joinable_workers: Option<Arc<Mutex<Vec<ThreadId>>>>,
}

impl<N: Fsm, H: PollHandler<N>> Poller<N, H> {
    fn fetch_fsm(&mut self, batch: &mut Batch<N>) -> bool {
        false
    }

    pub fn poll(&mut self) {}
}

impl<N, H> Drop for Poller<N, H> {
    fn drop(&mut self) {
        if let Some(joinable_workers) = &self.joinable_workers {
            joinable_workers.lock().unwrap().push(current().id());
        }
    }
}

pub enum HandleResult {
    KeepProcessing,
    StopAt { progress: usize, skip_end: bool },
}

pub trait PollHandler<N>: Send + 'static {
    fn begin(&mut self, batch_size: usize);
    fn handle(&mut self, normal: &mut impl DerefMut<Target = N>) -> HandleResult;
    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = N>>]);
    fn end(&mut self, _batch: &mut [Option<impl DerefMut<Target = N>>]);
    fn pause(&mut self);
}
