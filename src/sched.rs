use crate::batch::FsmTypes;
use crate::fsm::Fsm;
use crossbeam_channel::Sender;

pub trait FsmScheduler {
    type F: Fsm;
    fn schedule(&self, fsm: Box<Self::F>);
    fn shutdown(&self);
}

pub struct NormalScheduler<N> {
    pub sender: Sender<FsmTypes<N>>,
}

impl<N> Clone for NormalScheduler<N> {
    fn clone(&self) -> NormalScheduler<N> {
        NormalScheduler {
            sender: self.sender.clone(),
        }
    }
}

impl<N> FsmScheduler for NormalScheduler<N>
where
    N: Fsm,
{
    type F = N;

    fn schedule(&self, fsm: Box<Self::F>) {
        let _ = self.sender.send(FsmTypes::Normal(fsm));
    }

    fn shutdown(&self) {
        let _ = self.sender.send(FsmTypes::Empty);
    }
}
