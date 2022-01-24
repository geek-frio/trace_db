use super::fsm::Fsm;
use super::fsm::FsmState;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::sync::Arc;

pub struct BasicMailbox<Owner: Fsm> {
    pub sender: mpsc::Sender<Owner::Message>,
    pub state: Arc<FsmState<Owner>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    pub fn new(
        sender: mpsc::Sender<Owner::Message>,
        fsm: Box<Owner>,
        state_cnt: Arc<AtomicUsize>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(FsmState::new(fsm, state_cnt)),
        }
    }
}
