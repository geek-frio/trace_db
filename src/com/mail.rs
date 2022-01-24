// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::fsm::Fsm;
use super::fsm::FsmState;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::sync::mpsc::SendError;
use std::sync::Arc;

pub struct BasicMailbox<Owner: Fsm> {
    pub(crate) sender: mpsc::Sender<Owner::Message>,
    pub(crate) state: Arc<FsmState<Owner>>,
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

    pub(crate) fn release(&self, fsm: Box<Owner>) {
        self.state.release(fsm);
    }

    pub(crate) fn take_fsm(&self) -> Option<Box<Owner>> {
        self.state.take_fsm()
    }

    pub fn send(&self, msg: Owner::Message) -> Result<(), SendError<Owner::Message>> {
        self.sender.send(msg)?;
        todo!("引入scheduler");
    }

    pub(crate) fn close(&self) {
        self.state.clear();
    }
}
