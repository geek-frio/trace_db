// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::fsm::Fsm;
use crate::fsm::FsmState;
use crate::sched::FsmScheduler;
use crossbeam_channel::{SendError, Sender};
use std::borrow::Cow;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub struct BasicMailbox<Owner: Fsm> {
    pub(crate) sender: Sender<Owner::Message>,
    pub(crate) state: Arc<FsmState<Owner>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    pub fn new(
        sender: Sender<Owner::Message>,
        fsm: Box<Owner>,
        state_cnt: Arc<AtomicUsize>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(FsmState::new(fsm, state_cnt)),
        }
    }

    pub fn release(&self, fsm: Box<Owner>) {
        self.state.release(fsm);
    }

    pub fn take_fsm(&self) -> Option<Box<Owner>> {
        self.state.take_fsm()
    }

    pub fn send<S: FsmScheduler<F = Owner>>(
        &self,
        msg: Owner::Message,
        scheduler: &S,
    ) -> Result<(), SendError<Owner::Message>> {
        if self.sender.len() > 5000 {
            println!(
                "Channel has the risk of being full, sender's length:{}",
                self.sender.len()
            );
        }
        self.sender.send(msg)?;
        self.state.notify(scheduler, Cow::Borrowed(self));
        Ok(())
    }

    pub fn notify<S: FsmScheduler<F = Owner>>(&self, scheduler: &S) {
        self.state.notify(scheduler, Cow::Borrowed(self));
    }

    pub(crate) fn close(&self) {
        self.state.clear();
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }

    pub fn is_empty(&self) -> bool {
        return self.sender.is_empty();
    }
}
