use std::{borrow::Cow, sync::atomic::AtomicUsize};

use crate::com::{fsm::Fsm, mail::BasicMailbox};
use crossbeam_channel::Receiver;
use skproto::tracing::SegmentData;

use super::engine::TagWriteEngine;

impl Drop for TagFsm {
    fn drop(&mut self) {}
}

pub struct TagFsm {
    pub receiver: Receiver<SegmentData>,
    pub mailbox: Option<BasicMailbox<TagFsm>>,
    pub engine: TagWriteEngine,
    pub last_idx: u64,
    pub counter: u64,
}

impl TagFsm {
    // TODO: use batch logic, currently directly write to disk
    pub fn handle_tasks(&mut self, msgs: &mut Vec<SegmentData>) {
        for msg in msgs {
            self.engine.add_record(msg);
            self.counter += 1;
        }
        if self.counter > 5000 {
            let result = self.engine.flush();
            println!(
                "#############Engine flushed success!, flused count is:{}",
                self.counter
            );
            match result {
                Ok(idx) => {
                    self.last_idx = idx;
                }
                Err(e) => {
                    //TODO: error process
                    println!("flush to db error:{:?}", e);
                }
            }
            self.counter = 0;
        }
    }
}

impl Fsm for TagFsm {
    type Message = SegmentData;

    fn is_stopped(&self) -> bool {
        // TODO: later we will add condition control
        false
    }

    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}
