use std::borrow::Cow;

use crate::com::{ack::AckCallback, fsm::Fsm, mail::BasicMailbox};
use crossbeam_channel::Receiver;
use skproto::tracing::SegmentData;
use tracing::Span;

use super::engine::TracingTagEngine;

impl Drop for TagFsm {
    fn drop(&mut self) {}
}
pub struct SegmentDataCallback {
    pub data: SegmentData,
    pub callback: AckCallback,
    pub span: Span,
}

pub struct TagFsm {
    pub receiver: Receiver<SegmentDataCallback>,
    pub mailbox: Option<BasicMailbox<TagFsm>>,
    pub engine: TracingTagEngine,
    pub last_idx: u64,
    pub counter: u64,
}

impl TagFsm {
    pub fn new(
        receiver: Receiver<SegmentDataCallback>,
        mailbox: Option<BasicMailbox<TagFsm>>,
        engine: TracingTagEngine,
    ) -> TagFsm {
        TagFsm {
            receiver,
            mailbox,
            engine,
            last_idx: 0,
            counter: 0,
        }
    }
    // TODO: use batch logic, currently directly write to disk
    pub fn handle_tasks(&mut self, msgs: &mut Vec<SegmentDataCallback>) {
        for msg in msgs {
            self.engine.add_record(&msg.data);
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
    type Message = SegmentDataCallback;

    // Currently we never quit
    fn is_stopped(&self) -> bool {
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
