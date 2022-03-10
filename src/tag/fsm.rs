use std::borrow::Cow;

use crate::com::{fsm::Fsm, mail::BasicMailbox};
use crossbeam_channel::Receiver;
use skproto::tracing::SegmentData;

impl Drop for TagFsm {
    fn drop(&mut self) {}
}

pub struct TagFsm {
    pub receiver: Receiver<SegmentData>,
    pub mailbox: Option<BasicMailbox<TagFsm>>,
}

impl TagFsm {
    fn handle_tasks(msgs: &mut Vec<SegmentData>) {}
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
