use super::msg::TagMsg;
use crate::com::{fsm::Fsm, mail::BasicMailbox};
use crossbeam_channel::Receiver;
pub struct TagFsm {
    pub receiver: Receiver<TagMsg>,
    stopped: bool,
    has_ready: bool,
    mailbox: Option<BasicMailbox<Self>>,
}

impl Drop for TagFsm {
    fn drop(&mut self) {}
}

impl Fsm for TagFsm {
    type Message = TagMsg;

    fn is_stopped(&self) -> bool {
        todo!()
    }

    fn set_mailbox(&mut self, _mailbox: std::borrow::Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        todo!()
    }

    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        todo!()
    }
}
