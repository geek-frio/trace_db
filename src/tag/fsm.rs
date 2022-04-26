use std::borrow::Cow;

use crate::com::{ack::AckCallback, fsm::Fsm, mail::BasicMailbox};
use crossbeam_channel::Receiver;
use skproto::tracing::SegmentData;
use tracing::{error, trace, Span};

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
        }
    }
    // TODO: use batch logic, currently directly write to disk
    pub fn handle_tasks(&mut self, msgs: &mut Vec<SegmentDataCallback>) {
        for msg in msgs {
            let span = &msg.span;
            let _entered = span.enter();
            self.engine.add_record(&msg.data);
            trace!(
                trace_id = msg.data.get_trace_id(),
                seq_id = msg.data.get_meta().get_seqId(),
                "Segment has adeed to Tag Engine, but not be flushed!"
            );
        }
    }

    pub fn commit(&mut self, msgs: &Vec<SegmentDataCallback>) {
        let res = self.engine.flush();
        for msg in msgs {
            let span = &msg.span;
            let _entered = span.enter();
            msg.callback.callback(msg.data.get_meta().get_seqId());
            trace!(
                trace_id = msg.data.get_trace_id(),
                seq_id = msg.data.get_meta().get_seqId(),
                "segment has been callback"
            );
        }
        match res {
            Ok(_) => {}
            Err(e) => {
                error!("We should do something to backup these data, it's a CAN'T RETRY ERROR so currently we do nothing;");
                error!("Serious problem, backup data!:{:?}", e);
                // TODO: We should do something to backup these data, it's a CAN'T RETRY ERROR
                //  currently we do nothing;
            }
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
