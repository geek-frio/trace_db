use std::borrow::Cow;
use std::slice::Iter;

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
    receiver: Receiver<SegmentDataCallback>,
    mailbox: Option<BasicMailbox<TagFsm>>,
    engine: TracingTagEngine,
    tick: bool,
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
            tick: false,
        }
    }
}

pub(crate) trait FsmExecutor {
    type Msg;

    fn try_fill_batch(&mut self, msg_buf: &mut Vec<Self::Msg>, counter: &mut usize) -> bool;

    fn handle_tasks(&mut self, msg_buf: &mut Vec<Self::Msg>, msg_cnt: &mut usize);

    fn commit(&mut self, msg_buf: &Vec<Self::Msg>);

    fn remain_msgs(&self) -> usize;
}

impl FsmExecutor for TagFsm {
    type Msg = SegmentDataCallback;

    fn try_fill_batch(&mut self, msg_buf: &mut Vec<Self::Msg>, counter: &mut usize) -> bool {
        let mut keep_process = false;
        const TAG_POLLER_BATCH_SIZE: usize = 5000;
        loop {
            match self.receiver.try_recv() {
                Ok(msg) => {
                    *counter += 1;
                    trace!("Received a new msg");
                    msg_buf.push(msg);
                    if msg_buf.len() >= TAG_POLLER_BATCH_SIZE {
                        trace!("Batch max has overceeded");
                        keep_process = self.receiver.len() > 0;
                        break;
                    }
                }
                Err(_) => {
                    trace!("Mailbox's msgs has consumed");
                    break;
                }
            }
        }
        keep_process
    }

    fn handle_tasks(&mut self, msg_buf: &mut Vec<Self::Msg>, msg_cnt: &mut usize) {
        let slice = msg_buf.as_slice();
        for i in *msg_cnt..msg_buf.len() {
            if i < *msg_cnt {
                continue;
            }
            let msg = &slice[i];
            let span = &msg.span;
            let _entered = span.enter();
            self.engine.add_record(&msg.data);
            trace!(
                trace_id = msg.data.get_trace_id(),
                seq_id = msg.data.get_meta().get_seqId(),
                "Segment has added to Tag Engine, but not be flushed!"
            );
            *msg_cnt += 1;
        }
    }

    fn commit(&mut self, msgs: &Vec<Self::Msg>) {
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
        if let Err(e) = res {
            error!("We should do something to backup these data, it's a CAN'T RETRY ERROR so currently we do nothing;");
            error!("Serious problem, backup data!:{:?}", e);
        }
    }

    fn remain_msgs(&self) -> usize {
        self.receiver.len()
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

    fn tag_tick(&mut self) {
        self.tick = true;
    }

    fn is_tick(&self) -> bool {
        self.tick
    }

    fn untag_tick(&mut self) {
        self.tick = true;
    }
}
