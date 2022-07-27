use crate::com::{
    ack::{AckCallback, CallbackStat},
    mail::BasicMailbox,
};
use crate::fsm::Fsm;
use crossbeam_channel::Receiver;
use skproto::tracing::SegmentData;
use std::borrow::Cow;
use tracing::{error, trace, Span};

use super::engine::TracingTagEngine;

pub struct SegmentDataCallback {
    pub data: SegmentData,
    pub callback: AckCallback,
    pub span: Span,
}

impl SegmentDataCallback {
    pub fn new(data: SegmentData, callback: AckCallback, span: Span) -> SegmentDataCallback {
        SegmentDataCallback {
            data,
            callback,
            span,
        }
    }
}

pub struct TagFsm {
    receiver: Receiver<SegmentDataCallback>,
    mailbox: Option<BasicMailbox<TagFsm>>,
    engine: TracingTagEngine,
    tick: bool,
    batch_size: usize,
}

impl TagFsm {
    pub fn new(
        receiver: Receiver<SegmentDataCallback>,
        mailbox: Option<BasicMailbox<TagFsm>>,
        engine: TracingTagEngine,
        batch_size: usize,
    ) -> TagFsm {
        TagFsm {
            receiver,
            mailbox,
            engine,
            tick: false,
            batch_size,
        }
    }

    pub fn get_engine(&self) -> &TracingTagEngine {
        &self.engine
    }

    pub fn get_mut_engine(&mut self) -> &mut TracingTagEngine {
        &mut self.engine
    }
}

pub(crate) trait FsmExecutor {
    type Msg;

    fn try_fill_batch(&mut self, msg_buf: &mut Vec<Self::Msg>, counter: &mut usize) -> bool;

    fn handle_tasks(&mut self, msg_buf: &mut Vec<Self::Msg>, msg_cnt: &mut usize);

    fn commit(&mut self, msg_buf: &mut Vec<Self::Msg>);

    fn remain_msgs(&self) -> usize;
}

impl FsmExecutor for TagFsm {
    type Msg = SegmentDataCallback;

    fn try_fill_batch(&mut self, msg_buf: &mut Vec<Self::Msg>, counter: &mut usize) -> bool {
        let mut keep_process = false;

        loop {
            match self.receiver.try_recv() {
                Ok(msg) => {
                    *counter += 1;

                    trace!("Received a new msg");
                    msg_buf.push(msg);

                    if msg_buf.len() >= self.batch_size {
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

    fn commit(&mut self, msgs: &mut Vec<Self::Msg>) {
        let res = self.engine.flush();
        match res {
            Err(e) => {
                error!(%e, "Flush data failed!");
                while let Some(msg) = msgs.pop() {
                    let span = &msg.span;
                    let _entered = span.enter();

                    msg.callback
                        .callback(CallbackStat::IOErr(e.clone(), msg.data.clone().into()));

                    error!("call back error to client");
                }
            }
            Ok(_tantivy_id) => {
                while let Some(msg) = msgs.pop() {
                    let span = &msg.span;
                    let _entered = span.enter();

                    msg.callback
                        .callback(CallbackStat::Ok(msg.data.clone().into()));

                    trace!(
                        trace_id = msg.data.get_trace_id(),
                        seq_id = msg.data.get_meta().get_seqId(),
                        "segment has been callback notify success"
                    );
                }
            }
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

    fn chan_msgs(&self) -> usize {
        self.receiver.len()
    }
}

#[cfg(test)]
mod tests {
    use super::SegmentDataCallback;
    use super::*;
    use crate::{
        com::test_util::gen_segcallback, log::init_console_logger, router::Router,
        tag::engine::TagEngineError,
    };
    use crossbeam_channel::Sender;

    fn setup(batch_size: usize) -> (TagFsm, Sender<SegmentDataCallback>) {
        init_console_logger();

        Router::create_test_tag_fsm_with_size(batch_size).unwrap()
    }

    #[test]
    fn test_fsm_exec_fill_batch() {
        let mut batch = Vec::new();
        let mut counter = 0;

        let (mut tag_fsm, sender) = setup(2);

        for _ in 0..3 {
            let seg = gen_segcallback(1, 10);
            let _ = sender.send(seg.0);
        }

        let keep_processing = tag_fsm.try_fill_batch(&mut batch, &mut counter);
        assert!(keep_processing);
    }

    #[test]
    fn test_tag_fsm_handle_tasks() {
        let mut batch = Vec::new();
        let mut counter = 0;

        let (mut tag_fsm, sender) = setup(10);

        for _ in 0..10 {
            let seg = gen_segcallback(1, 10);
            let _ = sender.send(seg.0);
        }

        tag_fsm.try_fill_batch(&mut batch, &mut counter);

        let mut cnt = 5;
        tag_fsm.handle_tasks(&mut batch, &mut cnt);

        let engine = tag_fsm.get_mut_engine();
        let writer = engine.get_mut_index_writer();

        let res = writer.commit();
        assert!(res.is_ok());

        let res = writer.delete_all_documents();
        assert!(res.is_ok());
    }

    // #[cfg(feature = "fail_test")]
    #[test]
    fn test_commit_error() {
        use fail::FailScenario;

        let scen = FailScenario::setup();
        fail::cfg("flush-err", "return").unwrap();

        let (mut tag_fsm, sender) = setup(10);
        let mut callbacks = Vec::new();

        for _ in 0..10 {
            let (seg, callback_rev) = gen_segcallback(1, 10);
            let _ = sender.send(seg);

            callbacks.push(callback_rev);
        }

        let mut batch = Vec::new();
        let mut counter = 0;

        tag_fsm.try_fill_batch(&mut batch, &mut counter);

        let mut cnt = 0;
        tag_fsm.handle_tasks(&mut batch, &mut cnt);

        for r in callbacks.into_iter() {
            let res = r.blocking_recv();
            assert!(res.is_ok());

            match res.unwrap() {
                CallbackStat::IOErr(tag_e, _) => {
                    if let TagEngineError::RecordsCommitError(_) = tag_e {
                    } else {
                        panic!();
                    }
                }
                _ => panic!(),
            }
        }

        scen.teardown();
    }

    #[test]
    fn test_commit_ok() {
        let (mut tag_fsm, sender) = setup(10);
        let mut callbacks = Vec::new();

        for _ in 0..10 {
            let (seg, callback_rev) = gen_segcallback(1, 10);
            let _ = sender.send(seg);

            callbacks.push(callback_rev);
        }

        let mut batch = Vec::new();
        let mut counter = 0;

        tag_fsm.try_fill_batch(&mut batch, &mut counter);

        let mut cnt = 0;
        tag_fsm.handle_tasks(&mut batch, &mut cnt);

        tag_fsm.commit(&mut batch);

        for r in callbacks.into_iter() {
            let res = r.blocking_recv();
            assert!(res.is_ok());

            match res.unwrap() {
                CallbackStat::Ok(_) => {}
                _ => panic!(),
            }
        }
    }
}
