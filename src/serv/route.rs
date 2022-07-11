use anyhow::Error as AnyError;
use protobuf::Message;
use std::{
    borrow::Cow,
    collections::HashMap,
    marker::PhantomData,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Duration,
};
use tantivy::{
    schema::{Schema, INDEXED, STORED, STRING, TEXT},
    Index,
};
use tokio::{sync::mpsc::UnboundedReceiver, time::sleep};
use tracing::{error, info, trace, trace_span};

use crate::tag::engine::*;
use crate::tag::fsm::TagFsm;
use crate::{com::mail::BasicMailbox, router::Router};
use crate::{
    com::{
        ack::CallbackStat,
        index::{ConvertIndexAddr, MailKeyAddress},
    },
    conf::GlobalConfig,
    serv::ShutdownEvent,
    TOKIO_RUN,
};
use crate::{fsm::Fsm, sched::FsmScheduler};
use crate::{
    router::RouteMsg,
    tag::{engine::TracingTagEngine, fsm::SegmentDataCallback},
};

use super::ShutdownSignal;

pub struct LocalSegmentMsgConsumer<N: Fsm, S> {
    router: Router<N, S>,
    receiver: UnboundedReceiver<SegmentDataCallback>,
}

impl<S> LocalSegmentMsgConsumer<TagFsm, S>
where
    S: FsmScheduler<F = TagFsm> + Clone,
{
    pub fn new(
        router: Router<TagFsm, S>,
        receiver: UnboundedReceiver<SegmentDataCallback>,
    ) -> LocalSegmentMsgConsumer<TagFsm, S> {
        LocalSegmentMsgConsumer { router, receiver }
    }

    fn create_tag_fsm(
        addr: MailKeyAddress,
        dir: &str,
    ) -> Result<(TagFsm, crossbeam::channel::Sender<<TagFsm as Fsm>::Message>), TagEngineError>
    {
        let engine = TracingTagEngine::new(addr, dir)?;
        let (s, r) = crossbeam_channel::unbounded();
        Ok((TagFsm::new(r, None, engine), s))
    }

    pub async fn loop_poll(&mut self, shutdown_signal: ShutdownSignal) -> Result<(), AnyError> {
        let mut recv = shutdown_signal.recv;
        let drop_notify = shutdown_signal.drop_notify;

        loop {
            tokio::select! {
                segment_callback = self.receiver.recv() => {
                    match segment_callback {
                        Some(segment_callback) => {
                            let _consume_span = trace_span!(
                                "recv_and_process",
                                trace_id = ?segment_callback.data.trace_id
                            )
                            .entered();
                            trace!(parent: &segment_callback.span, "Has received the segment, try to route to mailbox.");

                            let addr = segment_callback.data.biz_timestamp.with_index_addr().unwrap();
                            if let Err(stat) = self.router.route_msg(addr, segment_callback, Self::create_tag_fsm) {
                                let seg = stat.0;
                                seg.callback.callback(CallbackStat::IOErr(TagEngineError::IndexDirCreateFailed, seg.data.into()));
                            }
                        }
                        None => return Err(AnyError::msg("LocalSegmentMsgConsumer's sender has closed")),
                    }
                }
                event = recv.recv() => {
                    match event.unwrap() {
                        ShutdownEvent::GracefulStop => {
                            while let Ok(segment_callback) = self.receiver.try_recv() {
                                info!("loop consume all the segments waiting in the channel");

                                let addr = segment_callback.data.biz_timestamp.with_index_addr().unwrap();
                                if let Err(stat) = self.router.route_msg(addr, segment_callback, Self::create_tag_fsm) {
                                    let seg = stat.0;
                                    seg.callback.callback(CallbackStat::IOErr(TagEngineError::IndexDirCreateFailed, seg.data.into()));
                                }
                            }
                            info!("Wait more 10 seconds of consuming operation..");
                            TOKIO_RUN.spawn(async {
                                info!("Start to counting down!");
                                for i in (1..11).rev() {
                                    info!("Shutdown seconds remain: {}", i);
                                    sleep(Duration::from_secs(1)).await;
                                }
                            });
                            sleep(Duration::from_secs(10)).await;
                            info!("Batch system bridge route task has shutdown..");
                            drop(drop_notify);
                            break Ok(());
                        }
                        _ => {
                            info!("Has receive force shutdown event, quit");
                            break Ok(());
                        }
                    }

                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        com::{
            ack::{AckCallback, CallbackStat},
            index::MailKeyAddress,
            mail::BasicMailbox,
        },
        conf::GlobalConfig,
        fsm::Fsm,
        router::RouteMsg,
        tag::fsm::SegmentDataCallback,
    };
    use chrono::offset::Local;
    use protobuf::Message;
    use skproto::tracing::SegmentData;
    use std::borrow::Cow;
    use tokio::sync::oneshot::Receiver;
    use tracing::{span, Level};

    use super::LocalSegmentMsgConsumer;

    struct MockRoute {}

    struct MockFsm {
        mailbox: Option<BasicMailbox<Self>>,
    }

    impl Fsm for MockFsm {
        type Message = ();

        fn is_stopped(&self) -> bool {
            todo!()
        }

        fn set_mailbox(
            &mut self,
            mailbox: std::borrow::Cow<'_, crate::com::mail::BasicMailbox<Self>>,
        ) where
            Self: Sized,
        {
            let mailbox = mailbox.into_owned();
            self.mailbox = Some(mailbox);
        }

        fn take_mailbox(&mut self) -> Option<crate::com::mail::BasicMailbox<Self>>
        where
            Self: Sized,
        {
            self.mailbox.take()
        }

        fn tag_tick(&mut self) {
            todo!()
        }

        fn untag_tick(&mut self) {
            todo!()
        }

        fn is_tick(&self) -> bool {
            todo!()
        }
    }

    fn gen_segcallback(days: i64, secs: i64) -> (SegmentDataCallback, Receiver<CallbackStat>) {
        let cur = Local::now();
        let date_time = cur
            .checked_sub_signed(chrono::Duration::days(days))
            .unwrap();
        let date_time = date_time
            .checked_sub_signed(chrono::Duration::seconds(secs))
            .unwrap();

        let mut segment = SegmentData::new();
        segment.set_biz_timestamp(date_time.timestamp_millis() as u64);

        let span = span!(Level::INFO, "my_span");
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let callback = AckCallback::new(sender);

        (SegmentDataCallback::new(segment, callback, span), receiver)
    }

    #[tokio::test]
    async fn test_invalid_segment() {
        let (seg_callback, receiver) = gen_segcallback(40, 60);
    }
}
