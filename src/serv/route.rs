use super::ShutdownSignal;
use crate::router::Router;
use crate::tag::engine::*;
use crate::tag::fsm::TagFsm;
use crate::{
    com::{
        ack::CallbackStat,
        index::{ConvertIndexAddr, MailKeyAddress},
    },
    serv::ShutdownEvent,
    TOKIO_RUN,
};
use crate::{fsm::Fsm, sched::FsmScheduler};
use crate::{
    router::RouteMsg,
    tag::{engine::TracingTagEngine, fsm::SegmentDataCallback},
};
use anyhow::Error as AnyError;
use std::time::Duration;
use tokio::{sync::mpsc::UnboundedReceiver, time::sleep};
use tracing::{info, trace, trace_span};

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
        let mut shutdown = shutdown_signal.recv;
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

                            let res_addr = segment_callback.data.biz_timestamp.with_index_addr();
                            match res_addr {
                                Ok(addr) => {
                                    if let Err(stat) = self.router.route_msg(addr, segment_callback, Self::create_tag_fsm) {
                                        let seg = stat.0;
                                        seg.callback.callback(CallbackStat::IOErr(stat.1, seg.data.into()));
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("invalid segment! data time expire 30 days,e:{:?}", e);
                                    segment_callback.callback.callback(CallbackStat::ExpiredData(segment_callback.data.into()));
                                }
                            }

                       }
                        None => return Err(AnyError::msg("LocalSegmentMsgConsumer's sender has closed")),
                    }
                }
                event = shutdown.recv() => {
                    match event.unwrap() {
                        ShutdownEvent::GracefulStop => {
                            info!("LocalSegmentMsgConsumer is shutting down...");
                            // Fail all the pending request
                            while let Ok(segment_callback) = self.receiver.try_recv() {
                                info!("loop consume all the segments waiting in the channel");

                                let addr = segment_callback.data.biz_timestamp.with_index_addr().unwrap();
                                if let Err(stat) = self.router.route_msg(addr, segment_callback, Self::create_tag_fsm) {
                                    let seg = stat.0;
                                    seg.callback.callback(CallbackStat::IOErr(stat.1, seg.data.into()));
                                }
                            }

                            info!("Wait more 10 seconds of consuming operation..");

                            Self::shutdown_counting_down();
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
                else => {
                    panic!("Unexpected error stat! LocalSegmentMsgConsumer is quit");
                }
            }
        }
    }

    fn shutdown_counting_down() {
        TOKIO_RUN.spawn(async {
            info!("Start to counting down!");
            for i in (1..11).rev() {
                info!("Shutdown seconds remain: {}", i);
                sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::LocalSegmentMsgConsumer;
    use crate::{
        batch::FsmTypes,
        com::ack::{AckCallback, CallbackStat},
        log::init_console_logger,
        router::Router,
        sched::NormalScheduler,
        serv::{ShutdownEvent, ShutdownSignal},
        tag::fsm::{SegmentDataCallback, TagFsm},
        TOKIO_RUN,
    };
    use chrono::offset::Local;
    use core::panic;
    use skproto::tracing::SegmentData;
    use std::sync::{atomic::AtomicUsize, Arc};
    use tokio::sync::oneshot::Receiver;
    use tracing::{span, Level};

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

    type BatchActor = crossbeam_channel::Receiver<FsmTypes<TagFsm>>;
    type RemoteMsgHandle = tokio::sync::mpsc::UnboundedSender<SegmentDataCallback>;

    struct Init {
        shutdown: ShutdownSignal,
        local: LocalSegmentMsgConsumer<TagFsm, NormalScheduler<TagFsm>>,
        remote_handle: RemoteMsgHandle,
        batch_receiver: BatchActor,
    }

    fn setup() -> Init {
        init_console_logger();

        let (s, r) = crossbeam_channel::unbounded::<FsmTypes<TagFsm>>();

        let fsm_sche = NormalScheduler { sender: s };
        let atomic = AtomicUsize::new(1);

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let router = Router::new(fsm_sche, Arc::new(atomic), Arc::new(Default::default()));
        let local_cons = LocalSegmentMsgConsumer::new(router, receiver);

        let (b_sender, _b_receiver) = tokio::sync::broadcast::channel(1);
        let (shutdown_signal, _recv) = ShutdownSignal::chan(b_sender);

        Init {
            shutdown: shutdown_signal,
            local: local_cons,
            remote_handle: sender,
            batch_receiver: r,
        }
    }

    #[tokio::test]
    async fn test_expired_segdata() {
        let mut init = setup();

        let (seg_callback, receiver) = gen_segcallback(40, 60);

        let _ = init.remote_handle.send(seg_callback);
        tokio::spawn(async move {
            let _ = init.local.loop_poll(init.shutdown).await;
        });

        let r = receiver.await.unwrap();
        match r {
            CallbackStat::ExpiredData(_) => {}
            _ => {
                panic!();
            }
        }
    }

    #[tokio::test]
    async fn test_normal_msg() {
        let mut init = setup();

        let (seg_callback, _receiver) = gen_segcallback(10, 10);

        let _ = init.remote_handle.send(seg_callback);
        TOKIO_RUN.spawn(async move {
            let _ = init.local.loop_poll(init.shutdown).await;
        });

        let join = TOKIO_RUN.spawn_blocking(move || {
            let f = init.batch_receiver.recv();
            assert!(f.is_ok());
        });
        let _ = join.await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_shutdown_graceful() {
        let mut init = setup();

        let shutdown = init.shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let _ = init.shutdown.sender.send(ShutdownEvent::GracefulStop);
        });

        let _ = init.local.loop_poll(shutdown).await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_shutdown_force() {
        let mut init = setup();

        let shutdown = init.shutdown.clone();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let _ = init.shutdown.sender.send(ShutdownEvent::ForceStop);
        });

        let _ = init.local.loop_poll(shutdown).await;
    }
}
