use super::ShutdownEvent;
use crate::com::ack::{AckCallback, CallbackStat};
use crate::com::pkt::PktHeader;
use crate::serv::CONN_MANAGER;
use crate::tag::fsm::SegmentDataCallback;
use crate::TOKIO_RUN;
use anyhow::Error as AnyError;
use async_trait::async_trait;
use futures::SinkExt;
use futures::{Stream, StreamExt};
use futures_sink::Sink;
use grpcio::Error as GrpcError;
use grpcio::{Result as GrpcResult, WriteFlags};
use pin_project::pin_project;
use skproto::tracing::{Meta, Meta_RequestType, SegmentData, SegmentRes};
use std::error::Error;
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, span, trace, warn, Level};

#[pin_project]
pub struct RemoteMsgPoller<L, S> {
    #[pin]
    source_stream: L,
    sink: S,
    local_sender: UnboundedSender<SegmentDataCallback>,
    broad_shutdown_receiver: tokio::sync::broadcast::Receiver<ShutdownEvent>,
}

impl<L, S> RemoteMsgPoller<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: futures_sink::Sink<(SegmentRes, WriteFlags)> + Send + 'static,
    S::Error: Send + Sync + Into<anyhow::Error> + 'static,
{
    fn sink_handshake_response(conn_id: i32, sink_handle: SinkHandler) {
        let mut resp = SegmentRes::new();
        let mut meta = Meta::new();

        meta.connId = conn_id;
        meta.field_type = Meta_RequestType::HANDSHAKE;
        info!(meta = ?meta, %conn_id, resp = ?resp, "Send handshake resp");
        resp.set_meta(meta);

        sink_handle.send_event(SinkEvent::HandshakeSuccess((resp, WriteFlags::default())));
    }

    fn sink_success_response(pkt: PktHeader, sink_handle: SinkHandler) {
        let mut seg_res = SegmentRes::new();
        let mut meta = Meta::new();

        meta.set_connId(pkt.conn_id);
        meta.set_seqId(pkt.seq_id);
        meta.set_field_type(Meta_RequestType::TRANS_ACK);
        seg_res.set_meta(meta);

        sink_handle.send_event(SinkEvent::AckReq((seg_res, WriteFlags::default())));
    }

    fn sink_retry_response(pkt: PktHeader, sink_handle: SinkHandler) {
        let mut seg_res = SegmentRes::new();
        let mut meta = Meta::new();

        meta.set_connId(pkt.conn_id);
        meta.set_seqId(pkt.seq_id);
        meta.set_resend_count(pkt.resend_count + 1);
        meta.set_field_type(Meta_RequestType::NEED_RESEND);

        seg_res.set_meta(meta);

        tracing::info!("retry response is:{:?}", seg_res);
        sink_handle.send_event(SinkEvent::NeedResend((seg_res, WriteFlags::default())));
    }

    fn sink_reconnect(seg_data: SegmentData, sink_handle: SinkHandler) {
        sink_handle.send_event(SinkEvent::ServerShuttingDown((
            seg_data,
            WriteFlags::default(),
        )));
    }
}

impl<L, S> RemoteMsgPoller<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: futures_sink::Sink<(SegmentRes, WriteFlags)> + Send + Unpin + 'static,
    S::Error: Send + Sync + Error + Into<anyhow::Error> + 'static,
{
    // Loop poll remote from grpc stream, until:
    // 1. Have met serious grpc transport problem;
    // 2. Have received shutdown signal;
    pub(crate) async fn loop_poll(mut self) -> Result<(), AnyError> {
        let mut stream = self.source_stream.fuse();
        let mut shut_recv = self.broad_shutdown_receiver;
        let sender = &mut self.local_sender;

        let (handler, actor) = SinkActor::spawn(self.sink);

        // In charge of receiving sink response to client
        TOKIO_RUN.spawn(async move {
            let r = actor.run().await;

            match r {
                Ok(_) => {
                    warn!("Sink sender is dropped, actor exit!");
                }

                Err(e) => {
                    error!(%e, "Sink remote has shut down");
                }
            }
        });

        loop {
            tokio::select! {
                seg = stream.next() => {
                    if let Some(seg) = seg {

                        let (callback_sender, callback_receiver) = tokio::sync::oneshot::channel();
                        let redirect_res = Self::redirect_batch_exec(seg, sender, callback_sender).await;

                        if let Err(e) = redirect_res {
                            error!("Poll grpc request failed!e:{:?}", e);
                            break;
                        }

                        let sink_handle = handler.clone();
                        TOKIO_RUN.spawn(async move{
                            let call_state = callback_receiver.await.unwrap();

                            match call_state {
                                CallbackStat::Handshake(conn_id) => {
                                    Self::sink_handshake_response(conn_id, sink_handle);
                                }
                                CallbackStat::Ok(pkt) => {
                                    Self::sink_success_response(pkt, sink_handle);
                                },
                                CallbackStat::IOErr(e, pkt) => {
                                    error!(%e, "data flushed to tanvity failed! Notify client to retry, pkt:{:?}", pkt);
                                    Self::sink_retry_response(pkt, sink_handle);
                                },
                                CallbackStat::ExpiredData(pkt) => {
                                    Self::sink_success_response(pkt, sink_handle);
                                }
                                CallbackStat::ShuttingDown(seg_data) => {
                                    Self::sink_reconnect(seg_data, sink_handle);
                                }
                            }
                        });
                    }
                },
                _ = shut_recv.recv() => {
                    info!("Received shutdown signal, ignore all the data in the stream");
                    break;
                },
                else => {
                    break;
                },
            }
        }
        Ok(())
    }

    async fn redirect_batch_exec<'a>(
        data: GrpcResult<SegmentData>,
        mail: &'a mut UnboundedSender<SegmentDataCallback>,
        callback: tokio::sync::oneshot::Sender<CallbackStat>,
    ) -> Result<(), AnyError> {
        match data {
            Ok(segment_data) => {
                let mut segment_processor: ExecutorStat<'a> =
                    segment_data.with_process(mail, callback).into();
                loop {
                    let exec_rs =
                        <ExecutorStat<'a> as SegmentCallbackWrap>::exec(segment_processor).await;

                    match exec_rs {
                        ExecutorStat::HandShake(o)
                        | ExecutorStat::NeedTrans(o)
                        | ExecutorStat::Trans(o) => {
                            segment_processor = o.into();
                            continue;
                        }
                        ExecutorStat::Last => break,
                    }
                }
            }
            Err(e) => match e {
                GrpcError::Codec(_) | GrpcError::InvalidMetadata(_) => {
                    warn!("Invalid rpc remote request, e:{}, not influence loop poll, we just skip this request", e);
                    return Ok(());
                }
                _ => return Err(e.into()),
            },
        }
        Ok(())
    }
}

#[async_trait]
trait SegmentProcess {
    fn with_process<'a>(
        self,
        mail: &'a mut UnboundedSender<SegmentDataCallback>,
        callback: tokio::sync::oneshot::Sender<CallbackStat>,
    ) -> SegmentProcessor<'a>;
}

pub enum SinkEvent {
    HandshakeSuccess((SegmentRes, WriteFlags)),
    AckReq((SegmentRes, WriteFlags)),
    NeedResend((SegmentRes, WriteFlags)),
    ServerShuttingDown((SegmentData, WriteFlags)),
}

#[derive(Clone)]
pub struct SinkHandler {
    sender: tokio::sync::mpsc::UnboundedSender<SinkEvent>,
}

impl SinkHandler {
    fn send_event(&self, event: SinkEvent) {
        let _ = self.sender.send(event);
    }
}

pub struct SinkActor<S> {
    sink: S,
    receiver: tokio::sync::mpsc::UnboundedReceiver<SinkEvent>,
}

impl<S> SinkActor<S>
where
    S: futures_sink::Sink<(SegmentRes, WriteFlags)> + Unpin + 'static,
    S::Error: Send + std::error::Error + Sync + 'static,
{
    async fn run(self) -> Result<(), anyhow::Error> {
        let mut recv = self.receiver;
        let mut sink = self.sink;

        while let Some(event) = recv.recv().await {
            match event {
                SinkEvent::HandshakeSuccess((resp, flags)) => {
                    info!("Send handshake success to remote client");
                    sink.send((resp, flags)).await?;
                }
                SinkEvent::NeedResend((resp, flags)) => {
                    warn!("Request has met io error, need resend resp:{:?}", resp);
                    sink.send((resp, flags)).await?;
                }
                SinkEvent::AckReq((resp, flags)) => {
                    sink.send((resp, flags)).await?;
                }
                SinkEvent::ServerShuttingDown((seg_data, flags)) => {
                    warn!("Server is shutting down,this request is ignore");

                    let mut segment_res = SegmentRes::new();
                    let mut meta = Meta::new();

                    meta.set_connId(seg_data.get_meta().get_connId());
                    meta.set_seqId(seg_data.get_meta().get_seqId());

                    segment_res.set_meta(meta);
                    segment_res.set_org_data(seg_data);

                    sink.send((segment_res, flags)).await?;
                }
            }
        }
        warn!("grpc duplex sink is shutted down");
        Ok(())
    }

    fn spawn(sink: S) -> (SinkHandler, SinkActor<S>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        (SinkHandler { sender }, SinkActor { sink, receiver })
    }
}

struct SegmentProcessor<'a> {
    data: SegmentData,
    batch_handle: &'a mut UnboundedSender<SegmentDataCallback>,
    callback: tokio::sync::oneshot::Sender<CallbackStat>,
}

impl<'a> From<SegmentProcessor<'a>> for ExecutorStat<'a> {
    fn from(s: SegmentProcessor<'a>) -> Self {
        let meta_type = s.data.get_meta().field_type;
        match meta_type {
            Meta_RequestType::HANDSHAKE => ExecutorStat::HandShake(s),
            Meta_RequestType::NEED_RESEND => ExecutorStat::NeedTrans(s),
            Meta_RequestType::TRANS => return ExecutorStat::Trans(s),
            _ => ExecutorStat::Last,
        }
    }
}

#[async_trait]
impl SegmentProcess for SegmentData {
    fn with_process<'a>(
        self,
        batch_handle: &'a mut UnboundedSender<SegmentDataCallback>,
        callback: tokio::sync::oneshot::Sender<CallbackStat>,
    ) -> SegmentProcessor<'a> {
        SegmentProcessor {
            data: self,
            batch_handle,
            callback,
        }
    }
}

enum ExecutorStat<'a> {
    HandShake(SegmentProcessor<'a>),
    Trans(SegmentProcessor<'a>),
    NeedTrans(SegmentProcessor<'a>),
    Last,
}

#[async_trait]
impl<'a> SegmentCallbackWrap for ExecutorStat<'a> {
    type Next = ExecutorStat<'a>;

    async fn exec(self) -> Self::Next {
        match self {
            ExecutorStat::HandShake(s) => {
                let conn_id = CONN_MANAGER.gen_new_conn_id();
                let _ = s.callback.send(CallbackStat::Handshake(conn_id));
                return ExecutorStat::Last;
            }

            ExecutorStat::Trans(s) => {
                let seq_id = (&s.data).get_meta().get_seqId();

                let span = span!(Level::TRACE, "trans_receiver_consume", data = ?s.data);
                let data = SegmentDataCallback::new(s.data, AckCallback::new(s.callback), span);

                trace!("Send new data to batch system:{}", seq_id);
                let _ = s.batch_handle.send(data);
                return ExecutorStat::Last;
            }

            ExecutorStat::NeedTrans(s) => {
                trace!(meta = ?s.data.get_meta(), conn_id = s.data.get_meta().get_connId(), "Has received need send data!");
                let span = span!(Level::TRACE, "trans_receiver_consume_need_resend");

                let data = SegmentDataCallback::new(s.data, AckCallback::new(s.callback), span);
                let _ = s.batch_handle.send(data);

                trace!("NEED_RESEND: Has sent segment to local channel(Waiting for storage operation and callback)");
                return ExecutorStat::Last;
            }
            ExecutorStat::Last => {
                return ExecutorStat::Last;
            }
        }
    }
}

#[async_trait]
pub trait SegmentCallbackWrap {
    type Next: SegmentCallbackWrap;

    async fn exec(self) -> Self::Next;
}

impl<L, S> RemoteMsgPoller<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: Sink<(SegmentRes, WriteFlags)> + Unpin + Sync + Send,
    AnyError: From<S::Error>,
    S::Error: Debug + Sync + Send,
{
    pub fn new(
        source: L,
        sink: S,
        local_sender: UnboundedSender<SegmentDataCallback>,
        broad_shutdown_receiver: tokio::sync::broadcast::Receiver<ShutdownEvent>,
    ) -> RemoteMsgPoller<L, S> {
        RemoteMsgPoller {
            source_stream: source,
            sink,
            local_sender,
            broad_shutdown_receiver,
        }
    }
}

#[cfg(test)]
mod test_remote_msg_poller {
    use self::mock_handshake::HandshakeStream;
    use super::*;
    use crate::log::init_console_logger;
    use crate::serv::ShutdownSignal;
    use crate::tag::engine::TagEngineError;
    use futures::Sink;
    use skproto::tracing::{Meta, Meta_RequestType};
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use tokio::sync::mpsc::UnboundedReceiver;

    fn setup() {
        init_console_logger();
    }

    fn teardown() {
        info!("Service is shutting down...");
    }

    pub struct TestSink {
        pub send: UnboundedSender<(SegmentRes, WriteFlags)>,
    }

    impl Sink<(SegmentRes, WriteFlags)> for TestSink {
        type Error = grpcio::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(
            self: Pin<&mut Self>,
            item: (SegmentRes, WriteFlags),
        ) -> Result<(), Self::Error> {
            let _ = self.send.send(item);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    mod mock_handshake {
        use super::*;

        pub struct HandshakeStream {
            pub is_ready: bool,
        }

        impl Stream for HandshakeStream {
            type Item = GrpcResult<SegmentData>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _ctx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if !self.is_ready {
                    self.is_ready = true;

                    let mut seg = SegmentData::new();
                    let mut meta = Meta::new();
                    meta.set_field_type(Meta_RequestType::HANDSHAKE);
                    seg.set_meta(meta);

                    Poll::Ready(Some(Ok(seg)))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn init_remote_poller() -> (
        RemoteMsgPoller<HandshakeStream, TestSink>,
        UnboundedReceiver<SegmentDataCallback>,
        UnboundedReceiver<(SegmentRes, WriteFlags)>,
    ) {
        use mock_handshake::*;

        let (local_sender, local_recveiver) = tokio::sync::mpsc::unbounded_channel();
        let (broad_sender, _broad_recv) = tokio::sync::broadcast::channel(1);

        let (_shutdown_signal, _recv) = ShutdownSignal::chan(broad_sender.clone());

        let (sink_send, sink_res) = tokio::sync::mpsc::unbounded_channel();
        (
            RemoteMsgPoller {
                source_stream: HandshakeStream { is_ready: false },
                sink: TestSink { send: sink_send },
                local_sender,
                broad_shutdown_receiver: broad_sender.subscribe(),
            },
            local_recveiver,
            sink_res,
        )
    }

    #[tokio::test]
    async fn test_handshake_logic() {
        setup();

        let (poller, _local_recv, mut sink_recv) = init_remote_poller();

        tokio::spawn(async move {
            let _ = poller.loop_poll().await;
        });
        let seg = sink_recv.recv().await;

        match seg {
            Some((s, _w)) => {
                let meta = s.get_meta();
                assert_eq!(meta.get_field_type(), Meta_RequestType::HANDSHAKE);
                assert!(meta.get_connId() > 0);
            }
            None => {
                unreachable!();
            }
        }

        teardown();
    }

    mod normal_req {
        use crate::{com::test_util::mock_seg, serv::ShutdownEvent};

        use super::*;

        pub struct NormalReq {
            is_ready: bool,
        }

        impl NormalReq {
            fn new() -> NormalReq {
                NormalReq { is_ready: false }
            }
        }

        impl Stream for NormalReq {
            type Item = Result<skproto::tracing::SegmentData, grpcio::Error>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if !self.is_ready {
                    let seg = mock_seg(1, 1, 1);
                    self.is_ready = true;
                    Poll::Ready(Some(Ok(seg)))
                } else {
                    Poll::Pending
                }
            }
        }

        pub fn init_remote_poller() -> (
            RemoteMsgPoller<NormalReq, TestSink>,
            UnboundedReceiver<SegmentDataCallback>,
            UnboundedReceiver<(SegmentRes, WriteFlags)>,
            tokio::sync::broadcast::Sender<ShutdownEvent>,
        ) {
            let (local_sender, local_recveiver) = tokio::sync::mpsc::unbounded_channel();
            let (broad_sender, _broad_recv) = tokio::sync::broadcast::channel(1);

            let (shutdown_signal, _recv) = ShutdownSignal::chan(broad_sender.clone());

            let (sink_send, sink_res) = tokio::sync::mpsc::unbounded_channel();
            (
                RemoteMsgPoller {
                    source_stream: NormalReq::new(),
                    sink: TestSink { send: sink_send },
                    local_sender,
                    broad_shutdown_receiver: broad_sender.subscribe(),
                },
                local_recveiver,
                sink_res,
                broad_sender,
            )
        }
    }

    #[tokio::test]
    async fn test_normal_req_basic_ok() {
        setup();
        let (poller, mut local_recv, mut sink_recv, _shutdown_sender) =
            normal_req::init_remote_poller();

        tokio::spawn(async {
            let _ = poller.loop_poll().await;
        });

        let segment_callback = local_recv.recv().await;
        match segment_callback {
            Some(seg_callback) => {
                let seg = seg_callback.data;
                seg_callback.callback.callback(CallbackStat::Ok(seg.into()));
            }
            None => {
                unreachable!()
            }
        }

        if let Some((res, _flags)) = sink_recv.recv().await {
            let conn_id = res.get_meta().get_connId();
            let seq_id = res.get_meta().get_seqId();
            let meta_type = res.get_meta().get_field_type();

            assert_eq!(conn_id, 1);
            assert_eq!(seq_id, 1);
            assert_eq!(meta_type, Meta_RequestType::TRANS_ACK);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_retry_resp() {
        info!("testing retry resp");
        setup();

        let (poller, mut local_recv, mut sink_recv, _shutdown_sender) =
            normal_req::init_remote_poller();

        tokio::spawn(async {
            let _ = poller.loop_poll().await;
        });

        let segment_callback = local_recv.recv().await;
        match segment_callback {
            Some(seg_callback) => {
                let seg = seg_callback.data;
                let pkt_header = seg.into();

                seg_callback.callback.callback(CallbackStat::IOErr(
                    TagEngineError::IndexNotExist,
                    pkt_header,
                ));

                let resp = sink_recv.recv().await;

                if let Some((seg_resp, _)) = resp {
                    let meta = seg_resp.get_meta();

                    assert_eq!(1, meta.get_connId());
                    assert_eq!(1, meta.get_seqId());
                    assert_eq!(Meta_RequestType::NEED_RESEND, meta.get_field_type());
                } else {
                    unreachable!();
                }
            }
            None => {
                unreachable!()
            }
        }
    }
}
