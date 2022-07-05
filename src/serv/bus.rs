use super::ShutdownSignal;
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
use grpcio::{DuplexSink, Error as GrpcError};
use grpcio::{Result as GrpcResult, WriteFlags};
use pin_project::pin_project;
use skproto::tracing::{Meta, Meta_RequestType, SegmentData, SegmentRes};
use std::fmt::Debug;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, span, trace, warn, Level};

#[pin_project]
pub struct RemoteMsgPoller<L, S> {
    #[pin]
    source_stream: L,
    sink: S,
    local_sender: UnboundedSender<SegmentDataCallback>,
    shutdown_signal: ShutdownSignal,
}

impl<L> RemoteMsgPoller<L, DuplexSink<SegmentRes>>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
{
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
        meta.set_field_type(Meta_RequestType::NEED_RESEND);
        seg_res.set_meta(meta);

        sink_handle.send_event(SinkEvent::NeedResend((seg_res, WriteFlags::default())));
    }

    fn sink_reconnect(seg_data: SegmentData, sink_handle: SinkHandler) {
        sink_handle.send_event(SinkEvent::ServerShuttingDown((
            seg_data,
            WriteFlags::default(),
        )));
    }
}

impl<L> RemoteMsgPoller<L, DuplexSink<SegmentRes>>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
{
    // Loop poll remote from grpc stream, until:
    // 1. Have met serious grpc transport problem;
    // 2. Have received shutdown signal;
    pub(crate) async fn loop_poll(mut self) -> Result<(), AnyError> {
        let mut stream = self.source_stream.fuse();
        let mut shut_recv = self.shutdown_signal;
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
                        let redirect_res = Self::redirect_batch_exec(seg, sender, handler.clone(), callback_sender).await;

                        if let Err(_) = redirect_res {
                            error!("Poll grpc request failed!");
                            continue;
                        }

                        let sink_handle = handler.clone();
                        TOKIO_RUN.spawn(async move{
                            let call_state = callback_receiver.await.unwrap();

                            match call_state {
                                CallbackStat::Handshake => {
                                    //do nothing
                                }
                                CallbackStat::Ok(pkt) => {
                                    Self::sink_success_response(pkt, sink_handle);
                                },
                                CallbackStat::IOErr(e, pkt) => {
                                    error!(%e, "data flushed to tanvity failed! Notify client to retry");
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
                _ = shut_recv.recv.recv() => {
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
        sink: SinkHandler,
        callback: tokio::sync::oneshot::Sender<CallbackStat>,
    ) -> Result<(), AnyError> {
        match data {
            Ok(segment_data) => {
                let mut segment_processor: ExecutorStat<'a> =
                    segment_data.with_process(mail, sink, callback).into();
                loop {
                    let exec_rs =
                        <ExecutorStat<'a> as SegmentExecute>::exec(segment_processor).await;

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

    pub async fn ack_seg(sink: &mut DuplexSink<SegmentRes>, seq_id: i64) -> Result<(), AnyError> {
        trace!(seq_id = seq_id, "Has received ack callback");
        let seg = {
            let mut s = SegmentRes::default();
            let mut meta = Meta::new();
            meta.set_field_type(Meta_RequestType::TRANS_ACK);
            meta.set_seqId(seq_id);
            s.set_meta(meta);
            (s, WriteFlags::default())
        };
        sink.send(seg).await?;
        Ok(())
    }
}

#[async_trait]
trait SegmentProcess {
    fn with_process<'a>(
        self,
        mail: &'a mut UnboundedSender<SegmentDataCallback>,
        sink: SinkHandler,
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

pub struct SinkActor {
    sink: grpcio::DuplexSink<SegmentRes>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<SinkEvent>,
}

impl SinkActor {
    async fn run(self) -> Result<(), anyhow::Error> {
        let mut recv = self.receiver;
        let mut sink = self.sink;

        while let Some(event) = recv.recv().await {
            match event {
                SinkEvent::HandshakeSuccess((resp, flags)) => {
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

    fn spawn(sink: grpcio::DuplexSink<SegmentRes>) -> (SinkHandler, SinkActor) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        (SinkHandler { sender }, SinkActor { sink, receiver })
    }
}

struct SegmentProcessor<'a> {
    data: SegmentData,
    batch_handle: &'a mut UnboundedSender<SegmentDataCallback>,
    sink: SinkHandler,
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
        sink: SinkHandler,
        callback: tokio::sync::oneshot::Sender<CallbackStat>,
    ) -> SegmentProcessor<'a> {
        SegmentProcessor {
            data: self,
            batch_handle,
            sink,
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
impl<'a> SegmentExecute for ExecutorStat<'a> {
    type Next = ExecutorStat<'a>;

    async fn exec(self) -> Self::Next {
        match self {
            ExecutorStat::HandShake(s) => {
                let conn_id = CONN_MANAGER.gen_new_conn_id();

                let mut resp = SegmentRes::new();
                let mut meta = Meta::new();
                meta.connId = conn_id;
                meta.field_type = Meta_RequestType::HANDSHAKE;
                info!(meta = ?meta, %conn_id, resp = ?resp, "Send handshake resp");
                resp.set_meta(meta);

                s.sink
                    .send_event(SinkEvent::HandshakeSuccess((resp, WriteFlags::default())));

                let _ = s.callback.send(CallbackStat::Handshake);
                return ExecutorStat::Last;
            }
            ExecutorStat::Trans(s) => {
                let span = span!(Level::TRACE, "trans_receiver_consume", data = ?s.data);

                let data = SegmentDataCallback {
                    data: s.data,
                    callback: AckCallback::new(s.callback),
                    span,
                };

                let _ = s.batch_handle.send(data);
                return ExecutorStat::Last;
            }
            ExecutorStat::NeedTrans(s) => {
                trace!(meta = ?s.data.get_meta(), conn_id = s.data.get_meta().get_connId(), "Has received need send data!");
                let span = span!(Level::TRACE, "trans_receiver_consume_need_resend");

                let data = SegmentDataCallback {
                    data: s.data,
                    callback: AckCallback::new(s.callback),
                    span,
                };

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
pub trait SegmentExecute {
    type Next: SegmentExecute;

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
        shutdown_signal: ShutdownSignal,
    ) -> RemoteMsgPoller<L, S> {
        RemoteMsgPoller {
            source_stream: source,
            sink,
            local_sender,
            shutdown_signal,
        }
    }
}

#[cfg(test)]
mod test_remote_msg_poller {
    use super::*;
    use crate::test::gen::{_gen_data_binary, _gen_tag};
    use chrono::Local;
    use skproto::tracing::{Meta, Meta_RequestType};
    use std::pin::Pin;
    use std::task::Poll;

    struct MockSink;

    impl Sink<(SegmentRes, WriteFlags)> for MockSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            return std::task::Poll::Ready(Ok(()));
        }

        fn start_send(
            self: Pin<&mut Self>,
            _item: (SegmentRes, WriteFlags),
        ) -> Result<(), Self::Error> {
            return Ok(());
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            return std::task::Poll::Ready(Ok(()));
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            todo!()
        }
    }

    fn mock_seg(conn_id: i32, api_id: i32, seq_id: i64) -> SegmentData {
        let mut segment = SegmentData::new();
        let mut meta = Meta::new();
        meta.connId = conn_id;
        meta.field_type = Meta_RequestType::TRANS;
        meta.seqId = seq_id;
        let now = Local::now();
        meta.set_send_timestamp(now.timestamp_nanos() as u64);
        let uuid = uuid::Uuid::new_v4();
        segment.set_meta(meta);
        segment.set_trace_id(uuid.to_string());
        segment.set_api_id(api_id);
        segment.set_payload(_gen_data_binary());
        segment.set_zone(_gen_tag(3, 5, 'a'));
        segment.set_biz_timestamp(now.timestamp_millis() as u64);
        segment.set_seg_id(uuid.to_string());
        segment.set_ser_key(_gen_tag(4, 3, 's'));
        segment
    }

    struct MockStream {
        idx: usize,
        api_id: i32,
        seq_id: i64,
    }

    impl Stream for MockStream {
        type Item = GrpcResult<SegmentData>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.idx > 100 {
                return Poll::Ready(None);
            }
            self.api_id += 1;
            self.seq_id += 1;
            self.idx += 1;
            let seg = mock_seg(1, self.api_id, self.seq_id);
            return Poll::Ready(Some(GrpcResult::Ok(seg)));
        }
    }
}
