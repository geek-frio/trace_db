use crate::com::ack::{AckCallback, CallbackStat};
use crate::serv::CONN_MANAGER;
use crate::tag::fsm::SegmentDataCallback;
use crate::TOKIO_RUN;
use anyhow::Error as AnyError;
use async_trait::async_trait;
// use futures::channel::mpsc::{unbounded as funbounded, Sender, UnboundedSender};
use futures::future::FutureExt;
use futures::{select, SinkExt};
use futures::{Stream, StreamExt};
use futures_sink::Sink;
use grpcio::Error as GrpcError;
use grpcio::{Result as GrpcResult, WriteFlags};
use pin_project::pin_project;
use skproto::tracing::{Meta, Meta_RequestType, SegmentData, SegmentRes};
use std::fmt::{Debug, Display};
use std::pin::Pin;
use tokio::sync::mpsc::{unbounded_channel as funbounded, UnboundedSender};
// use tokio::sync::oneshot::Receiver;
use tracing::{error, info, span, trace, trace_span, warn, Level};

#[pin_project]
pub struct RemoteMsgPoller<L, S> {
    #[pin]
    source_stream: L,
    sink: S,
    local_sender: UnboundedSender<SegmentDataCallback>,
    shut_recv: tokio::sync::broadcast::Receiver<()>,
}

impl<L, S> RemoteMsgPoller<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: Sink<(SegmentRes, WriteFlags)> + Unpin + Sync + Send,
    AnyError: From<S::Error>,
    S::Error: Send + std::error::Error + Sync,
{
    // Loop poll remote from grpc stream, until:
    // 1. Have met serious grpc transport problem;
    // 2. Have received shutdown signal;
    pub(crate) async fn loop_poll(mut self) -> Result<(), AnyError> {
        let mut stream = self.source_stream.fuse();
        let mut shut_recv = self.shut_recv;
        let sender = &mut self.local_sender;
        let sink = &mut self.sink;

        loop {
            tokio::select! {
                seg = stream.next() => {
                    if let Some(seg) = seg {
                        // let (send, recv) = tokio::sync::oneshot::channel();

                        // Self::redirect_batch_exec(seg, sender, sink, send).await;
                        // TOKIO_RUN.spawn(async {
                        //     recv.
                        // });
                    }
                },
                shut_recv = shut_recv.recv() => {
                    info!("Received shutdown signal, ignore all the data in the stream");
                    break;
                },
                else => {
                    break;
                },
            }
        }
        // let mut source_stream = this.source_stream.fuse();
        // // let mut ack_stream = Pin::new(&mut ack_r);
        // let mut shut_recv = this.shut_recv.fuse();

        // let mut sink = this.sink;
        // let mut local_sender = this.local_sender;

        // TOKIO_RUN.spawn(async move {
        //     let ack_data = callback_recv.await;
        //     match ack_data {
        //         Ok(data) => match data {
        //             CallbackStat::Ok(id) => {}
        //             CallbackStat::IOErr(e, id) => {}
        //         },
        //         Err(_) => {}
        //     }
        // });
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

    pub async fn ack_seg(sink: &mut S, seq_id: i64) -> Result<(), AnyError> {
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

pub enum SinkEvent {}

#[derive(Clone)]
pub struct SinkHandler {
    sender: tokio::sync::mpsc::UnboundedSender<SinkEvent>,
}

pub struct SinkActor {
    sink: grpcio::DuplexSink<(SegmentRes, WriteFlags)>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<SinkEvent>,
}

impl SinkActor {
    async fn run(self) {
        let mut recv = self.receiver;
        while let Some(event) = recv.recv().await {}
    }
}

struct SegmentProcessor<'a> {
    data: SegmentData,
    mail: &'a mut UnboundedSender<SegmentDataCallback>,
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
        mail: &'a mut UnboundedSender<SegmentDataCallback>,
        sink: SinkHandler,
        callback: tokio::sync::oneshot::Sender<CallbackStat>,
    ) -> SegmentProcessor<'a> {
        SegmentProcessor {
            data: self,
            mail,
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
            ExecutorStat::HandShake(mut s) => {
                let conn_id = CONN_MANAGER.gen_new_conn_id();
                let mut resp = SegmentRes::new();
                let mut meta = Meta::new();
                meta.connId = conn_id;
                meta.field_type = Meta_RequestType::HANDSHAKE;
                info!(meta = ?meta, %conn_id, resp = ?resp, "Send handshake resp");
                resp.set_meta(meta);
                let mut sink = Pin::new(&mut s.sink);
                // We don't care handshake is success or not, client should retry for this
                // let send_res = sink.send((resp, WriteFlags::default())).await;
                // if let Err(e) = send_res {
                //     error!(conn_id, "Handshake send response failed!");
                //     return Err(ExecuteErr::SinkErr(e));
                // }
                // info!("Send handshake resp success");
                // let _ = sink.flush().await;
                return ExecutorStat::Last;
            }
            ExecutorStat::Trans(s) => {
                let span = span!(Level::TRACE, "trans_receiver_consume", data = ?s.data);
                let data = SegmentDataCallback {
                    data: s.data,
                    callback: AckCallback::new(Some(s.callback)),
                    span,
                };
                // Unbounded sender, ignore
                let _ = s.mail.send(data);
                return ExecutorStat::Last;
            }
            ExecutorStat::NeedTrans(s) => {
                trace!(meta = ?s.data.get_meta(), conn_id = s.data.get_meta().get_connId(), "Has received need send data!");
                let span = span!(Level::TRACE, "trans_receiver_consume_need_resend");
                let data = SegmentDataCallback {
                    data: s.data,
                    callback: AckCallback::new(Some(s.callback)),
                    span,
                };
                let _ = s.mail.send(data);
                trace!("NEED_RESEND: Has sent segment to local channel(Waiting for storage operation and callback)");
                return ExecutorStat::Last;
            }
            ExecutorStat::Last => {
                return ExecutorStat::Last;
            }
        }
    }
}

#[derive(Debug)]
enum ExecuteErr<SinkErr: Debug> {
    SinkErr(SinkErr),
}

impl<SinkErr: Debug> Display for ExecuteErr<SinkErr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = f.write_fmt(format_args!("{:?}", self));
        Ok(())
    }
}

impl<SinkErr: Debug> std::error::Error for ExecuteErr<SinkErr> {}

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
        shut_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> RemoteMsgPoller<L, S> {
        RemoteMsgPoller {
            source_stream: source,
            sink,
            local_sender,
            shut_recv,
        }
    }
}

#[cfg(test)]
mod test_remote_msg_poller {
    use crate::test::gen::{_gen_data_binary, _gen_tag};

    use super::*;
    use chrono::Local;
    use skproto::tracing::{Meta, Meta_RequestType};
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
