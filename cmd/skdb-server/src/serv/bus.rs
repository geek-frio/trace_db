use crate::serv::CONN_MANAGER;
use anyhow::Error as AnyError;
use async_trait::async_trait;
use futures::channel::mpsc::{unbounded as funbounded, Sender, UnboundedSender};
use futures::future::FutureExt;
use futures::{select, SinkExt};
use futures::{Stream, StreamExt};
use futures_sink::Sink;
use grpcio::Error as GrpcError;
use grpcio::{Result as GrpcResult, WriteFlags};
use pin_project::pin_project;
use skdb::com::ack::{AckCallback, AckWindow, WindowErr};
use skdb::tag::fsm::SegmentDataCallback;
use skproto::tracing::{Meta, Meta_RequestType, SegmentData, SegmentRes};
use std::fmt::{Debug, Display};
use std::pin::Pin;
use tokio::sync::oneshot::Receiver;
use tracing::{error, info, span, trace, trace_span, warn, Level};

#[pin_project]
pub struct RemoteMsgPoller<L, S> {
    #[pin]
    source_stream: L,
    sink: S,
    ack_win: AckWindow,
    local_sender: Sender<SegmentDataCallback>,
    shut_recv: Receiver<()>,
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
    pub(crate) async fn loop_poll(self: Pin<&mut Self>) -> Result<(), AnyError> {
        let this = self.project();

        let (ack_s, mut ack_r) = funbounded::<i64>();

        let mut source_stream = this.source_stream.fuse();
        let mut ack_stream = Pin::new(&mut ack_r);
        let mut shut_recv = this.shut_recv.fuse();

        let mut ack_win = this.ack_win;
        let mut sink = this.sink;
        let mut local_sender = this.local_sender;

        loop {
            let _one_msg = trace_span!("recv_msg_one_loop");
            select! {
                remote_data = source_stream.next() => {
                    if let Some(remote_data) = remote_data{
                        let r = Self::remote_msg_exec(remote_data, &mut ack_win, &mut local_sender, &mut sink, ack_s.clone()).await;
                        if let Err(e) = r {
                            error!("Serious error:{:?}", e);
                            return Err(e);
                        }
                    };
                },
                ack_data = ack_stream.next() => {
                    let _ = Self::remote_msg_callback(&mut sink, &mut ack_win, ack_data).await;
                }
                _ = shut_recv => {
                    info!("Received shutdown signal, quit");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn remote_msg_exec<'a>(
        data: GrpcResult<SegmentData>,
        ack_win: &'a mut AckWindow,
        mail: &'a mut Sender<SegmentDataCallback>,
        sink: &'a mut S,
        callback: UnboundedSender<i64>,
    ) -> Result<(), AnyError> {
        match data {
            Ok(segment_data) => {
                let mut segment_processor: ExecutorStat<'a, S> = segment_data
                    .with_process(ack_win, mail, sink, callback)
                    .into();
                loop {
                    let exec_rs =
                        <ExecutorStat<'a, S> as SegmentExecute>::exec(segment_processor).await;
                    // mail.clone();
                    match exec_rs {
                        Ok(ExecutorStat::HandShake(o))
                        | Ok(ExecutorStat::NeedTrans(o))
                        | Ok(ExecutorStat::Trans(o)) => {
                            segment_processor = o.into();
                            continue;
                        }
                        Err(e) => {
                            match e {
                                ExecuteErr::SinkErr(e) => {
                                    error!("Segment process execute failed, e:{:?}", e);
                                }
                                ExecuteErr::WindowFullErr => {
                                    error!("Server Window is full!");
                                }
                            }
                            break;
                        }

                        Ok(ExecutorStat::Last) => break,
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

    pub async fn remote_msg_callback(
        sink: &mut S,
        ack_win: &mut AckWindow,
        seq_id: Option<i64>,
    ) -> Result<(), AnyError> {
        trace!(seq_id = seq_id, "Has received ack callback");
        match seq_id {
            Some(seq_id) if seq_id > 0 => {
                let r = ack_win.ack(seq_id);
                match r {
                    Ok(_) => {
                        if ack_win.is_ready() {
                            ack_win.clear();
                        }
                        let seg = {
                            let mut s = SegmentRes::default();
                            let mut meta = Meta::new();
                            meta.set_field_type(Meta_RequestType::TRANS_ACK);
                            meta.set_seqId(ack_win.curr_ack_id());
                            s.set_meta(meta);
                            (s, WriteFlags::default())
                        };
                        let r = sink.send(seg).await;

                        if let Err(e) = r {
                            error!("Send trans ack failed!e:{:?}", e);
                        } else {
                            trace!(seq_id = ack_win.curr_ack_id(), "Sending ack to client");
                        }
                    }
                    Err(e) => {
                        error!(%seq_id, "Have got an unexpeced seqid to ack; e:{:?}, just ignore this segment", e);
                    }
                }
            }
            _ => {
                return Err(AnyError::msg("Invalid seqid"));
            }
        }
        Ok(())
    }
}

#[async_trait]
trait SegmentProcess<S> {
    fn with_process<'a>(
        self,
        ack_win: &'a mut AckWindow,
        mail: &'a mut Sender<SegmentDataCallback>,
        sink: &'a mut S,
        callback: UnboundedSender<i64>,
    ) -> SegmentProcessor<'a, S>;
}

struct SegmentProcessor<'a, S> {
    data: SegmentData,
    ack_win: &'a mut AckWindow,
    mail: &'a mut Sender<SegmentDataCallback>,
    sink: &'a mut S,
    callback: UnboundedSender<i64>,
}

impl<'a, S> From<SegmentProcessor<'a, S>> for ExecutorStat<'a, S> {
    fn from(s: SegmentProcessor<'a, S>) -> Self {
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
impl<S: Sync + Send> SegmentProcess<S> for SegmentData {
    fn with_process<'a>(
        self,
        ack_win: &'a mut AckWindow,
        mail: &'a mut Sender<SegmentDataCallback>,
        sink: &'a mut S,
        callback: UnboundedSender<i64>,
    ) -> SegmentProcessor<'a, S> {
        SegmentProcessor {
            data: self,
            ack_win,
            mail,
            sink,
            callback,
        }
    }
}

enum ExecutorStat<'a, S> {
    HandShake(SegmentProcessor<'a, S>),
    Trans(SegmentProcessor<'a, S>),
    NeedTrans(SegmentProcessor<'a, S>),
    Last,
}

#[async_trait]
impl<'a, S: Send + Sink<(SegmentRes, WriteFlags)> + Unpin> SegmentExecute for ExecutorStat<'a, S>
where
    S::Error: Send + std::error::Error,
{
    type Next = ExecutorStat<'a, S>;
    type Error = ExecuteErr<S::Error>;

    async fn exec(self) -> Result<Self::Next, Self::Error> {
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
                let send_res = sink.send((resp, WriteFlags::default())).await;
                if let Err(e) = send_res {
                    error!(conn_id, "Handshake send response failed!");
                    return Err(ExecuteErr::SinkErr(e));
                }
                info!("Send handshake resp success");
                let _ = sink.flush().await;
                return Ok(ExecutorStat::Last);
            }
            ExecutorStat::Trans(s) => {
                let r = s.ack_win.send(s.data.get_meta().get_seqId());
                // trace!(seq_id = data.get_meta().get_seqId(), meta = ?data.get_meta(), "Has received handshake packet meta");
                match r {
                    Ok(_) => {
                        let span = span!(Level::TRACE, "trans_receiver_consume", data = ?s.data);
                        let data = SegmentDataCallback {
                            data: s.data,
                            callback: AckCallback::new(Some(s.callback)),
                            span,
                        };
                        // Unbounded sender, ignore
                        let _ = s.mail.send(data).await;
                        trace!("Has sent segment to local channel(Waiting for storage operation and callback)");
                    }
                    Err(w) => match w {
                        // Hang on, send current ack back
                        WindowErr::Full => {
                            let segment_resp = SegmentRes::new();
                            let mut meta = Meta::new();
                            meta.set_field_type(Meta_RequestType::TRANS_ACK);
                            meta.set_seqId(s.ack_win.curr_ack_id());
                            warn!(conn_id = meta.get_connId(), meta = ?meta, "Receiver window is full, send full signal to remote sender");
                            let r = s.sink.send((segment_resp, WriteFlags::default())).await;
                            if let Err(e) = r {
                                error!("Send full signal failed!e:{:?}", e);
                                return Err(ExecuteErr::SinkErr(e));
                            }
                            return Err(ExecuteErr::WindowFullErr);
                        }
                    },
                }
                return Ok(ExecutorStat::Last);
            }
            ExecutorStat::NeedTrans(s) => {
                trace!(meta = ?s.data.get_meta(), conn_id = s.data.get_meta().get_connId(), "Has received need send data!");
                let span = span!(Level::TRACE, "trans_receiver_consume_need_resend");
                let data = SegmentDataCallback {
                    data: s.data,
                    callback: AckCallback::new(Some(s.callback)),
                    span,
                };
                let _ = s.mail.try_send(data);
                trace!("NEED_RESEND: Has sent segment to local channel(Waiting for storage operation and callback)");
                return Ok(ExecutorStat::Last);
            }
            ExecutorStat::Last => {
                return Ok(ExecutorStat::Last);
            }
        }
    }
}

#[derive(Debug)]
enum ExecuteErr<SinkErr: Debug> {
    SinkErr(SinkErr),
    WindowFullErr,
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
    type Error: std::error::Error;

    async fn exec(self) -> Result<Self::Next, Self::Error>;
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
        local_sender: Sender<SegmentDataCallback>,
        shut_recv: Receiver<()>,
    ) -> RemoteMsgPoller<L, S> {
        RemoteMsgPoller {
            source_stream: source,
            ack_win: Default::default(),
            sink,
            local_sender,
            shut_recv,
        }
    }
}

#[cfg(test)]
mod test_remote_msg_poller {
    use super::*;
    use chrono::Local;
    use skdb::test::gen::*;
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
