use anyhow::Error as AnyError;
use futures::channel::mpsc::{unbounded as funbounded, Sender};
use futures::{
    future::{select, Either},
    stream::Fuse,
    Stream, StreamExt,
};
use futures_sink::Sink;
use grpcio::{Result as GrpcResult, WriteFlags};
use skdb::com::ack::AckWindow;
use skdb::tag::fsm::SegmentDataCallback;
use skproto::tracing::{SegmentData, SegmentRes};
use std::fmt::Debug;
use std::pin::Pin;
use tracing::{error, trace_span};

use super::proto::ProtoLogic;

pub struct RemoteMsgPoller<L, S> {
    source_stream: Fuse<L>,
    sink: S,
    ack_win: AckWindow,
    local_sender: Sender<SegmentDataCallback>,
}

impl<L, S> RemoteMsgPoller<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: Sink<(SegmentRes, WriteFlags)> + Unpin,
    AnyError: From<S::Error>,
    S::Error: Debug + Sync + Send,
{
    pub fn new(
        source: L,
        sink: S,
        local_sender: Sender<SegmentDataCallback>,
    ) -> RemoteMsgPoller<L, S> {
        RemoteMsgPoller {
            source_stream: source.fuse(),
            ack_win: Default::default(),
            sink,
            local_sender,
        }
    }

    pub async fn loop_poll(&mut self) {
        let (ack_s, mut ack_r) = funbounded::<i64>();
        loop {
            let _one_msg = trace_span!("recv_msg_one_loop");
            let mut left = Pin::new(&mut self.source_stream);
            let mut right = Pin::new(&mut ack_r);
            match select(left.next(), right.next()).await {
                Either::Left((seg_data, _)) => {
                    if let Some(seg) = seg_data {
                        if !cfg!(test) {
                            let r = ProtoLogic::execute(
                                seg,
                                &mut self.sink,
                                &mut self.ack_win,
                                ack_s.clone(),
                                &mut self.local_sender,
                            )
                            .await;
                            if let Err(e) = r {
                                error!("Has met seriously problem, e:{:?}", e);
                                return;
                            }
                        } else {
                            println!("seg:{:?}", seg);
                        }
                    }
                }
                // Callback ack seqid;
                Either::Right((seq_id, _)) => {
                    if !cfg!(test) {
                        let _ =
                            ProtoLogic::handle_callback(&mut self.sink, &mut self.ack_win, seq_id)
                                .await;
                    } else {
                        println!("Has received acked seq_id");
                    }
                }
            }
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
            let seg = mock_seg(1, self.api_id, self.seq_id);
            return Poll::Ready(Some(GrpcResult::Ok(seg)));
        }
    }

    #[tokio::test]
    async fn test_loop_poll() -> Result<(), AnyError> {
        let (local_send, mut local_recv) = futures::channel::mpsc::channel(5000);

        let source = MockStream {
            idx: 0,
            api_id: 1,
            seq_id: 1,
        };
        let sink = MockSink;
        let mut remote = RemoteMsgPoller::new(source, sink, local_send);
        tokio::spawn(async move {
            while let Some(data) = local_recv.next().await {
                println!("data callback :{:?}", data);
            }
        });
        remote.loop_poll().await;
        Ok(())
    }
}
