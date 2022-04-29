use anyhow::Error as AnyError;
// use crossbeam_channel::Sender;
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

pub struct MsgPoller<L, S> {
    source_stream: Fuse<L>,
    sink: S,
    ack_win: AckWindow,
    local_sender: Sender<SegmentDataCallback>,
}

impl<L, S> MsgPoller<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: Sink<(SegmentRes, WriteFlags)> + Unpin,
    AnyError: From<S::Error>,
    S::Error: Debug + Sync + Send,
{
    pub fn new(source: L, sink: S, local_sender: Sender<SegmentDataCallback>) -> MsgPoller<L, S> {
        MsgPoller {
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
                    }
                }
                Either::Right((seq_id, _)) => {
                    let _ = ProtoLogic::handle_callback(&mut self.sink, &mut self.ack_win, seq_id)
                        .await;
                }
            }
        }
    }
}
