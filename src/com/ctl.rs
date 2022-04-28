use std::pin::Pin;

use super::ack::AckWindow;
use futures::channel::mpsc::unbounded as funbounded;
use futures::{
    future::{select, Either},
    stream::Fuse,
    Stream, StreamExt,
};
use futures_sink::Sink;
use skproto::tracing::SegmentRes;
use tracing::trace_span;
pub(crate) struct MsgTracker<L, R, S> {
    source_stream: Fuse<L>,
    ack_stream: Fuse<R>,
    sink: S,
    ack_win: AckWindow,
}

impl<L, R, S> MsgTracker<L, R, S>
where
    L: Stream + Unpin,
    R: Stream + Unpin,
    S: Sink<SegmentRes>,
{
    pub(crate) fn new(source: L, ack: R, sink: S) -> MsgTracker<L, R, S> {
        MsgTracker {
            source_stream: source.fuse(),
            ack_stream: ack.fuse(),
            ack_win: Default::default(),
            sink,
        }
    }

    async fn process_segment(&mut self) {}

    pub(crate) async fn pull_redirect(&mut self) {
        let (ack_s, mut ack_r) = funbounded::<i64>();
        loop {
            let _one_msg = trace_span!("recv_msg_one_loop");

            let mut left = Pin::new(&mut self.source_stream);
            let mut right = Pin::new(&mut self.ack_stream);
            match select(left.next(), right.next()).await {
                Either::Left((seg_data, _)) => {}
                Either::Right((seq_id, _)) => {}
            }
        }
        // loop {
        //     let _one_msg = trace_span!("recv_msg_one_loop");
        //     let _ = match select(stream.try_next(), ack_r.next()).await {
        //         FutureEither::Left((seg_data, _)) => {
        //             let r = process_segment(
        //                 seg_data,
        //                 &mut ack_win,
        //                 &mut sink,
        //                 s.clone(),
        //                 ack_s.clone(),
        //             )
        //             .instrument(trace_span!("process_segment"))
        //             .await;
        //             if let Err(e) = r {
        //                 println!("Has met seriously problem, e:{:?}", e);
        //                 return;
        //             }
        //         }
        //         FutureEither::Right((seq_id, _)) => {
        //             trace!(seq_id = seq_id, "Has received ack callback");
        //             if let Some(seq_id) = seq_id {
        //                 if seq_id <= 0 {
        //                     error!(%seq_id, "Invalid seq_id");
        //                     continue;
        //                 }
        //                 let r = ack_win.ack(seq_id);
        //                 if let Err(e) = r {
        //                     error!(%seq_id, "Have got an unexpeced seqid to ack; e:{:?}", e);
        //                     continue;
        //                 } else {
        //                     // Make ack window enter into ready status
        //                     if ack_win.is_ready() {
        //                         info!("Ack window is ready");
        //                         ack_win.clear();
        //                     }
        //                 }
        //                 let mut seg_res = SegmentRes::default();
        //                 let mut meta = Meta::new();
        //                 meta.set_field_type(Meta_RequestType::TRANS_ACK);
        //                 meta.set_seqId(ack_win.curr_ack_id());
        //                 seg_res.set_meta(meta);
        //                 let d = (seg_res, WriteFlags::default());
        //                 let r = sink.send(d).await;
        //                 if let Err(e) = r {
        //                     error!("Send trans ack failed!e:{:?}", e);
        //                 } else {
        //                     trace!(seq_id = ack_win.curr_ack_id(), "Sending ack to client");
        //                 }
        //             }
        //         }
        //     };
        // }
    }
}
