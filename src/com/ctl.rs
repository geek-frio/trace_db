use crate::com::ack::{AckCallback, WindowErr};
use crate::tag::fsm::SegmentDataCallback;

use super::ack::AckWindow;
use anyhow::Error as AnyError;
use crossbeam_channel::Sender;
use futures::channel::mpsc::{unbounded as funbounded, UnboundedSender};
use futures::SinkExt;
use futures::{
    future::{select, Either},
    stream::Fuse,
    Stream, StreamExt,
};
use futures_sink::Sink;
use grpcio::{Result as GrpcResult, WriteFlags};
use skproto::tracing::{Meta, Meta_RequestType, SegmentData, SegmentRes};
use std::fmt::Debug;
use std::pin::Pin;
use tracing::{error, info, info_span, span, trace, trace_span, warn, Instrument, Level};
pub struct MsgTracker<L, S> {
    source_stream: Fuse<L>,
    sink: S,
    ack_win: AckWindow,
    local_sender: Sender<SegmentDataCallback>,
}

impl<L, S> MsgTracker<L, S>
where
    L: Stream<Item = GrpcResult<SegmentData>> + Unpin,
    S: Sink<(SegmentRes, WriteFlags)> + Unpin,
    AnyError: From<S::Error>,
    S::Error: Debug,
{
    pub fn new(source: L, sink: S, local_sender: Sender<SegmentDataCallback>) -> MsgTracker<L, S> {
        MsgTracker {
            source_stream: source.fuse(),
            ack_win: Default::default(),
            sink,
            local_sender,
        }
    }

    async fn handshake_process(&mut self) -> Result<(), AnyError> {
        // let conn_id = CONN_MANAGER.gen_new_conn_id();
        // let mut resp = SegmentRes::new();
        // let mut meta = Meta::new();
        // meta.connId = conn_id;
        // meta.field_type = Meta_RequestType::HANDSHAKE;
        // info!(meta = ?meta, %conn_id, resp = ?resp, "Send handshake resp");
        // resp.set_meta(meta);
        // // We don't care handshake is success or not, client should retry for this
        // sink.send((resp, WriteFlags::default())).await?;
        // println!("Has sent handshake response!");
        // info!("Send handshake resp success");
        // let _ = sink.flush().await;
        Ok(())
    }

    // Processing segment data from remote client
    async fn process_segment(
        &mut self,
        seg: Option<GrpcResult<SegmentData>>,
        ack_sender: UnboundedSender<i64>,
    ) -> Result<(), AnyError> {
        if let Some(seg) = seg {
            let data = seg?;
            if !data.has_meta() {
                return Ok(());
            }
            match data.get_meta().get_field_type() {
                Meta_RequestType::HANDSHAKE => {
                    info!(meta = ?data.get_meta(), "Has received handshake packet meta");
                    self.handshake_process()
                        .instrument(info_span!(
                            "handshake_exec",
                            conn_id = data.get_meta().get_connId(),
                            meta = ?data.get_meta()
                        ))
                        .await?;
                }
                Meta_RequestType::TRANS => {
                    let r = self.ack_win.send(data.get_meta().get_seqId());
                    trace!(seq_id = data.get_meta().get_seqId(), meta = ?data.get_meta(), "Has received handshake packet meta");
                    match r {
                        Ok(_) => {
                            let span = span!(Level::TRACE, "trans_receiver_consume", data = ?data);
                            let data = SegmentDataCallback {
                                data,
                                callback: AckCallback::new(Some(ack_sender)),
                                span,
                            };
                            let _ = self.local_sender.try_send(data);
                            trace!("Has sent segment to local channel(Waiting for storage operation and callback)");
                        }
                        Err(w) => match w {
                            // Hang on, send current ack back
                            WindowErr::Full => {
                                let segment_resp = SegmentRes::new();
                                let mut meta = Meta::new();
                                meta.set_field_type(Meta_RequestType::TRANS_ACK);
                                meta.set_seqId(self.ack_win.curr_ack_id());
                                warn!(conn_id = meta.get_connId(), meta = ?meta, "Receiver window is full, send full signal to remote sender");
                                let _ = self
                                    .sink
                                    .send((segment_resp, WriteFlags::default()))
                                    .await?;
                            }
                        },
                    }
                }
                // 处理重发的数据
                Meta_RequestType::NEED_RESEND => {
                    // Need Resend data should not go ack_win control
                    trace!(meta = ?data.get_meta(), conn_id = data.get_meta().get_connId(), "Has received need send data!");
                    let span = span!(Level::TRACE, "trans_receiver_consume_need_resend");
                    let data = SegmentDataCallback {
                        data,
                        callback: AckCallback::new(Some(ack_sender)),
                        span,
                    };
                    let _ = self.local_sender.try_send(data);
                    trace!(
                    "NEED_RESEND: Has sent segment to local channel(Waiting for storage operation and callback)"
                );
                }
                _ => {
                    unreachable!();
                }
            }
        }
        Ok(())
    }

    pub async fn pull_redirect(&mut self) {
        let (ack_s, mut ack_r) = funbounded::<i64>();
        loop {
            let _one_msg = trace_span!("recv_msg_one_loop");

            let mut left = Pin::new(&mut self.source_stream);
            let mut right = Pin::new(&mut ack_r);
            match select(left.next(), right.next()).await {
                Either::Left((seg_data, _)) => {
                    let r = self
                        .process_segment(seg_data, ack_s.clone())
                        .instrument(trace_span!("process_segment"))
                        .await;
                    if let Err(e) = r {
                        error!("Has met seriously problem, e:{:?}", e);
                        return;
                    }
                }
                Either::Right((seq_id, _)) => {
                    trace!(seq_id = seq_id, "Has received ack callback");
                    if let Some(seq_id) = seq_id {
                        if seq_id <= 0 {
                            error!(%seq_id, "Invalid seq_id");
                            continue;
                        }
                        let r = self.ack_win.ack(seq_id);
                        if let Err(e) = r {
                            error!(%seq_id, "Have got an unexpeced seqid to ack; e:{:?}", e);
                            continue;
                        } else {
                            // Make ack window enter into ready status
                            if self.ack_win.is_ready() {
                                info!("Ack window is ready");
                                self.ack_win.clear();
                            }
                        }
                        let mut seg_res = SegmentRes::default();
                        let mut meta = Meta::new();
                        meta.set_field_type(Meta_RequestType::TRANS_ACK);
                        meta.set_seqId(self.ack_win.curr_ack_id());
                        seg_res.set_meta(meta);
                        let d = (seg_res, WriteFlags::default());
                        let r = self.sink.send(d).await;
                        if let Err(e) = r {
                            error!("Send trans ack failed!e:{:?}", e);
                        } else {
                            trace!(seq_id = self.ack_win.curr_ack_id(), "Sending ack to client");
                        }
                    }
                }
            }
        }
    }
}
