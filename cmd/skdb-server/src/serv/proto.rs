use std::marker::PhantomData;

use anyhow::Error as AnyError;
use async_trait::async_trait;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use futures_sink::Sink;
use grpcio::Result as GrpcResult;
use grpcio::WriteFlags;
use skdb::com::ack::AckCallback;
use skdb::com::ack::AckWindow;
use skdb::com::ack::WindowErr;
use skdb::tag::fsm::SegmentDataCallback;
use skproto::tracing::Meta;
use skproto::tracing::Meta_RequestType;
use skproto::tracing::{SegmentData, SegmentRes};
use std::fmt::Debug;
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::span;
use tracing::trace;
use tracing::warn;
use tracing::Instrument;
use tracing::Level;

use crate::serv::CONN_MANAGER;

pub(crate) struct ProtoLogic<T> {
    _sink: PhantomData<T>,
}

// #[async_trait]
// trait SegmentExecutor {
//     type ErrorProcess: SegmentExecutor;
//     type Next: SegmentExecutor;

//     async fn exec<Item, S: Sink<Item>>(
//         self,
//         ack: &mut AckWindow,
//         sink: S,
//     ) -> Result<Self::Next, Self::ErrorProcess>;
// }

// struct ErrorExec(SegmentData);
// #[async_trait]
// impl SegmentExecutor for ErrorExec {
//     type ErrorProcess = Last;

//     type Next = Last;

//     async fn exec<Item, S: Sink<Item>>(
//         self,
//         ack: &mut AckWindow,
//         sink: S,
//     ) -> Result<Self::Next, Self::ErrorProcess> {
//     }
// }

// struct Last(Result<(), AnyError>);
// #[async_trait]
// impl SegmentExecutor for Last {
//     type ErrorProcess = ErrorExec;

//     type Next = Last;

//     async fn exec<Item, S: Sink<Item>>(
//         self,
//         ack: &mut AckWindow,
//         sink: S,
//     ) -> Result<Self::Next, Self::ErrorProcess> {
//         Ok(Last(Ok(())))
//     }
// }

impl<T> ProtoLogic<T>
where
    T: Sink<(SegmentRes, WriteFlags)> + Unpin,
    <T as Sink<(SegmentRes, WriteFlags)>>::Error: Sync + Send + Debug,
    AnyError: From<T::Error>,
{
    async fn handle_handshake(sink: &mut T) -> Result<(), AnyError> {
        let conn_id = CONN_MANAGER.gen_new_conn_id();
        let mut resp = SegmentRes::new();
        let mut meta = Meta::new();
        meta.connId = conn_id;
        meta.field_type = Meta_RequestType::HANDSHAKE;
        info!(meta = ?meta, %conn_id, resp = ?resp, "Send handshake resp");
        resp.set_meta(meta);
        // We don't care handshake is success or not, client should retry for this
        sink.send((resp, WriteFlags::default())).await?;
        info!("Send handshake resp success");
        let _ = sink.flush().await;
        Ok(())
    }

    async fn handle_trans_v2(data: SegmentData) -> Result<(), AnyError> {
        // let seg_process_handle = data.tag_process_start();
        // seg_process_handle.mailto_processor();
        Ok(())
    }

    async fn handle_trans(
        data: SegmentData,
        ack_win: &mut AckWindow,
        sink: &mut T,
        ack_sender: UnboundedSender<i64>,
        local_sender: &mut Sender<SegmentDataCallback>,
    ) -> Result<(), AnyError> {
        let r = ack_win.send(data.get_meta().get_seqId());
        trace!(seq_id = data.get_meta().get_seqId(), meta = ?data.get_meta(), "Has received handshake packet meta");
        match r {
            Ok(_) => {
                let span = span!(Level::TRACE, "trans_receiver_consume", data = ?data);
                let data = SegmentDataCallback {
                    data,
                    callback: AckCallback::new(Some(ack_sender)),
                    span,
                };
                let _ = local_sender.send(data).await;
                trace!(
                    "Has sent segment to local channel(Waiting for storage operation and callback)"
                );
            }
            Err(w) => match w {
                // Hang on, send current ack back
                WindowErr::Full => {
                    let segment_resp = SegmentRes::new();
                    let mut meta = Meta::new();
                    meta.set_field_type(Meta_RequestType::TRANS_ACK);
                    meta.set_seqId(ack_win.curr_ack_id());
                    warn!(conn_id = meta.get_connId(), meta = ?meta, "Receiver window is full, send full signal to remote sender");
                    let _ = sink.send((segment_resp, WriteFlags::default())).await?;
                }
            },
        }
        Ok(())
    }

    async fn handle_resend(
        data: SegmentData,
        ack_sender: UnboundedSender<i64>,
        local_sender: &mut Sender<SegmentDataCallback>,
    ) -> Result<(), AnyError> {
        trace!(meta = ?data.get_meta(), conn_id = data.get_meta().get_connId(), "Has received need send data!");
        let span = span!(Level::TRACE, "trans_receiver_consume_need_resend");
        let data = SegmentDataCallback {
            data,
            callback: AckCallback::new(Some(ack_sender)),
            span,
        };
        let _ = local_sender.try_send(data);
        trace!(
                "NEED_RESEND: Has sent segment to local channel(Waiting for storage operation and callback)"
            );
        Ok(())
    }

    pub async fn handle_callback(
        sink: &mut T,
        ack_win: &mut AckWindow,
        seq_id: Option<i64>,
    ) -> Result<(), AnyError> {
        trace!(seq_id = seq_id, "Has received ack callback");
        match seq_id {
            Some(seq_id) if seq_id > 0 => {
                if seq_id <= 0 {
                    error!(%seq_id, "Invalid seq_id");
                    return Ok(());
                }
                let r = ack_win.ack(seq_id);
                if let Err(e) = r {
                    error!(%seq_id, "Have got an unexpeced seqid to ack; e:{:?}", e);
                    return Ok(());
                } else {
                    // Make ack window enter into ready status
                    if ack_win.is_ready() {
                        info!("Ack window is ready");
                        ack_win.clear();
                    }
                }
                let mut seg_res = SegmentRes::default();
                let mut meta = Meta::new();
                meta.set_field_type(Meta_RequestType::TRANS_ACK);
                meta.set_seqId(ack_win.curr_ack_id());
                seg_res.set_meta(meta);
                let d = (seg_res, WriteFlags::default());
                let r = sink.send(d).await;
                if let Err(e) = r {
                    error!("Send trans ack failed!e:{:?}", e);
                } else {
                    trace!(seq_id = ack_win.curr_ack_id(), "Sending ack to client");
                }
            }
            _ => {
                return Err(AnyError::msg("Invalid seqid"));
            }
        }
        Ok(())
    }

    pub async fn execute(
        seg: GrpcResult<SegmentData>,
        sink: &mut T,
        ack_win: &mut AckWindow,
        ack_sender: UnboundedSender<i64>,
        local_sender: &mut Sender<SegmentDataCallback>,
    ) -> Result<(), AnyError> {
        let data = seg?;
        if !data.has_meta() {
            return Ok(());
        }
        match data.get_meta().get_field_type() {
            Meta_RequestType::HANDSHAKE => {
                info!(meta = ?data.get_meta(), "Has received handshake packet meta");
                Self::handle_handshake(sink)
                    .instrument(info_span!(
                        "handshake_exec",
                        conn_id = data.get_meta().get_connId(),
                        meta = ?data.get_meta()
                    ))
                    .await?;
            }
            Meta_RequestType::TRANS => {
                let _ = Self::handle_trans(data, ack_win, sink, ack_sender, local_sender).await?;
            }
            // 处理重发的数据
            Meta_RequestType::NEED_RESEND => {
                let _ = Self::handle_resend(data, ack_sender, local_sender).await?;
            }
            _ => {
                unreachable!();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test_proto_handle {
    use super::*;
    struct MockSink {}

    // impl Sink<Item> for MockSink {

    // }
    #[test]
    fn test_handle_trans_ack_full() {}
}
