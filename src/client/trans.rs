use futures::pin_mut;
use skproto::tracing::Meta;
use skproto::tracing::Meta_RequestType;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;
use tracing::error;

use futures::Stream;
use futures::StreamExt;
use futures_sink::Sink;
use futures_util::sink::SinkExt;
use grpcio::WriteFlags;
use skproto::tracing::{SegmentData, SegmentRes};
use tokio::sync::oneshot::Sender as OneSender;

use crate::com::ring::RingQueue;
use crate::com::util::NoneFuture;

pub struct Transport<Si, St> {
    sink: Si,
    st: PhantomData<St>,
    ring: RingQueue<SegmentData>,
    callback_map: HashMap<i64, OneSender<Result<(), TransportErr>>>,
}

#[derive(Error, Debug)]
pub enum TransportErr {
    #[error("Grpc service sink channel closed")]
    SinkChanErr,
    #[error("Client request service request transport, callback sender is dropped")]
    RecvErr,
    #[error(
        "Client request service transport layer response stream channel is closed, pending request should be cleared"
    )]
    ReceiverChanClosed,
    #[error("When calling service, underground transport has been shutdown")]
    Shutdown,
    #[error("Client request service transport layer underground channel is full")]
    LocalChanFullOrClosed,
    #[error("IOError, auto retry has exceeded max retry times")]
    RetryLimit,
}

#[derive(Clone)]
pub struct RequestHandler {
    sender: tokio::sync::mpsc::UnboundedSender<(
        tokio::sync::oneshot::Sender<Result<(), TransportErr>>,
        SegmentData,
    )>,
    shutdown: Arc<AtomicBool>,
    counter: Arc<AtomicI64>,
}

impl RequestHandler {
    pub fn request(
        &mut self,
        seg: SegmentData,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), TransportErr>>, TransportErr> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(TransportErr::Shutdown);
        }

        let (s, r) = tokio::sync::oneshot::channel();
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let res = self.sender.send((s, seg));

        if let Err(_) = res {
            tracing::warn!("LocalChanFullOrClosed");
            return Err(TransportErr::LocalChanFullOrClosed);
        }
        return Ok(r);
    }
}

impl<Si, St> Transport<Si, St>
where
    Si: Sink<(SegmentData, WriteFlags)> + Send + Unpin + 'static,
    Si::Error: Send,
    St: Stream<Item = Result<SegmentRes, grpcio::Error>> + Send + Unpin + 'static,
{
    pub fn new(sink: Si) -> Transport<Si, St> {
        Transport {
            sink,
            st: PhantomData,
            ring: Default::default(),
            callback_map: HashMap::new(),
        }
    }

    pub async fn handshake(sink: &mut Si, recv: &mut St) -> Result<SegmentRes, anyhow::Error> {
        let mut segment = SegmentData::new();
        segment
            .mut_meta()
            .set_field_type(Meta_RequestType::HANDSHAKE);

        let res = sink.send((segment, WriteFlags::default())).await;

        match res {
            Ok(_) => {
                let resp = recv.next().await;
                match resp {
                    Some(r) => match r {
                        Ok(resp) => {
                            tracing::info!("handshake resp is:{:?}", resp);
                            Ok(resp)
                        }
                        Err(e) => Err(e.into()),
                    },
                    None => Err(anyhow::Error::msg("Receiving handshake resp failed!")),
                }
            }
            Err(_e) => Err(anyhow::Error::msg("Handshake send failed!")),
        }
    }

    pub fn init(mut sink: Si, mut st: St) -> RequestHandler {
        let (sender, mut recv) = tokio::sync::mpsc::unbounded_channel();
        let task_stat = Arc::new(AtomicBool::new(false));
        let task_stat_change = task_stat.clone();

        tokio::spawn(async move {
            let res_hand = Self::handshake(&mut sink, &mut st).await;

            if res_hand.is_err() {
                error!("Hanshake failed, transport quit!");
                return;
            }

            let conn_id = res_hand.unwrap().get_meta().get_connId();

            let mut trans = Transport {
                sink,
                st: PhantomData::<St>,
                ring: Default::default(),
                callback_map: HashMap::new(),
            };

            let mut stream_close = false;
            let mut sink_close = false;

            let counter = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
            let temp = counter.clone();

            tokio::spawn(async move {
                let mut last_count = 0i64;
                loop {
                    let cur_count = temp.load(std::sync::atomic::Ordering::Relaxed);
                    let incre = cur_count - last_count;

                    tracing::warn!("发送qps:{}", incre);
                    last_count = cur_count;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            });

            let mut window_is_full = false;
            loop {
                let t1 = recv.recv();
                let t2 = NoneFuture::new(st.next(), stream_close);

                pin_mut!(t1, t2);

                // window 打满后，请求会堆积在转发端 channel 之中
                //  等待新的ack发来以后解除 is_full
                if window_is_full && !stream_close {
                    let resp = t2.await;
                    match Self::unwrap_poll_resp(resp) {
                        Err(_) => {
                            stream_close = true;
                        }
                        Ok(resp) => {
                            trans.handle_resp(resp).await;
                            window_is_full = false;
                        }
                    }
                } else if !stream_close {
                    tokio::select! {
                        seg_res = t1 => {
                            match seg_res {
                                Some((sender, seg)) => {
                                    if trans.request(seg, sender, conn_id, &mut window_is_full).await {
                                        (&counter).fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                }
                                None => {
                                    tracing::warn!("sink is closed!");
                                    sink_close = true;
                                }
                            }
                        },
                        resp = t2 => {
                            match Self::unwrap_poll_resp(resp) {
                                Err(_) => {
                                    stream_close = true;
                                },
                                Ok(resp) => {
                                    trans.handle_resp(resp).await;
                                }
                            }
                        },
                        else => {
                            tracing::warn!("conn_id:{}, transport has closed", conn_id);

                            stream_close = true;
                            sink_close = true;
                        }
                    }
                }
                if stream_close {
                    for (_, sender) in trans.callback_map.drain() {
                        let _ = sender.send(Err(TransportErr::ReceiverChanClosed));
                    }
                }

                if sink_close && stream_close {
                    task_stat_change.store(true, Ordering::Relaxed);
                    return;
                }
            }
        });

        let counter = Arc::new(AtomicI64::new(0));

        RequestHandler {
            sender,
            shutdown: task_stat,
            counter: counter,
        }
    }

    fn unwrap_poll_resp(
        resp: Option<Result<SegmentRes, grpcio::Error>>,
    ) -> Result<SegmentRes, anyhow::Error> {
        match resp {
            Some(r) => Ok(r?),
            None => Err(anyhow::Error::msg("shutdown")),
        }
    }

    // We don't need response for tracing data request, and only care about the send status of this request
    pub async fn request(
        &mut self,
        mut segment: SegmentData,
        sender: tokio::sync::oneshot::Sender<Result<(), TransportErr>>,
        conn_id: i32,
        is_full: &mut bool,
    ) -> bool {
        segment.mut_meta().set_connId(conn_id);
        if self.ring.is_full() {
            *is_full = true;
            return false;
        }
        let seq_id = self.ring.push(segment.clone()).unwrap();

        segment.mut_meta().set_seqId(seq_id);

        let flags: WriteFlags = Default::default();
        let flags = flags.buffer_hint(true);

        let res = self.sink.send((segment, flags)).await;
        match res {
            Err(_) => {
                tracing::warn!("sink send msg failed!");
                let _ = sender.send(Err(TransportErr::SinkChanErr));
            }
            Ok(_) => {
                self.callback_map.insert(seq_id, sender);
            }
        }
        true
    }

    pub async fn handle_resp(&mut self, resp: SegmentRes) {
        let seq_id = resp.get_meta().get_seqId();

        match resp.get_meta().field_type {
            Meta_RequestType::NEED_RESEND => {
                let iter = self.ring.not_ack_iter_mut();

                for item in iter {
                    if item.1 == seq_id {
                        let mut segment = item.0.clone();
                        let mut meta = Meta::new();

                        meta.set_field_type(Meta_RequestType::NEED_RESEND);
                        meta.set_seqId(seq_id);

                        let resend_count = resp.get_meta().resend_count + 1;

                        tracing::trace!(
                            "Response resend count is:{:?}, seq_id is:{}",
                            resend_count,
                            seq_id
                        );

                        if resend_count >= 3 {
                            tracing::warn!(
                                "Has overceed retry limit times, resend count is:{}",
                                resend_count
                            );

                            let s = self.callback_map.remove(&seq_id);
                            if let Some(s) = s {
                                let _ = s.send(Err(TransportErr::RetryLimit));
                            }

                            self.ack_seq_id(seq_id, &resp);
                        } else {
                            meta.set_resend_count(resend_count + 1);
                            segment.set_meta(meta);

                            let _ = self.sink.send((segment, WriteFlags::default())).await;
                        }

                        break;
                    }
                }
            }
            Meta_RequestType::TRANS_ACK => {
                let seq_id = resp.get_meta().get_seqId();

                self.ack_seq_id(seq_id, &resp);
            }
            _ => unreachable!(),
        }
    }

    fn ack_seq_id(&mut self, seq_id: i64, resp: &SegmentRes) {
        let res = self.ring.ack(seq_id);

        match res {
            Ok(v) => {
                for seq_id in v.into_iter() {
                    let s = self.callback_map.remove(&seq_id);
                    if let Some(s) = s {
                        let _ = s.send(Ok(()));
                    }
                }
            }
            Err(e) => {
                error!(ack_id = seq_id, %e, ?resp, "Ack has met some problem");
            }
        }
    }
}

#[cfg(test)]
mod tests {}
