use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    select,
};
use skproto::tracing::Meta;
use skproto::tracing::Meta_RequestType;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tracing::error;

use futures::Stream;
use futures::StreamExt;
use futures_sink::Sink;
use futures_util::sink::SinkExt;
use grpcio::WriteFlags;
use skproto::tracing::{SegmentData, SegmentRes};
use tokio::sync::oneshot::Sender as OneSender;

use crate::com::ring::RingQueue;

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
}

#[derive(Clone)]
pub struct RequestScheduler {
    sender: Sender<(
        tokio::sync::oneshot::Sender<Result<(), TransportErr>>,
        SegmentData,
    )>,
    shutdown: Arc<AtomicBool>,
}

impl RequestScheduler {
    pub async fn request(&mut self, seg: SegmentData) -> Result<(), TransportErr> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(TransportErr::Shutdown);
        }
        let (s, r) = tokio::sync::oneshot::channel();
        let res = self.sender.send((s, seg)).await;
        if let Err(_) = res {
            return Err(TransportErr::LocalChanFullOrClosed);
        }
        let r = r.await;
        match r {
            Ok(r) => r,
            Err(_e) => Err(TransportErr::RecvErr),
        }
    }
}

impl<Si, St> Transport<Si, St>
where
    Si: Sink<(SegmentData, WriteFlags)> + Send + Unpin + 'static,
    St: Stream<Item = Result<SegmentRes, grpcio::Error>> + Send + Unpin + 'static,
{
    pub fn init(sink: Si, mut st: St) -> RequestScheduler {
        let (sender, mut recv) = tokio::sync::mpsc::channel(10000);
        let task_stat = Arc::new(AtomicBool::new(false));
        let task_stat_change = task_stat.clone();
        tokio::spawn(async move {
            let mut trans = Transport {
                sink,
                st: PhantomData::<St>,
                ring: Default::default(),
                callback_map: HashMap::new(),
            };
            let t1 = recv.recv().fuse();
            let t2 = st.next().fuse();
            pin_mut!(t1, t2);

            loop {
                let mut stream_close = false;
                let mut sink_close = false;

                select! {
                    seg_res = t1 => {
                        match seg_res {
                            Some((sender, seg)) => {
                                trans.request(seg, sender).await;
                            }
                            None => {
                                sink_close = true;
                            }
                        }

                    },
                    resp = t2 => {
                        match Self::unwrap_poll_resp(resp) {
                            Err(_) => {stream_close =true;},
                            Ok(resp) => {
                                trans.poll_resp(resp).await;
                            }
                        }
                    },
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
        RequestScheduler {
            sender,
            shutdown: task_stat,
        }
    }

    fn unwrap_poll_resp(
        resp: Option<Result<SegmentRes, grpcio::Error>>,
    ) -> Result<SegmentRes, anyhow::Error> {
        match resp {
            Some(r) => Ok(r?),
            None => Err(anyhow::Error::msg("")),
        }
    }

    // We don't need response for tracing data request, and only care about the send status of this request
    pub async fn request(
        &mut self,
        segment: SegmentData,
        sender: tokio::sync::oneshot::Sender<Result<(), TransportErr>>,
    ) {
        let seq_id = self.ring.async_push(segment.clone()).await.unwrap();
        let res = self.sink.send((segment, WriteFlags::default())).await;
        match res {
            Err(_e) => {
                let _ = sender.send(Err(TransportErr::SinkChanErr));
            }
            Ok(_) => {
                self.callback_map.insert(seq_id, sender);
            }
        }
    }

    pub async fn poll_resp(&mut self, item: SegmentRes) {
        let seq_id = item.get_meta().get_seqId();
        match item.get_meta().field_type {
            Meta_RequestType::NEED_RESEND => {
                let iter = self.ring.not_ack_iter();
                for item in iter {
                    let mut segment = item.clone();
                    let mut meta = Meta::new();
                    meta.set_field_type(Meta_RequestType::NEED_RESEND);
                    meta.set_seqId(seq_id);
                    segment.set_meta(meta);
                    let _ = self.sink.send((segment, WriteFlags::default())).await;
                }
            }
            Meta_RequestType::TRANS_ACK => {
                let seq_id = item.get_meta().get_seqId();
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
                        error!(ack_id = seq_id, %e, ?item, "Ack has met some problem");
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {}
