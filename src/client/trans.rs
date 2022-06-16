use std::collections::HashMap;

use futures::Stream;
use futures_sink::Sink;
use futures_util::sink::SinkExt;
use grpcio::WriteFlags;
use skproto::tracing::{SegmentData, SegmentRes};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::{Receiver as OneReceiver, Sender as OneSender};

use crate::com::ring::RingQueue;

async fn req<Req, Resp>(req: Req) -> Resp {
    todo!();
}

struct Transport<Si, St> {
    sink: Si,
    st: St,
    recv: Receiver<SegmentData>,
    send: Sender<SegmentData>,
    ring: RingQueue<SegmentData>,
    callback_map: HashMap<i64, OneSender<SegmentRes>>,
}

enum TransportErr {
    SinkChanErr,
    RecvErr,
    Timeout,
    ReceiverChanClosed,
}

impl<Si, St> Transport<Si, St>
where
    Si: Sink<(SegmentData, WriteFlags)> + Send + Unpin + 'static,
    St: Stream<Item = SegmentRes> + Send + Unpin + 'static,
{
    async fn req(&mut self, req: SegmentData) -> Result<SegmentRes, {
        let send_res = self.send.send(req).await;

        todo!();
    }

    async fn poll_send(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<SegmentRes, TransportErr> {
        match self.recv.recv().await {
            Some(segment) => {
                if self.ring.is_full() {
                    todo!("wait() ring is ok to send");
                }
                let seq_id = self.ring.push(segment.clone()).unwrap();
                let res = self.sink.send((segment, WriteFlags::default())).await;
                match res {
                    Err(_e) => Err(TransportErr::SinkChanErr),
                    Ok(_) => {
                        let (s, r) = tokio::sync::oneshot::channel();
                        self.callback_map.insert(seq_id, s);
                        let time_res = tokio::time::timeout(timeout, r).await;
                        match time_res {
                            Err(_) => Err(TransportErr::Timeout),
                            Ok(res) => res.map_err(|_| TransportErr::RecvErr),
                        }
                    }
                }
            }
            None => Err(TransportErr::ReceiverChanClosed),
        }
    }

    async fn poll_back(&mut self) {

    }
}
