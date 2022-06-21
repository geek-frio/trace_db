use anyhow::Error as AnyError;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::future::join_all;
use futures::future::Either;
use futures::stream::StreamExt;
use futures::Future;
use futures::Sink;
use futures::SinkExt;
use futures::TryFutureExt;
use grpcio::ClientDuplexReceiver;
use grpcio::StreamingCallSink;
use grpcio::WriteFlags;
use skproto::tracing::Meta_RequestType;
use skproto::tracing::SegmentData;
use skproto::tracing::SegmentRes;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::{marker::PhantomData, time::Duration};
use tokio::time::sleep;
use tower::buffer::Buffer;
use tower::{limit::RateLimit, Service, ServiceBuilder};
use tracing::error;
use tracing::warn;

pub mod cluster;
pub mod grpc_cli;
pub mod serv;
mod service;
pub mod sinker;
mod trans;

use crate::com::ring::RingQueue;
use crate::com::ring::RingQueueError;
use crate::TOKIO_RUN;

pub struct TracingConnection<Status, Req, Resp> {
    sink: Option<StreamingCallSink<Req>>,
    recv: Option<ClientDuplexReceiver<Resp>>,
    rpc_shutdown: Arc<AtomicBool>,
    marker: PhantomData<Status>,
}

impl<Status, Req, Resp> Unpin for TracingConnection<Status, Req, Resp> {}

pub struct Created;
pub struct HandShaked;

impl<Req, Resp> TracingConnection<Created, Req, Resp>
where
    Req: Clone,
{
    pub fn new(
        sink: StreamingCallSink<Req>,
        recv: ClientDuplexReceiver<Resp>,
    ) -> TracingConnection<Created, Req, Resp> {
        Self {
            sink: Some(sink),
            recv: Some(recv),
            rpc_shutdown: Arc::new(AtomicBool::new(false)),
            marker: PhantomData,
        }
    }

    pub async fn handshake(
        mut self,
        check_hand_resp: impl Fn(Resp, Req) -> (bool, i32),
        gen_hand_pkg: impl Fn() -> Req,
    ) -> Result<TracingConnection<HandShaked, Req, Resp>, AnyError> {
        if self.sink.is_none() || self.recv.is_none() {
            return Err(AnyError::msg("sink and receiver is not properly inited!"));
        }

        let sink = std::mem::replace(&mut (self.sink), None);
        let recv = std::mem::replace(&mut (self.recv), None);
        let mut sink = sink.unwrap();
        let mut recv = recv.unwrap();
        let pkt = gen_hand_pkg();

        let res = sink.send((pkt.clone(), WriteFlags::default())).await;

        match res {
            Ok(_) => {
                let resp: Option<Result<Resp, grpcio::Error>> = recv.next().await;
                match resp {
                    Some(r) => match r {
                        Ok(resp) => {
                            let (stat, _) = check_hand_resp(resp, pkt);
                            if stat {
                                Err(AnyError::msg(
                                    "Hanshake response check with request failed!",
                                ))
                            } else {
                                Ok(TracingConnection {
                                    sink: Some(sink),
                                    recv: Some(recv),
                                    rpc_shutdown: Arc::new(AtomicBool::new(false)),
                                    marker: PhantomData,
                                })
                            }
                        }
                        Err(e) => Err(e.into()),
                    },
                    None => Err(AnyError::msg("Receiving handshake resp failed!")),
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}

// #[derive(Debug, Clone, PartialEq)]
// pub enum SinkEvent<WrapReq> {
//     PushMsg(Option<WrapReq>),
// }

// impl<Req> Default for SinkEvent<Req>
// where
//     Req: Default,
// {
//     fn default() -> Self {
//         SinkEvent::PushMsg(Some(Default::default()))
//     }
// }

// impl Error for SinkErr {}

// impl Display for SinkErr {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_fmt(format_args!("{}", self))
//     }
// }

// #[derive(Debug, Clone)]
// pub enum SinkErr {
//     GrpcSinkErr(String),
//     NoReq,
//     CreateConnFail,
//     HandshakedFail,
//     GetRespTimeOut,
//     ConnBroken,
//     NoSeqId,
// }

// pub trait TracingMsgSink<F> {
//     type Item;
//     fn sink(&mut self, event: SinkEvent<Self::Item>) -> Result<(), SinkErr>;
// }

// pub struct TracingSinker<WrapReq, Req> {
//     sink: StreamingCallSink<Req>,
//     marker: PhantomData<WrapReq>,
// }

// impl<WrapReq, Req> Service<SinkEvent<WrapReq>> for TracingSinker<WrapReq, Req>
// where
//     Req: Send + 'static,
//     WrapReq: Into<Req> + ChangeResend,
// {
//     type Response = Option<i64>;

//     type Error = SinkErr;

//     type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

//     fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         return Poll::Ready(Ok(()));
//     }

//     fn call(&mut self, req: SinkEvent<WrapReq>) -> Self::Future {
//         let sink = Pin::new(&mut self.sink);
//         match req {
//             SinkEvent::PushMsg(req) => {
//                 if let Some(v) = req {
//                     let seq_id = v.seq_id();
//                     let send_res = sink.start_send((v.into(), WriteFlags::default()));
//                     let res = match send_res {
//                         Ok(_) => Ok(seq_id),
//                         Err(e) => Err(SinkErr::GrpcSinkErr(e.to_string())),
//                     };
//                     return Box::pin(futures::future::ready(res));
//                 } else {
//                     return Box::pin(futures::future::ready(Err(SinkErr::NoReq)));
//                 }
//             }
//         }
//     }
// }

// impl<WrapReq, Req> TracingSinker<WrapReq, Req>
// where
//     WrapReq: Debug + Send + 'static + ChangeResend + Clone,
//     Req: From<WrapReq> + Send + 'static + Clone,
// {
//     pub fn with_limit(
//         self,
//         buf: usize,
//         num: u64,
//         per: Duration,
//     ) -> (
//         Buffer<
//             RateLimit<RingService<TracingSinker<WrapReq, Req>, WrapReq>>,
//             RingServiceReqEvent<WrapReq>,
//         >,
//         Receiver<Waker>,
//     ) {
//         let service_builder = ServiceBuilder::new();
//         let (wak_send, wak_recv) = crossbeam_channel::unbounded();
//         let ring_service: RingService<TracingSinker<WrapReq, Req>, WrapReq> = RingService {
//             inner: self,
//             ring: Default::default(),
//             wak_sender: wak_send,
//         };
//         let service = service_builder
//             .buffer::<RingServiceReqEvent<WrapReq>>(buf)
//             .rate_limit(num, per)
//             .service(ring_service);
//         (service, wak_recv)
//     }
// }

// pub struct RingService<S, Req>
// where
//     Req: Debug + Clone,
// {
//     inner: S,
//     ring: RingQueue<Req>,
//     wak_sender: Sender<Waker>,
// }

// #[derive(Debug)]
// pub enum RingServiceErr<L, R> {
//     Left(L),
//     Right(R),
// }

// impl<L, R> Display for RingServiceErr<L, R>
// where
//     L: Display + Debug,
//     R: Display + Debug,
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_fmt(format_args!("self:{:?}", self))
//     }
// }

// impl<L, R> std::error::Error for RingServiceErr<L, R>
// where
//     L: Display + Debug,
//     R: Display + Debug,
// {
// }

// #[derive(Debug, Clone)]
// pub enum RingServiceReqEvent<Req: Debug> {
//     Ack(Req),
//     NeedResend(usize),
//     Msg(Req),
// }

// pub trait ChangeResend {
//     fn change_resend_meta(&mut self);
//     fn fill_seq_id(&mut self, seq_id: i64);
//     fn seq_id(&self) -> Option<i64>;
// }

// impl<S, Request> Service<RingServiceReqEvent<Request>> for RingService<S, Request>
// where
//     S: Service<SinkEvent<Request>>,
//     S::Response: Send + 'static,
//     S::Error: Send + 'static,
//     S::Future: Send + 'static,
//     Request: Debug + Clone + ChangeResend,
// {
//     type Response = Either<(), S::Response>;

//     type Error = RingServiceErr<S::Error, RingQueueError>;

//     type Future =
//         Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Send>>;

//     fn poll_ready<'a>(&mut self, cx: &mut std::task::Context<'a>) -> Poll<Result<(), Self::Error>> {
//         if self.ring.is_full() {
//             let _ = self.wak_sender.send(cx.waker().clone());
//             Poll::Pending
//         } else {
//             Poll::Ready(Ok(()))
//         }
//     }

//     fn call(&mut self, req: RingServiceReqEvent<Request>) -> Self::Future {
//         match req {
//             RingServiceReqEvent::Ack(r) => {
//                 if let Some(ack_seq_id) = r.seq_id() {
//                     let _ = self.ring.ack(ack_seq_id);
//                 }
//                 return Box::pin(futures::future::ready(Ok(Either::Left(()))));
//             }
//             RingServiceReqEvent::Msg(mut req) => {
//                 let res = self.ring.push(req.clone());
//                 match res {
//                     Ok(seq_id) => {
//                         req.fill_seq_id(seq_id);
//                         let inner_res = self.inner.call(SinkEvent::PushMsg(Some(req)));
//                         return Box::pin(
//                             inner_res
//                                 .map_ok(|a| Either::Right(a))
//                                 .map_err(|e| RingServiceErr::Left(e)),
//                         );
//                     }
//                     Err(e) => {
//                         return Box::pin(futures::future::ready(Err(RingServiceErr::Right(e))))
//                     }
//                 }
//             }
//             RingServiceReqEvent::NeedResend(_) => {
//                 let reqs = self.ring.not_ack_iter().collect::<Vec<&Request>>();
//                 let futs = reqs
//                     .into_iter()
//                     .map(|r| {
//                         let mut req = r.clone();
//                         req.change_resend_meta();
//                         self.inner.call(SinkEvent::PushMsg(Some(req)))
//                     })
//                     .collect::<Vec<_>>();
//                 Box::pin(async {
//                     join_all(futs).await;
//                     Ok(Either::Left(()))
//                 })
//             }
//         }
//     }
// }

// impl TracingConnection<HandShaked, WrapSegmentData, SegmentData, SegmentRes> {
//     pub fn waiting_for_resp(
//         &mut self,
//         wak_recv: Receiver<Waker>,
//         sched: tokio::sync::mpsc::Sender<RingServiceReqEvent<WrapSegmentData>>,
//     ) -> Result<(), AnyError> {
//         let is_shutdown = self.rpc_shutdown.clone();
//         let recv = self.recv.take();
//         if recv.is_none() {
//             return Err(AnyError::msg(
//                 "Not have receiver, maybe waiting task has already started!",
//             ));
//         }
//         TOKIO_RUN.spawn(async move {
//             let mut r = recv.unwrap();
//             while let Some(resp) = r.next().await {
//                 match resp {
//                     Ok(resp) => {
//                         if resp.get_meta().get_field_type() == Meta_RequestType::TRANS_ACK {
//                             if let Ok(w) = wak_recv.try_recv() {
//                                 w.wake();
//                             }
//                             let seqid = resp.get_meta().seqId;
//                             let mut seg = SegmentData::new();
//                             seg.mut_meta().set_seqId(seqid);
//                             let _ = sched.send(RingServiceReqEvent::Ack(WrapSegmentData(seg)));
//                         }
//                    }
//                     Err(e) => match e {
//                         grpcio::Error::RemoteStopped | grpcio::Error::QueueShutdown => {
//                             is_shutdown.store(true, Ordering::Relaxed);
//                             warn!("Stream has stopped!");
//                             return;
//                         }
//                         x => {
//                             error!("Basically should not come here, will retry after 1 second, x:{:?}", x);
//                             sleep(Duration::from_secs(1)).await;
//                         }
//                     },
//                 }
//             }
//         });
//         Ok(())
//     }
// }
