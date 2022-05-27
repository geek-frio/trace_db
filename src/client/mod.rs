use anyhow::Error as AnyError;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::future::Either;
use futures::ready;
use futures::stream::StreamExt;
use futures::Future;
use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::TryFutureExt;
use grpcio::ClientDuplexReceiver;
use grpcio::StreamingCallSink;
use grpcio::WriteFlags;
use pin_project::pin_project;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::{marker::PhantomData, time::Duration};
use tower::buffer::Buffer;
use tower::{limit::RateLimit, Service, ServiceBuilder};

use crate::com::ring::RingQueueError;
use crate::com::ring::{RingQueue, SeqId};

pub struct TracingConnection<Status, Req, Resp> {
    sink: Option<StreamingCallSink<Req>>,
    recv: Option<ClientDuplexReceiver<Resp>>,
    marker: PhantomData<Status>,
}

impl<Status, Req, Resp> Drop for TracingConnection<Status, Req, Resp> {
    fn drop(&mut self) {
        todo!();
    }
}

pub struct Created;
pub struct HandShaked;

pub trait Connector<Req, Resp> {
    fn create_new_conn(&self) -> TracingConnection<Created, Req, Resp>;
    fn conn_count(&self) -> usize;
}

pub trait Handshake {
    fn gen_handshake_pkt() -> Self;
}

impl<Req, Resp> TracingConnection<Created, Req, Resp>
where
    Req: Handshake + Clone,
{
    pub async fn handshake(
        mut self,
        f: impl FnOnce(Resp, Req) -> bool,
    ) -> Result<TracingConnection<HandShaked, Req, Resp>, AnyError> {
        if self.sink.is_none() || self.recv.is_none() {
            return Err(AnyError::msg("sink and receiver is not properly inited!"));
        }

        let sink = std::mem::replace(&mut (self.sink), None);
        let recv = std::mem::replace(&mut (self.recv), None);
        let mut sink = sink.unwrap();
        let mut recv = recv.unwrap();
        let pkt = <Req as Handshake>::gen_handshake_pkt();

        let res = sink.send((pkt.clone(), WriteFlags::default())).await;

        match res {
            Ok(_) => {
                let resp: Option<Result<Resp, grpcio::Error>> = recv.next().await;
                match resp {
                    Some(r) => match r {
                        Ok(resp) => {
                            if !f(resp, pkt) {
                                Err(AnyError::msg(
                                    "Hanshake response check with request failed!",
                                ))
                            } else {
                                Ok(TracingConnection {
                                    sink: Some(sink),
                                    recv: Some(recv),
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

#[derive(Debug, Clone, PartialEq)]
pub enum SinkEvent<Req> {
    PushMsg(Option<Req>),
}

impl<T> Ack for SinkEvent<T> {
    fn is_ack(&self) -> bool {
        todo!()
    }
}

impl<T> Default for SinkEvent<T> {
    fn default() -> Self {
        todo!()
    }
}

impl<T> SeqId for SinkEvent<T>
where
    T: SeqId,
{
    fn seq_id(&self) -> usize {
        todo!()
    }
}

impl Error for SinkErr {}

impl Display for SinkErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

#[derive(Debug, Clone)]
pub enum SinkErr {
    GrpcSinkErr(String),
    NoReq,
}

pub trait TracingMsgSink<F> {
    type Item;
    fn sink(&mut self, event: SinkEvent<Self::Item>) -> Result<(), SinkErr>;
}

pub trait TracingMsgStream<T> {
    type Item;
    fn poll_remote(&mut self) -> Option<SinkEvent<Self::Item>>;
    fn poll_local(&mut self) -> Option<SinkEvent<Self::Item>>;
}

pub struct TracingSinker<T> {
    sink: StreamingCallSink<T>,
}

impl<T> Service<SinkEvent<T>> for TracingSinker<T>
where
    T: Send + 'static,
{
    type Response = ();

    type Error = SinkErr;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        return Poll::Ready(Ok(()));
    }

    fn call(&mut self, req: SinkEvent<T>) -> Self::Future {
        let sink = Pin::new(&mut self.sink);
        match req {
            SinkEvent::PushMsg(req) => {
                if let Some(v) = req {
                    let send_res = sink.start_send((v, WriteFlags::default()));
                    let res = match send_res {
                        Ok(_) => Ok(()),
                        Err(e) => Err(SinkErr::GrpcSinkErr(e.to_string())),
                    };

                    return Box::pin(futures::future::ready(res));
                } else {
                    return Box::pin(futures::future::ready(Err(SinkErr::NoReq)));
                }
            }
        }
    }
}

impl<T: SeqId + Debug + Clone + Send + 'static> TracingSinker<T> {
    pub fn with_limit(
        self,
        buf: usize,
        num: u64,
        per: Duration,
    ) -> (
        Buffer<RateLimit<RingService<TracingSinker<T>, SinkEvent<T>>>, SinkEvent<T>>,
        Receiver<Waker>,
    ) {
        let service_builder = ServiceBuilder::new();
        let (wak_send, wak_recv) = crossbeam_channel::unbounded();
        let ring_service: RingService<TracingSinker<T>, SinkEvent<T>> = RingService {
            inner: self,
            ring: Default::default(),
            wak_sender: wak_send,
        };
        (
            service_builder
                .buffer::<SinkEvent<T>>(buf)
                .rate_limit(num, per)
                .service(ring_service),
            wak_recv,
        )
    }
}

pub struct RingService<S, Req>
where
    Req: SeqId + Sized + Debug + Default + Clone,
{
    inner: S,
    ring: RingQueue<Req>,
    wak_sender: Sender<Waker>,
}

#[derive(Debug)]
pub enum RingServiceErr<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Display for RingServiceErr<L, R>
where
    L: Display + Debug,
    R: Display + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("self:{:?}", self))
    }
}

impl<L, R> std::error::Error for RingServiceErr<L, R>
where
    L: Display + Debug,
    R: Display + Debug,
{
}

impl<S, Request> Service<Request> for RingService<S, Request>
where
    S: Service<Request>,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
    Request: SeqId + Sized + Debug + Default + Clone + Ack,
{
    type Response = Either<(), S::Response>;

    type Error = RingServiceErr<S::Error, RingQueueError>;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Send>>;

    fn poll_ready<'a>(&mut self, cx: &mut std::task::Context<'a>) -> Poll<Result<(), Self::Error>> {
        if self.ring.is_full() {
            let _ = self.wak_sender.send(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if req.is_ack() {
            self.ring.ack(req.seq_id());
            return Box::pin(futures::future::ready(Ok(Either::Left(()))));
        }
        let res = self.ring.send(req.clone());
        match res {
            Ok(_) => {
                let inner_res = self.inner.call(req);
                return Box::pin(
                    inner_res
                        .map_ok(|a| Either::Right(a))
                        .map_err(|e| RingServiceErr::Left(e)),
                );
            }
            Err(e) => return Box::pin(futures::future::ready(Err(RingServiceErr::Right(e)))),
        }
    }
}

// keep polling msg from remote
#[pin_project]
pub struct TracingStreamer<Resp> {
    #[pin]
    recv: ClientDuplexReceiver<Resp>,
    wak_recv: Receiver<Waker>,
}

trait Ack {
    fn is_ack(&self) -> bool;
}

impl<Resp> Stream for TracingStreamer<Resp>
where
    Resp: Ack,
{
    type Item = Result<Resp, AnyError>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let recv = this.recv;
        let wak = this.wak_recv;
        let msg = ready!(recv.poll_next(cx));
        match msg {
            None => return Poll::Ready(None),
            Some(res) => {
                let res = res
                    .map(|v| {
                        if v.is_ack() {
                            if let Ok(waker) = wak.try_recv() {
                                waker.wake();
                            }
                        }
                        v
                    })
                    .map_err(|e| <AnyError as From<grpcio::Error>>::from(e.into()));
                Poll::Ready(Some(res))
            }
        }
    }
}

impl<T> TracingStreamer<T> {}
