use anyhow::Error as AnyError;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::future::Either;
use futures::ready;
use futures::Future;
use futures::Stream;
use grpcio::ClientDuplexReceiver;
use grpcio::StreamingCallSink;
use pin_project::pin_project;
use redis::streams::StreamMaxlen;
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

pub struct TracingConnection<Status, T> {
    sink: Option<StreamingCallSink<T>>,
    recv: Option<ClientDuplexReceiver<T>>,
    marker: PhantomData<Status>,
}

impl<Status, T> Drop for TracingConnection<Status, T> {
    fn drop(&mut self) {
        todo!();
    }
}

pub struct Created;
pub struct HandShaked;

pub trait Connector<T> {
    fn create_new_conn(&self) -> TracingConnection<Created, T>;
    fn conn_count(&self) -> usize;
}

impl<T> TracingConnection<Created, T> {
    pub fn handshake(mut self) -> TracingConnection<HandShaked, T> {
        let sink = std::mem::replace(&mut (self.sink), None);
        let recv = std::mem::replace(&mut (self.recv), None);
        TracingConnection {
            sink,
            recv,
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SinkEvent<T> {
    PushMsg(T),
    Blank,
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
pub enum SinkErr {}

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
    _item: PhantomData<T>,
}
pub enum SinkResp {
    Success,
}

impl<T> Service<SinkEvent<T>> for TracingSinker<T> {
    type Response = SinkResp;

    type Error = SinkErr;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn call(&mut self, req: SinkEvent<T>) -> Self::Future {
        todo!()
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
    Request: SeqId + Sized + Debug + Default + Clone,
{
    type Response = S::Response;

    type Error = RingServiceErr<S::Error, RingQueueError>;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Send>>;

    fn poll_ready<'a>(&mut self, cx: &mut std::task::Context<'a>) -> Poll<Result<(), Self::Error>> {
        if self.ring.is_full() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let res = self.ring.send(req);
        match res {
            Ok(_) => {}
            Err(e) => return Box::pin(futures::future::ready(Err(RingServiceErr::Right(e)))),
        }
        Box::pin(async move { todo!() })
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

impl<T> TracingConnection<HandShaked, T> {
    pub fn split(self) -> (TracingSinker<T>, TracingStreamer<T>) {
        todo!();
    }
}
