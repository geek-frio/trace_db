use std::error::Error;
use std::fmt::Display;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::{marker::PhantomData, time::Duration};

use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::Future;
use tower::buffer::Buffer;
use tower::{limit::RateLimit, Layer, Service, ServiceBuilder};

use crate::com::ring::{RingQueue, SeqId};

pub struct TracingConnection<Status> {
    marker: PhantomData<Status>,
}

impl<Status> Drop for TracingConnection<Status> {
    fn drop(&mut self) {
        todo!();
    }
}

pub struct Created;
pub struct HandShaked;

pub trait Connector {
    fn create_new_conn(&self) -> TracingConnection<Created>;
    fn conn_count(&self) -> usize;
}

impl TracingConnection<Created> {
    pub fn handshake(self) -> TracingConnection<HandShaked> {
        TracingConnection {
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SinkEvent {
    PushMsg,
    Blank,
}

// impl BlankElement for SinkEvent {
//     type Item = SinkEvent;

//     fn is_blank(&self) -> bool {
//         todo!();
//     }

//     fn blank_val() -> Self::Item {
//         SinkEvent::Blank
//     }
// }

impl Error for SinkErr {}

impl Display for SinkErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

#[derive(Debug, Clone)]
pub enum SinkErr {}

pub trait TracingMsgSink<F> {
    fn sink(&mut self, event: SinkEvent) -> Result<(), SinkErr>;
}

pub trait TracingMsgStream {
    fn poll_remote(&mut self) -> Option<SinkEvent>;
    fn poll_local(&mut self) -> Option<SinkEvent>;
}

// loop poll SinkEvent
pub struct TracingSinker;
pub enum SinkResp {
    Success,
}

impl Service<SinkEvent> for TracingSinker {
    type Response = SinkResp;

    type Error = SinkErr;

    type Future =
        Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Unpin + Send>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn call(&mut self, req: SinkEvent) -> Self::Future {
        todo!()
    }
}

impl TracingSinker {
    pub fn with_limit(self, buf: usize, num: u64, per: Duration) {
        let service_builder = ServiceBuilder::new();
        let (wak_send, wak_recv) = crossbeam_channel::unbounded();
        let ring_service: RingService<TracingSinker, SinkEvent> = RingService {
            inner: self,
            ring: Default::default(),
            wak_sender: wak_send,
        };
        // let a = service_builder
        //     .buffer::<SinkEvent>(buf)
        //     .rate_limit(num, per)
        //     .service(ring_service);
    }
}

struct RingService<S, Req> {
    inner: S,
    ring: RingQueue<Req>,
    wak_sender: Sender<Waker>,
}

impl<S, Req> RingService<S, Req>
where
    Req: std::fmt::Debug + SeqId + Clone,
{
    fn need_wake(&mut self, waker: Waker) {}
}

impl<S, Request> Service<Request> for RingService<S, Request>
where
    S: Service<Request>,
    Request: std::fmt::Debug + SeqId + Clone,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = Box<dyn Future<Output = Result<S::Response, S::Error>> + Unpin + 'static + Send>;

    fn poll_ready<'a>(&mut self, cx: &mut std::task::Context<'a>) -> Poll<Result<(), Self::Error>> {
        if self.ring.is_full() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        todo!()
    }
}

// keep polling msg from remote
pub struct TracingStreamer {
    wak_receiver: Receiver<Waker>,
}

impl TracingStreamer {}

impl TracingConnection<HandShaked> {
    pub fn split(self) -> (TracingSinker, TracingStreamer) {
        todo!();
    }
}
