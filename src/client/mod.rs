use std::error::Error;
use std::fmt::Display;
use std::{marker::PhantomData, time::Duration};

use futures::Future;
use tower::buffer::Buffer;
use tower::{
    layer::util::Identity,
    limit::{rate::Rate, RateLimit},
    Layer, Service, ServiceBuilder,
};

use crate::com::util::Freq;

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

#[derive(Debug, Clone)]
pub enum SinkEvent {
    PushMsg,
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
    fn sink(&mut self, event: SinkEvent) -> Result<(), SinkErr>;
}

pub trait TracingMsgStream {
    fn poll_remote(&mut self) -> Option<SinkEvent>;
    fn poll_local(&mut self) -> Option<SinkEvent>;
}

// loop poll SinkEvent
pub struct TracingSinker;

impl Service<SinkEvent> for TracingSinker {
    type Response = ();

    type Error = SinkErr;

    type Future =
        Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Unpin + Send>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn call(&mut self, req: SinkEvent) -> Self::Future {
        todo!()
    }
}

impl TracingSinker {
    pub fn with_limit(
        self,
        buf: usize,
        num: u64,
        per: Duration,
    ) -> Buffer<RateLimit<TracingSinker>, SinkEvent> {
        let service_builder = ServiceBuilder::new();
        service_builder
            .buffer::<SinkEvent>(buf)
            .rate_limit(num, per)
            .service(self)
    }
}

// keep polling msg from remote
pub struct TracingStreamer;

impl TracingConnection<HandShaked> {
    pub fn split(self) -> (TracingSinker, TracingStreamer) {
        todo!();
    }
}
