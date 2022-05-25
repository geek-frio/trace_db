use std::marker::PhantomData;

use futures::Future;
use tower::Service;

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

#[derive(Debug, Clone)]
pub enum SinkErr {}

pub trait TracingMsgSink<F>
where
    F: Freq,
{
    fn sink(&mut self, event: SinkEvent) -> Result<(), SinkErr>;
    fn with_freq(&mut self, f: F);
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

    type Future = Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Unpin>;

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
// keep polling msg from remote
pub struct TracingStreamer;

impl TracingConnection<HandShaked> {
    pub fn split(self) -> (TracingSinker, TracingStreamer) {
        todo!();
    }
}
