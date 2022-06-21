use std::{pin::Pin, task::Poll};

use futures::Future;
use skproto::tracing::SegmentData;
use tower::Service;

use super::trans::{RequestScheduler, TransportErr};

pub struct Endpoint {
    sched: RequestScheduler,
}

impl Endpoint {
    pub fn new(sched: RequestScheduler) -> Endpoint {
        Endpoint { sched }
    }
}

impl Service<SegmentData> for Endpoint {
    type Response = Result<(), TransportErr>;

    type Error = String;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        return Poll::Ready(Ok(()));
    }

    fn call(&mut self, req: SegmentData) -> Self::Future {
        let mut sched = self.sched.clone();
        Box::pin(async move {
            let r = sched.request(req).await;
            Ok(r)
        })
    }
}
