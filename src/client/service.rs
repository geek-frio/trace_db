use std::{pin::Pin, task::Poll};

use futures::Future;
use skproto::tracing::SegmentData;
use tower::{load::Load, Service};

use super::trans::{RequestScheduler, TransportErr};

pub struct EndpointService {
    sched: RequestScheduler,
    broken_notify: tokio::sync::mpsc::Sender<i32>,
    id: i32,
}

impl Load for EndpointService {
    type Metric = i32;

    fn load(&self) -> Self::Metric {
        0
    }
}

impl EndpointService {
    pub fn new(
        sched: RequestScheduler,
        broken_notify: tokio::sync::mpsc::Sender<i32>,
        id: i32,
    ) -> EndpointService {
        EndpointService {
            sched,
            broken_notify,
            id,
        }
    }
}

impl Service<SegmentData> for EndpointService {
    type Response = tokio::sync::oneshot::Receiver<Result<(), TransportErr>>;

    type Error = String;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, String>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        return Poll::Ready(Ok(()));
    }

    fn call(&mut self, req: SegmentData) -> Self::Future {
        let mut sched = self.sched.clone();
        let broken_notify = self.broken_notify.clone();
        let id = self.id;
        Box::pin(async move {
            let r = sched.request(req);
            if let Err(e) = r.as_ref() {
                match e {
                    &TransportErr::Shutdown
                    | &TransportErr::ReceiverChanClosed
                    | &TransportErr::RecvErr => {
                        let _ = broken_notify.send(id).await;
                    }
                    _ => {}
                }
            }
            r.map_err(|e| e.to_string())
        })
    }
}
