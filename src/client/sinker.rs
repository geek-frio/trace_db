use futures::{ready, Future, SinkExt, StreamExt};
use futures::{sink::Sink, FutureExt};
use grpcio::{StreamingCallSink, WriteFlags};
use skproto::tracing::{Meta_RequestType, SegmentData, SegmentRes, SkyTracingClient};
use std::{marker::PhantomData, pin::Pin, task::Poll};
use tower::Service;
use tracing::warn;
use tracing::{error, trace};

use super::HandShaked;
use super::{
    grpc_cli::WrapSegmentData, ChangeResend, Created, SinkErr, SinkEvent, TracingConnection,
};
use grpcio::Error as GrpcErr;
struct TracingSinker<WrapReq, Req, Resp> {
    client: SkyTracingClient,
    conn: Option<TracingConnection<HandShaked, WrapReq, Req, Resp>>,
    marker: PhantomData<WrapReq>,
}

fn create_trace_conn(
    client: &mut SkyTracingClient,
) -> Result<TracingConnection<Created, WrapSegmentData, SegmentData, SegmentRes>, SinkErr> {
    match client.push_segments() {
        Ok((sink, recv)) => {
            trace!("Push segments called success! sink:(StreamingCallSink) and receiver:(ClientDuplexReceiver is created!");
            Ok(TracingConnection::new(sink, recv))
        }
        Err(e) => {
            error!("Create conn failed:{:?}", e);

            Err(SinkErr::CreateConnFail)
        }
    }
}

impl Service<SinkEvent<WrapSegmentData>>
    for TracingSinker<WrapSegmentData, SegmentData, SegmentRes>
{
    type Response = i64;

    type Error = SinkErr;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.conn.is_none() {
            let trace_conn = create_trace_conn(&mut self.client)?;
            let res = ready!(Box::pin(trace_conn.handshake(
                |resp, _req| {
                    let conn_id = resp.get_meta().get_connId();
                    (true, conn_id)
                },
                || {
                    let mut segment = SegmentData::new();
                    segment
                        .mut_meta()
                        .set_field_type(Meta_RequestType::HANDSHAKE);
                    Default::default()
                }
            ))
            .poll_unpin(cx));
            match res {
                Ok(conn) => {
                    self.conn = Some(conn);
                    Poll::Ready(Ok(()))
                }
                Err(_e) => Poll::Ready(Err(SinkErr::CreateConnFail)),
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: SinkEvent<WrapSegmentData>) -> Self::Future {
        let mut conn = self.conn.as_mut().unwrap();
        let sink = Pin::new(conn.sink.as_mut().unwrap());
        let mut stream = Pin::new(conn.recv.as_mut().unwrap());

        match req {
            SinkEvent::PushMsg(req) => Box::pin(async move {
                if req.is_none() {
                    return Err(SinkErr::NoSeqId);
                }
                let req = req.unwrap();
                let seq_id = req.seq_id();
                if let Some(seq_id) = seq_id {
                    let send_res = sink.start_send((req.into(), WriteFlags::default()));
                    let res = match send_res {
                        Ok(_) => Ok(seq_id),
                        Err(e) => Err(SinkErr::GrpcSinkErr(e.to_string())),
                    };

                    while let Some(segment_res) = stream.next().await {
                        match segment_res {
                            Ok(segment_resp) => {
                                let r_seq_id = segment_resp.get_meta().get_seqId();
                                if r_seq_id >= seq_id {
                                    return Ok(segment_resp.get_meta().get_seqId());
                                }
                            }
                            Err(e) => match e {
                                GrpcErr::RemoteStopped | GrpcErr::QueueShutdown => {
                                    return Err(SinkErr::ConnBroken);
                                }
                                _ => {
                                    warn!("Unexpected exception process logic is reached!");
                                }
                            },
                        }
                    }
                    Err(SinkErr::ConnBroken)
                } else {
                    Err(SinkErr::NoSeqId)
                }
            }),
        }
    }
}

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
//     ) -> (impl Service<RingServiceReqEvent<WrapReq>>, Receiver<Waker>) {
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
