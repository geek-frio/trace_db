// use crossbeam_channel::Receiver;
// use futures::{ready, Future};
// use futures::{sink::Sink, FutureExt};
// use grpcio::WriteFlags;
// use skproto::tracing::{Meta_RequestType, SegmentData, SegmentRes, SkyTracingClient};
// use std::sync::atomic::Ordering;
// use std::task::Waker;
// use std::{marker::PhantomData, pin::Pin, task::Poll};
// use tokio::sync::mpsc::Sender;
// use tower::Service;
// use tracing::{error, trace};

// use super::{
//     grpc_cli::WrapSegmentData, ChangeResend, Created, SinkErr, SinkEvent, TracingConnection,
// };
// use super::{HandShaked, RingServiceReqEvent};
// struct TracingSinker<WrapReq: std::fmt::Debug, Req, Resp> {
//     client: SkyTracingClient,
//     conn: Option<TracingConnection<HandShaked, WrapReq, Req, Resp>>,

//     wak_recv: Receiver<Waker>,
//     sched: Sender<RingServiceReqEvent<WrapReq>>,
//     marker: PhantomData<WrapReq>,
// }

// fn create_trace_conn(
//     client: &mut SkyTracingClient,
// ) -> Result<TracingConnection<Created, WrapSegmentData, SegmentData, SegmentRes>, SinkErr> {
//     match client.push_segments() {
//         Ok((sink, recv)) => {
//             trace!("Push segments called success! sink:(StreamingCallSink) and receiver:(ClientDuplexReceiver is created!");
//             Ok(TracingConnection::new(sink, recv))
//         }
//         Err(e) => {
//             error!("Create conn failed:{:?}", e);

//             Err(SinkErr::CreateConnFail)
//         }
//     }
// }

// impl Service<SinkEvent<WrapSegmentData>>
//     for TracingSinker<WrapSegmentData, SegmentData, SegmentRes>
// {
//     type Response = i64;

//     type Error = SinkErr;

//     type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

//     fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
//         if self.conn.is_none()
//             || self
//                 .conn
//                 .as_ref()
//                 .unwrap()
//                 .rpc_shutdown
//                 .load(Ordering::Relaxed)
//         {
//             let trace_conn = create_trace_conn(&mut self.client)?;
//             let res = ready!(Box::pin(trace_conn.handshake(
//                 |resp, _req| {
//                     let conn_id = resp.get_meta().get_connId();
//                     (true, conn_id)
//                 },
//                 || {
//                     let mut segment = SegmentData::new();
//                     segment
//                         .mut_meta()
//                         .set_field_type(Meta_RequestType::HANDSHAKE);
//                     Default::default()
//                 }
//             ))
//             .poll_unpin(cx));
//             match res {
//                 Ok(mut conn) => {
//                     conn.rpc_shutdown.store(false, Ordering::Relaxed);
//                     let _ = conn.waiting_for_resp(self.wak_recv.clone(), self.sched.clone());
//                     self.conn = Some(conn);
//                     Poll::Ready(Ok(()))
//                 }
//                 Err(_e) => Poll::Ready(Err(SinkErr::CreateConnFail)),
//             }
//         } else {
//             Poll::Ready(Ok(()))
//         }
//     }

//     fn call(&mut self, req: SinkEvent<WrapSegmentData>) -> Self::Future {
//         let conn = self.conn.as_mut().unwrap();
//         let sink = Pin::new(conn.sink.as_mut().unwrap());

//         match req {
//             SinkEvent::PushMsg(req) => {
//                 if req.is_none() {
//                     return Box::pin(futures::future::ready(Err(SinkErr::NoReq)));
//                 }
//                 let req = req.unwrap();
//                 let seq_id = req.seq_id();
//                 if let Some(seq_id) = seq_id {
//                     let send_res = sink.start_send((req.into(), WriteFlags::default()));
//                     let res = match send_res {
//                         Ok(_) => Ok(seq_id),
//                         Err(e) => Err(SinkErr::GrpcSinkErr(e.to_string())),
//                     };
//                     Box::pin(futures::future::ready(res))
//                 } else {
//                     Box::pin(futures::future::ready(Err(SinkErr::NoSeqId)))
//                 }
//             }
//         }
//     }
// }
