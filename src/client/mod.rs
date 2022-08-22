use anyhow::Error as AnyError;
use futures::stream::StreamExt;
use futures::SinkExt;
use grpcio::ClientDuplexReceiver;
use grpcio::StreamingCallSink;
use grpcio::WriteFlags;
use std::marker::PhantomData;

pub mod cluster;
pub mod grpc_cli;
pub mod local_trans;
mod service;
pub mod trans;

pub struct TracingConnection<Status, Req, Resp> {
    pub sink: Option<StreamingCallSink<Req>>,
    pub recv: Option<ClientDuplexReceiver<Resp>>,
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
            marker: PhantomData,
        }
    }

    pub async fn handshake(
        self,
        check_hand_resp: impl Fn(Resp, Req) -> (bool, i32),
        gen_hand_pkg: impl Fn() -> Req,
    ) -> Result<TracingConnection<HandShaked, Req, Resp>, AnyError> {
        if self.sink.is_none() || self.recv.is_none() {
            return Err(AnyError::msg("sink and receiver is not properly inited!"));
        }

        let mut sink = self.sink.unwrap();
        let mut recv = self.recv.unwrap();

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
                                    marker: PhantomData,
                                })
                            }
                        }
                        Err(e) => Err(e.into()),
                    },
                    None => Err(AnyError::msg("Receiving handshake resp failed!")),
                }
            }
            Err(e) => {
                tracing::error!("adfasfafadsfdsafdasfdasfdafadsfdasfdase:{:?}", e);
                Err(e.into())
            }
        }
    }
}
