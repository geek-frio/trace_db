use std::sync::Arc;

use futures::SinkExt;
use futures::StreamExt;
use futures_util::stream;
use futures_util::TryStreamExt as _;
use grpcio::ClientDuplexReceiver;
use grpcio::StreamingCallSink;
use grpcio::{ChannelBuilder, Environment};
use grpcio::{Error as GrpcError, WriteFlags};
use skproto::tracing::SkyTracingClient;
use skproto::tracing::*;
pub struct Connector;

#[derive(Debug)]
pub enum ConnectStatus {
    GrpcCallFailed(GrpcError),
    HandShakeSendFailed(GrpcError),
    HandShakeRespFailed(GrpcError),
    HandShakeSuccess(i32),
    FAILED,
}

impl Connector {
    // Connect and complete handshake
    // TODO: Need to add config for address
    pub async fn sk_connect_handshake() -> Result<
        (
            StreamingCallSink<SegmentData>,
            ClientDuplexReceiver<SegmentRes>,
            i32,
        ),
        ConnectStatus,
    > {
        let env = Environment::new(3);
        // TODO: config change
        let channel = ChannelBuilder::new(Arc::new(env)).connect("127.0.0.1:9000");

        let client = Box::new(SkyTracingClient::new(channel));
        match client.push_segments() {
            Ok((mut sink, mut r)) => {
                // handshake logic
                let mut send_data = vec![];
                let mut s = SegmentData::default();
                let mut meta = Meta::default();
                meta.field_type = Meta_RequestType::HANDSHAKE;
                s.set_meta(meta);
                send_data.push(s);

                let mut send_stream =
                    stream::iter(send_data).map(|item| Ok((item, WriteFlags::default())));

                let res = sink.send_all(&mut send_stream).await;
                println!("we have sent the handshake packet!");
                let a = match res {
                    Err(e) => Err(ConnectStatus::HandShakeSendFailed(e)),
                    Ok(_) => match r.try_next().await {
                        Ok(resp) => {
                            // TODO: we may store the client somewhere to be used in the future
                            // currently we don't need it, so we leak it
                            Box::leak(client);
                            Ok((sink, r, resp.unwrap().meta.unwrap().connId))
                        }
                        Err(e) => Err(ConnectStatus::HandShakeRespFailed(e)),
                    },
                };
                return a;
            }
            Err(e) => {
                return Err(ConnectStatus::GrpcCallFailed(e));
            }
        }
    }
}
