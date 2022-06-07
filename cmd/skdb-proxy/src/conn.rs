// use std::sync::Arc;

// use futures::SinkExt;
// use futures_util::TryStreamExt as _;
// use grpcio::ClientDuplexReceiver;
// use grpcio::StreamingCallSink;
// use grpcio::{ChannelBuilder, Environment};
// use grpcio::{Error as GrpcError, WriteFlags};
// use skproto::tracing::SkyTracingClient;
// use skproto::tracing::*;
// use tracing::{error, info, instrument, trace};
// pub struct Connector;

// #[derive(Debug)]
// pub enum ConnectStatus {
//     GrpcCallFailed(GrpcError),
//     HandShakeSendFailed(GrpcError),
//     HandShakeRespFailed(GrpcError),
// }

// impl Connector {
//     // Connect and complete handshake
//     // TODO: Need to add config for address
//     #[instrument]
//     pub async fn sk_connect_handshake() -> Result<
//         (
//             StreamingCallSink<SegmentData>,
//             ClientDuplexReceiver<SegmentRes>,
//             i32,
//             SkyTracingClient,
//         ),
//         ConnectStatus,
//     > {
//         let env = Environment::new(3);
//         let remote_addr = "127.0.0.1:9000";
//         // TODO: config change
//         let channel = ChannelBuilder::new(Arc::new(env)).connect("127.0.0.1:9000");
//         trace!(
//             %remote_addr,
//             "Connecting to push segment service"
//         );
//         let client = SkyTracingClient::new(channel);
//         match client.push_segments() {
//             Ok((mut sink, mut r)) => {
//                 trace!("Push segments called success! sink:(StreamingCallSink) and receiver:(ClientDuplexReceiver is created!");
//                 // handshake logic
//                 let mut segment = SegmentData::default();
//                 let mut meta = Meta::default();
//                 meta.field_type = Meta_RequestType::HANDSHAKE;
//                 segment.set_meta(meta);
//                 info!(?segment, "Sending handshake segment:{:?}", segment);
//                 let res = sink.send((segment, WriteFlags::default())).await;
//                 info!(?res, "Handshake sent completed");
//                 let a = match res {
//                     Err(e) => Err(ConnectStatus::HandShakeSendFailed(e)),
//                     Ok(_) => match r.try_next().await {
//                         Ok(resp) => {
//                             // TODO: we may store the client somewhere to be used in the future
//                             // currently we don't need it, so we leak it
//                             Ok((sink, r, resp.unwrap().meta.unwrap().connId, client))
//                         }
//                         Err(e) => Err(ConnectStatus::HandShakeRespFailed(e)),
//                     },
//                 };
//                 return a;
//             }
//             Err(e) => {
//                 error!(?e, "connect push segment service failed!");
//                 return Err(ConnectStatus::GrpcCallFailed(e));
//             }
//         }
//     }
// }
