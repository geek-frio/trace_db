use std::{sync::Arc, time::Duration};

use anyhow::Error as AnyError;
use grpcio::{Channel, ChannelBuilder, Environment};
// use skdb::{
//     client::{ChangeResend, Created, HandShaked, TracingConnection},
//     com::util::CalcSleepTime,
// };
use skproto::tracing::{Meta, Meta_RequestType, SegmentData, SegmentRes, SkyTracingClient};
use tokio::time::sleep;
use tracing::{error, trace};

use crate::com::util::CalcSleepTime;

use super::{Created, HandShaked, TracingConnection};

// pub(crate) fn init_grpc_chan(addr: &str) -> Channel {
//     let env = Environment::new(3);
//     ChannelBuilder::new(Arc::new(env)).connect(addr)
// }
// pub(crate) fn init_push_msg_conn(
//     chan: Channel,
// ) -> Result<
//     (
//         TracingConnection<Created, WrapSegmentData, SegmentData, SegmentRes>,
//         SkyTracingClient,
//     ),
//     AnyError,
// > {
//     let client = SkyTracingClient::new(chan);
//     split_client(client)
// }

pub(crate) fn split_client(
    client: SkyTracingClient,
) -> Result<
    (
        TracingConnection<Created, SegmentData, SegmentRes>,
        SkyTracingClient,
    ),
    AnyError,
> {
    match client.push_segments() {
        Ok((sink, recv)) => {
            trace!("Push segments called success! sink:(StreamingCallSink) and receiver:(ClientDuplexReceiver is created!");
            Ok((TracingConnection::new(sink, recv), client))
        }
        Err(e) => {
            error!(?e, "connect push segment service failed!");
            Err(e.into())
        }
    }
}

// pub(crate) fn gen_hand_pkt() -> SegmentData {
//     let mut segment = SegmentData::default();
//     let mut meta = Meta::default();
//     meta.field_type = Meta_RequestType::HANDSHAKE;
//     segment.set_meta(meta);
//     segment
// }

// fn check_hand_resp(_: SegmentRes, req: SegmentData) -> (bool, i32) {
//     (true, req.get_meta().connId)
// }

// // It's a block function, this function will retry until we get a handshake successful result
// pub(crate) async fn handshake(
//     addr: &str,
// ) -> (
//     TracingConnection<HandShaked, WrapSegmentData, SegmentData, SegmentRes>,
//     SkyTracingClient,
// ) {
//     let mut counter = 0usize;
//     loop {
//         let chan = init_grpc_chan(addr);
//         let conn = init_push_msg_conn(chan);
//         match conn {
//             Ok((conn, client)) => {
//                 let res = conn.handshake(check_hand_resp, gen_hand_pkt).await;
//                 match res {
//                     Ok(conn) => return (conn, client),
//                     Err(e) => {
//                         error!("Hanshake failed, wait 1 second,and continue, e:{:?}", e);
//                         sleep(counter.caculate_sleep_time(Duration::from_secs(1))).await;
//                     }
//                 }
//             }
//             Err(e) => {
//                 error!(
//                     "Init connection failed! Wait 1 second and will try to reconnect again!e:{:?}",
//                     e
//                 );
//                 sleep(counter.caculate_sleep_time(Duration::from_secs(1))).await;
//             }
//         }
//     }
// }
