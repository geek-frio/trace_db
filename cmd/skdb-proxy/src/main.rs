// use grpc_cli::{handshake, WrapSegmentData};
use skdb::client::{RingServiceErr, SinkErr};
use skdb::com::ring::RingQueueError;
use skdb::{client::RingServiceReqEvent, TOKIO_RUN};
use skproto::tracing::{Meta_RequestType, SegmentRes};
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tower::{Service, ServiceExt};
use tracing::{error, info};
use tracing_subscriber::fmt::Subscriber;

// pub(crate) mod grpc_cli;
// mod serv;

fn main() {
    let fmt_scriber = Subscriber::new();
    tracing::subscriber::set_global_default(fmt_scriber).expect("Set global subscriber");

    // let (send, mut recv) = unbounded_channel::<WrapSegmentData>();
    // let exec = async {
    //     let (tracing_conn, client) = handshake("127.0.0.1:9000").await;
    //     let ack_fun = Box::new(|arg: &SegmentRes| {
    //         let meta = arg.get_meta();
    //         meta.get_field_type() == Meta_RequestType::TRANS_ACK
    //     });
    //     let (mut service, client) = tracing_conn.split(20000, 10, Duration::from_secs(1), ack_fun);

    //     loop {
    //         let wrap_segment = recv.recv().await;
    //         match wrap_segment {
    //             Some(seg) => {
    //                 let event = RingServiceReqEvent::Msg(seg);
    //                 // Currently our service poll ready don't throw Error
    //                 let _ = service.ready().await;

    //                 let req_res = service.call(event).await;
    //                 if let Err(e) = req_res {
    //                     let err = e.downcast::<RingServiceErr<SinkErr, RingQueueError>>();
    //                     if let Ok(e) = err {
    //                         match *e {
    //                             RingServiceErr::Left(sink_err) => {
    //                                 if let SinkErr::GrpcSinkErr(e) = sink_err {
    //                                     error!("Grpc sink has met serious problem, we should drop current connection,e:{}", e);
    //                                     break;
    //                                 }
    //                             }
    //                             RingServiceErr::Right(ring_queue_err) => match ring_queue_err {
    //                                 RingQueueError::Full(_, _) => {
    //                                     unreachable!("Every request will be called poll ready, poll ready will check the ringqueue's length");
    //                                 }
    //                                 RingQueueError::InvalidAckId(cur_id, seq_id) => {
    //                                     error!("Invalid ackId, maybe we got a error response from remote peer, cur_id:{}, seq_id:{}", cur_id, seq_id);
    //                                 }
    //                             },
    //                         }
    //                     }
    //                 }
    //             }
    //             None => {
    //                 info!("Sender has been dropped..");
    //                 break;
    //             }
    //         }
    //     }
    // };
    // TOKIO_RUN.block_on(exec);
}

// // pub(crate) mod chan;
// pub(crate) mod conn;

// use chrono::Local;
// use clap::Parser;
// use skdb::test::gen::*;
// use skdb::TOKIO_RUN;
// use skproto::tracing::*;
// use tokio::time::{sleep, Duration};
// use tracing::{info_span, Instrument};
// use tracing_subscriber::fmt::Subscriber;

// // use crate::chan::{SegmentDataWrap, SeqMail};
// use crate::conn::Connector;
// use futures::SinkExt;

// #[derive(Debug)]
// enum QpsSetValue {
//     // 7,500
//     Normal,
//     // 10,000 (Our target)
//     Ok,
//     // 15,000
//     FeelHigh,
// }

// impl QpsSetValue {
//     fn val_of<'a>(val: &'a str) -> Self {
//         match val {
//             "Normal" => Self::Normal,
//             "Ok" => Self::Ok,
//             "FeelHigh" => Self::FeelHigh,
//             _ => panic!("Not correct value"),
//         }
//     }

//     fn record_num_every_10ms(&self) -> usize {
//         match self {
//             QpsSetValue::Normal => 75,
//             QpsSetValue::Ok => 100,
//             QpsSetValue::FeelHigh => 150,
//         }
//     }
// }
// /// Simple program to greet a person
// #[derive(Parser, Debug)]
// #[clap(author, version, about, long_about = None)]
// pub struct Args {
//     #[clap(short, long, default_value_t = 9000)]
//     port: usize,

//     #[clap(short, long, default_value = "127.0.0.1")]
//     ip: String,

//     #[clap(short, long, default_value = "Normal")]
//     qps: String,
// }

// fn mock_seg(conn_id: i32, api_id: i32, seq_id: i64) -> SegmentData {
//     let mut segment = SegmentData::new();
//     let mut meta = Meta::new();
//     meta.connId = conn_id;
//     meta.field_type = Meta_RequestType::TRANS;
//     meta.seqId = seq_id;
//     let now = Local::now();
//     meta.set_send_timestamp(now.timestamp_nanos() as u64);
//     let uuid = uuid::Uuid::new_v4();
//     segment.set_meta(meta);
//     segment.set_trace_id(uuid.to_string());
//     segment.set_api_id(api_id);
//     segment.set_payload(_gen_data_binary());
//     segment.set_zone(_gen_tag(3, 5, 'a'));
//     segment.set_biz_timestamp(now.timestamp_millis() as u64);
//     segment.set_seg_id(uuid.to_string());
//     segment.set_ser_key(_gen_tag(4, 3, 's'));
//     segment
// }

// fn main() {
//     // Set global subscriber to console log
//     let fmt_scriber = Subscriber::new();
//     tracing::subscriber::set_global_default(fmt_scriber).expect("Set global subscriber");
//     let args = Args::parse();
//     println!(
//         "Use pressure config is port:{}, ip:{}, qps: {}",
//         args.port, args.ip, args.qps
//     );

//     let exec_func = async {
//         // let (sink, r, conn_id, _client) = Connector::sk_connect_handshake().await.unwrap();
//         // let window_size = 64 * 100;
//         // let mut sender = SeqMail::start_task(sink, r, window_size, conn_id)
//         //     .instrument(info_span!("start_task"))
//         //     .await;
//         // let qps_set = QpsSetValue::val_of(&args.qps);
//         // let mut seq_id = 1;
//         // loop {
//         //     for i in 0..qps_set.record_num_every_10ms() {
//         //         let segment = mock_seg(conn_id, i as i32, seq_id);
//         //         let _ = sender.send(SegmentDataWrap(segment)).await;
//         //         seq_id += 1;
//         //     }
//         //     sleep(Duration::from_millis(10)).await;
//         //     if seq_id > 1000000 {
//         //         break;
//         //     }
//         // }
//     }
//     .instrument(info_span!("main"));

//     TOKIO_RUN.block_on(exec_func);
// }

// #[cfg(test)]
// mod tests {

//     use super::conn::Connector;
//     use skdb::*;

//     #[test]
//     fn test_handshake_success() {
//         TOKIO_RUN.block_on(async {
//             let res = Connector::sk_connect_handshake().await;
//             match res {
//                 Ok((_sink, _recv, conn_id, _client)) => {
//                     println!("connect id is:{:?}", conn_id);
//                 }
//                 Err(e) => {
//                     println!("handshake failed, connect status is:{:?}", e);
//                 }
//             }
//         });
//     }
// }
