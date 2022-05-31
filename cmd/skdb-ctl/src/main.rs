// pub(crate) mod chan;
pub(crate) mod conn;

use chrono::Local;
use clap::Parser;
use skdb::test::gen::*;
use skdb::TOKIO_RUN;
use skproto::tracing::*;
use tokio::time::{sleep, Duration};
use tracing::{info_span, Instrument};
use tracing_subscriber::fmt::Subscriber;

// use crate::chan::{SegmentDataWrap, SeqMail};
use crate::conn::Connector;
use futures::SinkExt;

#[derive(Debug)]
enum QpsSetValue {
    // 7,500
    Normal,
    // 10,000 (Our target)
    Ok,
    // 15,000
    FeelHigh,
}

impl QpsSetValue {
    fn val_of<'a>(val: &'a str) -> Self {
        match val {
            "Normal" => Self::Normal,
            "Ok" => Self::Ok,
            "FeelHigh" => Self::FeelHigh,
            _ => panic!("Not correct value"),
        }
    }

    fn record_num_every_10ms(&self) -> usize {
        match self {
            QpsSetValue::Normal => 75,
            QpsSetValue::Ok => 100,
            QpsSetValue::FeelHigh => 150,
        }
    }
}
/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value_t = 9000)]
    port: usize,

    #[clap(short, long, default_value = "127.0.0.1")]
    ip: String,

    #[clap(short, long, default_value = "Normal")]
    qps: String,
}

fn mock_seg(conn_id: i32, api_id: i32, seq_id: i64) -> SegmentData {
    let mut segment = SegmentData::new();
    let mut meta = Meta::new();
    meta.connId = conn_id;
    meta.field_type = Meta_RequestType::TRANS;
    meta.seqId = seq_id;
    let now = Local::now();
    meta.set_send_timestamp(now.timestamp_nanos() as u64);
    let uuid = uuid::Uuid::new_v4();
    segment.set_meta(meta);
    segment.set_trace_id(uuid.to_string());
    segment.set_api_id(api_id);
    segment.set_payload(_gen_data_binary());
    segment.set_zone(_gen_tag(3, 5, 'a'));
    segment.set_biz_timestamp(now.timestamp_millis() as u64);
    segment.set_seg_id(uuid.to_string());
    segment.set_ser_key(_gen_tag(4, 3, 's'));
    segment
}

fn main() {
    // Set global subscriber to console log
    let fmt_scriber = Subscriber::new();
    tracing::subscriber::set_global_default(fmt_scriber).expect("Set global subscriber");
    let args = Args::parse();
    println!(
        "Use pressure config is port:{}, ip:{}, qps: {}",
        args.port, args.ip, args.qps
    );

    let exec_func = async {
        // let (sink, r, conn_id, _client) = Connector::sk_connect_handshake().await.unwrap();
        // let window_size = 64 * 100;
        // let mut sender = SeqMail::start_task(sink, r, window_size, conn_id)
        //     .instrument(info_span!("start_task"))
        //     .await;
        // let qps_set = QpsSetValue::val_of(&args.qps);
        // let mut seq_id = 1;
        // loop {
        //     for i in 0..qps_set.record_num_every_10ms() {
        //         let segment = mock_seg(conn_id, i as i32, seq_id);
        //         let _ = sender.send(SegmentDataWrap(segment)).await;
        //         seq_id += 1;
        //     }
        //     sleep(Duration::from_millis(10)).await;
        //     if seq_id > 1000000 {
        //         break;
        //     }
        // }
    }
    .instrument(info_span!("main"));

    TOKIO_RUN.block_on(exec_func);
}

#[cfg(test)]
mod tests {

    use super::conn::Connector;
    use skdb::*;

    #[test]
    fn test_handshake_success() {
        TOKIO_RUN.block_on(async {
            let res = Connector::sk_connect_handshake().await;
            match res {
                Ok((_sink, _recv, conn_id, _client)) => {
                    println!("connect id is:{:?}", conn_id);
                }
                Err(e) => {
                    println!("handshake failed, connect status is:{:?}", e);
                }
            }
        });
    }
}
