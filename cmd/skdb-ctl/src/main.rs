pub(crate) mod chan;
pub(crate) mod conn;
pub(crate) mod gen;
pub(crate) mod send_ctl;

use std::time::Instant;

use chrono::Local;
use clap::Parser;
use grpcio::WriteFlags;
use skdb::test::gen::*;
use skdb::TOKIO_RUN;
use skproto::tracing::*;
use tokio::time::{sleep, Duration};

use crate::chan::SeqMail;
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

fn mock_seg(conn_id: i32, api_id: i32) -> SegmentData {
    let mut segment = SegmentData::new();
    let mut meta = Meta::new();
    meta.connId = conn_id;
    meta.field_type = Meta_RequestType::TRANS;
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
    let args = Args::parse();
    println!(
        "Use pressure config is port:{}, ip:{}, qps: {}",
        args.port, args.ip, args.qps
    );

    let exec_func = async {
        let (mut seq_mail, mut recv) = SeqMail::new(100000);
        let (mut sink, mut r, conn_id) = Connector::sk_connect_handshake().await.unwrap();
        let qps_set = QpsSetValue::val_of(&args.qps);
        TOKIO_RUN.spawn(async move {
            let mut count = 0;
            let mut time_counter = Instant::now();
            loop {
                for i in 0..qps_set.record_num_every_10ms() {
                    let segment = mock_seg(conn_id, i as i32);
                    let send_rs = seq_mail.send_msg(segment).await;
                    match send_rs {
                        Ok(seq_id) => {}
                        Err(e) => {}
                    }
                }
                sleep(Duration::from_millis(10)).await;
                count += 1;
                if count % 100 == 0 {
                    count = 0;
                    println!(
                        "Elapsed time is:{} millis",
                        time_counter.elapsed().as_millis()
                    );
                    time_counter = Instant::now();
                }
            }
        });
    };

    TOKIO_RUN.block_on(exec_func);
}

#[cfg(test)]
mod tests {

    use super::conn::Connector;
    use crate::gen::*;
    use skdb::*;

    #[test]
    fn test_handshake_success() {
        TOKIO_RUN.block_on(async {
            let res = Connector::sk_connect_handshake().await;
            match res {
                Ok((_sink, _recv, conn_id)) => {
                    println!("connect id is:{:?}", conn_id);
                }
                Err(e) => {
                    println!("handshake failed, connect status is:{:?}", e);
                }
            }
        });
    }

    #[test]
    fn test_send_msg() {
        TOKIO_RUN.block_on(async {
            test_unbounded_gen_sksegments(1).await;
        });
    }
}
