pub(crate) mod chan;
pub(crate) mod conn;
pub(crate) mod gen;

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
    // 10,000
    Normal,
    // 20,000 (Our target)
    Ok,
    // 50,000
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
            QpsSetValue::Normal => 100,
            QpsSetValue::Ok => 200,
            QpsSetValue::FeelHigh => 500,
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

fn main() {
    let args = Args::parse();
    println!(
        "Use pressure config is port:{}, ip:{}, qps: {}",
        args.port, args.ip, args.qps
    );

    let exec_func = async {
        let (seq_mail, mut recv) = SeqMail::new(100000);
        let (mut sink, _, conn_id) = Connector::sk_connect_handshake().await.unwrap();
        let qps_set = QpsSetValue::val_of(&args.qps);
        println!("Handshake and connect success ,conn_id is:{}", conn_id);

        println!("Current send speed is {:?}", qps_set);
        TOKIO_RUN.spawn(async move {
            loop {
                for i in 0..qps_set.record_num_every_10ms() {
                    let mut segment = SegmentData::new();
                    let mut meta = Meta::new();
                    meta.connId = conn_id;
                    meta.field_type = Meta_RequestType::TRANS;
                    let now = Local::now();
                    meta.set_send_timestamp(now.timestamp_nanos() as u64);
                    let uuid = uuid::Uuid::new_v4();
                    segment.set_meta(meta);
                    segment.set_trace_id(uuid.to_string());
                    segment.set_api_id(i as i32);
                    segment.set_payload(_gen_data_binary());
                    segment.set_zone(_gen_tag(3, 5, 'a'));
                    segment.set_biz_timestamp(now.timestamp_millis() as u64);
                    segment.set_seg_id(uuid.to_string());
                    segment.set_ser_key(_gen_tag(4, 3, 's'));
                    println!("sent traceid is:{}", uuid.to_string());
                    let send_rs = seq_mail.try_send_msg(segment, ()).await;
                    match send_rs {
                        Ok(seq_id) => {
                            if seq_id % 11 == 1 {
                                println!("current seqid:{}", seq_id);
                            }
                        }
                        Err(e) => {
                            println!("Send failed!, error is:{:?}", e);
                        }
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            }
        });
        // Consume messages and sent to grpc service
        loop {
            let seg = recv.recv().await;
            let r = sink.send((seg.unwrap(), WriteFlags::default())).await;
            if let Err(e) = r {
                println!("Send msgs failed!error:{}", e);
            }
        }
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
