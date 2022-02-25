use super::conn;
use crate::{
    chan::{SeqIdFill, SeqMail},
    conn::Connector,
};
use futures::{SinkExt, StreamExt};
use futures_util::stream;
use futures_util::TryStreamExt as _;
use grpcio::WriteFlags;
use skdb::test::gen::*;
use skdb::*;
use std::time::Duration;
use tokio::time::sleep;
use uuid;

use super::chan;
use skproto::tracing::{Meta, Meta_RequestType, SegmentData};

impl SeqIdFill for SegmentData {
    fn fill_seqid(&mut self, seq_id: i64) {
        let meta = self.mut_meta();
        meta.seqId = seq_id;
    }
}

// Unlimited gen skywalking record
pub async fn test_unbounded_gen_sksegments(qp_10ms: usize) {
    let (seq_mail, mut recv) = SeqMail::new(100000);
    let (mut sink, mut r, conn_id) = Connector::sk_connect_handshake().await.unwrap();
    TOKIO_RUN.spawn(async move {
        loop {
            for i in 0..qp_10ms {
                let mut segment = SegmentData::new();
                let mut meta = Meta::new();
                meta.connId = conn_id;
                meta.field_type = Meta_RequestType::TRANS;
                segment.set_meta(meta);
                let uuid = uuid::Uuid::new_v4();
                segment.set_trace_id(uuid.to_string());
                segment.set_api_id(i as i32);
                segment.set_payload(_gen_data_binary());
                segment.set_zone(_gen_tag(3, 5, 'a'));

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
            }
            sleep(Duration::from_millis(qp_10ms as u64)).await;
        }
    });

    println!("Handshake and connect success ,conn_id is:{}", conn_id);
    TOKIO_RUN.spawn(async move {
        let segment = r.try_next().await;
        match segment {
            Ok(s) => {
                println!("Received server response");
            }
            Err(e) => {
                println!("Error is :{:?}", e);
            }
        }
    });

    loop {
        let seg = recv.recv().await;
        println!("seg data is:{:?}", seg);
        let s = sink.send((seg.unwrap(), WriteFlags::default())).await;
        println!("sent result is:{:?}", s);
    }
}
