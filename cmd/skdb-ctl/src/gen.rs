use super::conn;
use crate::{
    chan::{SeqIdFill, SeqMail},
    conn::Connector,
};
use futures::{SinkExt, StreamExt};
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
                println!("send result is:{:?}", send_rs);
            }
            sleep(Duration::from_millis(qp_10ms as u64)).await;
        }
    });

    // create client and handshake

    TOKIO_RUN.spawn(async move {
        while let a = r.next().await {
            println!("Get a TRANS response from server, a:{:?}", a);
        }
    });
    println!("Handshake and connect success ,conn_id is:{}", conn_id);
    loop {
        let seg = recv.recv().await;
        let s = sink.send((seg.unwrap(), WriteFlags::default())).await;
        println!("sent result is:{:?}", s);
    }
}
