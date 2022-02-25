use futures::SinkExt;
use grpcio::WriteFlags;
use skproto::tracing::*;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct AckCtl {
    cur_seq: i64,
    lask_ack_time: std::time::Instant,
}

impl AckCtl {
    pub fn new() -> AckCtl {
        AckCtl {
            cur_seq: 0,
            lask_ack_time: Instant::now(),
        }
    }

    pub async fn process_timely_ack_ctl(
        &mut self,
        segment: SegmentData,
        sink: &mut grpcio::DuplexSink<SegmentRes>,
    ) {
        let elapsed_time = self.lask_ack_time.elapsed();
        if elapsed_time > Duration::from_secs(1) {
            println!("elapse time is coming!!!!");
            // Gen ack packet
            let mut resp = SegmentRes::default();
            let mut meta = Meta::new();
            meta.set_seqId(segment.get_meta().get_seqId());
            meta.set_field_type(Meta_RequestType::TRANS_ACK);
            resp.set_meta(meta);
            // Send ack packet
            let _ = sink.send((resp, WriteFlags::default())).await;
            self.cur_seq = segment.get_meta().get_seqId();
            self.lask_ack_time = Instant::now();
        } else {
            self.cur_seq = segment.get_meta().get_seqId();
        }
    }
}
