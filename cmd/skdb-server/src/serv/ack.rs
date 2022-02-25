use futures::SinkExt;
use grpcio::WriteFlags;
use skproto::tracing::*;
use std::time::{Duration, Instant};
pub struct AckCtl {
    cur_seq: i64,
    lask_ack_time: std::time::Instant,
    sink: Option<grpcio::DuplexSink<SegmentRes>>,
}

impl AckCtl {
    pub fn new() -> AckCtl {
        AckCtl {
            cur_seq: 0,
            lask_ack_time: Instant::now(),
            sink: None,
        }
    }

    pub async fn process_timely_ack_ctl(&mut self, segment: SegmentData) {
        if self.sink.is_none() {
            return;
        }

        let elapsed_time = self.lask_ack_time.elapsed();
        if elapsed_time > Duration::from_secs(1) {
            let resp = SegmentRes::default();
            let _ = self
                .sink
                .as_mut()
                .unwrap()
                .send((resp, WriteFlags::default()))
                .await;
            self.cur_seq = segment.get_meta().get_seqId();
            self.lask_ack_time = Instant::now();
        } else {
            self.cur_seq = segment.get_meta().get_seqId();
        }
    }
}
