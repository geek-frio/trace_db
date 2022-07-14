use chrono::Local;
use rand::prelude::*;
use skproto::tracing::SegmentData;
use tokio::sync::oneshot::Receiver;
use tracing::{span, Level};

use crate::tag::fsm::SegmentDataCallback;

use super::{
    ack::{AckCallback, CallbackStat},
    index::{ConvertIndexAddr, MailKeyAddress},
};

pub fn gen_segcallback(days: i64, secs: i64) -> (SegmentDataCallback, Receiver<CallbackStat>) {
    let cur = Local::now();
    let date_time = cur
        .checked_sub_signed(chrono::Duration::days(days))
        .unwrap();
    let date_time = date_time
        .checked_sub_signed(chrono::Duration::seconds(secs))
        .unwrap();

    let mut segment = SegmentData::new();
    segment.set_biz_timestamp(date_time.timestamp_millis() as u64);

    let span = span!(Level::INFO, "my_span");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let callback = AckCallback::new(sender);

    (SegmentDataCallback::new(segment, callback, span), receiver)
}

pub fn gen_valid_mailkeyadd() -> MailKeyAddress {
    let cur = Local::now();

    let mut rng = rand::thread_rng();

    let before_days = rng.gen_range(1i64..30i64);
    let before_seconds = rng.gen_range(1i64..3600 * 12i64);

    let date_time = cur
        .checked_sub_signed(chrono::Duration::days(before_days))
        .unwrap();
    date_time
        .checked_sub_signed(chrono::Duration::seconds(before_seconds))
        .unwrap()
        .timestamp_millis()
        .with_index_addr()
        .unwrap()
}
