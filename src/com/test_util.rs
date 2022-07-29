use chrono::Local;
use rand::prelude::*;
use skproto::tracing::{BatchSegmentData, Meta, Meta_RequestType, SegmentData};
use tokio::sync::oneshot::Receiver;
use tracing::{span, Level};

use crate::tag::fsm::SegmentDataCallback;

use super::{
    ack::{AckCallback, CallbackStat},
    gen::{_gen_data_binary, _gen_tag},
    index::{ConvertIndexAddr, MailKeyAddress, EXPIRED_DAYS},
};

pub fn mock_seg(conn_id: i32, api_id: i32, seq_id: i64) -> SegmentData {
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

pub fn mock_seg_with_hour(conn_id: i32, api_id: i32, seq_id: i64, hours: i64) -> SegmentData {
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

    let before_timestamp = now
        .checked_sub_signed(chrono::Duration::hours(hours))
        .unwrap();

    segment.set_biz_timestamp(before_timestamp.timestamp_millis() as u64);
    segment.set_seg_id(uuid.to_string());
    segment.set_ser_key(_gen_tag(4, 3, 's'));
    segment
}

pub fn random_mock_batch(size: usize) -> BatchSegmentData {
    let mut batch = BatchSegmentData::new();

    let mut v = Vec::new();
    for i in 1..size + 1 {
        let seg = mock_seg_with_hour(1 as i32, i as i32, i as i64, i as i64);
        v.push(seg);
    }
    batch.set_datas(v.into());

    batch
}

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

    let before_days = rng.gen_range(1i64..(EXPIRED_DAYS - 1));
    let before_seconds = rng.gen_range(1i64..3600 * 12i64);

    let date_time = cur
        .checked_sub_signed(chrono::Duration::days(before_days))
        .unwrap();
    date_time
        .checked_sub_signed(chrono::Duration::seconds(before_seconds))
        .unwrap()
        .timestamp_millis()
        .with_index_addr()
}

pub fn gen_expired_mailkeyadd() -> MailKeyAddress {
    let cur = Local::now();

    let mut rng = rand::thread_rng();

    let before_days = rng.gen_range(EXPIRED_DAYS..(EXPIRED_DAYS + 10));
    let before_seconds = rng.gen_range(1i64..3600 * 12i64);

    let date_time = cur
        .checked_sub_signed(chrono::Duration::days(before_days))
        .unwrap();
    date_time
        .checked_sub_signed(chrono::Duration::seconds(before_seconds))
        .unwrap()
        .timestamp_millis()
        .with_index_addr()
}

pub(crate) mod redis {
    use redis::Value;

    use crate::redis::{Record, RedisTTLSet, KEY};

    pub(crate) fn create_redis_client() -> redis::Client {
        redis::Client::open("redis://127.0.0.1:6379").unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn create_redis_conn() -> redis::Connection {
        let client = create_redis_client();
        client.get_connection().unwrap()
    }

    pub(crate) fn gen_virtual_servers(num: usize) -> Vec<String> {
        gen_virtual_servers_with_ip(num, "192.168.0")
    }

    pub(crate) fn gen_virtual_servers_with_ip(num: usize, ip_prefix: &str) -> Vec<String> {
        let mut start_num = 0;
        let mut ip_vec = Vec::new();

        let mut conn = create_redis_conn();
        for _ in 0..num {
            let mut gen_ip = || {
                start_num += 1;
                format!("{}.{}", ip_prefix, start_num)
            };

            let ip = gen_ip();
            ip_vec.push(ip.clone());

            let meta_s = format!("{}:{}", ip, 9999);

            let redis_ttl: RedisTTLSet = Default::default();
            let _ = redis_ttl.push(&mut conn, meta_s);
        }

        ip_vec
    }

    pub(crate) fn offline_some_servers(num: usize) {
        let conn = &mut create_redis_conn();

        let val = redis::cmd("HGETALL").arg(KEY).query::<Value>(conn).unwrap();
        match val {
            Value::Bulk(vals) => {
                for i in 0..usize::min(num * 2, vals.len()) {
                    if i % 2 == 1 {
                        continue;
                    }

                    let ip_port = vals.get(i).unwrap();

                    if let Value::Data(byts) = ip_port {
                        let s = String::from_utf8(byts.to_vec()).unwrap();
                        redis::cmd("HDEL")
                            .arg(KEY)
                            .arg(s)
                            .query::<Value>(conn)
                            .unwrap();
                    }
                    let _ = vals.get(i + 1).unwrap();
                }
            }
            _ => {}
        }
    }

    pub(crate) fn gen_expired_virtual_servers(num: usize) -> Vec<String> {
        let mut start_num = 0;
        let mut ip_vec = Vec::new();

        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = client.get_connection().unwrap();

        for _ in 0..num {
            let mut gen_ip = || {
                start_num += 1;
                format!("192.168.0.{}", start_num)
            };

            let ip = gen_ip();
            ip_vec.push(ip.clone());

            let meta_s = format!("{}:{}", ip, 9999);
            let mut record: Record = meta_s.try_into().unwrap();
            record.meta.expire_time = -1;

            let redis_ttl: RedisTTLSet = Default::default();
            let _ = redis_ttl.push(&mut conn, record);
        }

        ip_vec
    }

    pub(crate) fn redis_servers_clear() {
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let conn = &mut client.get_connection().unwrap();

        let val = redis::cmd("HGETALL").arg(KEY).query::<Value>(conn).unwrap();
        match val {
            Value::Bulk(vals) => {
                for i in 0..vals.len() {
                    if i % 2 == 1 {
                        continue;
                    }

                    let ip_port = vals.get(i).unwrap();

                    if let Value::Data(byts) = ip_port {
                        let s = String::from_utf8(byts.to_vec()).unwrap();
                        redis::cmd("HDEL")
                            .arg(KEY)
                            .arg(s)
                            .query::<Value>(conn)
                            .unwrap();
                    }
                    let _ = vals.get(i + 1).unwrap();
                }
            }
            _ => {}
        }
    }
}
