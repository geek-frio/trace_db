extern crate rand;
extern crate rocksdb;
extern crate uuid;

mod db;
use db::init_db;
use rand::Rng;
use std::sync::mpsc;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};
use uuid::Uuid;

// gen skywalking binary data
fn gen_data_binary() -> String {
    let mut rng = rand::thread_rng();
    let mut s = DATA_BINARY.clone().to_string();
    s.replace_range(2000.., "");

    let step = 10;

    let mut idx: usize = rng.gen_range(0..step);
    loop {
        if idx > s.len() {
            break;
        }
        let rand_idx: usize = rng.gen_range(0..25);
        let c: char = (('a' as u8) + 25) as char;
        s.replace_range(rand_idx..rand_idx + 1, &c.to_string());
        idx += step;
    }
    s
}

fn test_point_put_max_ops() {
    let dbs = init_db();

    let (tx, rx) = mpsc::channel();
    // writer
    let w = thread::spawn(move || {
        let mut ticks = 600;
        println!("测试ticks数目: {}", ticks);
        let mut total_time = 0;
        let mut total_num = 0;

        loop {
            if ticks <= 0 {
                break;
            }
            let now = Instant::now();
            let qps = 50000;
            let mut db_idx = 0;
            //
            println!("current qps is {}", qps);
            for i in 0..qps {
                let uuid = Uuid::new_v4().to_string();
                let data: String = gen_data_binary();

                dbs[db_idx].put(uuid.as_bytes(), data.as_bytes()).unwrap();
                if i % 10000 == 1 {
                    println!("key:{}", uuid);
                    tx.send(uuid).unwrap();
                }
                total_num += 1;
                db_idx %= dbs.len();
            }
            let elapse = now.elapsed().as_millis();
            total_time += elapse;
            println!("到目前为止导入时速: {}", total_num * 1000 / total_time);
            println!("elpase:{}ms", elapse);
            if elapse < 1000 {
                sleep(Duration::from_millis((1000 - elapse) as u64))
            }
            ticks -= 1;
        }
    });

    // reader
    let r = thread::spawn(move || {
        let mut total_time = 0;
        let mut num = 0;
        loop {
            let query_key_r = rx.recv();
            if let Ok(query_key) = query_key_r {
                let (_, time) = key_get(query_key);
                println!("本次查询耗时:{} micros; ", time);
                total_time += time;
                num += 1;

                println!("平均耗时:{} micro ", total_time / num);
            } else {
                return;
            }
        }
    });

    let _ = w.join();
    let _ = r.join();
}

fn key_get(key: String) -> (String, u128) {
    let mut dbs = init_db();
    let db = dbs.pop().unwrap();
    let now = Instant::now();
    (
        String::from_utf8(db.get(key.as_bytes()).unwrap().unwrap()).unwrap(),
        now.elapsed().as_micros(),
    )
}

fn main() {
    test_point_put_max_ops();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xx() {
        let s = gen_data_binary();
        println!("{}", s);
    }
}

const DATA_BINARY: &'static str = "CiAzNDkyYTFlYTUzOGE0ODc1YWFlNmQ0YTE3M2I4ZjNjNxI2ZDNjMmExZjNmODVkNDY4MTg0MzBiNGJlNTZlZmRkNzIuODQxLjE2NDE4ODMwMDc1NTQ4MjI4GoQDCAEYw/TpvuQvIMP06b7kLzIZL1NoYXJkaW5nU3BoZXJlL3BhcnNlU1FML0ACUDxiGQoKb3JpZ2luX2FwcBILYXBpLWdhdGV3YXlitwIKDGRiLnN0YXRlbWVudBKmAnNlbGVjdAogICAgIAogICAgZ3JvdXBfaWQsIHVzZXJfaWQsIHJvbGUsIGJpel9yb2xlLCB2ZXN0X3VzZXJfaWQsIGpvaW5fdGltZSwgbGVhdmVfdGltZSwgaW52aXRlX3VpZCwgCiAgICBnbXRfY3JlYXRlLCBnbXRfbW9kaWZpZWQsIHN0YXR1cywgbXNnX3Zpc2libGVfc3RhdHVzLCBzcGVha19zdGF0dXMsIGdyb3VwX3R5cGUsIGJpel9pZCwKICAgIGN1c3RvbV9uYW1lLCBjdXN0b21faWNvbgogICAKICAgIGZyb20gY2hhdF9ncm91cF91c2VyX2hvdAogICAgd2hlcmUgZ3JvdXBfaWQgPSA/IGFuZCBzdGF0dXM9J0lOJxr+AwgEEAMYw/TpvuQvIMb06b7kLzIkTXlzcWwvSkRCSS9QcmVwYXJlZFN0YXRlbWVudC9leGVjdXRlOjBybS1icDE3MDB1dGF6ODNhejZxay5teXNxbC5yZHMuYWxpeXVuY3MuY29tOjMzMDZAAUgBUCFiDgoHZGIudHlwZRIDc3FsYhUKC2RiLmluc3RhbmNlEgZpbV9ob3RiuwIKDGRiLnN0YXRlbWVudBKqAnNlbGVjdAogICAgIAogICAgZ3JvdXBfaWQsIHVzZXJfaWQsIHJvbGUsIGJpel9yb2xlLCB2ZXN0X3VzZXJfaWQsIGpvaW5fdGltZSwgbGVhdmVfdGltZSwgaW52aXRlX3VpZCwgCiAgICBnbXRfY3JlYXRlLCBnbXRfbW9kaWZpZWQsIHN0YXR1cywgbXNnX3Zpc2libGVfc3RhdHVzLCBzcGVha19zdGF0dXMsIGdyb3VwX3R5cGUsIGJpel9pZCwKICAgIGN1c3RvbV9uYW1lLCBjdXN0b21faWNvbgogICAKICAgIGZyb20gY2hhdF9ncm91cF91c2VyX2hvdF8yODMKICAgIHdoZXJlIGdyb3VwX2lkID0gPyBhbmQgc3RhdHVzPSdJTidiJwoRZGIuc3FsLnBhcmFtZXRlcnMSElsxMDQ5ODA0OTAyMTU5NDI0XRpOCAMQAhjD9Om+5C8gxvTpvuQvMhsvU2hhcmRpbmdTcGhlcmUvZXhlY3V0ZVNRTC9AAlA8YhkKCm9yaWdpbl9hcHASC2FwaS1nYXRld2F5GlAIAhjD9Om+5C8gxvTpvuQvMh8vU2hhcmRpbmdTcGhlcmUvSkRCQ1Jvb3RJbnZva2UvQAJQPGIZCgpvcmlnaW5fYXBwEgthcGktZ2F0ZXdheRqPAggFGMb06b7kLyDG9Om+5C8yGS9TaGFyZGluZ1NwaGVyZS9wYXJzZVNRTC9AAlA8YhkKCm9yaWdpbl9hcHASC2FwaS1nYXRld2F5YsIBCgxkYi5zdGF0ZW1lbnQSsQFzZWxlY3QKICAgICAKICAgIGRvY3Rvcl9pZCwgYXNzaXN0YW50X2lkLCBpc19kZWxldGUsIGdtdF9jcmVhdGUsIGdtdF9tb2RpZmllZAogICAKICAgIGZyb20gY2hhdF9kb2N0b3JfYXNzaXN0YW50X3JlbGF0aW9uX2FsbAogICAgd2hlcmUgZG9jdG9yX2lkIGluICgKICAgICAgCiAgICAgID8KICAgICAKICAgICka/wIICBAHGMb06b7kLyDJ9Om+5C8yJE15c3FsL0pEQkkvUHJlcGFyZWRTdGF0ZW1lbnQvZXhlY3V0ZTowcm0tYnAxNzAwdXRhejgzYXo2cWsubXlzcWwucmRzLmFsaXl1bmNzLmNvbTozMzA2QAFIAVAhYg4KB2RiLnR5cGUSA3NxbGIVCgtkYi5pbnN0YW5jZRIGaW1faG90YsIBCgxkYi5zdGF0ZW1lbnQSsQFzZWxlY3QKICAgICAKICAgIGRvY3Rvcl9pZCwgYXNzaXN0YW50X2lkLCBpc19kZWxldGUsIGdtdF9jcmVhdGUsIGdtdF9tb2RpZmllZAogICAKICAgIGZyb20gY2hhdF9kb2N0b3JfYXNzaXN0YW50X3JlbGF0aW9uX2FsbAogICAgd2hlcmUgZG9jdG9yX2lkIGluICgKICAgICAgCiAgICAgID8KICAgICAKICAgICliIQoRZGIuc3FsLnBhcmFtZXRlcnMSDFsxMjc3Nzc2MTA2XRpOCAcQBhjG9Om+5C8gyfTpvuQvMhsvU2hhcmRpbmdTcGhlcmUvZXhlY3V0ZVNRTC9AAlA8YhkKCm9yaWdpbl9hcHASC2FwaS1nYXRld2F5GlAIBhjG9Om+5C8gyfTpvuQvMh8vU2hhcmRpbmdTcGhlcmUvSkRCQ1Jvb3RJbnZva2UvQAJQPGIZCgpvcmlnaW5fYXBwEgthcGktZ2F0ZXdheRryAggJGMn06b7kLyDJ9Om+5C8yGS9TaGFyZGluZ1NwaGVyZS9wYXJzZVNRTC9AAlA8YhkKCm9yaWdpbl9hcHASC2FwaS1nYXRld2F5YqUCCgxkYi5zdGF0ZW1lbnQSlAJzZWxlY3QKICAgICAKICAgIGlkLCBkb2N0b3JfaWQsIHBhdGllbnRfaWQsIHR5cGUsIG5hbWUsIGRlbGV0ZWQsIGdtdF9jcmVhdGUsIGdtdF9tb2RpZmllZCwgbmlja19uYW1lLCAKICAgIHByb3h5X2RvY3Rvcl9pZCwgZ3JvdXBfdGl0bGUsIHRhZywgZ3JvdXBfYXZhdGFyLCBzdGF0dXMsIGJpel9pZCwgc2NlbmVfaWQKICAgCiAgICAsCiAgICAgCiAgICBjb21tZW50CiAgIAogICAgZnJvbSBjaGF0X2dyb3VwX2hvdAogICAgd2hlcmUgaWQgaW4KICAgICAoICAKICAgICAgPwogICAgICka7AMIDBALGMn06b7kLyDM9Om+5C8yJE15c3FsL0pEQkkvUHJlcGFyZWRTdGF0ZW1lbnQvZXhlY3V0ZTowcm0tYnAxNzAwdXRhejgzYXo2cWsubXlzcWwucmRzLmFsaXl1bmNzLmNvbTozMzA2QAFIAVAhYg4KB2RiLnR5cGUSA3NxbGIVCgtkYi5pbnN0YW5jZRIGaW1faG90YqkCCgxkYi5zdGF0ZW1lbnQSmAJzZWxlY3QKICAgICAKICAgIGlkLCBkb2N0b3JfaWQsIHBhdGllbnRfaWQsIHR5cGUsIG5hbWUsIGRlbGV0ZWQsIGdtdF9jcmVhdGUsIGdtdF9tb2RpZmllZCwgbmlja19uYW1lLCAKICAgIHByb3h5X2RvY3Rvcl9pZCwgZ3JvdXBfdGl0bGUsIHRhZywgZ3JvdXBfYXZhdGFyLCBzdGF0dXMsIGJpel9pZCwgc2NlbmVfaWQKICAgCiAgICAsCiAgICAgCiAgICBjb21tZW50CiAgIAogICAgZnJvbSBjaGF0X2dyb3VwX2hvdF8yODMKICAgIHdoZXJlIGlkIGluCiAgICAgKCAgCiAgICAgID8KICAgICApYicKEWRiLnNxbC5wYXJhbWV0ZXJzEhJbMTA0OTgwNDkwMjE1OTQyNF0aTggLEAoYyfTpvuQvIMz06b7kLzIbL1NoYXJkaW5nU3BoZXJlL2V4ZWN1dGVTUUwvQAJQPGIZCgpvcmlnaW5fYXBwEgthcGktZ2F0ZXdheRpQCAoYyfTpvuQvIMz06b7kLzIfL1NoYXJkaW5nU3BoZXJlL0pEQkNSb290SW52b2tlL0ACUDxiGQoKb3JpZ2luX2FwcBILYXBpLWdhdGV3YXkangUIDhANGMz06b7kLyDT9Om+5C8yE1RhYmxlc3RvcmUvR2V0UmFuZ2U6PWh0dHA6Ly9pbXNlcnZlci1pbXNlcnZlci1wcm8uY24taGFuZ3pob3UudnBjLm90cy5hbGl5dW5jcy5jb21AAUgBUF9iFQoHZGIudHlwZRIKVGFibGVzdG9yZWIbCgtkYi5pbnN0YW5jZRIMaW1zZXJ2ZXItcHJvYicKCnRhYmxlX25hbWUSGWNoYXRfd2FpdGluZ19zZW50X21lc3NhZ2ViDQoHY29sdW1ucxICW11iwwMKC3ByaW1hcnlfa2V5ErMDW3sicHJpbWFyeUtleSI6W3sibmFtZSI6Imdyb3VwX2lkIiwidmFsdWUiOnsidmFsdWUiOjEwNDk4MDQ5MDIxNTk0MjQsInR5cGUiOiJJTlRFR0VSIiwiZGF0YVNpemUiOjh9LCJkYXRhU2l6ZSI6LTF9LHsibmFtZSI6ImlkIiwidmFsdWUiOnsidmFsdWUiOjkyMjMzNzIwMzY4NTQ3NzU4MDcsInR5cGUiOiJJTlRFR0VSIiwiZGF0YVNpemUiOjh9LCJkYXRhU2l6ZSI6LTF9XSwiZGF0YVNpemUiOi0xfSx7InByaW1hcnlLZXkiOlt7Im5hbWUiOiJncm91cF9pZCIsInZhbHVlIjp7InZhbHVlIjoxMDQ5ODA0OTAyMTU5NDI0LCJ0eXBlIjoiSU5URUdFUiIsImRhdGFTaXplIjo4fSwiZGF0YVNpemUiOi0xfSx7Im5hbWUiOiJpZCIsInZhbHVlIjp7InZhbHVlIjowLCJ0eXBlIjoiSU5URUdFUiIsImRhdGFTaXplIjo4fSwiZGF0YVNpemUiOi0xfV0sImRhdGFTaXplIjotMX1dGrABCA0YzPTpvuQvINP06b7kLzIgVGFibGVzdG9yZS5nZXRSYW5nZUJ5UHJpbWFyeUtleXNAAmIZCgpvcmlnaW5fYXBwEgthcGktZ2F0ZXdheWIpCgx0ZC50YWJsZU5hbWUSGWNoYXRfd2FpdGluZ19zZW50X21lc3NhZ2ViNAoMdGQucmVxdWVzdElkEiQwMDA1ZDU0OC1hZDZkLWNhYTQtY2RjZC03ZjBhMjNhYzkxODcagwQIDxjT9Om+5C8g0/TpvuQvMhkvU2hhcmRpbmdTcGhlcmUvcGFyc2VTUUwvQAJQPGIZCgpvcmlnaW5fYXBwEgthcGktZ2F0ZXdheWK2AwoMZGIuc3RhdGVtZW50EqUDc2VsZWN0CiAgICAgCiAgICBpZCwgZ3JvdXBfaWQsIG93bmVyX2lkLCBjbGllbnRfbXNnX2lkLCB0eXBlLCBnbXRfY3JlYXRlLCBnbXRfbW9kaWZpZWQsIGFwcF92ZXIsIHBsYXRmb3JtLCAKICAgIHZlcnNpb24sIGFwcF90eXBlLCBtZXNzYWdlX2Zvcm1hdCwgc2lnbgogICAKICAgICwKICAgICAKICAgIGJvZHksIGZlYXR1cmUKICAgCiAgICBmcm9tIGNoYXRfbWVzc2FnZV9ob3QKICAgIHdoZXJlIGdyb3VwX2lkID0gPyBhbmQgaWQgaW4gKAogICAgICAKICAgICAgPwogICAgICwgCiAgICAgID8KICAgICAsIAogICAgICA/CiAgICAgLCAKICAgICAgPwogICAgICwgCiAgICAgID8KICAgICAsIAogICAgICA/CiAgICAgLCAKICAgICAgPwogICAgICwgCiAgICAgID8KICAgICAsIAogICAgICA/CiAgICAgLCAKICAgICAgPwogICAgIAogICAgKRqpBggSEBEY0/TpvuQvINb06b7kLzIkTXlzcWwvSkRCSS9QcmVwYXJlZFN0YXRlbWVudC9leGVjdXRlOjBybS1icDE3MDB1dGF6ODNhejZxay5teXNxbC5yZHMuYWxpeXVuY3MuY29tOjMzMDZAAUgBUCFiDgoHZGIudHlwZRIDc3FsYhUKC2RiLmluc3RhbmNlEgZpbV9ob3RiugMKDGRiLnN0YXRlbWVudBKpA3NlbGVjdAogICAgIAogICAgaWQsIGdyb3VwX2lkLCBvd25lcl9pZCwgY2xpZW50X21zZ19pZCwgdHlwZSwgZ210X2NyZWF0ZSwgZ210X21vZGlmaWVkLCBhcHBfdmVyLCBwbGF0Zm9ybSwgCiAgICB2ZXJzaW9uLCBhcHBfdHlwZSwgbWVzc2FnZV9mb3JtYXQsIHNpZ24KICAgCiAgICAsCiAgICAgCiAgICBib2R5LCBmZWF0dXJlCiAgIAogICAgZnJvbSBjaGF0X21lc3NhZ2VfaG90XzI4MwogICAgd2hlcmUgZ3JvdXBfaWQgPSA/IGFuZCBpZCBpbiAoCiAgICAgIAogICAgICA/CiAgICAgLCAKICAgICAgPwogICAgICwgCiAgICAgID8KICAgICAsIAogICAgICA/CiAgICAgLCAKICAgICAgPwogICAgICwgCiAgICAgID8KICAgICAsIAogICAgICA/CiAgICAgLCAKICAgICAgPwogICAgICwgCiAgICAgID8KICAgICAsIAogICAgICA/CiAgICAgCiAgICApYtIBChFkYi5zcWwucGFyYW1ldGVycxK8AVsxMDQ5ODA0OTAyMTU5NDI0LDEwNDk4MDQ5MTI0NDkwMjQsMTA0OTgwNDkxMTEyMTI4MCwxMDQ5ODA0OTEwMjM2NjA4LDEwNDk4MDQ5MDk4NzYxNjAsMTA0OTgwNDkwOTUzMjA5NiwxMDQ5ODA0OTA5MTIyNDk2LDEwNDk4MDQ5MDg3MjkyODAsMTA0OTgwNDkwODM2ODgzMiwxMDQ5ODA0OTA2NTk5MTY4LDEwNDk4MDQ5MDUzNzEyMDBdGk4IERAQGNP06b7kLyDW9Om+5C8yGy9TaGFyZGluZ1NwaGVyZS9leGVjdXRlU1FML0ACUDxiGQoKb3JpZ2luX2FwcBILYXBpLWdhdGV3YXkaUAgQGNP06b7kLyDW9Om+5C8yHy9TaGFyZGluZ1NwaGVyZS9KREJDUm9vdEludm9rZS9AAlA8YhkKCm9yaWdpbl9hcHASC2FwaS1nYXRld2F5GoQBCBMY1vTpvuQvINn06b7kLzJAdXNlci1jZW50ZXIudXNlckluZm9SZWFkU2VydmljZTpERVYtMS4wLjA6Z2V0QmFzZVVzZXJSZXN1bHRCeUlkczoPMTI3LjAuMC4xOjMwMDAxQAFIAlBeYhkKCm9yaWdpbl9hcHASC2FwaS1nYXRld2F5GsACCBQY2fTpvuQvINn06b7kLzIWUmVkaXNzb24vQkFUQ0hfRVhFQ1VURTowci1icDFmeTBzMTY4bnN0MjR6MzEucmVkaXMucmRzLmFsaXl1bmNzLmNvbTo2Mzc5QAFIBVA4YhAKB2RiLnR5cGUSBVJlZGlzYiAKC2RiLmluc3RhbmNlEhExNzIuMTYuMzAuMzU6NjM3OWKpAQoMZGIuc3RhdGVtZW50EpgBR0VUIHNoYXJlX2ltX2NlbnRlcl91bnJlYWRfbnVtX2dyb3VwX2lkXzEwNDk4MDQ5MDIxNTk0MjRfdXNlcl9pZF8xMjc3Nzc2MTA2O0dFVCBzaGFyZV9pbV9jZW50ZXJfdW5yZWFkX251bV9ncm91cF9pZF8xMDQ5ODA0OTAyMTU5NDI0X3VzZXJfaWRfMTU5MTU2NjA1MTsa/gMIFhAVGNn06b7kLyDf9Om+5C8yFlRhYmxlc3RvcmUvQmF0Y2hHZXRSb3c6PWh0dHA6Ly9pbXNlcnZlci1pbXNlcnZlci1wcm8uY24taGFuZ3pob3UudnBjLm90cy5hbGl5dW5jcy5jb21AAUgBUF9iFQoHZGIudHlwZRIKVGFibGVzdG9yZWIbCgtkYi5pbnN0YW5jZRIMaW1zZXJ2ZXItcHJvYiYKCnRhYmxlX25hbWUSGFsiY2hhdF91c2VyX3VucmVhZF9udW0iXWImCgdjb2x1bW5zEht7ImNoYXRfdXNlcl91bnJlYWRfbnVtIjpbXX1iiAIKC3ByaW1hcnlfa2V5EvgBeyJjaGF0X3VzZXJfdW5yZWFkX251bSI6W3sicHJpbWFyeUtleSI6W3sibmFtZSI6InVzZXJfaWQiLCJ2YWx1ZSI6eyJ2YWx1ZSI6MTI3Nzc3NjEwNiwidHlwZSI6IklOVEVHRVIiLCJkYXRhU2l6ZSI6OH0sImRhdGFTaXplIjotMX0seyJuYW1lIjoiZ3JvdXBfaWQiLCJ2YWx1ZSI6eyJ2YWx1ZSI6MTA0OTgwNDkwMjE1OTQyNCwidHlwZSI6IklOVEVHRVIiLCJkYXRhU2l6ZSI6OH0sImRhdGFTaXplIjotMX1dLCJkYXRhU2l6ZSI6LTF9XX0aoQEIFRjZ9Om+5C8g3/TpvuQvMhZUYWJsZXN0b3JlLmdldEJhdGNoUm93QAJiGQoKb3JpZ2luX2FwcBILYXBpLWdhdGV3YXliJAoMdGQudGFibGVOYW1lEhRjaGF0X3VzZXJfdW5yZWFkX251bWI0Cgx0ZC5yZXF1ZXN0SWQSJDAwMDVkNTQ4LWFkNmQtZmMxNi1jNjYxLTIwMGExZjVjMWU3NRruAQgXGN/06b7kLyDf9Om+5C8yFlJlZGlzc29uL0JBVENIX0VYRUNVVEU6MHItYnAxZnkwczE2OG5zdDI0ejMxLnJlZGlzLnJkcy5hbGl5dW5jcy5jb206NjM3OUABSAVQOGIQCgdkYi50eXBlEgVSZWRpc2IgCgtkYi5pbnN0YW5jZRIRMTcyLjE2LjMwLjM1OjYzNzliWAoMZGIuc3RhdGVtZW50EkhHRVQgSU06UkVNQVJLOjE1OTE1NjYwNTFfMTI3Nzc3NjEwNjtHRVQgSU06UkVNQVJLOjE1OTE1NjYwNTFfMTU5MTU2NjA1MTsayQgQ////////////ARjC9Om+5C8g4PTpvuQvKtcBEiAzNDkyYTFlYTUzOGE0ODc1YWFlNmQ0YTE3M2I4ZjNjNxo3ZjQ1MjgzYzBiZTNlNGIzZWIxYWFlZDg2ZTIzYzUyNjQuMTk1NS4xNjQxODgzMDA3NTUyNDg2NiADKglkcnVnc3RvcmUyKmY5NzkzNmE2OTM2YzRmM2JiYWM2Mzc3NjJhNTkxMDQ1QDEwLjUuNi43MDowZHJ1Z3N0b3JlLmltTmV3UnBjU2VydmljZToxLjAuMDpxdWVyeU1lc3NhZ2VMaXN0Qg8xMjcuMC4wLjE6MzAwMDEyMnNoYXJlLWltLWNlbnRlci5tZXNzYWdlU2VydmljZTo0LjAuMDpxdWVyeU1lc3NhZ2VzSAJQXmIZCgpvcmlnaW5fYXBwEgthcGktZ2F0ZXdheWIlCgxfT1JJR0lOX1BBVEgSFWFwaS1nYXRld2F5L2RydWdzdG9yZWIqCgZfVFJBQ0USIDM0OTJhMWVhNTM4YTQ4NzVhYWU2ZDRhMTczYjhmM2M3YgcKBV9DQUxMYikKBVRva2VuEiBGMzhEQUMzMUNERUY0QUVDODk4RkI4MzJENDg2Qjc1RmIRCg9zdzgtY29ycmVsYXRpb25iDwoGX0FTWU5DEgVmYWxzZWIXCgtfVFJBQ0VfUk9PVBIIQVBJOjM0ODBiEwoFT25lSWQSCjE1OTE1NjYwNTFiQQoMQmFzZVVzZXJJbmZvEjF7IlU6dWlkOkRSVUdTVE9SRSI6MTU5MTU2NjA1MSwiUjpjciI6IkRSVUdTVE9SRSJ9YgcKBXN3OC14YgUKA3N3OGIUCgZVc2VySWQSCjE1OTE1NjYwNTFiGAoKX0xJTktfWk9ORRIKZ3JheS05OTI1N2IICgZfREVQVEhiFgoDYXBwEg9zaGFyZS1pbS1jZW50ZXJiGwoKZW50cnlfdGltZRINMTY0MTg4MzAwNzU1NGIsCgh0cmFjZV9pZBIgMzQ5MmExZWE1MzhhNDg3NWFhZTZkNGExNzNiOGYzYzdiFwoGbWV0aG9kEg1xdWVyeU1lc3NhZ2VzYg4KBHR5cGUSBnNlcnZlcmIdCgxwcm9jZXNzX3RpbWUSDTE2NDE4ODMwMDc1NTRiGwoHY2FsbF9pZBIQZTlkOGI0ZGM1Mzc5NGVlYmIUCgdzcmNfYXBwEglkcnVnc3RvcmViEgoEem9uZRIKZ3JheS05OTI1N2IvCgdzZXJ2aWNlEiRzaGFyZS1pbS1jZW50ZXIubWVzc2FnZVNlcnZpY2U6NC4wLjBiFgoKdHJhY2Vfcm9vdBIIQVBJOjM0ODBiIQoEdGltZRIZMjAyMi0wMS0xMVQxNDozNjo0NyswODowMGILCgJvaxIFZmFsc2ViFwoJdGltZXN0YW1wEgoxNjQxODgzMDA3YhcKCXNlcnZlcl9pcBIKMTAuNS4zLjE4OCIPc2hhcmUtaW0tY2VudGVyKiszOWZjNjRkNDBiMjc0MTFmODE5ODM4Y2RhMjY4ZGQ4MkAxMC41LjMuMTg4";
