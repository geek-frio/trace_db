extern crate rand;
extern crate rocksdb;
extern crate uuid;

mod db;
mod gen;
use db::init_db;
use gen::gen_data_binary;
use rocksdb::{DBWithThreadMode, SingleThreaded};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn test_point_put_max_ops() {
    let dbs = Arc::new(Mutex::new(init_db()));

    let dbs_w = dbs.clone();
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
            println!("current qps is {}", qps);
            for i in 0..qps {
                let uuid = Uuid::new_v4().to_string();
                let data: String = gen_data_binary();

                let dbs = dbs_w.lock().unwrap();
                dbs[db_idx].put(uuid.as_bytes(), data.as_bytes()).unwrap();
                if i % 10000 == 1 {
                    tx.send(uuid).unwrap();
                }
                total_num += 1;
                db_idx %= dbs.len();
                drop(dbs);
            }
            let elapse = now.elapsed().as_millis();
            total_time += elapse;
            println!(
                "写#############到目前为止导入时速: {}",
                total_num * 1000 / total_time
            );
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
        // delay 查询, 尽量不命中缓存
        sleep(Duration::from_secs(60));
        loop {
            let query_key_r = rx.recv();
            if let Ok(query_key) = query_key_r {
                let (_, time) = key_get(query_key, dbs.clone());
                println!("读@@@@@@@@@@@@本次查询耗时:{} micros; ", time);
                total_time += time;
                num += 1;

                println!("读@@@@@@@@@@@@平均耗时:{} micro ", total_time / num);
                sleep(Duration::from_secs(1));
            } else {
                return;
            }
        }
    });

    let _ = w.join();
    let _ = r.join();
}

fn key_get(key: String, dbs: Arc<Mutex<Vec<DBWithThreadMode<SingleThreaded>>>>) -> (String, u128) {
    let dbs = dbs.lock().unwrap();
    let now = Instant::now();
    (
        String::from_utf8(dbs[0].get(key.as_bytes()).unwrap().unwrap()).unwrap(),
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
