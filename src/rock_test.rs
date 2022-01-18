use super::db::*;
use super::gen::*;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub fn test_point_put_max_ops(qps: u16, mut ticks: u8, delay: u8, sample: u16) {
    let dbs = Arc::new(Mutex::new(init_db()));

    let dbs_w = dbs.clone();
    let (tx, rx) = mpsc::channel();
    // writer
    let w = thread::spawn(move || {
        println!("测试ticks数目: {}", ticks);
        let mut total_time = 0;
        let mut total_num = 0;
        println!("Current qps set is {}", qps);

        loop {
            if ticks <= 0 {
                break;
            }
            let now = Instant::now();
            let mut db_idx = 0;
            for i in 0..qps {
                let uuid = Uuid::new_v4().to_string();
                let data: String = gen_data_binary();

                let dbs = dbs_w.lock().unwrap();
                dbs[db_idx].put(uuid.as_bytes(), data.as_bytes()).unwrap();
                if i % sample == 1 {
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
        sleep(Duration::from_secs(delay as u64));
        loop {
            let query_key_r = rx.recv_timeout(Duration::from_secs(10));
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
