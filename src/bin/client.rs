use std::time::Instant;

use clap::Parser;
use skdb::{
    client::cluster::ClusterActiveWatcher, log::init_console_logger_with_level, random_mock_batch,
};
use skproto::tracing::BatchSegmentData;
use tracing::metadata::LevelFilter;

fn setup() {
    init_console_logger_with_level(LevelFilter::INFO);
}

#[derive(Parser, Debug)]
/// Skdb client 压力测试程序
struct TestConfig {
    /// qps配置
    #[clap(name = "qps", short = 'q', default_value_t = 5000)]
    qps: usize,

    /// 测试时间长度，单位秒
    #[clap(name = "time", short = 't', default_value_t = 10)]
    time: u64,
}

fn main() -> Result<(), anyhow::Error> {
    setup();

    let config = TestConfig::parse();

    let local_ip = local_ip_address::local_ip()
        .expect("Get local ip address")
        .to_string();
    let addr = format!("{}:9999", local_ip);

    let mut addrs = Vec::new();
    addrs.push(addr);

    let batch_size = 30;
    let concurrent_num = 20;

    let (send, recv) = crossbeam_channel::unbounded::<BatchSegmentData>();

    let mut counter = 0usize;
    let mut now = std::time::Instant::now();

    let task_exec_time = config.time;

    // Task Spawner
    for _ in 0..3 {
        let temp_send = send.clone();
        std::thread::spawn(move || {
            let start_time = Instant::now();
            loop {
                if start_time.elapsed().as_secs() > task_exec_time {
                    tracing::info!("所有待发送数据已经发送完毕。");
                    break;
                }

                let loop_size = config.qps / batch_size / 10 / 3;

                for _ in 0..loop_size {
                    let batch = random_mock_batch(batch_size);

                    temp_send.send(batch).unwrap();

                    counter += batch_size;
                }

                if now.elapsed() > std::time::Duration::from_secs(1) {
                    tracing::info!("已经发送 {} 条数据", counter);

                    now = std::time::Instant::now();
                    counter = 0;
                }

                std::thread::sleep(std::time::Duration::from_millis(100))
            }
        });
    }
    let (notify, wait) = std::sync::mpsc::channel::<()>();

    let moni_recv = recv.clone();

    // Task Monitor
    std::thread::spawn(move || {
        let mut now = Instant::now();

        loop {
            if now.elapsed().as_secs() > 1 {
                let remain_size = moni_recv.len();

                tracing::info!("剩余堆积大小:{}", remain_size);

                now = Instant::now();
            }
        }
    });

    // Concurrrent exec task
    for _ in 0..concurrent_num {
        let temp_recv = recv.clone();
        let temp_addrs = addrs.clone();

        let temp_addrs = temp_addrs.iter().map(|a| (a.as_str(), 1)).collect();

        let (client, _id) = ClusterActiveWatcher::create_grpc_conns(temp_addrs)
            .pop()
            .unwrap();

        let temp_notify = notify.clone();
        std::thread::spawn(move || {
            let mut counter = 0usize;

            let loop_start_time = Instant::now();
            let mut last = Instant::now();

            loop {
                // Every new loop we give the current thread qps
                if last.elapsed().as_secs() > 1 {
                    tracing::info!(
                        "Current thread call qps is:{}",
                        counter as u64 / loop_start_time.elapsed().as_secs()
                    );

                    last = Instant::now();
                }

                let batch_req = temp_recv.recv();

                if batch_req.is_err() {
                    break;
                }

                let batch_req = batch_req.unwrap();

                let batch_size = batch_req.datas.len();

                client.batch_req_segments(&batch_req).unwrap();
                counter += batch_size;
            }

            drop(temp_notify);
        });
    }

    drop(notify);
    let _ = wait.recv();

    Ok(())
}
