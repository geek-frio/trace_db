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
struct TestConfig {
    /// qps配置
    #[clap(name = "qps", short = 'q', default_value_t = 5000)]
    qps: usize,

    /// 测试时间长度，单位秒
    #[clap(name = "time", short = 't', default_value_t = 10)]
    time: i64,
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

    let batch_size = 20;
    let concurrent_num = 10;

    let (send, recv) = crossbeam_channel::unbounded::<BatchSegmentData>();

    let mut counter = 0usize;
    let mut now = std::time::Instant::now();

    std::thread::spawn(move || loop {
        let loop_size = config.qps / batch_size / 10;

        for _ in 0..loop_size {
            let batch = random_mock_batch(batch_size);
            send.send(batch).unwrap();
            counter += batch_size;
        }

        if now.elapsed() > std::time::Duration::from_secs(1) {
            tracing::info!("Has sent {} num data", counter);

            now = std::time::Instant::now();
            counter = 0;
        }

        std::thread::sleep(std::time::Duration::from_millis(100))
    });

    let (notify, wait) = std::sync::mpsc::channel::<()>();

    for _ in 0..concurrent_num {
        let temp_recv = recv.clone();
        let temp_addrs = addrs.clone();

        let temp_addrs = temp_addrs.iter().map(|a| (a.as_str(), 1)).collect();
        let (client, _id) = ClusterActiveWatcher::create_grpc_conns(temp_addrs)
            .pop()
            .unwrap();

        let temp_notify = notify.clone();
        std::thread::spawn(move || {
            let batch_req = temp_recv.recv().unwrap();
            client.batch_req_segments(&batch_req).unwrap();
            drop(temp_notify);
        });
    }

    drop(notify);
    let _ = wait.recv();

    Ok(())
}
