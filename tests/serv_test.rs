mod common;

use common::*;
use skdb::{log::init_console_logger_with_level, random_mock_batch};
use std::time::Duration;

#[test]
#[ignore]
fn test_inte_basic_write() {
    let shutdown_sender = setup();

    let client = get_test_grpc_client();

    let batch = random_mock_batch(5);
    let res = client.batch_req_segments(&batch);

    assert!(res.is_ok());

    std::thread::sleep(Duration::from_secs(2));
    teardown(shutdown_sender);
}

#[test]
#[ignore]
fn test_inte_too_many_write() {
    let shutdown_sender = setup();
    let client = get_test_grpc_client();

    for i in 1..10 {
        let batch = random_mock_batch(50);
        let _res = client.batch_req_segments(&batch);
        tracing::info!("loop num: {}", i);
    }
    teardown(shutdown_sender);
}

#[test]
#[ignore]
fn test_write_only_client() {
    init_console_logger_with_level(tracing::level_filters::LevelFilter::TRACE);

    let client = get_test_grpc_client_with_addrs(vec![("192.168.121.171:9991", 1)]);

    for i in 1..100 {
        let batch = random_mock_batch(50);
        let _res = client.batch_req_segments(&batch);
        tracing::info!("loop num: {}", i);
    }

    std::thread::sleep(Duration::from_secs(10));
}
