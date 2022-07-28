mod common;

use common::*;
use skdb::random_mock_batch;
use std::time::Duration;

#[test]
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
fn test_inte_too_many_write() {
    let shutdown_sender = setup();

    let client = get_test_grpc_client();

    for i in 1..50 {
        let batch = random_mock_batch(100);
        let _res = client.batch_req_segments(&batch);
        tracing::info!("loop num: {}", i);
        std::thread::sleep(Duration::from_secs(5))
    }

    teardown(shutdown_sender);
}
