mod common;

use common::*;
use skdb::random_mock_batch;
use std::time::Duration;

#[test]
fn test_inte_basic_write() {
    let shutdown_sender = setup();

    let client = get_test_grpc_client();

    let batch = random_mock_batch(5);
    let _ = client.batch_req_segments(&batch);

    // assert!(res.is_ok());
    std::thread::sleep(Duration::from_secs(100));
    teardown(shutdown_sender);
}
