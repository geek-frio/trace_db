mod common;

use common::*;

#[test]
fn test_basic() {
    setup();
    println!("this is a first integration test..");
    teardown();
}
