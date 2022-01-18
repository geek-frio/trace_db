use rocksdb::{DBWithThreadMode, SingleThreaded};
use rocksdb::{Options, DB};
use std::sync::{Arc, Mutex};
use std::time::Instant;
pub fn init_db() -> Vec<DB> {
    let mut dbs = Vec::new();
    let mut opts = Options::default();
    opts.set_disable_auto_compactions(true);
    opts.create_if_missing(true);

    for i in 0..1 {
        let path = format!("/home/frio/rocksdb{}", i);
        dbs.push(DB::open(&opts, &path).unwrap());
    }
    dbs
}

pub fn key_get(
    key: String,
    dbs: Arc<Mutex<Vec<DBWithThreadMode<SingleThreaded>>>>,
) -> (String, u128) {
    let dbs = dbs.lock().unwrap();
    let now = Instant::now();
    (
        String::from_utf8(dbs[0].get(key.as_bytes()).unwrap().unwrap()).unwrap(),
        now.elapsed().as_micros(),
    )
}
