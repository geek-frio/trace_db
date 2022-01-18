use rocksdb::{Options, DB};

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
