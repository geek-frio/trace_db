extern crate rand;
extern crate rocksdb;
extern crate tantivy;
extern crate uuid;

#[allow(dead_code)]
pub mod com;
pub mod kv;
pub mod tag;
mod test;
use lazy_static::lazy_static;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

lazy_static! {
    pub static ref TOKIO_RUN: Runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("grpc worker")
        .enable_time()
        .build()
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::gen::gen_data_binary;

    #[test]
    fn test_xx() {
        let s = gen_data_binary();
        println!("{}", s);
    }
}
