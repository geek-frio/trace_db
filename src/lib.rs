extern crate rand;
extern crate rocksdb;
extern crate tantivy;
extern crate uuid;

#[allow(dead_code)]
pub mod com;
pub mod kv;
pub mod tag;
pub mod test;
use com::config::GlobalConfig;
use lazy_static::lazy_static;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use crate::com::config::ConfigManager;

lazy_static! {
    pub static ref TOKIO_RUN: Runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("grpc worker")
        .enable_time()
        .build()
        .unwrap();
    pub static ref GLOBAL_CONFIG: GlobalConfig = ConfigManager::load("./skdb_config.yaml");
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::gen::_gen_data_binary;

    #[test]
    fn test_xx() {
        let s = _gen_data_binary();
        println!("{}", s);
    }
}
