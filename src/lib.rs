#![feature(test)]
#![feature(array_into_iter_constructors)]
#![feature(cell_leak)]
extern crate pnet;
extern crate rand;
extern crate tantivy;
extern crate test;
extern crate uuid;

mod batch;
mod com;
mod fsm;
mod redis;
mod router;
mod sched;
mod tag;
mod tracing_log;

pub mod client;
pub mod conf;
pub mod log;
pub mod serv;

pub use com::test_util::*;

use lazy_static::lazy_static;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

lazy_static! {
    pub static ref TOKIO_RUN: Runtime = Builder::new_multi_thread()
        .worker_threads(100)
        .thread_name("grpc worker")
        .enable_time()
        .build()
        .unwrap();
}

#[cfg(test)]
mod tests {
    use test::Bencher;

    #[bench]
    fn bench_nothing_slowly(_b: &mut Bencher) {}

    #[bench]
    fn bench_nothing_fast(_b: &mut Bencher) {}
}
