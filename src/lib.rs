#![feature(test)]
#![feature(array_into_iter_constructors)]

extern crate test;

extern crate rand;
extern crate tantivy;
extern crate uuid;

pub mod client;
mod com;
pub mod conf;
pub mod log;
pub mod serv;
mod tag;

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
    use test::Bencher;

    #[bench]
    fn bench_nothing_slowly(_b: &mut Bencher) {}

    #[bench]
    fn bench_nothing_fast(_b: &mut Bencher) {}
}
