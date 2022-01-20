extern crate rand;
extern crate rocksdb;
extern crate uuid;

mod db;
mod gen;
mod rock_test;
mod tag;
use clap::Parser;
use rock_test::*;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value_t = 30000)]
    qps: u16,

    #[clap(short, long, default_value_t = 10)]
    ticks: u32,

    #[clap(short, long, default_value_t = -1)]
    delay: i32,

    #[clap(short, long, default_value_t = 1000)]
    sample: u16,

    #[clap(short, long)]
    path: String,
}

fn main() {
    // let args = Args::parse();
    // test_point_put_max_ops(args.path, args.qps, args.ticks, args.delay, args.sample);
    tag::test_search();
}

#[cfg(test)]
mod tests {
    use super::*;
    use gen::gen_data_binary;

    #[test]
    fn test_xx() {
        let s = gen_data_binary();
        println!("{}", s);
    }
}
