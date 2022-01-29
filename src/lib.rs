extern crate rand;
extern crate rocksdb;
extern crate tantivy;
extern crate uuid;

pub mod com;
pub mod kv;
pub mod tag;
mod test;

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
