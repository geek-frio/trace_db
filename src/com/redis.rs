use redis::{Connection, Value};

type Secs = usize;

const KEY: &'static str = "RedisTTLSet";

struct RedisTTLSet {
    ttl: Secs,
}

impl RedisTTLSet {
    fn get_all(mut conn: Connection) -> Result<Vec<String>, ()> {
        let r = redis::cmd("SMEMBERS").arg(KEY).query::<Value>(&mut conn);
        Err(())
    }

    fn expire_vals(source: Vec<String>) -> Vec<String> {
        todo!()
    }

    fn push(val: String) -> Result<(), ()> {
        todo!()
    }
}
