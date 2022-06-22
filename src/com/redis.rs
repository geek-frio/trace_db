use anyhow::Error as AnyError;
use chrono::Local;
use redis::Client as RedisClient;
use redis::{Connection, Value};
use regex::Regex;

type Secs = i64;

const KEY: &'static str = "SK_DB_SERVER_ADDR";
pub const LEASE_TIME_OUT: i64 = 15;

pub struct RedisAddr {
    addr: String,
    client: Option<RedisClient>,
}

impl RedisAddr {
    pub fn client(&mut self) -> Result<RedisClient, AnyError> {
        match self.client {
            Some(ref client) => Ok(client.clone()),
            None => {
                let client = redis::Client::open(self.addr.as_str())?;
                Ok(client)
            }
        }
    }
}

impl<'a> TryInto<RedisAddr> for &'a str {
    type Error = String;

    fn try_into(self) -> Result<RedisAddr, Self::Error> {
        let re = Regex::new(r"\d+\.\d+\.\d+\.\d+:\d+").unwrap();
        if re.is_match(self) {
            let mut s = String::new();
            s.push_str("redis://");
            s.push_str(self);
            return Ok(RedisAddr {
                addr: s,
                client: None,
            });
        }
        Err("Invalid redis address".to_string())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RedisTTLSet {
    pub(crate) ttl: Secs,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) struct MetaInfo {
    expire_time: i64,
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub(crate) struct Record {
    pub(crate) meta: MetaInfo,
    pub(crate) sub_key: String,
}

impl MetaInfo {
    fn is_expired(&self, ttl: Secs) -> bool {
        let local = Local::now().timestamp();
        local - self.expire_time >= ttl
    }
}

impl ToString for MetaInfo {
    fn to_string(&self) -> String {
        self.expire_time.to_string()
    }
}

impl<'a> TryFrom<&'a Value> for MetaInfo {
    type Error = AnyError;

    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        match value {
            Value::Data(data) => {
                let s = std::str::from_utf8(data.as_ref())?;
                Ok(MetaInfo {
                    expire_time: s.parse::<i64>()?,
                })
            }
            _ => Err(AnyError::msg("Convert to MetaInfo failed!")),
        }
    }
}

struct WrapStr(String);

impl<'a> TryFrom<&'a Value> for WrapStr {
    type Error = AnyError;

    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        match value {
            Value::Data(data) => {
                let s = std::str::from_utf8(data.as_ref())?;
                Ok(WrapStr(s.to_string()))
            }
            _ => Err(AnyError::msg("Convert to MetaInfo failed!")),
        }
    }
}

impl Default for RedisTTLSet {
    fn default() -> Self {
        Self { ttl: 5 }
    }
}

impl RedisTTLSet {
    pub(crate) fn new(ttl: Secs) -> RedisTTLSet {
        RedisTTLSet { ttl }
    }

    pub(crate) fn query_all(&self, conn: &mut Connection) -> Result<Vec<Record>, AnyError> {
        let r = redis::cmd("HGETALL").arg(KEY).query::<Value>(conn)?;
        match r {
            Value::Bulk(v) => {
                let mut records = Vec::new();
                for i in 0..v.len() {
                    if i % 2 != 0 {
                        continue;
                    }
                    let field = v
                        .get(i)
                        .ok_or(AnyError::msg("RedisTTL: Convert to string key failed"))?;
                    let wstr = <WrapStr as TryFrom<&'_ Value>>::try_from(field)?;
                    let meta_info = <MetaInfo as TryFrom<&'_ Value>>::try_from(
                        v.get(i + 1)
                            .ok_or(AnyError::msg("RedisTTL: Convert to meta info failed!"))?,
                    )?;
                    // Expired records will be removed automatically
                    if meta_info.is_expired(self.ttl) {
                        let _ = redis::cmd("HDEL")
                            .arg(KEY)
                            .arg(wstr.0.clone())
                            .query::<Value>(conn)?;
                        continue;
                    }
                    let record = Record {
                        meta: meta_info,
                        sub_key: wstr.0,
                    };
                    records.push(record);
                }
                return Ok(records);
            }
            _ => {
                return Err(AnyError::msg("RedisTTL: Get all hset data failed!"));
            }
        }
    }

    pub(crate) fn push<T>(&self, conn: &mut Connection, val: T) -> Result<(), AnyError>
    where
        T: TryInto<Record>,
        <T as TryInto<Record>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let record = <T as TryInto<Record>>::try_into(val)?;
        if record.meta.is_expired(self.ttl) {
            return Err(AnyError::msg("Value has already expired!"));
        }
        let _ = redis::cmd("HSET")
            .arg(KEY)
            .arg(record.sub_key)
            .arg(record.meta.to_string())
            .query::<Value>(conn)?;
        return Ok(());
    }
}

impl<T> From<T> for Record
where
    T: AsRef<str>,
{
    fn from(s: T) -> Self {
        let local = Local::now().timestamp();
        let target_timestamp = local + LEASE_TIME_OUT;

        Record {
            meta: MetaInfo {
                expire_time: target_timestamp,
            },
            sub_key: s.as_ref().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test::gen;
    use rand::Rng;

    use super::*;
    #[test]
    fn test_get_all() -> Result<(), AnyError> {
        let client = redis::Client::open("redis://127.0.0.1:6379")?;
        let mut con = client.get_connection()?;
        let redis = RedisTTLSet { ttl: 5 };
        println!("result: {:?}", redis.query_all(&mut con));
        Ok(())
    }

    #[test]
    fn test_push() -> Result<(), AnyError> {
        let client = redis::Client::open("redis://127.0.0.1:6379")?;
        let mut conn = client.get_connection()?;

        let redis = RedisTTLSet { ttl: 5 };
        println!(
            "Push result is :{:?}",
            redis.push(&mut conn, rand_gen_record())
        );
        println!(
            "Push result is :{:?}",
            redis.push(&mut conn, rand_gen_record())
        );
        println!(
            "Push result is :{:?}",
            redis.push(&mut conn, rand_gen_record())
        );
        println!(
            "Push result is :{:?}",
            redis.push(&mut conn, rand_gen_record())
        );
        println!("After push result is:{:?}", redis);
        println!("Query all result is:{:?}", redis.query_all(&mut conn));
        Ok(())
    }

    fn rand_gen_record() -> Record {
        let local = Local::now();
        let mut rand = rand::thread_rng();
        let i = rand.gen_range(1..30);
        Record {
            meta: MetaInfo {
                expire_time: local.timestamp() + (i as i64),
            },
            sub_key: gen::_gen_tag(3, 5, 'a'),
        }
    }

    #[test]
    fn test_into_redis_addr() {
        let s = "127.0.0.1:6379";
        let _: RedisAddr = s.try_into().unwrap();
    }
}
