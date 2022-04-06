use anyhow::Error as AnyError;
use chrono::Local;
use redis::{Connection, Value};

type Secs = i64;

const KEY: &'static str = "RedisTTLSet";

struct RedisTTLSet {
    ttl: Secs,
}

struct TimeRecord {
    data_timestamp: i64,
    val: String,
}

impl TimeRecord {
    fn is_expired(&self, ttl: Secs) -> bool {
        let local = Local::now().timestamp();
        if local - self.data_timestamp >= ttl {
            return true;
        }
        false
    }

    fn key(&self) -> String {
        format!("{}:{}", self.val, self.data_timestamp)
    }

    pub fn format_value(s: String) -> Result<TimeRecord, AnyError> {
        let v: Vec<&str> = s.split(":").collect();
        if v.len() != 2 {
            return Err(AnyError::msg("Not correct format".to_string()));
        }
        Ok(TimeRecord {
            data_timestamp: v.get(1).unwrap().parse::<i64>()?,
            val: v.get(0).unwrap().to_string(),
        })
    }
}

impl TryFrom<String> for TimeRecord {
    type Error = AnyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::format_value(value)
    }
}

impl RedisTTLSet {
    fn get_all(&self, mut conn: Connection) -> Result<Vec<TimeRecord>, AnyError> {
        let r = redis::cmd("SMEMBERS").arg(KEY).query::<Value>(&mut conn)?;
        match r {
            Value::Bulk(vals) => {
                let r = vals
                    .into_iter()
                    .filter(|v| {
                        if let Value::Data(_) = v {
                            return true;
                        }
                        false
                    })
                    .map(|val| match val {
                        Value::Data(data) => {
                            std::str::from_utf8(data.as_ref()).unwrap().to_string()
                        }
                        _ => unreachable!("Will never come here"),
                    })
                    .map(|val| val.try_into())
                    .filter(|r| r.is_ok())
                    .map(|r| r.unwrap())
                    .collect::<Vec<TimeRecord>>();
                return Ok(r);
            }
            _ => {
                return Err(AnyError::msg("Not expected redis ttl set data"));
            }
        }
    }

    fn expire_els(
        &self,
        mut conn: Connection,
        mut records: Vec<TimeRecord>,
    ) -> Result<(), AnyError> {
        records
            .iter()
            .try_for_each(|record| -> Result<(), AnyError> {
                if record.is_expired(self.ttl) {
                    redis::cmd("SPOP")
                        .arg(record.key())
                        .query::<Value>(&mut conn)?;
                }
                Ok(())
            })?;
        records.retain(|r| !r.is_expired(self.ttl));
        Ok(())
    }

    fn push<T>(&self, mut conn: Connection, val: T) -> Result<(), AnyError>
    where
        T: TryInto<TimeRecord>,
    {
        let res = val.try_into();
        match res {
            Ok(record) => {
                if record.is_expired(self.ttl) {
                    return Err(AnyError::msg("Value has already expired!"));
                }
                let exec_res = redis::cmd("SADD")
                    .arg(record.key())
                    .query::<Value>(&mut conn)?;
                match exec_res {
                    Value::Int(_) => return Ok(()),
                    _ => return Err(AnyError::msg("Add failed!")),
                }
            }
            Err(_) => Err(AnyError::msg(
                "Not correct type to convert to TimeRecord!".to_string(),
            )),
        }
    }
}

mod tests {
    use super::*;
    #[test]
    fn test_get_all() -> Result<(), AnyError> {
        let client = redis::Client::open("redis://127.0.0.1:6379/")?;
        let con = client.get_connection()?;
        let redis = RedisTTLSet { ttl: 5 };
        let result = redis.get_all(con);
        // println!("result is:{:?}", result);
        Ok(())
    }
}
