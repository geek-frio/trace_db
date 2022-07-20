use chrono::prelude::*;
use chrono::{TimeZone, Utc};
use std::path::{Path, PathBuf};

pub type IndexAddr = i64;
pub const EXPIRED_DAYS: i64 = 15;

#[allow(dead_code)]
#[derive(Debug, Copy, Clone)]
pub struct MailKeyAddress {
    pub timestamp: i64,
}

pub trait ConvertIndexAddr {
    fn with_index_addr(self) -> MailKeyAddress;
}

impl ConvertIndexAddr for i64 {
    fn with_index_addr(self) -> MailKeyAddress {
        assert!(self > 0);
        assert!(self < i64::MAX);
        // let d = Utc.timestamp_millis(self);

        // let month = d.month();
        // let day = d.day();
        // let hour = d.hour();
        // let minute = d.minute() / 15;

        // let s = format!("{:0>2}{:0>2}{:0>2}{:0>2}", month, day, hour, minute);
        // tracing::trace!("addr: {}", s);
        // let val = s.parse::<i64>()?;

        MailKeyAddress { timestamp: self }
    }
}

impl ConvertIndexAddr for u64 {
    fn with_index_addr(self) -> MailKeyAddress {
        assert!(self > 0);
        assert!(self < i64::MAX as u64);
        MailKeyAddress {
            timestamp: self as i64,
        }
    }
}

impl Into<i64> for MailKeyAddress {
    fn into(self) -> i64 {
        Self::format_dir(self.timestamp)
            .unwrap()
            .parse::<i64>()
            .unwrap()
    }
}

impl MailKeyAddress {
    pub fn get_idx_path(&self, dir: &str) -> Result<PathBuf, anyhow::Error> {
        let dir_path: &Path = dir.as_ref();
        Ok(dir_path.join(Self::format_dir(self.timestamp)?))
    }

    pub fn is_expired(&self, expired_days: i64) -> bool {
        let cur = Utc::now();
        let orig = Utc.timestamp_millis(self.timestamp);

        let v = cur
            .checked_sub_signed(chrono::Duration::days(expired_days))
            .unwrap();

        orig >= v
    }

    pub fn format_dir<T: num_traits::cast::ToPrimitive>(
        timestamp: T,
    ) -> Result<String, anyhow::Error> {
        let val = timestamp.to_i64();
        match val {
            Some(val) => {
                let datetime = Utc.timestamp_millis(val);
                Ok(format!(
                    "{:0>2}{:0>2}{:0>2}{}",
                    datetime.month(),
                    datetime.day(),
                    datetime.hour(),
                    datetime.minute() / 15
                ))
            }
            None => Err(anyhow::Error::msg(
                "Timestamp converted to u64 type failed, maybe it's a invalid timestamp value",
            )),
        }
    }
}
