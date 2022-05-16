use std::path::{Path, PathBuf};

use anyhow::Error as AnyError;
use chrono::{prelude::*, Duration};
use chrono::{TimeZone, Utc};

pub type IndexAddr = i64;

#[derive(Debug, Copy, Clone)]
pub struct MailKeyAddress {
    val: i64,
    day: u32,
    hour: u32,
    minute: u32,
    timestamp: i64,
}

pub trait ConvertIndexAddr {
    fn with_index_addr(self) -> Result<MailKeyAddress, AnyError>;
}

impl ConvertIndexAddr for i64 {
    fn with_index_addr(self) -> Result<MailKeyAddress, AnyError> {
        assert!(self > 0);
        assert!(self < i64::MAX);
        let now = Utc::now();
        let d = Utc.timestamp_millis(self);

        // We only need biz data 30 days ago
        if now - Duration::days(30) > d {
            return Err(AnyError::msg("Data biztime has exceeded 30 days"));
        }

        let day = d.day();
        let hour = d.hour();
        let minute = d.minute() / 15;

        // Only reserve last 30 days segment data
        let s = format!("{}{:0>2}{:0>2}", day, hour, minute);
        let val = s.parse::<i64>()?;

        Ok(MailKeyAddress {
            val,
            day,
            hour,
            minute,
            timestamp: self,
        })
    }
}

impl ConvertIndexAddr for u64 {
    fn with_index_addr(self) -> Result<MailKeyAddress, AnyError> {
        assert!(self > 0);
        assert!(self < i64::MAX as u64);
        let now = Utc::now();
        let d = Utc.timestamp_millis(self as i64);

        // We only need biz data 30 days ago
        if now - Duration::days(30) > d {
            return Err(AnyError::msg("Data biztime has exceeded 30 days"));
        }

        let day = d.day();
        let hour = d.hour();
        let minute = d.minute() / 15;

        // Only reserve last 30 days segment data
        let s = format!("{}{:0>2}{:0>2}", day, hour, minute);
        let val = s.parse::<i64>()?;

        Ok(MailKeyAddress {
            val,
            day,
            hour,
            minute,
            timestamp: self as i64,
        })
    }
}

impl Into<i64> for MailKeyAddress {
    fn into(self) -> i64 {
        self.val
    }
}

impl MailKeyAddress {
    pub fn get_idx_path(&self, dir: &str) -> PathBuf {
        let dir_path: &Path = dir.as_ref();
        dir_path.join(<String as AsRef<Path>>::as_ref(&self.val.to_string()))
    }

    //TODO
    pub fn check_expire(&self) -> bool {
        true
    }
}
