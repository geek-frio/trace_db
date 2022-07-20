use chrono::prelude::*;
use chrono::{TimeZone, Utc};
use std::path::{Path, PathBuf};
use tokio::fs::DirEntry;

pub type IndexAddr = i64;
pub const EXPIRED_DAYS: i64 = 15;

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

    pub fn list_expired_dir_items<T: AsRef<Path>>(
        path: T,
    ) -> std::io::Result<Vec<std::fs::DirEntry>> {
        let path = path.as_ref();

        let read_dir = std::fs::read_dir(path)?;

        Ok(read_dir
            .into_iter()
            .filter(|item| item.is_ok())
            .filter(|item| {
                let entry = item.as_ref().unwrap();

                let file_name = entry.file_name();
                Self::is_expired_by_filename(file_name.to_str().unwrap(), EXPIRED_DAYS)
            })
            .map(|item| item.unwrap())
            .collect::<Vec<std::fs::DirEntry>>())
    }

    fn is_expired_by_filename(name: &str, days: i64) -> bool {
        let re = regex::Regex::new(r"\d{2}\d{2}\d{2}\d").unwrap();

        if re.is_match(name) {
            let bound_end = Utc::now();
            let bound_start = bound_end
                .checked_sub_signed(chrono::Duration::days(days))
                .unwrap();

            let bound_start_file_name = format!(
                "{:0>2}{:0>2}{:0>2}{}",
                bound_start.month(),
                bound_start.day(),
                bound_start.hour(),
                bound_start.minute() / 15
            );

            name < bound_start_file_name.as_str()
        } else {
            false
        }
    }
}
