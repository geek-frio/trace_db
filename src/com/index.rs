use chrono::prelude::*;
use chrono::{TimeZone, Utc};
use std::path::{Path, PathBuf};

pub type IndexAddr = i64;
pub const EXPIRED_DAYS: i64 = 30;

#[derive(Debug, Copy, Clone, PartialEq, Hash, Eq)]
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
    pub fn get_idx_path<T: AsRef<Path>>(&self, dir: T) -> Result<PathBuf, anyhow::Error> {
        let dir_path: &Path = dir.as_ref();
        Ok(dir_path.join(Self::format_dir(self.timestamp)?))
    }

    pub fn is_expired(&self, expired_days: i64) -> bool {
        let cur = Utc::now();
        let orig = Utc.timestamp_millis(self.timestamp);

        let v = cur
            .checked_sub_signed(chrono::Duration::days(expired_days))
            .unwrap();

        orig < v
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

    pub fn convert_to_index_addr_key(self) -> IndexAddr {
        let val = self.timestamp as i64;

        let datetime = Utc.timestamp_millis(val);
        let s = format!(
            "{:0>2}{:0>2}{:0>2}{}",
            datetime.month(),
            datetime.day(),
            datetime.hour(),
            datetime.minute() / 15
        );

        s.parse::<IndexAddr>().unwrap()
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

            tracing::info!("left: {}, right:{}", name, bound_start_file_name);
            name < bound_start_file_name.as_str()
        } else {
            false
        }
    }

    pub(crate) fn get_not_expired_bound_start() -> String {
        let now = Utc::now();

        let bound_start = now
            .checked_sub_signed(chrono::Duration::days(EXPIRED_DAYS))
            .unwrap();

        Self::format_dir(bound_start.timestamp_millis()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;

    use crate::log::init_console_logger;

    use super::MailKeyAddress;

    static DIR_PATH: AtomicI32 = AtomicI32::new(0);

    fn setup() {
        init_console_logger()
    }

    fn create_expired_dir_name(expired_days: i64) -> String {
        let datetime = chrono::Utc::now();
        let datetime = datetime
            .checked_sub_signed(chrono::Duration::days(expired_days))
            .unwrap();

        tracing::trace!("Created expired datetime:{}", datetime);
        MailKeyAddress::format_dir(datetime.timestamp_millis()).unwrap()
    }

    fn teardown<T: AsRef<std::path::Path>>(root_dir: T) {
        let r = std::fs::remove_dir_all(root_dir.as_ref());
        if r.is_ok() {
            tracing::info!("Remove all temp directory success!");
        }
    }

    #[test]
    fn test_list_expired_dir_items() {
        setup();

        let temp = std::env::temp_dir();
        let temp = temp.join("abc");

        let root_dir_name = format!(
            "{}",
            DIR_PATH.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );

        let root_dir = temp.join(root_dir_name);
        tracing::info!("root dir is:{:?}", root_dir);

        if !root_dir.exists() {
            tracing::info!("Waiting to create dir is:{:?}", root_dir);
            std::fs::create_dir_all(&root_dir).unwrap();
        }

        let expired_dir1 = create_expired_dir_name(31);
        let _ = std::fs::create_dir(root_dir.join(expired_dir1));

        let expired_dir2 = create_expired_dir_name(32);
        let _ = std::fs::create_dir(root_dir.join(expired_dir2));

        let not_expired_dir3 = create_expired_dir_name(1);
        let _ = std::fs::create_dir(root_dir.join(not_expired_dir3));

        let _ = std::fs::create_dir(root_dir.join("12345"));

        let expired_items = MailKeyAddress::list_expired_dir_items(&root_dir).unwrap();
        assert_eq!(expired_items.len(), 2);

        teardown(&root_dir);
    }
}
