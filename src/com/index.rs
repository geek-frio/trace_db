use std::path::{Path, PathBuf};

use anyhow::Error as AnyError;
use chrono::{prelude::*, Duration};
use chrono::{TimeZone, Utc};

pub type IndexAddr = i64;

pub struct IndexPath;

impl IndexPath {
    pub fn compute_index_addr(biz_timestamp: i64) -> Result<i64, AnyError> {
        let now = Utc::now();
        let d = Utc.timestamp_millis(biz_timestamp);

        // We only need biz data 30 days ago
        if now - Duration::days(30) > d {
            return Err(AnyError::msg("Data biztime has exceeded 30 days"));
        }

        let day = d.day();
        let hour = d.hour();
        let minute = d.minute() / 15;

        // Only reserve last 30 days segment data
        let s = format!("{}{:0>2}{:0>2}", day, hour, minute);
        Ok(s.parse::<i64>()?)
    }

    pub fn gen_idx_path(addr: IndexAddr, dir: String) -> PathBuf {
        let dir_path: &Path = dir.as_ref();
        dir_path.join(<String as AsRef<Path>>::as_ref(&addr.to_string()))
    }
}
