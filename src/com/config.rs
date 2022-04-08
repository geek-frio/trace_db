use std::env;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

const SK_DB_CONFIG_PATH: &'static str = "SK_DB_CONFIG_PATH";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub(crate) grpc_port: u32,
    pub(crate) redis_addr: String,
}

pub(crate) struct ConfigManager;

impl ConfigManager {
    // Enviroment config has the highest priority
    pub(crate) fn load(p: PathBuf) -> GlobalConfig {
        let s = env::var(SK_DB_CONFIG_PATH)
            .map(|a| {
                let a: PathBuf = a.into();
                a
            })
            .unwrap_or(p);
        let r = fs::read(s).unwrap();
        serde_yaml::from_reader::<_, GlobalConfig>(r.as_slice()).expect("Load config file failed!")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() {
        let p = <PathBuf as From<String>>::from("/tmp/skdb_test.yaml".to_string());
        let global: GlobalConfig = ConfigManager::load(p);
        println!("global:{:?}", global);
    }
}
