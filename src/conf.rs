use std::env;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

const SK_DB_CONFIG_PATH: &'static str = "SK_DB_CONFIG_PATH";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub grpc_port: u32,
    pub redis_addr: String,
    pub index_dir: String,
    pub env: String,
    pub log_path: String,
    pub app_name: String,
    pub server_ip: String,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            grpc_port: 9999,
            redis_addr: "127.0.0.1:6379".to_string(),
            index_dir: "/tmp/skdb".to_string(),
            env: "local".to_string(),
            log_path: "/tmp/".to_string(),
            app_name: "skdb".to_string(),
            server_ip: "127.0.0.1".to_string(),
        }
    }
}

pub struct ConfigManager;

impl ConfigManager {
    // Enviroment config has the highest priority
    pub fn load(p: PathBuf) -> GlobalConfig {
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
