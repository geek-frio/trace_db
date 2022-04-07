use std::env;
use std::fs;
use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};

const SK_DB_CONFIG_PATH: &'static str = "SK_DB_CONFIG_PATH";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalConfig {
    grpc_port: u32,
}

pub(crate) struct ConfigManager;

impl ConfigManager {
    // Enviroment config has the highest priority
    pub(crate) fn load<T>(p: T) -> GlobalConfig
    where
        T: AsRef<PathBuf>,
    {
        let s = env::var(SK_DB_CONFIG_PATH)
            .map(|a| {
                let a: PathBuf = a.into();
                a
            })
            .unwrap_or(p.as_ref().clone());
        let r = fs::read(s).unwrap();
        serde_yaml::from_reader::<_, GlobalConfig>(r.as_slice()).expect("Load config file failed!")
    }
}
