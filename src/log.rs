use std::{
    path::PathBuf,
    sync::{Arc, Once},
};

use lazy_static::lazy_static;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, Registry};

use crate::{com::tracing::RollingFileMaker, conf::GlobalConfig, TOKIO_RUN};

lazy_static! {
    pub static ref INIT_LOGGER: Once = Once::new();
}

pub fn init_tracing_logger(cfg: Arc<GlobalConfig>) {
    INIT_LOGGER.call_once(|| {
        let stdout_log = tracing_subscriber::fmt::layer().pretty();
        let subscriber = Registry::default().with(stdout_log);
        const SET_GLOBAL_SUBSCRIBER_ERR: &'static str = "";
        match cfg.env.as_str() {
            "local" => {
                tracing::subscriber::set_global_default(subscriber)
                    .expect(SET_GLOBAL_SUBSCRIBER_ERR);
            }
            "pre" | "dev" | "pro" => {
                let (shut_notify_sender, _log_file_recv) = tokio::sync::mpsc::channel(256);
                let mut log_dir = PathBuf::new();
                log_dir.push(cfg.log_path.as_str());
                let (maker, _shut_downsender) = TOKIO_RUN
                    .block_on(RollingFileMaker::init(
                        cfg.app_name.clone(),
                        log_dir,
                        256 * 1024,
                        shut_notify_sender,
                    ))
                    .expect("Init rolling file failed!");
                let json_log = tracing_subscriber::fmt::layer().json().with_writer(maker);
                let subscriber = subscriber.with(json_log);
                tracing::subscriber::set_global_default(subscriber)
                    .expect(SET_GLOBAL_SUBSCRIBER_ERR)
            }
            _ => {
                panic!("Not expected enviroment config!");
            }
        }
    });
}
