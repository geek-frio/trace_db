use std::{path::PathBuf, sync::Arc};

use tokio::sync::OnceCell;
use tracing::metadata::LevelFilter;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, Layer, Registry};

use crate::{
    conf::GlobalConfig,
    serv::ShutdownSignal,
    tracing_log::{MsgEvent, RollingFileMaker},
    TOKIO_RUN,
};

static INIT_LOGGER_ONCE: OnceCell<()> = tokio::sync::OnceCell::const_new();

pub fn init_console_logger() {
    TOKIO_RUN.block_on(async {
        INIT_LOGGER_ONCE
            .get_or_init(|| async {
                let stdout_log = tracing_subscriber::fmt::layer().pretty();
                let subscriber =
                    Registry::default().with(stdout_log.with_filter(LevelFilter::TRACE));

                tracing::subscriber::set_global_default(subscriber)
                    .expect("Console log init failed!");
            })
            .await;
    })
}

pub fn init_console_logger_with_level(level: LevelFilter) {
    TOKIO_RUN.block_on(async {
        INIT_LOGGER_ONCE
            .get_or_init(|| async {
                let stdout_log = tracing_subscriber::fmt::layer().pretty();
                let subscriber = Registry::default().with(stdout_log.with_filter(level));

                tracing::subscriber::set_global_default(subscriber)
                    .expect("Console log init failed!");
            })
            .await;
    });
}

pub async fn init_tracing_logger(cfg: Arc<GlobalConfig>, mut signal: ShutdownSignal) {
    INIT_LOGGER_ONCE
        .get_or_init(|| async {
            let stdout_log = tracing_subscriber::fmt::layer().pretty();
            let subscriber = Registry::default().with(stdout_log);

            const SET_GLOBAL_SUBSCRIBER_ERR: &'static str = "local log init failed!";

            match cfg.env.as_str() {
                "local" => {
                    tracing::subscriber::set_global_default(subscriber)
                        .expect(SET_GLOBAL_SUBSCRIBER_ERR);
                }
                "pre" | "dev" | "pro" => {
                    let (shut_notify_sender, _log_file_recv) = tokio::sync::mpsc::channel(256);

                    let mut log_dir = PathBuf::new();
                    log_dir.push(cfg.log_path.as_str());

                    let (maker, shutdown_sender) = RollingFileMaker::init(
                        cfg.app_name.clone(),
                        log_dir,
                        256 * 1024,
                        shut_notify_sender,
                    )
                    .await
                    .expect("Init rolling file failed!");

                    TOKIO_RUN.spawn(async move {
                        let _ = signal.recv.recv().await;
                        let _ = shutdown_sender.send(MsgEvent::Shutdown).await;

                        tracing::info!("ShutdownSignal is dropped for logger");
                    });

                    let json_log = tracing_subscriber::fmt::layer().json().with_writer(maker);
                    let subscriber = subscriber.with(json_log);

                    tracing::subscriber::set_global_default(subscriber)
                        .expect(SET_GLOBAL_SUBSCRIBER_ERR)
                }
                _ => {
                    panic!("Not expected enviroment config!");
                }
            }
        })
        .await;
}
