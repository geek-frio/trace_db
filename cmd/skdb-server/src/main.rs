use lazy_static::lazy_static;

use skdb::com::tracing::RollingFileMaker;
use skdb::serv::MainServer;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Once;
use tracing::info;
use tracing_subscriber::{prelude::*, Registry};

use clap::Parser;
use skdb::com::config::ConfigManager;
use skdb::com::config::GlobalConfig;
use skdb::TOKIO_RUN;
use tracing::info_span;
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value = "127.0.0.1")]
    ip: String,

    #[clap(short, long)]
    config: String,
}

enum ShutdownEvent {
    Err(anyhow::Error),
    Normal,
}

lazy_static! {
    pub static ref INIT_LOGGER: Once = Once::new();
}

fn init_tracing_logger(cfg: Arc<GlobalConfig>) {
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

fn main() {
    let _span = info_span!("main");
    let args = Args::parse();
    println!("Server started begin to start, args:{:?}", args);
    let global_config = Arc::new(ConfigManager::load(args.config.into()));
    init_tracing_logger(global_config.clone());
    info!(global_config = ?global_config, "Server load global config");
    let mut main_server = MainServer::new(global_config, args.ip);
    main_server.start();
}
