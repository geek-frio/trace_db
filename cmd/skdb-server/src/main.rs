use lazy_static::lazy_static;

use skdb::serv::MainServer;
use std::sync::Arc;
use tracing::info;

use clap::Parser;
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
