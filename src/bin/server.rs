use clap::Parser;
use skdb::{conf::ConfigManager, serv::MainServer, TOKIO_RUN};
use std::sync::Arc;
use tracing::info_span;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value = "127.0.0.1")]
    ip: String,

    #[clap(short, long)]
    config: String,
}

fn main() {
    let _span = info_span!("main");
    let args = Args::parse();
    println!("Server started begin to start, args:{:?}", args);

    let global_config = Arc::new(ConfigManager::load(args.config.into()));

    let (shutdown_sender, _recv) = tokio::sync::broadcast::channel(1);
    let mut main_server = MainServer::new(global_config, args.ip);

    TOKIO_RUN.block_on(async move {
        main_server.block_start(shutdown_sender).await;
    });
}
