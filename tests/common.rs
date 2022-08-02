use skdb::{
    client::cluster::ClusterActiveWatcher,
    log::init_console_logger_with_level,
    serv::{MainServer, ShutdownEvent},
    TOKIO_RUN,
};
use skproto::tracing::SkyTracingClient;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tracing::metadata::LevelFilter;

pub fn setup() -> Sender<ShutdownEvent> {
    init_console_logger_with_level(LevelFilter::TRACE);

    let shutdown_handle = start_local_server();

    tracing::info!("Wait 3 second until grpc service is ok...");
    std::thread::sleep(std::time::Duration::from_secs(1));

    shutdown_handle
}

pub fn start_local_server() -> tokio::sync::broadcast::Sender<ShutdownEvent> {
    let global_config = Arc::new(Default::default());

    let (shutdown_sender, _recv) = tokio::sync::broadcast::channel(1);

    let mut main_server = MainServer::new(global_config, "127.0.0.1".to_string());

    let shutdown = shutdown_sender.clone();
    TOKIO_RUN.spawn(async move {
        main_server.block_start(shutdown).await;
    });

    shutdown_sender
}

pub fn get_test_grpc_client() -> SkyTracingClient {
    let addrs = vec![("127.0.0.1:9999", 1)];

    let mut client = ClusterActiveWatcher::create_grpc_conns(addrs);
    client.pop().unwrap().0
}

pub fn get_test_grpc_client_with_addrs(addrs: Vec<(&'static str, i32)>) -> SkyTracingClient {
    let mut client = ClusterActiveWatcher::create_grpc_conns(addrs);
    client.pop().unwrap().0
}

pub fn teardown(sender: Sender<ShutdownEvent>) {
    tracing::info!("Shutting down...");
    let _ = sender.send(ShutdownEvent::GracefulStop);
}
