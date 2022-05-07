use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossbeam_channel::*;
use grpcio::*;
use serv::service::*;
use skdb::com::batch::BatchSystem;
use skdb::com::batch::FsmTypes;
use skdb::com::config::ConfigManager;
use skdb::com::router::Router;
use skdb::com::sched::NormalScheduler;
use skdb::tag::fsm::TagFsm;
use skdb::TOKIO_RUN;
use skproto::tracing::*;
use tokio::time::sleep;
use tracing::info;
use tracing::info_span;
use tracing::trace;
use tracing::Instrument;

use crate::serv::route::LocalSegmentMsgConsumer;

mod serv;
/// Simple program to greet a person
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
    info!(args = ?args, "Server started begin to start...");
    let global_config = Arc::new(ConfigManager::load(args.config.into()));

    info!(global_config = ?global_config, "Server load global config");
    let (s, r) = unbounded::<FsmTypes<TagFsm>>();
    let fsm_sche = NormalScheduler { sender: s };
    let atomic = AtomicUsize::new(1);
    let router = Router::new(fsm_sche, Arc::new(atomic));

    let mut batch_system = BatchSystem::new(router.clone(), r, 1, 500);
    batch_system.spawn("Tag Poller".to_string());

    let (skytracing, local_receiver) =
        SkyTracingService::new(router.clone(), global_config.clone());
    let router_tick = router.clone();
    // 1.Periodically send tick event to TagPollHandler's all the mailbox
    TOKIO_RUN.spawn(
        async move {
            trace!("Sent tick event to TagPollHandler");
            let _ = router_tick.notify_all_idle_mailbox();
            sleep(Duration::from_secs(10))
        }
        .instrument(info_span!("tick_event")),
    );
    // 2.Start this local consumer to consume the tag segment msg event;
    let local_consumer =
        LocalSegmentMsgConsumer::new(router, global_config.clone(), local_receiver);
    TOKIO_RUN.spawn(async move {}.instrument(info_span!("local_consumer")));
    let env = Environment::new(1);
    let service = create_sky_tracing(skytracing);
    let mut server = ServerBuilder::new(Arc::new(env))
        .bind(args.ip, global_config.grpc_port as u16)
        .register_service(service)
        .build()
        .unwrap();
    server.start();
    use std::thread;
    thread::park();
}
