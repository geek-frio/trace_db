use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossbeam_channel::*;
use grpcio::*;
use serv::service::*;
use skdb::com::batch::BatchSystem;
use skdb::com::batch::FsmTypes;
use skdb::com::config::ConfigManager;
use skdb::com::config::GlobalConfig;
use skdb::com::router::Router;
use skdb::com::sched::NormalScheduler;
use skdb::tag::fsm::TagFsm;
use skdb::TOKIO_RUN;
use skproto::tracing::*;
use tokio::time::sleep;
use tracing::error;
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

pub struct MainServer {
    // shutdown_sender: OneshotSender<()>,
    global_config: Arc<GlobalConfig>,
    ip: String,
}

impl MainServer {
    fn new(global_config: Arc<GlobalConfig>, ip: String) -> MainServer {
        // let (shutdown_sender, receiver) = channel();
        MainServer {
            // shutdown_sender,
            global_config,
            ip,
        }
    }
    fn start(&mut self) {
        let (shutdown_sender, shutdown_receiver) = channel::<()>();
        let _span = info_span!("main_server");

        let (s, r) = unbounded::<FsmTypes<TagFsm>>();
        let fsm_sche = NormalScheduler { sender: s };
        let atomic = AtomicUsize::new(1);
        let router = Router::new(fsm_sche, Arc::new(atomic));

        let mut batch_system = BatchSystem::new(router.clone(), r, 1, 500);
        batch_system.spawn("Tag Poller".to_string());

        let (skytracing, local_receiver) =
            SkyTracingService::new(router.clone(), self.global_config.clone());
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
        let mut local_consumer =
            LocalSegmentMsgConsumer::new(router, self.global_config.clone(), local_receiver);
        TOKIO_RUN.spawn(
            async move {
                let r = local_consumer.loop_poll().await;
                match r {
                    Ok(_) => {
                        info!("Local segment consumer exit success!");
                    }
                    Err(e) => {
                        error!("Serious problem, local segment msg consumer exit:{:?}", e);
                        let _ = shutdown_sender.send(());
                    }
                }
            }
            .instrument(info_span!("local_consumer")),
        );
        let env = Environment::new(1);
        let service = create_sky_tracing(skytracing);
        let mut server = ServerBuilder::new(Arc::new(env))
            .bind(self.ip.as_str(), self.global_config.grpc_port as u16)
            .register_service(service)
            .build()
            .unwrap();
        server.start();
        let _ = shutdown_receiver.recv();
    }
}

fn main() {
    let _span = info_span!("main");
    let args = Args::parse();
    info!(args = ?args, "Server started begin to start...");
    let global_config = Arc::new(ConfigManager::load(args.config.into()));

    info!(global_config = ?global_config, "Server load global config");
    let mut main_server = MainServer::new(global_config, args.ip);
    main_server.start();
}
