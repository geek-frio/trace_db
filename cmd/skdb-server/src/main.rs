use lazy_static::lazy_static;
use std::fs::File;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tracing_subscriber::{prelude::*, Registry};

use clap::Parser;
use crossbeam_channel::Receiver as ShutdownReceiver;
use crossbeam_channel::Sender as ShutdownSender;
use futures::channel::mpsc::{Receiver, Sender};
use grpcio::*;
use serv::service::*;
use skdb::com::batch::BatchSystem;
use skdb::com::batch::FsmTypes;
use skdb::com::config::ConfigManager;
use skdb::com::config::GlobalConfig;
use skdb::com::router::Router;
use skdb::com::sched::NormalScheduler;
use skdb::tag::fsm::SegmentDataCallback;
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

enum ShutdownEvent {
    Err(anyhow::Error),
    Normal,
}

lazy_static! {
    pub static ref INIT_LOGGER: Once = Once::new();
}

pub struct MainServer {
    // shutdown_sender: OneshotSender<()>,
    global_config: Arc<GlobalConfig>,
    ip: String,
    shutdown_sender: ShutdownSender<ShutdownEvent>,
    shutdown_receiver: ShutdownReceiver<ShutdownEvent>,
}

impl MainServer {
    fn new(global_config: Arc<GlobalConfig>, ip: String) -> MainServer {
        let (shutdown_sender, shutdown_receiver) = crossbeam_channel::bounded(256);
        let shutdown_sender_clone = shutdown_sender.clone();
        ctrlc::set_handler(move || {
            let _ = shutdown_sender.send(ShutdownEvent::Normal);
        })
        .expect("Error setting ctrl+c handler");
        MainServer {
            // shutdown_sender,
            global_config,
            ip,
            shutdown_sender: shutdown_sender_clone,
            shutdown_receiver,
        }
    }
    fn start(&mut self) {
        let _span = info_span!("main_server");

        let (segment_sender, segment_receiver) = futures::channel::mpsc::channel(10000);
        let router = self.start_batch_system_for_segment();
        self.start_bridge_channel(segment_receiver, router, self.shutdown_sender.clone());
        self.start_grpc(segment_sender);
        let _ = self.shutdown_receiver.recv();
        info!("");
    }

    fn start_grpc(&mut self, sender: Sender<SegmentDataCallback>) {
        let skytracing = SkyTracingService::new(self.global_config.clone(), sender);
        let service = create_sky_tracing(skytracing);
        let env = Environment::new(1);
        let mut server = ServerBuilder::new(Arc::new(env))
            .bind(self.ip.as_str(), self.global_config.grpc_port as u16)
            .register_service(service)
            .build()
            .unwrap();
        server.start();
    }

    fn start_batch_system_for_segment(&self) -> Router<TagFsm, NormalScheduler<TagFsm>> {
        let (s, r) = crossbeam_channel::unbounded::<FsmTypes<TagFsm>>();
        let fsm_sche = NormalScheduler { sender: s };
        let atomic = AtomicUsize::new(1);
        let router = Router::new(fsm_sche, Arc::new(atomic));

        let mut batch_system = BatchSystem::new(router.clone(), r, 1, 500);
        batch_system.spawn("Tag Poller".to_string());
        let router_tick = router.clone();
        TOKIO_RUN.spawn(
            async move {
                trace!("Sent tick event to TagPollHandler");
                let _ = router_tick.notify_all_idle_mailbox();
                sleep(Duration::from_secs(10))
            }
            .instrument(info_span!("tick_event")),
        );
        return router;
    }

    fn start_bridge_channel(
        &self,
        receiver: Receiver<SegmentDataCallback>,
        router: Router<TagFsm, NormalScheduler<TagFsm>>,
        shutdown_sender: ShutdownSender<ShutdownEvent>,
    ) {
        let mut local_consumer =
            LocalSegmentMsgConsumer::new(router, self.global_config.clone(), receiver);
        TOKIO_RUN.spawn(
            async move {
                let r = local_consumer.loop_poll().await;
                match r {
                    Ok(_) => {
                        info!("Local segment consumer exit success!");
                    }
                    Err(e) => {
                        error!("Serious problem, local segment msg consumer exit:{:?}", e);
                        let _ = shutdown_sender.send(ShutdownEvent::Err(e));
                    }
                }
            }
            .instrument(info_span!("local_consumer")),
        );
    }
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
                let file = File::create(cfg.log_path.as_str()).expect("Create log failed!");
                let json_log = tracing_subscriber::fmt::layer().json().with_writer(file);
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
