pub mod bus;
pub mod conn;
pub mod route;
pub mod service;

use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Once};
use std::time::Duration;

use crate::client::cluster::{
    make_service, ClientEvent, ClusterActiveWatcher, Observe, Observer, Watch,
};
use crate::client::trans::TransportErr;
use crate::com::batch::{BatchSystem, FsmTypes};
use crate::com::config::GlobalConfig;
use crate::com::redis::RedisAddr;
use crate::com::router::Router;
use crate::com::sched::NormalScheduler;
use crate::tag::fsm::{SegmentDataCallback, TagFsm};
use crate::tag::search::Searcher;
use crate::TOKIO_RUN;
use conn::*;
use crossbeam_channel::Receiver as ShutdownReceiver;
use crossbeam_channel::Sender as ShutdownSender;
use grpcio::{Environment, ServerBuilder};
use lazy_static::*;
use skproto::tracing::{create_sky_tracing, SegmentData, SkyTracingClient};
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::sleep;
use tower::util::BoxCloneService;
use tracing::{error, info, trace};
use tracing::{info_span, Instrument};

use self::route::LocalSegmentMsgConsumer;
use self::service::SkyTracingService;

lazy_static! {
    pub static ref CONN_MANAGER: ConnManager = ConnManager::new();
}

pub enum ShutdownEvent {
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
    pub fn new(global_config: Arc<GlobalConfig>, ip: String) -> MainServer {
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
    pub fn start(&mut self) {
        let _span = info_span!("main_server");
        info!("Start to init search builder...");
        let (service, event_sender, conn_broken_receiver) = make_service();
        let grpc_clients_chg_receiver =
            self.create_cluster_watcher(conn_broken_receiver, event_sender);
        let clis_chg_recv = create_grpc_clients_watcher(grpc_clients_chg_receiver);

        let searcher = Searcher::new(clis_chg_recv);

        let (segment_sender, segment_receiver) = tokio::sync::mpsc::unbounded_channel();
        let router = self.start_batch_system_for_segment();
        self.start_bridge_channel(segment_receiver, router, self.shutdown_sender.clone());
        self.start_grpc(
            segment_sender.clone(),
            self.shutdown_receiver.clone(),
            service,
            searcher,
        );
        info!("Shut receiver has received.");
    }

    pub fn start_grpc(
        &mut self,
        sender: UnboundedSender<SegmentDataCallback>,
        receiver: ShutdownReceiver<ShutdownEvent>,
        service: BoxCloneService<
            SegmentData,
            Result<(), TransportErr>,
            Box<dyn Error + Send + Sync>,
        >,
        searcher: Searcher<SkyTracingClient>,
    ) {
        let skytracing =
            SkyTracingService::new(self.global_config.clone(), sender, service, searcher);
        let service = create_sky_tracing(skytracing);
        let env = Environment::new(1);
        let mut server = ServerBuilder::new(Arc::new(env))
            .bind(self.ip.as_str(), self.global_config.grpc_port as u16)
            .register_service(service)
            .build()
            .unwrap();
        server.start();
        let _ = receiver.recv();
    }

    pub fn start_batch_system_for_segment(&self) -> Router<TagFsm, NormalScheduler<TagFsm>> {
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

    pub fn start_bridge_channel(
        &self,
        receiver: UnboundedReceiver<SegmentDataCallback>,
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

    pub fn create_cluster_watcher(
        &self,
        chg_notify: tokio::sync::mpsc::Receiver<i32>,
        sender: tokio::sync::mpsc::Sender<ClientEvent>,
    ) -> Receiver<ClientEvent> {
        let mut obj = Observer::new();
        obj.regist(sender);
        let search_recv = obj.subscribe();

        let redis_cli = self.create_redis_cli();
        let clt = ClusterActiveWatcher::new(redis_cli);

        let config = self.global_config.clone();
        TOKIO_RUN.spawn(async move {
            let res = clt.block_watch(obj, chg_notify, config).await;
            if let Err(e) = res {
                error!(%e, "Unrecover exception, exit");
            }
        });
        search_recv
    }

    pub fn create_redis_cli(&self) -> redis::Client {
        let mut redis_addr: RedisAddr = (&self.global_config)
            .redis_addr
            .as_str()
            .try_into()
            .expect("Invalid redis addr config");

        redis_addr.client().expect("Redis connect failed!")
    }

    pub fn create_search_builder(
        &self,
        clients_chg_receiver: UnboundedReceiver<Vec<SkyTracingClient>>,
    ) -> Searcher<SkyTracingClient> {
        Searcher::new(clients_chg_receiver)
    }
}

// Every second check if the clients is changed
fn create_grpc_clients_watcher(
    mut grpc_clients_chg_receiver: Receiver<ClientEvent>,
) -> UnboundedReceiver<Vec<SkyTracingClient>> {
    let (send, recv) = tokio::sync::mpsc::unbounded_channel();
    TOKIO_RUN.spawn(async move {
        let mut clients = Vec::new();
        let mut is_change = false;
        loop {
            select! {
                _ = sleep(Duration::from_secs(1)) => {
                    if is_change {
                        let _ = send.send(clients.clone());
                        is_change = false;
                    }
                }
                res = grpc_clients_chg_receiver.recv()=> {
                    if let Some(event) = res {
                        if let ClientEvent::NewClient(cli) = event {
                            clients.push(cli.1);
                            is_change = true;
                        }
                    }
                }
            }
        }
    });
    recv
}
