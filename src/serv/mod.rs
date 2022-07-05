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
use crate::com::redis::{RedisAddr, RedisTTLSet};
use crate::com::router::Router;
use crate::com::sched::NormalScheduler;
use crate::conf::GlobalConfig;
use crate::tag::fsm::{SegmentDataCallback, TagFsm};
use crate::tag::search::Searcher;
use crate::TOKIO_RUN;
use conn::*;
use grpcio::{Environment, ServerBuilder};
use lazy_static::*;
use skproto::tracing::{create_sky_tracing, SegmentData, SkyTracingClient};
use tokio::select;
use tokio::sync::broadcast::{Receiver as BroadReceiver, Sender as BroadSenderHandle};
use tokio::sync::mpsc::{Receiver, Sender};
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

pub struct ShutdownSignal {
    pub recv: BroadReceiver<ShutdownEvent>,
    pub sender: BroadSenderHandle<ShutdownEvent>,
    pub drop_notify: Sender<()>,
}

impl Clone for ShutdownSignal {
    fn clone(&self) -> Self {
        Self {
            recv: self.sender.subscribe(),
            sender: self.sender.clone(),
            drop_notify: self.drop_notify.clone(),
        }
    }
}

impl ShutdownSignal {
    fn chan(broad_sender: BroadSenderHandle<ShutdownEvent>) -> (ShutdownSignal, Receiver<()>) {
        let (d_send, d_recv) = tokio::sync::mpsc::channel(1);
        let b_recv = broad_sender.clone().subscribe();
        let shut = ShutdownSignal {
            recv: b_recv,
            drop_notify: d_send,
            sender: broad_sender,
        };
        (shut, d_recv)
    }

    fn subscribe(&self, sender: BroadSenderHandle<ShutdownEvent>) -> ShutdownSignal {
        ShutdownSignal {
            sender: sender.clone(),
            recv: sender.subscribe(),
            drop_notify: self.drop_notify.clone(),
        }
    }
}

#[derive(Clone)]
pub enum ShutdownEvent {
    GracefulStop,
    ForceStop,
}

lazy_static! {
    pub static ref INIT_LOGGER: Once = Once::new();
}

pub struct MainServer {
    global_config: Arc<GlobalConfig>,
    ip: String,
}

impl MainServer {
    pub fn new(global_config: Arc<GlobalConfig>, ip: String) -> MainServer {
        MainServer { global_config, ip }
    }
    pub async fn block_start(&mut self, broad_sender: BroadSenderHandle<ShutdownEvent>) {
        let _span = info_span!("main_server");

        let (shutdown_signal, wait_recv) = ShutdownSignal::chan(broad_sender.clone());

        let ctrl_broad = broad_sender.clone();

        ctrlc::set_handler(move || {
            info!("Received shutdown event, broadcast shutdown event to all the spawned task!");
            let _ = ctrl_broad.send(ShutdownEvent::GracefulStop);
        })
        .expect("Error setting ctrl+c handler");

        info!("Start to init search builder...");
        let (service, event_sender, conn_broken_receiver) = make_service();
        let grpc_clients_chg_receiver = self.create_cluster_watcher(
            conn_broken_receiver,
            event_sender,
            shutdown_signal.subscribe(broad_sender.clone()),
        );
        self.start_periodical_keep_alive_addr(shutdown_signal.subscribe(broad_sender.clone()));

        let clis_chg_recv = create_grpc_clients_watcher(grpc_clients_chg_receiver);

        let searcher = Searcher::new(clis_chg_recv);
        let (segment_sender, segment_receiver) = tokio::sync::mpsc::unbounded_channel();

        let router =
            self.start_batch_system_for_segment(shutdown_signal.subscribe(broad_sender.clone()));

        self.start_bridge_channel(
            segment_receiver,
            router,
            shutdown_signal.subscribe(broad_sender.clone()),
        );

        self.start_grpc(
            segment_sender.clone(),
            wait_recv,
            service,
            searcher,
            shutdown_signal,
        )
        .await;

        info!("Shut receiver has received.");
    }

    pub fn start_periodical_keep_alive_addr(&mut self, shutdown_signal: ShutdownSignal) {
        let redis_cli = self.create_redis_cli();
        let grpc_port = self.global_config.grpc_port;
        let ip = self.global_config.server_ip.clone();

        TOKIO_RUN.spawn(async move {
            let mut recv = shutdown_signal.recv;
            loop {
                tokio::select! {
                    _ = sleep(Duration::from_secs(1)) => {
                        trace!(
                                "Periodically update ip:{}, grpc_port:{} in redis",
                                ip, grpc_port
                            );
                        let conn = redis_cli.get_connection();
                        match conn {
                            Ok(mut conn) => {
                                let s = format!("{}:{}", ip, grpc_port);
                                let redis_ttl: RedisTTLSet = Default::default();
                                let _ = redis_ttl.push(&mut conn, s.as_str());
                            }
                            Err(e) => {
                                error!(%e, "Create redis connection failed!");
                            }
                        }
                    }
                    _ = recv.recv() => {
                        info!("Keep alive task is stopping!");
                        break;
                    }
                }
            }
        });
    }

    pub async fn start_grpc(
        &mut self,
        sender: UnboundedSender<SegmentDataCallback>,
        mut wait_shutdown: Receiver<()>,
        service: BoxCloneService<
            SegmentData,
            Result<(), TransportErr>,
            Box<dyn Error + Send + Sync>,
        >,
        searcher: Searcher<SkyTracingClient>,
        shutdown_signal: ShutdownSignal,
    ) {
        let skytracing = SkyTracingService::new(
            self.global_config.clone(),
            sender,
            service,
            searcher,
            shutdown_signal,
        );
        let service = create_sky_tracing(skytracing);
        let env = Environment::new(1);
        let mut server = ServerBuilder::new(Arc::new(env))
            .bind(self.ip.as_str(), self.global_config.grpc_port as u16)
            .register_service(service)
            .build()
            .unwrap();
        server.start();
        wait_shutdown.recv().await;
    }

    pub fn start_batch_system_for_segment(
        &self,
        shutdown_signal: ShutdownSignal,
    ) -> Router<TagFsm, NormalScheduler<TagFsm>> {
        let (s, r) = crossbeam_channel::unbounded::<FsmTypes<TagFsm>>();
        let fsm_sche = NormalScheduler { sender: s };
        let atomic = AtomicUsize::new(1);
        let router = Router::new(fsm_sche, Arc::new(atomic));

        let mut batch_system = BatchSystem::new(router.clone(), r, 1, 500);
        batch_system.spawn("Tag Poller".to_string());
        let router_tick = router.clone();
        let mut recv = shutdown_signal.recv;
        let send = shutdown_signal.drop_notify;
        TOKIO_RUN.spawn(
            async move {
                loop {
                    tokio::select! {
                        _ = sleep(Duration::from_secs(10)) => {
                            trace!("Sent tick event to TagPollHandler");
                            let _ = router_tick.notify_all_idle_mailbox();
                        }
                        _ = recv.recv() => {
                            info!("Tick task received shutdown event, shutdown!");
                            drop(send);
                            return;
                        }
                    }
                }
            }
            .instrument(info_span!("tick_event")),
        );
        return router;
    }

    pub fn start_bridge_channel(
        &self,
        receiver: UnboundedReceiver<SegmentDataCallback>,
        router: Router<TagFsm, NormalScheduler<TagFsm>>,
        shutdown_signal: ShutdownSignal,
    ) {
        let mut local_consumer =
            LocalSegmentMsgConsumer::new(router, self.global_config.clone(), receiver);
        TOKIO_RUN.spawn(
            async move {
                let r = local_consumer.loop_poll(shutdown_signal).await;
                match r {
                    Ok(_) => {
                        info!("Local segment consumer exit success!");
                    }
                    Err(e) => {
                        error!("Serious problem, local segment msg consumer exit:{:?}", e);
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
        shutdown_signal: ShutdownSignal,
    ) -> Receiver<ClientEvent> {
        let mut obj = Observer::new();
        obj.regist(sender);
        let search_recv = obj.subscribe();

        let redis_cli = self.create_redis_cli();
        let clt = ClusterActiveWatcher::new(redis_cli);

        let config = self.global_config.clone();
        TOKIO_RUN.spawn(async move {
            let res = clt
                .block_watch(obj, chg_notify, config, shutdown_signal)
                .await;
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
pub(crate) fn create_grpc_clients_watcher(
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
