pub mod bus;
pub mod conn;
pub mod route;
pub mod service;

use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Once};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::batch::{BatchSystem, FsmTypes};
use crate::client::cluster::{
    make_service, ClientEvent, ClusterActiveWatcher, Observe, Observer, Watch,
};
use crate::client::trans::TransportErr;
use crate::com::mail::BasicMailbox;
use crate::conf::GlobalConfig;
use crate::log::init_tracing_logger;
use crate::redis::{RedisAddr, RedisTTLSet};
use crate::router::Router;
use crate::sched::NormalScheduler;
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
    pub fn chan(broad_sender: BroadSenderHandle<ShutdownEvent>) -> (ShutdownSignal, Receiver<()>) {
        let (d_send, d_recv) = tokio::sync::mpsc::channel(1);
        let b_recv = broad_sender.clone().subscribe();
        let shut = ShutdownSignal {
            recv: b_recv,
            drop_notify: d_send,
            sender: broad_sender,
        };
        (shut, d_recv)
    }

    pub fn subscribe(&self) -> ShutdownSignal {
        ShutdownSignal {
            sender: self.sender.clone(),
            recv: self.sender.subscribe(),
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

        init_tracing_logger(self.global_config.clone(), shutdown_signal.subscribe());

        let ctrl_broad = broad_sender;
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
            shutdown_signal.subscribe(),
        );
        let clis_chg_recv = create_grpc_clients_watcher(grpc_clients_chg_receiver);

        let searcher = Searcher::new(clis_chg_recv);
        let (segment_sender, segment_receiver) = tokio::sync::mpsc::unbounded_channel();

        self.start_periodical_keep_alive_addr(shutdown_signal.subscribe());
        let router = self.start_batch_system_for_segment(shutdown_signal.subscribe());
        self.start_bridge_channel(segment_receiver, router, shutdown_signal.subscribe());

        let broad_shutdown_sender = shutdown_signal.sender;
        let drop_sender = shutdown_signal.drop_notify;

        drop(drop_sender);
        self.start_grpc(
            segment_sender.clone(),
            wait_recv,
            service,
            self.global_config.clone(),
            searcher,
            broad_shutdown_sender,
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
            tracing::info!("ShutdownSignal is dropped.start_periodical_keep_alive_addr");
        });
    }

    pub async fn start_grpc(
        &mut self,
        sender: UnboundedSender<SegmentDataCallback>,
        mut wait_shutdown: Receiver<()>,
        service: BoxCloneService<
            SegmentData,
            tokio::sync::oneshot::Receiver<Result<(), TransportErr>>,
            Box<dyn Error + Send + Sync>,
        >,
        config: Arc<GlobalConfig>,
        searcher: Searcher<SkyTracingClient>,
        broad_shutdown_sender: tokio::sync::broadcast::Sender<ShutdownEvent>,
    ) {
        let skytracing =
            SkyTracingService::new(sender, service, config, searcher, broad_shutdown_sender);

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
        let router = Router::new(fsm_sche, Arc::new(atomic), self.global_config.clone());

        let mut batch_system = BatchSystem::new(router.clone(), r, 1, 500);
        batch_system.spawn("Tag Poller".to_string());
        let mut router_tick = router.clone();
        let mut recv = shutdown_signal.recv;
        let send = shutdown_signal.drop_notify;

        let index_dir = router.conf.index_dir.clone();
        TOKIO_RUN.spawn(
            async move {
                loop {
                    tokio::select! {
                        _ = sleep(Duration::from_secs(3)) => {
                            trace!("Sent tick event to TagPollHandler");

                            // 1: Periodically check fsm life scope
                            let res_v = (&mut router_tick).remove_expired_mailbox();
                            Self::clear_mailbox(res_v, index_dir.clone());

                            // 2: Periodically Tick Notify fsm mailbox
                            tracing::trace!("Start to periodically tick all the idle mailbox");

                            let cur = Instant::now();

                            tracing::info!("Start to notify all idle mailbox...");
                            let _ = router_tick.notify_all_idle_mailbox();
                            tracing::info!("Tag tick all mailbox success,cost:{}", cur.elapsed().as_millis());
                        }
                        _ = recv.recv() => {
                            info!("Tick task received shutdown event, shutdown!");
                            drop(send);
                            info!("ShutdownSignal is dropped, start_batch_system_for_segment");
                            return;
                        }
                        else => {
                            break;
                        }
                    }
                }
            }
            .instrument(info_span!("tick_event")),
        );
        return router;
    }

    fn clear_mailbox<T: AsRef<std::path::Path> + Send + 'static>(
        res_v: Result<Vec<BasicMailbox<TagFsm>>, anyhow::Error>,
        index_dir: T,
    ) -> JoinHandle<()> {
        // It's a blocking operation, so we have to do it in new thread while not in tokio runtime
        std::thread::spawn(move || {
            if let Ok(v) = res_v {
                for mailbox in v.into_iter() {
                    info!("Have found expired fsm dir need to be removed ");
                    // We only operate not busy fsm
                    if !mailbox.state.is_busy() {
                        let o_fsm = mailbox.take_fsm();

                        if let Some(mut fsm) = o_fsm {
                            let engine = fsm.get_mut_engine();
                            let index_writer = engine.get_mut_index_writer();

                            // 1.flush all the pending writes
                            let _ = index_writer.commit();

                            let _ = index_writer.garbage_collect_files();

                            // 2.delete all the documents
                            let _ = index_writer.delete_all_documents();

                            drop(index_writer);

                            // 3. Try to delete directory
                            if let Ok(dir) = engine.mailkey.get_idx_path(&index_dir) {
                                let res = std::fs::remove_dir_all(&dir);
                                match res {
                                    Ok(_) => {
                                        tracing::info!(
                                            "Remove dir:{:?} success, it's a expired mailkey dir",
                                            dir
                                        )
                                    }
                                    Err(e) => {
                                        tracing::warn!("Remove dir:{:?} failed, it's expired, but we can't remove it, reason is:{:?}", dir, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn start_bridge_channel(
        &self,
        receiver: UnboundedReceiver<SegmentDataCallback>,
        router: Router<TagFsm, NormalScheduler<TagFsm>>,
        shutdown_signal: ShutdownSignal,
    ) {
        let mut local_consumer = LocalSegmentMsgConsumer::new(router, receiver);
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
