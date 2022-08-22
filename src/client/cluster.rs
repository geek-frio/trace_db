use crate::conf::GlobalConfig;
use crate::redis::RedisTTLSet;
use crate::serv::bus::RemoteMsgPoller;
use crate::serv::{ShutdownEvent, ShutdownSignal};
use crate::tag::fsm::SegmentDataCallback;
use async_trait::async_trait;
use chashmap::CHashMap;
use futures::never::Never;
use futures::{ready, Stream};
use grpcio::{ChannelBuilder, Environment};
use local_ip_address::linux::local_ip;
use once_cell::sync::OnceCell;
use redis::Client as RedisClient;
use skproto::tracing::{SegmentData, SkyTracingClient};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tower::balance::p2c::Balance;
use tower::discover::Change;
use tower::util::BoxCloneService;
use tower::ServiceBuilder;
use tracing::{error, info};

use super::grpc_cli::split_client;
use super::local_trans::{LocalSink, LocalStream, RemoteStream};
use super::service::EndpointService;
use super::trans::{Transport, TransportErr};

pub struct ClusterPassive {
    recv: Receiver<ClientEvent>,
    clients: Arc<CHashMap<i32, SkyTracingClient>>,
    conn_broken_notify: tokio::sync::mpsc::Sender<i32>,
    send: tokio::sync::mpsc::UnboundedSender<SegmentDataCallback>,
    shutdown_recv: tokio::sync::broadcast::Receiver<ShutdownEvent>,
}

impl ClusterPassive {
    pub fn new(
        recv: Receiver<ClientEvent>,
        conn_broken_notify: tokio::sync::mpsc::Sender<i32>,
        send: tokio::sync::mpsc::UnboundedSender<SegmentDataCallback>,
        shutdown_recv: tokio::sync::broadcast::Receiver<ShutdownEvent>,
    ) -> ClusterPassive {
        ClusterPassive {
            recv,
            clients: Arc::new(CHashMap::new()),
            conn_broken_notify,
            send,
            shutdown_recv,
        }
    }
}

#[derive(Clone)]
pub enum ClientEvent {
    DropEvent(i32),
    NewClient((i32, SkyTracingClient)),
}

static LOCAL_SERVICE_CTRL: OnceCell<()> = once_cell::sync::OnceCell::new();
impl Stream for ClusterPassive {
    type Item = Result<Change<i32, EndpointService>, Never>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Return local service only once
        if LOCAL_SERVICE_CTRL.get().is_none() {
            let _ = LOCAL_SERVICE_CTRL.get_or_init(|| {});

            let (local_send1, remote_recv1) = tokio::sync::mpsc::unbounded_channel();
            let (remote_send2, local_recv2) = tokio::sync::mpsc::unbounded_channel();

            let local_sink = LocalSink::new(local_send1);
            let local_stream = LocalStream::new(local_recv2);
            let request_handler = Transport::init(local_sink, local_stream);
            // Not exists real connection, so we will never use this broken notify
            let (broken_notify, _) = tokio::sync::mpsc::channel(1);
            let end_point = EndpointService::new(request_handler, broken_notify, 1);

            // Start remote msg poller
            let remote_stream = RemoteStream::new(remote_recv1);
            let remote_msg_poller = RemoteMsgPoller::new(source, sink, local_sender, broad_shutdown_receiver)
            // tokio::spawn(future)
            // 1: 提前返回event
            // 2: 原本的event需要进行再次进行poll获取
            cx.waker().wake_by_ref();
            return Poll::Ready(Some(Ok(Change::Insert(1, end_point))));
        }

        let chg_opt = ready!(self.recv.poll_recv(cx));
        match chg_opt {
            Some(chg) => match chg {
                ClientEvent::NewClient((id, client)) => {
                    // We skip local tcp service
                    if id == 1 {
                        return Poll::Pending;
                    }
                    let res = split_client(client);
                    match res {
                        Ok((conn, client)) => {
                            self.clients.insert(id, client);

                            let mut sink = conn.sink.unwrap();
                            sink.enhance_batch(true);

                            let stream = conn.recv.unwrap();

                            let sched = Transport::init(sink, stream);
                            let service =
                                EndpointService::new(sched, self.conn_broken_notify.clone(), id);
                            return Poll::Ready(Some(Ok(Change::Insert(id, service))));
                        }
                        Err(_e) => {
                            error!("Handshake error!");
                            let _ = self.clients.remove(&id);
                            return Poll::Pending;
                        }
                    }
                }
                ClientEvent::DropEvent(id) => {
                    info!("Received client drop event!");
                    let _ = self.clients.remove(&id);
                    return Poll::Ready(Some(Ok(Change::Remove(id))));
                }
            },
            None => return Poll::Pending,
        }
    }
}

pub fn make_service_with(
    cluster: ClusterPassive,
) -> BoxCloneService<
    SegmentData,
    tokio::sync::oneshot::Receiver<Result<(), TransportErr>>,
    Box<dyn Error + Send + Sync>,
> {
    let s = ServiceBuilder::new()
        .buffer(100000)
        // .concurrency_limit(5000)
        .service(Balance::new(cluster));
    let s = BoxCloneService::new(s);
    s
}

pub fn make_service(
    bridge_send: tokio::sync::mpsc::UnboundedSender<SegmentDataCallback>,
    shutdown_recv: tokio::sync::broadcast::Receiver<ShutdownEvent>,
) -> (
    BoxCloneService<
        SegmentData,
        tokio::sync::oneshot::Receiver<Result<(), TransportErr>>,
        Box<dyn Error + Send + Sync>,
    >,
    tokio::sync::mpsc::Sender<ClientEvent>,
    tokio::sync::mpsc::Receiver<i32>,
) {
    let (send, recv) = tokio::sync::mpsc::channel(1024);
    let (broken_notify, broken_recv) = tokio::sync::mpsc::channel::<i32>(1024);

    let passive = ClusterPassive::new(recv, broken_notify, bridge_send, shutdown_recv);
    let s = make_service_with(passive);
    (s, send, broken_recv)
}

pub trait Observe {
    fn subscribe(&mut self) -> Receiver<ClientEvent>;
    fn regist(&mut self, sender: Sender<ClientEvent>);
}

pub struct Observer {
    vec: Vec<Sender<ClientEvent>>,
}

impl Observer {
    pub fn new() -> Observer {
        Observer { vec: Vec::new() }
    }
}

impl Observe for Observer {
    fn subscribe(&mut self) -> Receiver<ClientEvent> {
        let (send, recv) = tokio::sync::mpsc::channel(1024);
        self.vec.push(send);
        recv
    }

    fn regist(&mut self, sender: Sender<ClientEvent>) {
        self.vec.push(sender);
    }
}

impl Observer {
    fn notify_all(&mut self, event: ClientEvent) {
        for s in self.vec.iter_mut() {
            let res = s.try_send(event.clone());

            if let Err(_e) = res {
                tracing::error!("Notify events failed!");
            }
        }
    }
}

#[async_trait]
pub trait Watch {
    async fn block_watch(
        &self,
        obj: Observer,
        chg_notify: Receiver<i32>,
        config: Arc<GlobalConfig>,
        shutdown_signal: ShutdownSignal,
    ) -> Result<(), anyhow::Error>;
}

pub struct ClusterActiveWatcher {
    counter: AtomicI32,
    watcher_started: Arc<AtomicBool>,
    client: RedisClient,
}

impl ClusterActiveWatcher {
    pub fn new(client: RedisClient) -> ClusterActiveWatcher {
        ClusterActiveWatcher {
            counter: AtomicI32::new(2),
            watcher_started: Arc::new(AtomicBool::new(false)),
            client: client,
        }
    }
}

#[async_trait]
impl Watch for ClusterActiveWatcher {
    async fn block_watch(
        &self,
        mut obj: Observer,
        mut chg_notify: Receiver<i32>,
        _config: Arc<GlobalConfig>,
        shutdown_signal: ShutdownSignal,
    ) -> Result<(), anyhow::Error> {
        let r = self.watcher_started.compare_exchange(
            false,
            true,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        if r.is_err() || r.unwrap() {
            return Err(anyhow::Error::msg("Has already started!"));
        }

        let redis_client = self.client.clone();
        let mut old_addrs = HashMap::<String, i32>::new();
        let mut b_recv = shutdown_signal.recv;
        let drop_notify = shutdown_signal.drop_notify;

        loop {
            select! {
                _ = sleep(Duration::from_secs(1)) => {
                    let current_addrs = Self::query_redis_cur_addrs(redis_client.clone());
                    match current_addrs {
                        Ok(current_addrs) => {
                            let (del_addrs, add_addrs) = self.compare_diff_addrs(&old_addrs, &current_addrs);

                            let del_events = Self::gen_del_events(&del_addrs, &old_addrs);
                            let add_events = Self::gen_add_events(&add_addrs);

                            if add_events.len() > 0 || del_events.len() > 0 {
                                tracing::trace!("add events length:{:?}, del events length:{:?}", add_events.len(), del_events.len());
                            }

                            Self::update_addrs(&mut old_addrs, add_addrs, del_addrs);

                            let events = del_events.into_iter().chain(add_events.into_iter());

                            events.for_each(|e| {
                                obj.notify_all(e);
                            });
                            tracing::trace!("old addrs is:{:?}", old_addrs);
                        }
                        Err(e) => {
                            error!(%e,"Query cluster info from redis failed!Wait to next retry.");
                        }
                    }
                }
                event = chg_notify.recv() => {
                    if let Some(id) = event {
                        obj.notify_all(ClientEvent::DropEvent(id));
                    }
                }
                _ =  b_recv.recv() => {
                    info!("Has received shutdown event, clusterActiveWatcher is stopping!");
                    break;
                }
            }
        }
        tracing::info!("drop_notify is dropped in ClusterActiveWatcher");

        drop(drop_notify);
        Ok(())
    }
}

impl ClusterActiveWatcher {
    pub fn create_grpc_conns<'a>(addrs: Vec<(&'a str, i32)>) -> Vec<(SkyTracingClient, i32)> {
        addrs
            .into_iter()
            .map(|(addr, id)| {
                let env = Environment::new(3);
                let channel = ChannelBuilder::new(Arc::new(env)).connect(addr);

                (SkyTracingClient::new(channel), id)
            })
            .collect::<Vec<(SkyTracingClient, i32)>>()
    }

    pub(crate) fn query_redis_cur_addrs(
        redis_cli: RedisClient,
    ) -> Result<HashSet<String>, anyhow::Error> {
        let redis_ttl: RedisTTLSet = Default::default();
        let mut conn = redis_cli.get_connection()?;
        let addrs = redis_ttl.query_all(&mut conn)?;

        let addrs: Vec<String> = addrs
            .into_iter()
            .map(|addr| format!("{}", addr.sub_key))
            .collect();

        Ok(HashSet::from_iter(addrs.into_iter()))
    }

    pub(crate) fn compare_diff_addrs(
        &self,
        old: &HashMap<String, i32>,
        addrs: &HashSet<String>,
    ) -> (Vec<String>, Vec<(String, i32)>) {
        let del: Vec<String> = old
            .keys()
            .filter(|key| !addrs.contains(*key))
            .map(|s| s.to_string())
            .collect();
        let add: Vec<(String, i32)> = addrs
            .iter()
            .filter(|s| !old.contains_key(*s))
            .map(|s| {
                let mut ip_port = s.split(":");
                let ip = ip_port.next().unwrap();

                let cur_ip = local_ip().unwrap().to_string();

                tracing::warn!("Split ip is:{}", ip);
                // We reserve id 1 for local transport
                if &cur_ip == ip || ip == "127.0.0.1" {
                    (s.to_string(), 1)
                } else {
                    (s.to_string(), self.counter.fetch_add(1, Ordering::Relaxed))
                }
            })
            .collect();
        (del, add)
    }

    pub(crate) fn gen_del_events(
        addrs: &Vec<String>,
        old: &HashMap<String, i32>,
    ) -> Vec<ClientEvent> {
        addrs
            .into_iter()
            .filter(|addr| old.contains_key(*addr))
            .map(|addr| *old.get(addr).unwrap())
            .map(|id| ClientEvent::DropEvent(id))
            .collect()
    }

    pub(crate) fn gen_add_events(addrs: &Vec<(String, i32)>) -> Vec<ClientEvent> {
        let addrs: Vec<(&str, i32)> = addrs.into_iter().map(|(s, id)| (s.as_str(), *id)).collect();
        Self::create_grpc_conns(addrs)
            .into_iter()
            .map(|(client, id)| ClientEvent::NewClient((id, client)))
            .collect()
    }

    pub(crate) fn update_addrs(
        old: &mut HashMap<String, i32>,
        add: Vec<(String, i32)>,
        del: Vec<String>,
    ) {
        for (addr, id) in add.into_iter() {
            old.insert(addr, id);
        }
        old.retain(|k, _v| !del.contains(k));
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use crate::{
        com::test_util::redis::{
            create_redis_client, gen_virtual_servers, gen_virtual_servers_with_ip,
            offline_some_servers, redis_servers_clear,
        },
        conf::GlobalConfig,
        log::*,
        serv::{ShutdownEvent, ShutdownSignal},
    };
    use std::collections::HashMap;
    use std::collections::HashSet;

    use super::{ClientEvent, ClusterActiveWatcher, Observe, Observer, Watch};
    use tokio::{
        sync::mpsc::{Receiver, Sender},
        time::sleep,
    };

    fn setup() {
        init_console_logger();
        redis_servers_clear();
    }

    #[test]
    #[ignore = "Use gnu make to start redis service first"]
    fn test_cluster_active_watcher_query_redis() {
        setup();
        gen_virtual_servers(3);

        let client = create_redis_client();
        let hashset = ClusterActiveWatcher::query_redis_cur_addrs(client).unwrap();

        assert_eq!(3, hashset.len());

        let re = Regex::new(r"\d+\.\d+\.\d+\.\d+:\d+").unwrap();

        for text in hashset.into_iter() {
            assert!(re.is_match(text.as_str()));
        }
    }

    #[test]
    fn test_compare_diff_addrs() {
        setup();

        let client = create_redis_client();
        let cluster = ClusterActiveWatcher::new(client);

        let mut old = HashMap::new();

        old.insert("192.168.0.1:9999".to_string(), 1);
        old.insert("192.168.0.2:9999".to_string(), 2);
        old.insert("192.168.0.3:9999".to_string(), 3);

        let mut new = HashSet::new();

        new.insert("192.168.0.1:9999".to_string());
        new.insert("192.168.0.2:9999".to_string());

        new.insert("192.168.0.8:9999".to_string());

        let (del, add) = cluster.compare_diff_addrs(&old, &new);

        assert!(del.contains(&"192.168.0.3:9999".to_string()));
        assert_eq!(add.len(), 1);

        let v = add.into_iter().map(|t| t.0).collect::<Vec<String>>();
        assert!(v.contains(&"192.168.0.8:9999".to_string()));
    }

    #[test]
    fn test_gen_del_events() {
        setup();

        let mut old = HashMap::new();

        old.insert("192.168.0.1:9999".to_string(), 1);
        old.insert("192.168.0.2:9999".to_string(), 2);
        old.insert("192.168.0.3:9999".to_string(), 3);

        let cur_addrs = vec![
            "192.168.0.2:9999".to_string(),
            "192.168.0.3:9999".to_string(),
            "192.168.0.10:9999".to_string(),
        ];

        let events = ClusterActiveWatcher::gen_del_events(&cur_addrs, &old);
        for event in events.into_iter() {
            match event {
                ClientEvent::DropEvent(_) => {}
                ClientEvent::NewClient(_) => {
                    panic!("The should not new clients");
                }
            }
        }
    }

    #[test]
    fn test_cluster_active_watcher_basics() {
        setup();

        let v = vec![
            ("192.168.0.1:9999".to_string(), 1),
            ("192.168.0.1:9999".to_string(), 2),
            ("192.168.0.1:9999".to_string(), 3),
        ];

        let clients = ClusterActiveWatcher::gen_add_events(&v);
        assert_eq!(3, clients.len());
    }

    struct Context {
        observer: Observer,

        events_recv: Receiver<ClientEvent>,

        chg_recv: Receiver<i32>,
        _chg_sender: Sender<i32>,

        shutdown_signal: ShutdownSignal,
        conf: std::sync::Arc<GlobalConfig>,

        cluster: ClusterActiveWatcher,
    }

    fn setup_context() -> Context {
        let client = create_redis_client();
        let cluster = ClusterActiveWatcher::new(client);

        let mut observer = Observer::new();

        let (events_sender, events_receiver) = tokio::sync::mpsc::channel(256);
        observer.regist(events_sender);

        let (chg_sender, chg_recv) = tokio::sync::mpsc::channel(256);
        let config: GlobalConfig = Default::default();

        let (shutdown_sender, _) = tokio::sync::broadcast::channel(1);
        let (shutdown_signal, _receiver) = ShutdownSignal::chan(shutdown_sender);

        Context {
            observer,
            events_recv: events_receiver,
            chg_recv,
            _chg_sender: chg_sender,

            shutdown_signal,
            conf: std::sync::Arc::new(config),
            cluster,
        }
    }

    #[tokio::test]
    #[ignore = "Need to mock redis service in local machine, use gnu make to start redis service first"]
    async fn test_block_watch() {
        setup();

        let ctx = setup_context();

        let cluster = ctx.cluster;
        let shutdown_signal = ctx.shutdown_signal;

        let s = shutdown_signal.subscribe();
        let mut events_recv = ctx.events_recv;

        let (panic_sender, mut panic_recv) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            gen_virtual_servers(5);
            sleep(std::time::Duration::from_secs(2)).await;

            tracing::info!("Start to offline 2 server");
            offline_some_servers(2);

            sleep(std::time::Duration::from_secs(2)).await;

            tracing::info!("Startto add new 2 server");
            gen_virtual_servers_with_ip(2, "192.168.1");
        });

        tokio::spawn(async move {
            let mut events = Vec::new();

            sleep(std::time::Duration::from_secs(2)).await;

            for _ in 0..5 {
                let e = events_recv.recv().await.unwrap();
                events.push(e);
            }
            assert_eq!(5, events.len());

            events.clear();

            // /////////////////////////////////////////////////////////////////////////////////
            for _ in 0..2 {
                let e = events_recv.recv().await.unwrap();

                events.push(e);
            }
            assert_eq!(2, events.len());

            for e in events.iter() {
                match e {
                    ClientEvent::DropEvent(_) => {}
                    _ => {
                        tracing::error!("There is not only drop event!");

                        let _ = panic_sender
                            .send("Not only drop event, unexpected!".to_string())
                            .await;
                        break;
                    }
                }
            }
            events.clear();
            ////////////////////////////////////////////////////////////////////////////////////
            for _ in 0..2 {
                let e = events_recv.recv().await.unwrap();
                events.push(e);
            }

            assert_eq!(2, events.len());

            let _ = s.sender.send(ShutdownEvent::ForceStop);
        });

        let _ = cluster
            .block_watch(ctx.observer, ctx.chg_recv, ctx.conf, shutdown_signal)
            .await;

        let msg = panic_recv.try_recv();
        if msg.is_ok() {
            let msg = msg.unwrap();
            panic!("{}", msg);
        }
    }

    #[tokio::test]
    async fn test_observer() {
        async_init_console_logger().await;

        let (send, mut recv) = tokio::sync::mpsc::channel(1024);

        let mut ob = Observer::new();
        ob.regist(send);

        tokio::spawn(async move {
            ob.notify_all(ClientEvent::DropEvent(1));
        });

        let e = recv.recv().await;
        tracing::info!("Has received event:{}", e.is_some());
        assert!(e.is_some());
    }
}
