use async_trait::async_trait;
use redis::Client as RedisClient;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tower::util::BoxCloneService;
use tracing::{error, info};

use crate::client::trans::Transport;
use crate::conf::GlobalConfig;
use crate::redis::RedisTTLSet;
use crate::serv::ShutdownSignal;
use chashmap::CHashMap;
use futures::never::Never;
use futures::{ready, FutureExt, Stream};
use grpcio::{ChannelBuilder, Environment};
use skproto::tracing::{Meta_RequestType, SegmentData, SkyTracingClient};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tower::balance::p2c::Balance;
use tower::discover::Change;
use tower::ServiceBuilder;

use super::grpc_cli::split_client;
use super::service::EndpointService;
use super::trans::TransportErr;

pub struct ClusterPassive {
    recv: Receiver<ClientEvent>,
    clients: Arc<CHashMap<i32, SkyTracingClient>>,
    conn_broken_notify: tokio::sync::mpsc::Sender<i32>,
}

impl ClusterPassive {
    pub fn new(
        recv: Receiver<ClientEvent>,
        conn_broken_notify: tokio::sync::mpsc::Sender<i32>,
    ) -> ClusterPassive {
        ClusterPassive {
            recv,
            clients: Arc::new(CHashMap::new()),
            conn_broken_notify,
        }
    }
}

#[derive(Clone)]
pub enum ClientEvent {
    DropEvent(i32),
    NewClient((i32, SkyTracingClient)),
}

impl Stream for ClusterPassive {
    type Item = Result<Change<i32, EndpointService>, Never>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let chg_opt = ready!(self.recv.poll_recv(cx));
        match chg_opt {
            Some(chg) => match chg {
                ClientEvent::NewClient((id, client)) => {
                    let clients = self.clients.clone();
                    let mut p = Box::pin(async move {
                        let res = split_client(client);
                        if let Ok((conn, client)) = res {
                            clients.insert(id, client);
                            let res = conn
                                .handshake(
                                    |resp, _req| {
                                        let conn_id = resp.get_meta().get_connId();
                                        (true, conn_id)
                                    },
                                    || {
                                        let mut segment = SegmentData::new();
                                        segment
                                            .mut_meta()
                                            .set_field_type(Meta_RequestType::HANDSHAKE);
                                        Default::default()
                                    },
                                )
                                .await;
                            match res {
                                Ok(conn) => {
                                    let sink = conn.sink.unwrap();
                                    let stream = conn.recv.unwrap();

                                    let sched = Transport::init(sink, stream);
                                    let service = EndpointService::new(
                                        sched,
                                        self.conn_broken_notify.clone(),
                                        id,
                                    );
                                    return Some(Ok(Change::Insert(id, service)));
                                }
                                Err(_e) => {
                                    let _ = clients.remove(&id);
                                    return None;
                                }
                            }
                        }
                        None
                    });
                    p.poll_unpin(cx)
                }
                ClientEvent::DropEvent(id) => {
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
) -> BoxCloneService<SegmentData, Result<(), TransportErr>, Box<dyn Error + Send + Sync>> {
    let s = ServiceBuilder::new()
        .buffer(100)
        .concurrency_limit(10)
        .service(Balance::new(cluster));
    let s = BoxCloneService::new(s);
    s
}

pub fn make_service() -> (
    BoxCloneService<SegmentData, Result<(), TransportErr>, Box<dyn Error + Send + Sync>>,
    tokio::sync::mpsc::Sender<ClientEvent>,
    tokio::sync::mpsc::Receiver<i32>,
) {
    let (send, recv) = tokio::sync::mpsc::channel(1024);
    let (broken_notify, broken_recv) = tokio::sync::mpsc::channel::<i32>(1024);

    let passive = ClusterPassive::new(recv, broken_notify);
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
            let _ = s.try_send(event.clone());
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
            counter: AtomicI32::new(1),
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
                _ = sleep(Duration::from_secs(5)) => {
                    let current_addrs = Self::query_redis_cur_addrs(redis_client.clone());
                    match current_addrs {
                        Ok(current_addrs) => {
                            let (del_addrs, add_addrs) = self.compare_diff_addrs(&old_addrs, &current_addrs);

                            let del_events = Self::gen_del_events(&del_addrs, &old_addrs);
                            let add_events = Self::gen_add_events(&add_addrs);

                            Self::update_addrs(&mut old_addrs, add_addrs, del_addrs);

                            let events = del_events.into_iter().chain(add_events.into_iter());

                            events.for_each(|e| {
                                obj.notify_all(e);
                            });
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
        drop(drop_notify);
        Ok(())
    }
}

impl ClusterActiveWatcher {
    pub(crate) fn create_grpc_conns<'a>(
        addrs: Vec<(&'a str, i32)>,
    ) -> Vec<(SkyTracingClient, i32)> {
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
            .map(|s| (s.to_string(), self.counter.fetch_add(1, Ordering::Relaxed)))
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
        com::test_util::redis::{create_redis_client, gen_virtual_servers},
        log::*,
    };
    use std::collections::HashMap;
    use std::collections::HashSet;

    use super::{ClientEvent, ClusterActiveWatcher};

    fn setup() {
        init_console_logger();
    }

    #[test]
    #[ignore]
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

    #[test]
    fn test_block_watch() {}
}
