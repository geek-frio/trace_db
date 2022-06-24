use async_trait::async_trait;
use redis::Client as RedisClient;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tower::util::BoxCloneService;
use tracing::error;

use crate::client::trans::Transport;
use crate::com::config::GlobalConfig;
use crate::com::redis::RedisTTLSet;
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
        config: Arc<GlobalConfig>,
    ) -> Result<(), anyhow::Error> {
        // Check if already started watch
        let r = self.watcher_started.compare_exchange(
            false,
            true,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        if r.is_err() || !r.unwrap() {
            return Err(anyhow::Error::msg("Has already started!"));
        }

        let redis_client = self.client.clone();
        let mut old_addrs = HashMap::<String, i32>::new();
        loop {
            select! {
                _ = sleep(Duration::from_secs(5)) => {
                    let current_addrs = Self::query_redis_cur_addrs(redis_client.clone(), &config);
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
            }
        }
    }
}

impl ClusterActiveWatcher {
    fn create_grpc_conns<'a>(addrs: Vec<(&'a str, i32)>) -> Vec<(SkyTracingClient, i32)> {
        addrs
            .into_iter()
            .map(|(addr, id)| {
                let env = Environment::new(3);
                let channel = ChannelBuilder::new(Arc::new(env)).connect(addr);
                (SkyTracingClient::new(channel), id)
            })
            .collect::<Vec<(SkyTracingClient, i32)>>()
    }

    fn query_redis_cur_addrs(
        redis_cli: RedisClient,
        config: &Arc<GlobalConfig>,
    ) -> Result<HashSet<String>, anyhow::Error> {
        let redis_ttl: RedisTTLSet = Default::default();
        let mut conn = redis_cli.get_connection()?;
        let addrs = redis_ttl.query_all(&mut conn)?;
        let addrs: Vec<String> = addrs
            .into_iter()
            .map(|addr| format!("{}:{}", addr.sub_key, config.grpc_port))
            .collect();
        Ok(HashSet::from_iter(addrs.into_iter()))
    }

    fn compare_diff_addrs(
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

    fn gen_del_events(addrs: &Vec<String>, old: &HashMap<String, i32>) -> Vec<ClientEvent> {
        addrs
            .into_iter()
            .filter(|addr| old.contains_key(*addr))
            .map(|addr| *old.get(addr).unwrap())
            .map(|id| ClientEvent::DropEvent(id))
            .collect()
    }

    fn gen_add_events(addrs: &Vec<(String, i32)>) -> Vec<ClientEvent> {
        let addrs: Vec<(&str, i32)> = addrs.into_iter().map(|(s, id)| (s.as_str(), *id)).collect();
        Self::create_grpc_conns(addrs)
            .into_iter()
            .map(|(client, id)| ClientEvent::NewClient((id, client)))
            .collect()
    }

    fn update_addrs(old: &mut HashMap<String, i32>, add: Vec<(String, i32)>, del: Vec<String>) {
        for (addr, id) in add.into_iter() {
            old.insert(addr, id);
        }
        old.retain(|k, _v| !del.contains(k));
    }
}

#[cfg(test)]
mod tests {
    use grpcio::{Server, ServerBuilder};
    use skproto::tracing::create_sky_tracing;
    use tokio::sync::mpsc::UnboundedReceiver;

    use super::*;
    use crate::{
        com::config::GlobalConfig,
        serv::{create_grpc_clients_watcher, service::SkyTracingService, MainServer},
        tag::fsm::SegmentDataCallback,
        TOKIO_RUN,
    };

    const GRPC_TEST_PORT: u32 = 6666;

    fn create_mock_grpc_server() {
        let config = Arc::new(GlobalConfig {
            grpc_port: GRPC_TEST_PORT,
            redis_addr: format!("127.0.0.1:{}", 6379),
            index_dir: String::from(""),
            env: String::from(""),
            log_path: String::from(""),
            app_name: String::from("test"),
            server_ip: String::from("127.0.0.1"),
        });

        let main_server = MainServer::new(config, "127.0.0.1".to_string());
    }

    #[tokio::test]
    async fn test_basic_func() {
        let channe_bus = create_mock_grpc_server();
    }
}
