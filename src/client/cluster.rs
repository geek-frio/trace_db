use async_trait::async_trait;
use redis::Client as RedisClient;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::task::Poll;
use std::time::Duration;
use std::{marker::PhantomData, sync::Arc};
use tracing::error;

use crate::client::trans::Transport;
use crate::com::config::GlobalConfig;
use crate::com::redis::RedisTTLSet;
use chashmap::CHashMap;
use futures::never::Never;
use futures::{ready, FutureExt, Stream};
use grpcio::{ChannelBuilder, Environment};
use skproto::tracing::{Meta_RequestType, SegmentData, SkyTracingClient};
use std::fmt::Debug;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tower::balance::p2c::Balance;
use tower::ServiceBuilder;
use tower::{discover::Change, Service};

use super::grpc_cli::split_client;
use super::service::Endpoint;

pub struct ClusterDiscover<Req, S>
where
    S: Service<Req>,
{
    recv: Receiver<ClientEvent>,
    clients: Arc<CHashMap<i32, SkyTracingClient>>,
    req_marker: PhantomData<Req>,
    service_marker: PhantomData<S>,
}

impl<Req, S> ClusterDiscover<Req, S>
where
    S: Service<Req> + Unpin + 'static,
    Req: Debug + Send + 'static + Clone + Unpin,
{
    fn new(recv: Receiver<ClientEvent>) -> ClusterDiscover<Req, S> {
        ClusterDiscover {
            recv,
            clients: Arc::new(CHashMap::new()),
            req_marker: PhantomData,
            service_marker: PhantomData,
        }
    }
}

pub enum ClientEvent {
    DropEvent(i32),
    NewClient((i32, SkyTracingClient)),
}

impl<Req, S> Stream for ClusterDiscover<Req, S>
where
    S: Service<Req> + Unpin + 'static,
    Req: Debug + Send + 'static + Clone + Unpin,
{
    type Item = Result<Change<i32, Endpoint>, Never>;

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
                                    let service = Endpoint::new(sched);
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

pub async fn make_service<S>(cluster: ClusterDiscover<SegmentData, S>) -> impl Service<SegmentData>
where
    S: Service<SegmentData> + Unpin + 'static + Send,
{
    let s = ServiceBuilder::new()
        .buffer(100)
        .concurrency_limit(10)
        .service(Balance::new(cluster));
    s
}

#[async_trait]
pub trait ClusterChangeWatcher {
    async fn block_watch(
        &self,
        sender: Sender<ClientEvent>,
        chg_notify: Receiver<i32>,
        config: Arc<GlobalConfig>,
    ) -> Result<(), anyhow::Error>;
}

pub struct ClusterActiveWatcher {
    counter: AtomicI32,
    watcher_started: Arc<AtomicBool>,
    client: RedisClient,
}

#[async_trait]
impl ClusterChangeWatcher for ClusterActiveWatcher {
    async fn block_watch(
        &self,
        sender: Sender<ClientEvent>,
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
                                let _ = sender.send(e);
                            });
                        }
                        Err(e) => {
                            error!(%e,"Query cluster info from redis failed!Wait to next retry.");
                        }
                    }
                }
                event = chg_notify.recv() => {
                    if let Some(id) = event {
                        let _ = sender.send(ClientEvent::DropEvent(id)).await;
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
