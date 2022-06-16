use std::collections::HashMap;
use std::task::Poll;
use std::{collections::HashSet, marker::PhantomData, sync::Arc, time::Duration};

use crate::com::redis::Record;
use futures::{pin_mut, ready, Stream};
use futures_util::future::poll_fn;
use grpcio::{ChannelBuilder, Environment};
use redis::{Client as RedisClient, Connection};
use skproto::tracing::SkyTracingClient;
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use tower::{
    discover::{Change, Discover},
    Service,
};
use tracing::error;

use super::grpc_cli::split_client;
use super::RingService;

pub struct ClusterManager<Req, S>
where
    S: Service<Req>,
{
    recv: Receiver<Change<i32, SkyTracingClient>>,
    srv_map: HashMap<i32, S>,
    client_map: HashMap<i32, SkyTracingClient>,
    wrap_req_marker: PhantomData<Req>,
}

impl<WrapReq, S> Unpin for ClusterManager<WrapReq, S> where S: Service<WrapReq> {}

impl<WrapReq, S> ClusterManager<WrapReq, S>
where
    S: Service<WrapReq>,
{
    pub fn new(recv: Receiver<Change<i32, SkyTracingClient>>) -> ClusterManager<WrapReq, S> {
        ClusterManager {
            recv,
            srv_map: HashMap::new(),
            client_map: HashMap::new(),
            wrap_req_marker: PhantomData,
        }
    }

    pub fn init(&mut self, redis_client: RedisClient) {
        let redis_client = redis_client.clone();
        // let clients = self.clients.clone();
        let mut conn = Self::loop_retrive_conn(redis_client.clone());
        // std::thread::spawn(move || {
        //     let mut old = HashSet::new();
        //     loop {
        //         let redis_ttlset = RedisTTLSet::new(LEASE_TIME_OUT);
        //         let res = redis_ttlset.query_all(&mut conn);
        //         match res {
        //             Ok(records) => {
        //                 let new = HashSet::from_iter(records.into_iter());
        //                 let diff = Self::diff_create(&old, &new);
        //                 if diff.len() > 0 {
        //                     let grpc_clients = Self::create_grpc_conns(diff);
        //                     for (c, rec) in grpc_clients {
        //                         clients.insert(
        //                             rec.sub_key.as_str().to_string(),
        //                             ClientStat::Created(c),
        //                         );
        //                     }
        //                 } else {
        //                     old = new;
        //                 }
        //             }
        //             Err(e) => {
        //                 conn = Self::loop_retrive_conn(redis_client.clone());
        //                 error!("Query local active cluster grpc server failed!,e:{:?}; Start a new redis conn", e);
        //                 std::thread::sleep(Duration::from_secs(1));
        //             }
        //         }
        //     }
        // });
    }

    pub fn loop_retrive_conn(redis_client: RedisClient) -> Connection {
        loop {
            let conn = redis_client.get_connection();
            match conn {
                Err(e) => {
                    error!("Get redis conn failed!Retry in one second!e:{:?}", e);
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }
                Ok(conn) => return conn,
            }
        }
    }

    fn create_grpc_conns<'a>(addrs: Vec<&'a Record>) -> Vec<(SkyTracingClient, &'a Record)> {
        addrs
            .into_iter()
            .map(|addr| {
                let ip_port = addr.sub_key.as_str();
                let env = Environment::new(3);
                // TODO: config change
                let channel = ChannelBuilder::new(Arc::new(env)).connect(ip_port);
                (SkyTracingClient::new(channel), addr)
            })
            .collect::<Vec<(SkyTracingClient, &'a Record)>>()
    }

    fn diff_create<'a>(old: &'a HashSet<Record>, new: &'a HashSet<Record>) -> Vec<&'a Record> {
        old.difference(new).collect()
    }
}

impl<WrapReq, S> Stream for ClusterManager<WrapReq, S>
where
    S: Service<WrapReq>,
    WrapReq: Debug + Send + 'static + Clone,
{
    type Item = Change<i32, RingService<S, WrapReq>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let chg_opt = ready!(self.recv.poll_recv(cx));
        match chg_opt {
            Some(chg) => match chg {
                Change::Insert(id, client) => {
                    let res = split_client(client);
                    match res {
                        Ok((conn, client)) => {}
                        Err(e) => {
                            self.srv_map.remove(&id);
                            self.client_map.remove(&id);
                            return Poll::Pending;
                        }
                    }
                }
                Change::Remove(id) => {}
            },
            None => return Poll::Pending,
        }
        todo!()
    }
}

// impl<WrapReq, S> Stream for ClusterManager<WrapReq, RingService<S, WrapReq>>
// where
//     WrapReq: Clone + Send + 'static + Debug,
//     S: Service<WrapReq>,
// {
//     type Item = Change<i32, RingService<WrapReq, Req>>;

//     fn poll_next(
//         mut self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         let chg_opt = ready!(self.recv.poll_recv(cx));
//         match chg_opt {
//             Some(chg) => match chg {
//                 Change::Insert(id, client) => {

//                     // self.srv_map.insert(k, v);
//                 }
//                 Change::Remove(id) => {}
//             },
//             None => return Poll::Pending,
//         }
//         todo!()
//     }
// }

async fn services_monitor<D: Discover>(services: D) {
    pin_mut!(services);
    while let Some(Ok(change)) = poll_fn(|cx| services.as_mut().poll_discover(cx)).await {
        match change {
            Change::Insert(key, svc) => {
                // a new service with identifier `key` was discovered
            }
            Change::Remove(key) => {
                // the service with identifier `key` has gone away
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client() {}
}
