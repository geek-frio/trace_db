use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::com::redis::{Record, RedisTTLSet, LEASE_TIME_OUT};
use chashmap::CHashMap;
use grpcio::{ChannelBuilder, Environment};
use redis::{Client as RedisClient, Connection};
use skproto::tracing::SkyTracingClient;
use std::fmt::Debug;
use tower::{buffer::Buffer, limit::RateLimit};
use tracing::error;

use super::{ChangeResend, RingService, RingServiceReqEvent, TracingSinker, TracingStreamer};

pub struct ClusterManager<Cli, WrapReq, Req, Resp>
where
    WrapReq: std::fmt::Debug + Clone + ChangeResend + Send + 'static,
    Req: Send + 'static + From<WrapReq> + Clone,
    Cli: Send + 'static,
{
    clients: Arc<CHashMap<String, ClientStat<Cli, WrapReq, Req, Resp>>>,
}

pub enum ClientStat<Cli, WrapReq, Req, Resp>
where
    WrapReq: std::fmt::Debug + Clone + ChangeResend + Send + 'static,
    Req: Send + 'static + From<WrapReq> + Clone,
    Cli: Send + 'static,
{
    Created(Cli),
    StreamCreated(
        Cli,
        Option<(
            Buffer<
                RateLimit<RingService<TracingSinker<WrapReq, Req>, WrapReq>>,
                RingServiceReqEvent<WrapReq>,
            >,
            TracingStreamer<Resp>,
        )>,
    ),
}

impl<WrapReq, Req, Resp> ClusterManager<SkyTracingClient, WrapReq, Req, Resp>
where
    WrapReq: Debug + Clone + ChangeResend + Send + 'static,
    Req: Send + 'static + From<WrapReq> + Clone,
{
    pub fn new() -> ClusterManager<SkyTracingClient, WrapReq, Req, Resp> {
        ClusterManager {
            clients: Arc::new(CHashMap::new()),
        }
    }

    pub fn init(&mut self, redis_client: RedisClient) {
        let redis_client = redis_client.clone();
        let clients = self.clients.clone();
        let mut conn = Self::loop_retrive_conn(redis_client.clone());
        std::thread::spawn(move || {
            let mut old = HashSet::new();
            loop {
                let redis_ttlset = RedisTTLSet::new(LEASE_TIME_OUT);
                let res = redis_ttlset.query_all(&mut conn);
                match res {
                    Ok(records) => {
                        let new = HashSet::from_iter(records.into_iter());
                        let diff = Self::diff_create(&old, &new);
                        if diff.len() > 0 {
                            let grpc_clients = Self::create_grpc_conns(diff);
                            for (c, rec) in grpc_clients {
                                clients.insert(
                                    rec.sub_key.as_str().to_string(),
                                    ClientStat::Created(c),
                                );
                            }
                        } else {
                            old = new;
                        }
                    }
                    Err(e) => {
                        conn = Self::loop_retrive_conn(redis_client.clone());
                        error!("Query local active cluster grpc server failed!,e:{:?}; Start a new redis conn", e);
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client() {}
}
