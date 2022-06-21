use std::task::Poll;
use std::{marker::PhantomData, sync::Arc};

use crate::client::trans::Transport;
use chashmap::CHashMap;
use futures::never::Never;
use futures::{ready, FutureExt, Stream};
use skproto::tracing::{Meta_RequestType, SegmentData, SkyTracingClient};
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use tower::balance::p2c::Balance;
use tower::load::Load;
use tower::{discover::Change, Service};

use super::grpc_cli::split_client;
use super::service::Endpoint;

pub struct ClusterManager<Req, S>
where
    S: Service<Req>,
{
    recv: Receiver<ClientEvent>,
    clients: Arc<CHashMap<i32, SkyTracingClient>>,
    req_marker: PhantomData<Req>,
    service_marker: PhantomData<S>,
}

impl<Req, S> ClusterManager<Req, S>
where
    S: Service<Req> + Unpin + 'static,
    Req: Debug + Send + 'static + Clone + Unpin,
{
    fn new(recv: Receiver<ClientEvent>) -> ClusterManager<Req, S> {
        ClusterManager {
            recv,
            clients: Arc::new(CHashMap::new()),
            req_marker: PhantomData,
            service_marker: PhantomData,
        }
    }
}

enum ClientEvent {
    DropEvent(i32),
    NewClient((i32, SkyTracingClient)),
}

impl<Req, S> Stream for ClusterManager<Req, S>
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

async fn make_service<S>(cluster: ClusterManager<SegmentData, S>) -> impl Service<SegmentData>
where
    S: Service<SegmentData> + Unpin + 'static,
{
    Balance::new(cluster)
}

// async fn req<S>(
//     req: SegmentData,
//     cluster: ClusterManager<SegmentData, S>,
// ) -> impl Service<SegmentData>
// where
//     S: Service<SegmentData> + Load + Unpin + 'static,
// {
//     Balance::new(cluster)
// }

// impl<WrapReq, S> ClusterManager<WrapReq, S>
// where
//     S: Service<WrapReq>,
// {
//     pub fn new(recv: Receiver<Change<i32, SkyTracingClient>>) -> ClusterManager<WrapReq, S> {
//         ClusterManager {
//             recv,
//             srv_map: HashMap::new(),
//             client_map: HashMap::new(),
//             wrap_req_marker: PhantomData,
//         }
//     }

//     pub fn init(&mut self, redis_client: RedisClient) {
//         let redis_client = redis_client.clone();
//         // let clients = self.clients.clone();
//         let mut conn = Self::loop_retrive_conn(redis_client.clone());
//     }

//     pub fn loop_retrive_conn(redis_client: RedisClient) -> Connection {
//         loop {
//             let conn = redis_client.get_connection();
//             match conn {
//                 Err(e) => {
//                     error!("Get redis conn failed!Retry in one second!e:{:?}", e);
//                     std::thread::sleep(Duration::from_secs(1));
//                     continue;
//                 }
//                 Ok(conn) => return conn,
//             }
//         }
//     }

//     fn create_grpc_conns<'a>(addrs: Vec<&'a Record>) -> Vec<(SkyTracingClient, &'a Record)> {
//         addrs
//             .into_iter()
//             .map(|addr| {
//                 let ip_port = addr.sub_key.as_str();
//                 let env = Environment::new(3);
//                 // TODO: config change
//                 let channel = ChannelBuilder::new(Arc::new(env)).connect(ip_port);
//                 (SkyTracingClient::new(channel), addr)
//             })
//             .collect::<Vec<(SkyTracingClient, &'a Record)>>()
//     }

//     fn diff_create<'a>(old: &'a HashSet<Record>, new: &'a HashSet<Record>) -> Vec<&'a Record> {
//         old.difference(new).collect()
//     }
// }
