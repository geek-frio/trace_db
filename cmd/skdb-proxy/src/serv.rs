use crossbeam::channel::Sender;
use grpcio::{Environment, ServerBuilder};
use skdb::com::{
    config::GlobalConfig,
    util::{CountTracker, LruCache},
};
use skproto::tracing::SegmentData;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tower::Service;

enum BatchStatus {
    Created,       // Batch received from remote client
    ServiceCalled, // Request has been called waiting to be sent
    Acked,         // Batch has been acked
}

struct Batch<T> {
    id: i64,
    data: Vec<T>,
}

struct ProxyService<S, Req>
where
    S: Service<Req>,
{
    sender: Sender<Batch<Req>>,
    global_config: Arc<GlobalConfig>,
    batch_stat: LruCache<i64, BatchStatus, CountTracker>,
    sinkers: Vec<S>,
    marker: PhantomData<Req>,
}

impl<S, Req> ProxyService<S, Req>
where
    S: Service<Req>,
{
    fn new(sender: Sender<Batch<Req>>, config: Arc<GlobalConfig>) -> ProxyService<S, Req> {
        ProxyService {
            sender,
            global_config: config,
            batch_stat: LruCache::with_capacity(100),
            sinkers: Vec::new(),
            marker: PhantomData,
        }
    }
}

fn start_proxy_service(local_ip: &str, global_config: Arc<GlobalConfig>) {
    let (send, recv) = crossbeam_channel::unbounded::<SegmentData>();
    // let skytracing = SkyTracingService::new(global_config.clone(), sender);
    // let service = create_sky_tracing(skytracing);
    // let env = Environment::new(1);
    // let mut server = ServerBuilder::new(Arc::new(env))
    //     .bind(local_ip, global_config.grpc_port as u16)
    //     .register_service(service)
    //     .build()
    //     .unwrap();
    // server.start();
    todo!()
}
