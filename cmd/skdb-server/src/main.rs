use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use clap::Parser;
use crossbeam_channel::*;
use grpcio::*;
use serv::service::*;
use skdb::com::batch::BatchSystem;
use skdb::com::batch::FsmTypes;
use skdb::com::router::Router;
use skdb::com::sched::NormalScheduler;
use skdb::tag::fsm::TagFsm;
use skproto::tracing::*;

mod serv;
/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value_t = 9000)]
    port: u16,

    #[clap(short, long, default_value = "127.0.0.1")]
    ip: String,
}

fn main() {
    let args = Args::parse();
    println!("service port:{}, ip:{}", args.port, args.ip);

    let (s, r) = unbounded::<FsmTypes<TagFsm>>();
    let fsm_sche = NormalScheduler { sender: s };
    let atomic = AtomicUsize::new(1);
    let router = Router::new(fsm_sche, Arc::new(atomic));

    let mut batch_system = BatchSystem::new(router.clone(), r, 1, 500);
    batch_system.start_poller("tag poll batch system".to_string(), 500);
    let skytracing = SkyTracingService::new_spawn(router.clone());
    let env = Environment::new(1);
    let service = create_sky_tracing(skytracing);
    let mut server = ServerBuilder::new(Arc::new(env))
        .bind(args.ip, args.port)
        .register_service(service)
        .build()
        .unwrap();
    server.start();
    use std::thread;
    thread::park();
}
