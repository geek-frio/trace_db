use std::sync::Arc;

use clap::Parser;
use grpcio::*;
use skdb::kv::service::tracing_grpc;
use skdb::kv::service::*;

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

    let sky_tracing = SkyTracingService;
    let service = tracing_grpc::create_sky_tracing(sky_tracing);
    let env = Environment::new(1);
    let mut server = ServerBuilder::new(Arc::new(env))
        .bind(args.ip, args.port)
        .register_service(service)
        .build()
        .unwrap();
    server.start();
    println!("Server started!");
    use std::thread;
    thread::park();
}
