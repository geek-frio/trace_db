use std::sync::Arc;

use clap::Parser;
use futures::SinkExt;
use futures::TryStreamExt;
use futures_util::{FutureExt as _, TryFutureExt as _, TryStreamExt as _};
use grpcio::*;
use skproto::tracing::*;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value_t = 9000)]
    port: u16,

    #[clap(short, long, default_value = "127.0.0.1")]
    ip: String,
}

#[derive(Clone)]
struct SkyTracingService;
impl SkyTracing for SkyTracingService {
    fn push_msgs(
        &mut self,
        ctx: ::grpcio::RpcContext,
        mut stream: ::grpcio::RequestStream<StreamReqData>,
        mut sink: ::grpcio::DuplexSink<StreamResData>,
    ) {
        let f = async move {
            let mut res_data = StreamResData::default();
            res_data.set_data("here comes response data".to_string());
            while let Some(data) = stream.try_next().await? {
                println!("Now we have the data:{:?}", data);
                sink.send((res_data.clone(), WriteFlags::default())).await?;
            }
            sink.close().await?;
            Ok(())
        }
        .map_err(|_: grpcio::Error| println!("xx"))
        .map(|_| ());

        ctx.spawn(f)
    }
}

fn main() {
    let args = Args::parse();
    println!("service port:{}, ip:{}", args.port, args.ip);

    let skytracing = SkyTracingService;
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
