use std::sync::Arc;

use clap::Parser;
use futures::SinkExt;
use grpcio::ChannelBuilder;
use grpcio::Environment;
use grpcio::WriteFlags;
use skdb::kv::service::tracing::StreamReqData;
use skdb::kv::service::tracing_grpc::SkyTracingClient;
use skdb::TOKIO_RUN;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long, default_value_t = 9000)]
    port: usize,

    #[clap(short, long, default_value = "127.0.0.1")]
    ip: String,
}

fn main() {
    let args = Args::parse();
    println!("port:{}, ip:{}", args.port, args.ip);
    let env = Environment::new(3);
    let channel = ChannelBuilder::new(Arc::new(env)).connect("127.0.0.1:9000");
    let client = SkyTracingClient::new(channel);
    let duplex = client.push_msgs().unwrap();
    let mut sink = duplex.0;
    // let source = duplex.1;

    TOKIO_RUN.block_on(async move {
        println!("Start to send data..");
        let mut req_data = StreamReqData::new();
        req_data.data = "xxxxxxxlll".to_string();
        let tuple = (req_data, WriteFlags::default());
        let res = sink.send(tuple).await;
        println!("sink res is:{:?}", res);
    });
}
