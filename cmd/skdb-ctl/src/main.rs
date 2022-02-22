use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::SinkExt;
use futures_util::{join, stream};
use futures_util::{
    FutureExt as _, SinkExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
};
use grpcio::Environment;
use grpcio::{
    ChannelBuilder, ClientStreamingSink, DuplexSink, EnvBuilder, RequestStream, RpcContext,
    ServerBuilder, ServerStreamingSink, UnarySink, WriteFlags,
};
use skdb::TOKIO_RUN;
use skproto::diner::*;
use skproto::diner_grpc::*;
use skproto::tracing::*;
use skproto::tracing_grpc::*;
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
    // let client = DinerClient::new(channel);
    // let mut order = Order::new();
    // let items = order.mut_items();
    // items.push(Item::SPAM);
    // items.push(Item::HAM);
    // println!("Query result is:{:?}", client.eat(&order));

    // let mut sink = duplex.0;
    // // let source = duplex.1;

    let client = SkyTracingClient::new(channel);
    let exec_f = async move {
        let (mut sink, mut receiver) = client.push_msgs().unwrap();

        let mut send_data = vec![];
        for i in 0..10 {
            let mut p = StreamReqData::default();
            p.set_data("xxxxxx".to_string());
            send_data.push(p);
        }

        let send_stream = stream::iter(send_data);

        let mut s = send_stream.map(|item| Ok((item, WriteFlags::default())));
        println!("send result is:{:?}", sink.send_all(&mut s).await);
        while let Ok(Some(r)) = receiver.try_next().await {
            println!("result is:{:?}", r);
        }
    };
    TOKIO_RUN.block_on(exec_f);
}
