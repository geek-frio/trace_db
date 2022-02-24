use std::sync::Arc;

pub(crate) mod chan;
pub(crate) mod conn;
pub(crate) mod gen;

use clap::Parser;
use futures::SinkExt;
use futures::StreamExt;
use futures_util::stream;
use futures_util::TryStreamExt as _;
use grpcio::Environment;
use grpcio::{ChannelBuilder, WriteFlags};
use skdb::TOKIO_RUN;
use skproto::tracing::*;

use conn::Connector;
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
    let exec_f = async move {
        let (mut sink, mut receiver) = client.push_msgs().unwrap();

        let mut send_data = vec![];
        for _ in 0..10 {
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

#[cfg(test)]
mod tests {

    use super::conn::Connector;
    use tokio::runtime::Runtime;

    #[test]
    fn test_handshake_success() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let res = Connector::sk_connect_handshake().await;
            match res {
                Ok((_sink, _recv, conn_id)) => {
                    println!("connect id is:{:?}", conn_id);
                }
                Err(e) => {
                    println!("handshake failed, connect status is:{:?}", e);
                }
            }
        });
    }
}
