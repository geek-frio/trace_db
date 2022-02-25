use super::*;
use ack::*;
use futures::task::SpawnExt;
use futures::SinkExt;
use futures::TryStreamExt;
use futures_util::{FutureExt as _, TryFutureExt as _, TryStreamExt as _};
use grpcio::*;
use skproto::tracing::*;
use tokio::runtime::Runtime;
#[derive(Clone)]
pub struct SkyTracingService;

impl SkyTracing for SkyTracingService {
    // Just for push msg test
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

    fn push_segments(
        &mut self,
        ctx: ::grpcio::RpcContext,
        mut stream: ::grpcio::RequestStream<SegmentData>,
        mut sink: ::grpcio::DuplexSink<SegmentRes>,
    ) {
        // Logic for handshake
        let handshake_exec = |_: SegmentData, mut sink: grpcio::DuplexSink<SegmentRes>| async {
            let conn_id = CONN_MANAGER.gen_new_conn_id();
            let mut resp = SegmentRes::new();
            let mut meta = Meta::new();
            meta.connId = conn_id;
            meta.field_type = Meta_RequestType::HANDSHAKE;
            resp.set_meta(meta);
            // We don't care handshake is success or not, client should retry for this
            let _ = sink.send((resp, WriteFlags::default())).await;
            println!("Has sent handshake response!");
            let _ = sink.flush().await;
            return sink;
        };

        // TODO: change a better name, process logic currently is not involved
        let mut ack_ctl = AckCtl::new();
        // Logic for processing segment datas
        let get_data_exec = async move {
            while let Some(data) = stream.try_next().await.unwrap() {
                if !data.has_meta() {
                    println!("Has no meta ,quit");
                    continue;
                }
                match data.get_meta().get_field_type() {
                    Meta_RequestType::HANDSHAKE => {
                        sink = handshake_exec(data, sink).await;
                    }
                    Meta_RequestType::TRANS => {
                        println!("TRANS data coming, data:{:?}", data);
                        ack_ctl.process_timely_ack_ctl(data, &mut sink).await;
                    }
                    _ => {
                        todo!();
                    }
                }
            }
        };
        ctx.spawn(get_data_exec);
    }
}
