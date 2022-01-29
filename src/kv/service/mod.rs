use crate::TOKIO_RUN;
use futures::StreamExt;

pub mod tracing;
pub mod tracing_grpc;

#[derive(Clone)]
pub struct SkyTracingService;

impl tracing_grpc::SkyTracing for SkyTracingService {
    fn push_msgs(
        &mut self,
        _: ::grpcio::RpcContext,
        stream: ::grpcio::RequestStream<tracing::StreamReqData>,
        sink: ::grpcio::DuplexSink<tracing::StreamResData>,
    ) {
        let f = async {
            let mut stream = stream.fuse();
            let record = stream.next();
            let record = record.await;
            println!("here comes a record:{:?}", record);
        };
        TOKIO_RUN.spawn(f);
    }
}

#[cfg(test)]
mod tests {}
