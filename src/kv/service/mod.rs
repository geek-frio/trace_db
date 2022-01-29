mod tracing;
mod tracing_grpc;


#[derive(Clone)]
struct SkyTracingService;

impl tracing_grpc::SkyTracing for SkyTracingService {
    fn push_msgs(&mut self, ctx: ::grpcio::RpcContext, _stream: ::grpcio::RequestStream<tracing::StreamReqData>, sink: ::grpcio::DuplexSink<tracing::StreamResData>) {
        println!("xx");
    }
}

#[cfg(test)]
mod tests {
    use super::tracing_grpc;
    use crate::kv::service::SkyTracingService;

    #[test]
    fn test_start_server() {
        let sky_tracing = SkyTracingService;
        let service = tracing_grpc::create_sky_tracing(sky_tracing);

    }
}