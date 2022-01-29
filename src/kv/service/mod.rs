mod tracing;
mod tracing_grpc;

#[derive(Clone)]
struct SkyTracingService;

impl tracing_grpc::SkyTracing for SkyTracingService {
    fn push_msgs(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _stream: ::grpcio::RequestStream<tracing::StreamReqData>,
        sink: ::grpcio::DuplexSink<tracing::StreamResData>,
    ) {
        println!("xx");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::tracing_grpc;
    use crate::kv::service::SkyTracingService;
    use ::grpcio::ServerBuilder;
    use crossbeam_channel::internal::SelectHandle;
    use grpcio::Environment;
    #[test]
    fn test_start_server() {
        let sky_tracing = SkyTracingService;
        let service = tracing_grpc::create_sky_tracing(sky_tracing);
        let env = Environment::new(1);
        let mut server = ServerBuilder::new(Arc::new(env))
            .bind("127.0.0.1", 9000)
            .register_service(service)
            .build()
            .unwrap();
        println!("Server started!");
        server.start();
        use std::thread;
        thread::park();
    }
}
