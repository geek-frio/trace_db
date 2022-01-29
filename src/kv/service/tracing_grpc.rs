// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_SKY_TRACING_PUSH_MSGS: ::grpcio::Method<super::tracing::StreamReqData, super::tracing::StreamResData> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/tracing.SkyTracing/pushMsgs",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct SkyTracingClient {
    client: ::grpcio::Client,
}

impl SkyTracingClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        SkyTracingClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn push_msgs_opt(&self, opt: ::grpcio::CallOption) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::tracing::StreamReqData>, ::grpcio::ClientDuplexReceiver<super::tracing::StreamResData>)> {
        self.client.duplex_streaming(&METHOD_SKY_TRACING_PUSH_MSGS, opt)
    }

    pub fn push_msgs(&self) -> ::grpcio::Result<(::grpcio::ClientDuplexSender<super::tracing::StreamReqData>, ::grpcio::ClientDuplexReceiver<super::tracing::StreamResData>)> {
        self.push_msgs_opt(::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Output=()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait SkyTracing {
    fn push_msgs(&mut self, ctx: ::grpcio::RpcContext, _stream: ::grpcio::RequestStream<super::tracing::StreamReqData>, sink: ::grpcio::DuplexSink<super::tracing::StreamResData>);
}

pub fn create_sky_tracing<S: SkyTracing + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_duplex_streaming_handler(&METHOD_SKY_TRACING_PUSH_MSGS, move |ctx, req, resp| {
        instance.push_msgs(ctx, req, resp)
    });
    builder.build()
}
