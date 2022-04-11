use super::*;
use ack::*;
use chrono::prelude::*;
use chrono::Duration;

use crossbeam_channel::*;
use futures::SinkExt;
use futures::TryStreamExt;
use futures_util::{FutureExt as _, TryFutureExt as _};
use grpcio::*;
use skdb::com::config::GlobalConfig;
use skdb::com::index::IndexAddr;
use skdb::com::index::IndexPath;
use skdb::com::mail::BasicMailbox;
use skdb::com::router::Either;
use skdb::com::router::Router;
use skdb::com::sched::NormalScheduler;
use skdb::tag::engine::TracingTagEngine;
use skdb::tag::engine::*;
use skdb::tag::fsm::TagFsm;
use skdb::*;
use skproto::tracing::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tantivy::schema::*;
use tantivy::Index;
use tantivy_query_grammar::*;

#[derive(Clone)]
pub struct SkyTracingService {
    sender: Sender<SegmentData>,
    router: Router<TagFsm, NormalScheduler<TagFsm>>,
    config: Arc<GlobalConfig>,
    // All the tag engine share the same index schema
    tracing_schema: Schema,
    index_map: Arc<Mutex<HashMap<String, Index>>>,
}

impl SkyTracingService {
    // do new and spawn two things
    pub fn new_spawn(
        router: Router<TagFsm, NormalScheduler<TagFsm>>,
        config: Arc<GlobalConfig>,
    ) -> SkyTracingService {
        let (s, r) = unbounded::<SegmentData>();
        let m_router = router.clone();
        let index_path = config.index_dir.clone();
        let schema = Self::init_tracing_schema();
        let mv_schema = schema.clone();
        thread::spawn(move || loop {
            let res = r.recv();
            match res {
                Ok(seg_data) => {
                    let addr = IndexPath::compute_index_addr(seg_data.biz_timestamp as IndexAddr);
                    match addr {
                        Ok(addr) => {
                            let res = m_router.send(addr, seg_data);
                            match res {
                                Either::Right(msg) => {
                                    // Mailbox not exists, so we regists a new one
                                    let (s, r) = unbounded();
                                    // TODO: use config struct
                                    let mut engine = TracingTagEngine::new(
                                        addr,
                                        index_path.clone(),
                                        mv_schema.clone(),
                                    );
                                    // TODO: error process logic is emitted currently
                                    let _ = engine.init();
                                    let fsm = Box::new(TagFsm {
                                        receiver: r,
                                        mailbox: None,
                                        engine,
                                        last_idx: 0,
                                        counter: 0,
                                    });
                                    let state_cnt = Arc::new(AtomicUsize::new(0));
                                    let mailbox = BasicMailbox::new(s, fsm, state_cnt);
                                    let fsm = mailbox.take_fsm();
                                    if let Some(mut f) = fsm {
                                        f.mailbox = Some(mailbox.clone());
                                        mailbox.release(f);
                                    }
                                    m_router.register(addr, mailbox);
                                    m_router.send(addr, msg);
                                }
                                Either::Left(_) => {
                                    continue;
                                }
                            }
                        }
                        Err(_) => {
                            println!("Invalid seg_data, data biztime has exceeded 30 days!");
                            continue;
                        }
                    }
                }
                Err(e) => {}
            }
        });

        SkyTracingService {
            sender: s,
            router,
            config,
            tracing_schema: schema.clone(),
            index_map: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    pub fn init_tracing_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field(ZONE, STRING);
        schema_builder.add_i64_field(API_ID, INDEXED);
        schema_builder.add_text_field(SERVICE, TEXT);
        schema_builder.add_u64_field(BIZTIME, STORED);
        schema_builder.add_text_field(TRACE_ID, STRING | STORED);
        schema_builder.add_text_field(SEGID, STRING);
        schema_builder.add_text_field(PAYLOAD, STRING);
        schema_builder.build()
    }
}

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
        let ack_ctl = AckCtl::new();
        let s = self.sender.clone();
        // Logic for processing segment datas
        let get_data_exec = async move {
            while let Some(data) = stream.try_next().await.unwrap() {
                if !data.has_meta() {
                    println!("Has no meta,quit");
                    continue;
                }
                match data.get_meta().get_field_type() {
                    Meta_RequestType::HANDSHAKE => {
                        sink = handshake_exec(data, sink).await;
                    }
                    Meta_RequestType::TRANS => {
                        let res = s.send(data);
                        // TODO: ACK logic will be designed later
                        // ack_ctl.process_timely_ack_ctl(data, &mut sink).await;
                    }
                    _ => {
                        todo!();
                    }
                }
            }
        };
        TOKIO_RUN.spawn(get_data_exec);
    }

    // Query data in local index.
    // TODO: Index Searcher search request currently is synchronized block operation, so it may block the grpc thread
    //   Maybe we should use a seperate pool to process the search request, or we can extend tantivy's Searcher to support
    //   callback search operation.
    fn query_sky_segments(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: SkyQueryParam,
        sink: ::grpcio::UnarySink<SkySegmentRes>,
    ) {
        // Check query param is ok
        if let Err(_) = parse_query(&req.query) {
            sink.fail(RpcStatus::with_message(
                RpcStatusCode::INVALID_ARGUMENT,
                "Invalid search query param".to_string(),
            ));
            return;
        }

        // let index = Index::create_in_dir(index_path, schema.clone())?;
        todo!()
    }
}
