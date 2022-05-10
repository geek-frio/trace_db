use super::bus::RemoteMsgPoller;
use anyhow::Error as AnyError;
use futures::channel::mpsc::Sender;
use futures::SinkExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures_util::{FutureExt as _, TryFutureExt as _};
use grpcio::RpcStatus;
use grpcio::RpcStatusCode;
use grpcio::WriteFlags;
use skdb::com::config::GlobalConfig;
use skdb::com::index::IndexAddr;
use skdb::com::index::IndexPath;
use skdb::tag::engine::*;
use skdb::tag::fsm::SegmentDataCallback;
use skdb::*;
use skproto::tracing::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::DocAddress;
use tantivy::Index;
use tantivy::Score;
use tantivy_common::BinarySerializable;
use tantivy_query_grammar::*;

#[derive(Clone)]
pub struct SkyTracingService {
    sender: Sender<SegmentDataCallback>,
    config: Arc<GlobalConfig>,
    // All the tag engine share the same index schema
    tracing_schema: Schema,
    index_map: Arc<Mutex<HashMap<IndexAddr, Index>>>,
}

impl SkyTracingService {
    // do new and spawn two things
    pub fn new(
        config: Arc<GlobalConfig>,
        data_sender: Sender<SegmentDataCallback>,
    ) -> SkyTracingService {
        // let (s, r) = crossbeam_channel::unbounded::<SegmentDataCallback>();
        // let (s, r) = channel(5000);
        let schema = Self::init_sk_schema();
        let index_map = Arc::new(Mutex::new(HashMap::default()));
        let service = SkyTracingService {
            sender: data_sender,
            config,
            tracing_schema: schema.clone(),
            index_map: index_map.clone(),
        };
        // thread::spawn(move || {
        //     let r = Self::recv_and_process(r, router1, index_path, schema, index_map);
        //     if let Err(e) = r {
        //         println!(
        //             "Channel of getting sky segment is empty and disconnected!e:{:?}",
        //             e
        //         );
        //     }
        // });
        // Periodicily send Tick event to notify Fsm
        // Every 5 secs we force active fsm to notify
        // TOKIO_RUN.spawn(
        //     async move {
        //         trace!("Sent tick event to TagPollHandler");
        //         let _ = router.notify_all_idle_mailbox();
        //         sleep(Duration::from_secs(10))
        //     }
        //     .instrument(info_span!("tick_event")),
        // );
        service
    }

    // fn recv_and_process(
    //     recv: Receiver<SegmentDataCallback>,
    //     router: Router<TagFsm, NormalScheduler<TagFsm>>,
    //     index_path: String,
    //     schema: Schema,
    //     index_map: Arc<Mutex<HashMap<i64, Index>>>,
    // ) -> Result<(), AnyError> {
    //     loop {
    //         let segment_callback = recv.recv()?;
    //         let _consume_span = trace_span!(
    //             "recv_and_process",
    //             trace_id = ?segment_callback.data.trace_id
    //         )
    //         .entered();
    //         trace!(parent: &segment_callback.span, "Has received the segment, try to route to mailbox.");

    //         let res =
    //             IndexPath::compute_index_addr(segment_callback.data.biz_timestamp as IndexAddr);
    //         trace!(index_addr = ?res, seq_id = segment_callback.data.get_meta().get_seqId(), trace_id = ?segment_callback.data.trace_id, "Has computed segment's address");
    //         match res {
    //             Ok(addr) => {
    //                 let sent_stat = router.send(addr, segment_callback);
    //                 trace!(index_addr = ?res, "Has computed segment's address");
    //                 match sent_stat {
    //                     // Can't find the mailbox which is dependent by this segment
    //                     Either::Right(msg) => {
    //                         // Mailbox not exists, so we regists a new one
    //                         trace!(
    //                             index_addr = addr,
    //                             "Can't find addr's mailbox, create a new one"
    //                         );
    //                         let (s, r) = crossbeam_channel::unbounded();
    //                         // TODO: use config struct
    //                         let mut engine =
    //                             TracingTagEngine::new(addr, index_path.clone(), schema.clone());
    //                         // TODO: error process logic is emitted currently
    //                         let res = engine.init();
    //                         match res {
    //                             Ok(index) => {
    //                                 index_map.lock().unwrap().insert(addr, index);
    //                                 let fsm = Box::new(TagFsm::new(r, None, engine));
    //                                 let state_cnt = Arc::new(AtomicUsize::new(0));
    //                                 let mailbox = BasicMailbox::new(s, fsm, state_cnt);
    //                                 let fsm = mailbox.take_fsm();
    //                                 if let Some(mut f) = fsm {
    //                                     f.mailbox = Some(mailbox.clone());
    //                                     mailbox.release(f);
    //                                 }
    //                                 router.register(addr, mailbox);
    //                                 router.send(addr, msg);
    //                             }
    //                             // TODO: This error can not fix by retry, so we just ack this msg
    //                             // Maybe we should store this msg anywhere
    //                             Err(e) => {
    //                                 error!(index_addr = addr, seq_id = msg.data.get_meta().get_seqId(), trace_id = ?msg.data.trace_id, "Init addr's TagEngine failed!Just callback this data.e:{:?}", e);
    //                                 msg.callback.callback(msg.data.get_meta().get_seqId());
    //                             }
    //                         }
    //                     }
    //                     Either::Left(v) => {
    //                         if let Err(e) = v {
    //                             error!(
    //                                 index_addr = addr,
    //                                 "Mailbox's receiver has dropped, {:?}", e
    //                             );
    //                         }
    //                         panic!("Should never come here!");
    //                     }
    //                 }
    //             }
    //             Err(e) => {
    //                 error!(
    //                     biz_timestamp = segment_callback.data.biz_timestamp,
    //                     "Incorrect segment data format, skip this data. e:{:?}", e
    //                 );
    //                 continue;
    //             }
    //         }
    //     }
    // }

    pub fn init_sk_schema() -> Schema {
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
            while let Some(_) = stream.try_next().await? {
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
        _: ::grpcio::RpcContext,
        stream: ::grpcio::RequestStream<SegmentData>,
        sink: ::grpcio::DuplexSink<SegmentRes>,
    ) {
        let mut msg_poller = RemoteMsgPoller::new(stream.fuse(), sink, self.sender.clone());
        TOKIO_RUN.spawn(async move {
            msg_poller.loop_poll().await;
        });
    }

    // Query data in local index.
    // TODO: Index Searcher search request currently is synchronized block operation, so it may block the grpc thread
    //   Maybe we should use a seperate pool to process the search request, or we can extend tantivy's Searcher to support
    //   callback search operation.
    fn query_sky_segments(
        &mut self,
        _: ::grpcio::RpcContext,
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

        if req.seg_range.is_none() {
            sink.fail(RpcStatus::with_message(
                RpcStatusCode::INVALID_ARGUMENT,
                "Invalid argument, addr is not given!".to_string(),
            ));
            return;
        }

        let addr = req.seg_range.as_ref().unwrap().addr;
        let res = self.index_map.lock();
        let idx = res
            .map(|a| {
                let idx = a.get(&addr).map(|a| a.clone());
                idx
            })
            .ok()
            .unwrap_or_else(|| {
                let index_path =
                    IndexPath::gen_idx_path(addr.to_owned(), self.config.index_dir.clone());
                let res = Index::create_in_dir(index_path, self.tracing_schema.clone());
                res.ok()
            });
        match idx {
            Some(index) => {
                let res = search(index, self.tracing_schema.to_owned(), &req.query, &req);
                match res {
                    Err(e) => {
                        let display = format!("{}", e);
                        println!("backtrace:{:?}", display);
                        sink.fail(RpcStatus::with_message(RpcStatusCode::INTERNAL, display));
                    }
                    Ok(scores) => {
                        let mut res = SkySegmentRes::new();
                        res.set_score_doc(scores.into());
                        sink.success(res);
                    }
                }
            }
            None => {
                sink.fail(RpcStatus::with_message(
                    RpcStatusCode::INVALID_ARGUMENT,
                    "Invalid argument, addr is not given!".to_string(),
                ));
            }
        }
    }
}

// async fn handshake_exec(sink: &mut DuplexSink<SegmentRes>) -> Result<(), AnyError> {
//     let conn_id = CONN_MANAGER.gen_new_conn_id();
//     let mut resp = SegmentRes::new();
//     let mut meta = Meta::new();
//     meta.connId = conn_id;
//     meta.field_type = Meta_RequestType::HANDSHAKE;
//     info!(meta = ?meta, %conn_id, resp = ?resp, "Send handshake resp");
//     resp.set_meta(meta);
//     // We don't care handshake is success or not, client should retry for this
//     sink.send((resp, WriteFlags::default())).await?;
//     println!("Has sent handshake response!");
//     info!("Send handshake resp success");
//     let _ = sink.flush().await;
//     Ok(())
// }

fn search(
    index: Index,
    schema: Schema,
    query: &str,
    param: &SkyQueryParam,
) -> Result<Vec<ScoreDoc>, AnyError> {
    let reader = index.reader()?;
    let searcher = reader.searcher();

    let default_fields = schema.fields().map(|a| a.0).collect::<Vec<Field>>();
    let query_parser = QueryParser::for_index(&index, default_fields);
    let query = query_parser.parse_query(query)?;

    let collector = TopDocs::with_limit((param.limit + param.offset) as usize);
    let top_docs: Vec<(Score, DocAddress)> = searcher.search(&query, &collector)?;

    let score_docs = top_docs
        .into_iter()
        .map(|(score, addr)| {
            let mut score_doc = ScoreDoc::new();
            score_doc.set_score(score);
            let doc = searcher.doc(addr);
            if let Ok(doc) = doc {
                let mut v = Vec::new();
                let _ = <Document as BinarySerializable>::serialize(&doc, &mut v);
                score_doc.set_doc(v);
            }
            score_doc
        })
        .collect::<Vec<ScoreDoc>>();
    Ok(score_docs)
}
