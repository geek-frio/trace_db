use super::bus::RemoteMsgPoller;
use super::ShutdownSignal;
use crate::client::trans::TransportErr;
use crate::com::index::ConvertIndexAddr;
use crate::com::index::IndexAddr;
use crate::com::index::MailKeyAddress;
use crate::com::index::EXPIRED_DAYS;
use crate::tag::fsm::SegmentDataCallback;
use crate::tag::schema::TRACING_SCHEMA;
use crate::tag::search::Searcher;
use crate::*;
use anyhow::Error as AnyError;
use futures::StreamExt;
use grpcio::RpcStatus;
use grpcio::RpcStatusCode;
use skproto::tracing::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::DocAddress;
use tantivy::Index;
use tantivy::Score;
use tantivy_common::BinarySerializable;
use tantivy_query_grammar::*;
use tokio::select;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::sleep;
use tower::util::BoxCloneService;
use tower::Service;
use tracing::error;

#[derive(Clone)]
pub struct SkyTracingService {
    sender: UnboundedSender<SegmentDataCallback>,
    tracing_schema: Schema,
    index_map: Arc<Mutex<HashMap<IndexAddr, Index>>>,
    service: BoxCloneService<
        SegmentData,
        Result<(), TransportErr>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    searcher: Searcher<SkyTracingClient>,
    shutdown_signal: ShutdownSignal,
}

impl SkyTracingService {
    pub fn new(
        batch_system_sender: UnboundedSender<SegmentDataCallback>,
        service: BoxCloneService<
            SegmentData,
            Result<(), TransportErr>,
            Box<dyn std::error::Error + Send + Sync>,
        >,
        searcher: Searcher<SkyTracingClient>,
        shutdown_signal: ShutdownSignal,
    ) -> SkyTracingService {
        let index_map = Arc::new(Mutex::new(HashMap::default()));
        let service = SkyTracingService {
            sender: batch_system_sender,
            tracing_schema: TRACING_SCHEMA.clone(),
            index_map: index_map.clone(),
            service,
            searcher,
            shutdown_signal: shutdown_signal,
        };
        service
    }
}

impl SkyTracing for SkyTracingService {
    fn push_segments(
        &mut self,
        _ctx: ::grpcio::RpcContext,
        stream: ::grpcio::RequestStream<SegmentData>,
        sink: ::grpcio::DuplexSink<SegmentRes>,
    ) {
        let shutdown_signal = self.shutdown_signal.clone();
        let msg_poller =
            RemoteMsgPoller::new(stream.fuse(), sink, self.sender.clone(), shutdown_signal);

        TOKIO_RUN.spawn(async move {
            let poll_res = msg_poller.loop_poll().await;
            if let Err(e) = poll_res {
                error!("Serious problem, loop poll failed!, sink will be dropped, client should reconnect e:{:?}", e);
            }
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
                let mail_addr = addr.with_index_addr();

                if mail_addr.is_expired(EXPIRED_DAYS) {
                    None
                } else {
                    Index::create_in_dir(
                        MailKeyAddress::format_dir(mail_addr.timestamp).unwrap(),
                        self.tracing_schema.clone(),
                    )
                    .ok()
                }
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
                    "Invalid argument, addr is correct!".to_string(),
                ));
            }
        }
    }

    fn batch_req_segments(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: BatchSegmentData,
        sink: ::grpcio::UnarySink<Stat>,
    ) {
        let service = self.service.clone();
        ctx.spawn(async move {
            select! {
                _ = sleep(Duration::from_secs(8)) => {
                    sink.fail(RpcStatus::new(RpcStatusCode::ABORTED));
                }
                stat = batch_req(service, req) => {
                    let _ = sink.success(stat).await;
                }
            }
        });
    }

    fn dist_query_sky_segments(
        &mut self,
        _ctx: ::grpcio::RpcContext,
        _req: SkyQueryParam,
        sink: ::grpcio::UnarySink<SkySegmentRes>,
    ) {
        let searcher = self.searcher.get_searcher();
        match searcher {
            Ok(_searcher) => {
                todo!();
                // searcher.search(req.query.as_str(), addr, offset, limit);
                // searcher.dist_
            }
            Err(_e) => {
                sink.fail(RpcStatus::new(RpcStatusCode::INTERNAL));
            }
        }
        // grpcio::unimplemented_call!(ctx, sink)
    }
}

async fn batch_req(
    mut service: BoxCloneService<
        SegmentData,
        Result<(), TransportErr>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    batch: BatchSegmentData,
) -> Stat {
    for segment in batch.datas.into_iter() {
        let resp = service.call(segment).await;
        if let Err(_e) = resp {
            let stat = gen_err_stat();
            return stat;
        }
    }
    gen_ok_stat()
}

fn gen_ok_stat() -> Stat {
    let mut stat = Stat::new();
    let mut err = Err::new();
    err.set_code(Err_ErrCode::Ok);
    stat.set_err(err);
    stat
}

fn gen_err_stat() -> Stat {
    let mut stat = Stat::new();
    let mut err = Err::new();
    err.set_code(Err_ErrCode::Unexpected);
    stat.set_err(err);
    stat
}

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
