use super::bus::RemoteMsgPoller;
use super::ShutdownEvent;
use crate::client::trans::TransportErr;
use crate::com::index::ConvertIndexAddr;
use crate::com::index::IndexAddr;
use crate::com::index::MailKeyAddress;
use crate::conf::GlobalConfig;
use crate::tag::fsm::SegmentDataCallback;
use crate::tag::schema::TRACING_SCHEMA;
use crate::tag::search::Searcher;
use crate::*;
use anyhow::Error as AnyError;
use futures::StreamExt;
use grpcio::RpcStatus;
use grpcio::RpcStatusCode;
use skproto::tracing::*;
use std::collections::hash_map::Entry;
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
use tower::ServiceExt;
use tracing::error;

#[derive(Clone)]
pub struct SkyTracingService {
    sender: UnboundedSender<SegmentDataCallback>,
    tracing_schema: Schema,
    index_map: Arc<Mutex<HashMap<IndexAddr, Index>>>,
    service: BoxCloneService<
        SegmentData,
        tokio::sync::oneshot::Receiver<Result<(), TransportErr>>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    searcher: Searcher<SkyTracingClient>,
    broad_shutdown_sender: tokio::sync::broadcast::Sender<ShutdownEvent>,
    config: Arc<GlobalConfig>,
}

impl SkyTracingService {
    pub fn new(
        batch_system_sender: UnboundedSender<SegmentDataCallback>,
        service: BoxCloneService<
            SegmentData,
            tokio::sync::oneshot::Receiver<Result<(), TransportErr>>,
            Box<dyn std::error::Error + Send + Sync>,
        >,
        config: Arc<GlobalConfig>,
        searcher: Searcher<SkyTracingClient>,
        broad_shutdown_sender: tokio::sync::broadcast::Sender<ShutdownEvent>,
    ) -> SkyTracingService {
        let index_map = Arc::new(Mutex::new(HashMap::default()));
        let service = SkyTracingService {
            sender: batch_system_sender,
            tracing_schema: TRACING_SCHEMA.clone(),
            index_map: index_map.clone(),
            service,
            config,
            searcher,
            broad_shutdown_sender,
        };
        service
    }

    pub fn convert_to_segment(doc: Document) -> NormalSegment {
        let field_vals = doc.field_values();
        let mut seg = NormalSegment::new();

        for f in field_vals {
            let field = f.field();

            let name = TRACING_SCHEMA.get_field_name(field);

            // TODO: we should use macro function to simplify code
            match name {
                "zone" => {
                    let zone = f.value().text();
                    if let Some(s) = zone {
                        seg.set_zone(s.to_string());
                    }
                }
                "api_id" => {
                    let api_id = f.value().i64_value();
                    if let Some(v) = api_id {
                        seg.set_api_id(v as i32);
                    }
                }
                "service" => {
                    let service = f.value().text();
                    if let Some(s) = service {
                        seg.set_ser_key(s.to_string());
                    }
                }
                "biztime" => {
                    let biztime = f.value().u64_value();
                    if let Some(v) = biztime {
                        seg.set_biz_timestamp(v);
                    }
                }
                "trace_id" => {
                    let trace_id = f.value().text();
                    if let Some(s) = trace_id {
                        seg.set_trace_id(s.to_string());
                    }
                }
                "seg_id" => {
                    let seg_id = f.value().text();
                    if let Some(s) = seg_id {
                        seg.set_seg_id(s.to_string());
                    }
                }
                "payload" => {
                    let payload = f.value().text();
                    if let Some(s) = payload {
                        seg.set_payload(s.to_string());
                    }
                }
                _ => {
                    unreachable!()
                }
            }
        }
        todo!()
    }
}

impl SkyTracingService {
    fn check_create_idx<T: AsRef<std::path::Path>>(
        index_map: &Arc<Mutex<HashMap<i64, Index>>>,
        index_dir: T,
        mailkey: MailKeyAddress,
    ) -> Result<(), anyhow::Error> {
        let mut guard = index_map
            .lock()
            .map_err(|_| anyhow::Error::msg("lock failed!"))?;

        let entry = guard.entry(mailkey.into());
        if let Entry::Vacant(entry) = entry {
            let index = Self::open_index(index_dir, mailkey)?;
            entry.insert(index);
        }
        Ok(())
    }

    fn open_index<T: AsRef<std::path::Path>>(
        path: T,
        mailkey: MailKeyAddress,
    ) -> Result<Index, anyhow::Error> {
        let dir = mailkey.get_idx_path(path)?;
        tracing::warn!("Generated dir is:{:?}", dir);
        let res_open = Index::open_in_dir(dir);
        res_open.map_err(|e| anyhow::Error::msg(e.to_string()))
    }
}

impl SkyTracing for SkyTracingService {
    fn push_segments(
        &mut self,
        _ctx: ::grpcio::RpcContext,
        stream: ::grpcio::RequestStream<SegmentData>,
        sink: ::grpcio::DuplexSink<SegmentRes>,
    ) {
        let shutdown_recv = self.broad_shutdown_sender.subscribe();
        let msg_poller =
            RemoteMsgPoller::new(stream.fuse(), sink, self.sender.clone(), shutdown_recv);

        TOKIO_RUN.spawn(async move {
            let poll_res = msg_poller.loop_poll().await;
            if let Err(e) = poll_res {
                error!("Serious problem, loop poll failed!, sink will be dropped, client should reconnect e:{:?}", e);
            }
        });
    }

    fn query_sky_segments(
        &mut self,
        _: ::grpcio::RpcContext,
        req: SkyQueryParam,
        sink: ::grpcio::UnarySink<SkySegmentRes>,
    ) {
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

        let biztime = req.seg_range.unwrap().addr;
        let res_index = Self::check_create_idx(
            &self.index_map,
            &self.config.index_dir,
            biztime.with_index_addr(),
        );

        match res_index {
            Ok(_) => {
                let guard = self.index_map.lock().unwrap();
                let idx = guard.get(&biztime).unwrap();

                let res = search(idx, &self.tracing_schema, &req.query, req.offset, req.limit);
                match res {
                    Err(e) => {
                        let err_msg = format!("{}", e);
                        tracing::warn!("Query failed:{}", err_msg);
                        sink.fail(RpcStatus::with_message(RpcStatusCode::INTERNAL, err_msg));
                    }
                    Ok(scores) => {
                        let mut res = SkySegmentRes::new();
                        res.set_score_doc(scores.into());
                        sink.success(res);
                    }
                }
            }
            Err(e) => {
                let display = format!("{}", e);
                sink.fail(RpcStatus::with_message(RpcStatusCode::INTERNAL, display));
            }
        }
    }

    fn batch_req_segments(
        &mut self,
        _ctx: ::grpcio::RpcContext,
        req: BatchSegmentData,
        sink: ::grpcio::UnarySink<Stat>,
    ) {
        let service = self.service.clone();

        TOKIO_RUN.spawn(async move {
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

    fn dist_query_by_t_query(
        &mut self,
        _ctx: ::grpcio::RpcContext,
        req: SkyQueryParam,
        sink: ::grpcio::UnarySink<NormalSegmentsRes>,
    ) {
        let searcher = self.searcher.get_searcher();

        match searcher {
            Ok(searcher) => {
                let query = &req.query;
                let addr = req.get_seg_range().addr;

                let offset = req.offset;
                let limit = req.limit;

                if offset < 0 || limit < 0 {
                    let mut res = NormalSegmentsRes::new();
                    res.set_stat(gen_err_stat("Invalid offset or limit"));

                    sink.success(res);
                    return;
                }

                let res_search = searcher.search(query, addr, offset as usize, limit as usize);
                match res_search {
                    Ok(docs) => {
                        let mut v = Vec::new();

                        for doc in docs {
                            let normal_seg = Self::convert_to_segment(doc.doc);
                            v.push(normal_seg);
                        }

                        let mut normal_seg_res = NormalSegmentsRes::new();

                        normal_seg_res.set_segments(v.into());
                        normal_seg_res.set_stat(gen_ok_stat());

                        sink.success(normal_seg_res);
                    }
                    Err(e) => {
                        let mut res = NormalSegmentsRes::new();
                        res.set_stat(gen_err_stat(&e.to_string()));
                        sink.success(res);
                    }
                }
            }
            Err(_) => {
                let mut res = NormalSegmentsRes::new();
                res.set_stat(gen_err_stat("Get searcher failed!"));

                sink.success(res);
            }
        }
    }
}

async fn batch_req(
    mut service: BoxCloneService<
        SegmentData,
        tokio::sync::oneshot::Receiver<Result<(), TransportErr>>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    batch: BatchSegmentData,
) -> Stat {
    let mut recvs = Vec::new();

    let cur = std::time::Instant::now();
    for segment in batch.datas.into_iter() {
        let service = service.ready().await.unwrap();
        let resp = service.call(segment).await;

        match resp {
            Ok(recv) => {
                recvs.push(recv);
            }
            Err(_e) => {
                error!("Request error, break current batch request");
                let stat = gen_err_stat("Unknow err");
                return stat;
            }
        }
    }
    tracing::info!(
        "Send all the requests cost:{} ms",
        cur.elapsed().as_millis()
    );
    let cur = std::time::Instant::now();

    if recvs.len() > 0 {
        let _ = recvs.pop().unwrap().await;

        tracing::info!(
            "batch request complete! Wait complete cost:{}",
            cur.elapsed().as_millis()
        );
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

fn gen_err_stat(msg: &str) -> Stat {
    let mut stat = Stat::new();
    let mut err = Err::new();

    err.set_code(Err_ErrCode::Unexpected);
    err.set_desc(msg.to_string());

    stat.set_err(err);
    stat
}

fn search(
    index: &Index,
    schema: &Schema,
    query: &str,
    offset: i32,
    limit: i32,
) -> Result<Vec<ScoreDoc>, AnyError> {
    if offset > 1000 || limit > 250 {
        return Err(AnyError::msg("offset max is: 1000, limit max is: 250"));
    }

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let default_fields = schema.fields().map(|a| a.0).collect::<Vec<Field>>();
    let query_parser = QueryParser::for_index(&index, default_fields);
    let query = query_parser.parse_query(query)?;

    let collector = TopDocs::with_limit((limit + offset) as usize);
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

#[cfg(test)]
mod tests {
    use crate::{
        com::{
            gen::{_gen_data_binary, _gen_tag},
            index::ConvertIndexAddr,
        },
        log::init_console_logger,
        tag::{engine::TracingTagEngine, schema::TRACING_SCHEMA},
    };
    use chrono::Local;
    use skproto::tracing::SegmentData;
    use std::sync::Once;

    use super::{search, SkyTracingService};

    fn setup() {
        init_console_logger();
    }

    fn mock_some_data(mut engine: TracingTagEngine) -> (tantivy::Index, String) {
        let init: Once = Once::new();
        let mut captured_val = String::new();
        let mut checked_trace_id = String::new();

        // let mut engine = TracingTagEngine::new_for_test().unwrap();

        for i in 0..10 {
            for j in 0..10 {
                let now = Local::now();
                let mut record = SegmentData::new();
                let uuid = uuid::Uuid::new_v4();
                record.set_api_id(j);
                record.set_biz_timestamp(now.timestamp_nanos() as u64);
                record.set_zone(_gen_tag(3, 3, 'a'));
                record.set_seg_id(uuid.to_string());
                record.set_trace_id(uuid.to_string());
                init.call_once(|| {
                    captured_val.push_str(&uuid.to_string());
                    println!("i:{}; j:{}; uuid:{}", i, j, uuid.to_string());

                    checked_trace_id = uuid.to_string();
                });
                record.set_ser_key(_gen_tag(20, 3, 'e'));
                record.set_payload(_gen_data_binary());
                engine.add_record(&record);
            }
        }

        engine.flush().unwrap();
        (engine.index.clone(), checked_trace_id)
    }

    #[test]
    fn test_search_basics() {
        setup();

        let engine = TracingTagEngine::new_for_test().unwrap();
        let (index, checked_traceid) = mock_some_data(engine);

        let search_recs = search(&index, &TRACING_SCHEMA, "api_id:2", 0, 10).unwrap();
        assert_eq!(10, search_recs.len());

        let search_recs = search(
            &index,
            &TRACING_SCHEMA,
            &format!("trace_id:{}", checked_traceid),
            0,
            10,
        )
        .unwrap();
        assert_eq!(1, search_recs.len());

        let search_recs = search(&index, &TRACING_SCHEMA, "api_id:2", 1001, 10);
        assert!(search_recs.is_err());
    }

    #[test]
    fn test_skytracing_check_create_index() {
        setup();

        let test_map = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut mailkey_vec = Vec::new();

        for i in 1..11 {
            let now = chrono::Local::now();
            let now = now.checked_sub_signed(chrono::Duration::days(i)).unwrap();

            let timestamp = now.timestamp_millis();
            let mailkey_addr = timestamp.with_index_addr();

            mailkey_vec.push(mailkey_addr);

            let engine = TracingTagEngine::new(timestamp.with_index_addr(), "/tmp").unwrap();
            mock_some_data(engine);
        }

        for addr in mailkey_vec.into_iter() {
            tracing::info!("into addr is:{:?}", addr);
            SkyTracingService::check_create_idx(&test_map, "/tmp", addr).unwrap();
        }

        assert_eq!(test_map.lock().unwrap().keys().len(), 10);
    }
}
