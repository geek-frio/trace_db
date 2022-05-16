use skproto::tracing::SegmentData;
use std::fmt::Display;
use tantivy::directory::MmapDirectory;
use tantivy::error::TantivyError;
use tantivy::schema::*;
use tantivy::{Document, Index, IndexReader, IndexWriter};

use crate::com::index::{IndexAddr, MailKeyAddress};

pub const ZONE: &'static str = "zone";
pub const API_ID: &'static str = "api_id";
pub const SERVICE: &'static str = "service";
pub const BIZTIME: &'static str = "biztime";
pub const TRACE_ID: &'static str = "trace_id";
pub const SEGID: &'static str = "seg_id";
pub const PAYLOAD: &'static str = "payload";

pub struct TracingTagEngine {
    dir: Box<String>,
    // tag data gps (Every 15 minutes of a day, we create a new directory)
    addr: MailKeyAddress,
    index_writer: Option<IndexWriter>,
    index: Option<Index>,
    schema: Schema,
}

#[derive(PartialEq, Eq, Hash)]
pub enum TagField {
    Zone,
    ApiId,
    Service,
    Biztime,
    TraceId,
    SegId,
    Payload,
}

#[derive(Debug)]
pub enum TagEngineError {
    DirOpenFailed,
    WriterCreateFailed,
    RecordsCommitError(TantivyError),
    WriterNotInit,
    IndexNotExist,
    Other(TantivyError),
    IndexDirCreateFailed,
}

impl std::error::Error for TagEngineError {}

impl Display for TagEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("EngineError:{:?}", self))
    }
}

impl TracingTagEngine {
    pub fn new(addr: MailKeyAddress, dir: Box<String>, schema: Schema) -> TracingTagEngine {
        TracingTagEngine {
            dir: dir,
            addr,
            index_writer: None,
            index: None,
            schema,
        }
    }

    pub fn init(&mut self) -> Result<Index, TagEngineError> {
        // TODO: check if it is an outdated directory
        let path = self.addr.get_idx_path(self.dir.as_str());

        let result = std::fs::create_dir_all(path.as_path());
        if result.is_err() {
            return Err(TagEngineError::IndexDirCreateFailed);
        }

        // TODO: check open operation valid
        let dir = MmapDirectory::open(path.as_path()).unwrap();
        let index = Index::open_or_create(dir, self.schema.clone()).unwrap();
        self.index_writer = Some(index.writer(100_100_000).unwrap());
        self.index = Some(index.clone());
        return Ok(index);
    }

    pub fn add_record(&self, data: &SegmentData) -> u64 {
        let document = self.create_doc(data);
        if let Some(writer) = self.index_writer.as_ref() {
            return writer.add_document(document);
        }
        // Index writer is null, 0 return instead
        0
    }

    fn create_doc(&self, data: &SegmentData) -> Document {
        let mut doc = Document::new();
        doc.add(FieldValue::new(
            self.schema.get_field(TRACE_ID).unwrap(),
            Value::Str(data.trace_id.clone()),
        ));
        doc.add(FieldValue::new(
            self.schema.get_field(ZONE).unwrap(),
            Value::Str(data.zone.clone()),
        ));
        doc.add(FieldValue::new(
            self.schema.get_field(SEGID).unwrap(),
            Value::Str(data.seg_id.clone()),
        ));
        doc.add(FieldValue::new(
            self.schema.get_field(API_ID).unwrap(),
            Value::I64(data.api_id as i64),
        ));
        doc.add(FieldValue::new(
            self.schema.get_field(BIZTIME).unwrap(),
            Value::I64(data.biz_timestamp as i64),
        ));
        doc.add(FieldValue::new(
            self.schema.get_field(SERVICE).unwrap(),
            Value::Str(data.ser_key.clone()),
        ));
        doc.add(FieldValue::new(
            self.schema.get_field(PAYLOAD).unwrap(),
            Value::Str(data.payload.clone()),
        ));
        doc
    }

    pub fn flush(&mut self) -> Result<u64, TagEngineError> {
        if let Some(writer) = &mut self.index_writer {
            match writer.commit() {
                Ok(commit_idx) => return Ok(commit_idx),
                Err(e) => return Err(TagEngineError::RecordsCommitError(e)),
            }
        }
        return Err(TagEngineError::WriterNotInit);
    }

    pub fn reader(&self) -> Result<(IndexReader, &Index), TagEngineError> {
        match &self.index {
            Some(i) => i
                .reader()
                .map(|r| (r, i))
                .map_err(|e| TagEngineError::Other(e)),
            None => Err(TagEngineError::IndexNotExist),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::gen::{_gen_data_binary, _gen_tag};
    use std::sync::Once;
    use tantivy::{chrono::Local, collector::TopDocs, query::QueryParser};

    pub fn init_tracing_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("zone", STRING);
        schema_builder.add_i64_field("api_id", INDEXED);
        schema_builder.add_text_field("service", TEXT);
        schema_builder.add_u64_field("biztime", STORED);
        schema_builder.add_text_field("trace_id", STRING | STORED);
        schema_builder.add_text_field("seg_id", STRING);
        schema_builder.add_text_field("payload", STRING);
        schema_builder.build()
    }
    #[test]
    fn create_multiple_dir_test() {
        println!(
            "Create result is:{:?}",
            std::fs::create_dir_all("/tmp/test1/abc1")
        );
        println!(
            "Retry create result is:{:?}",
            std::fs::create_dir_all("/tmp/test1/abc1")
        );
    }

    #[test]
    fn test_normal_write() {
        let mut engine = TracingTagEngine::new(
            123,
            Box::new("/tmp/tantivy_records".to_string()),
            init_tracing_schema(),
        );

        println!("{:?}", engine.init());

        // let (reader, index) = engine.reader().unwrap();
        // let query_parser = QueryParser::for_index(index, vec![engine.get_field(TagField::ApiId)]);
        // let searcher = reader.searcher();

        let init: Once = Once::new();
        let mut captured_val = String::new();
        for i in 0..10 {
            for j in 0..100 {
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
                });
                record.set_ser_key(_gen_tag(20, 3, 'e'));
                record.set_payload(_gen_data_binary());
                engine.add_record(&record);
            }
            let e = &mut engine;
            println!("flushed result is:{:?}", e.flush());
        }

        let (reader, index) = engine.reader().unwrap();
        let query_parser =
            QueryParser::for_index(index, vec![engine.schema.get_field(TRACE_ID).unwrap()]);
        let searcher = reader.searcher();
        let query = query_parser.parse_query(&captured_val).unwrap();
        let search_res = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        println!("Search result is:{:?}", search_res);
        println!("captured value:{}", captured_val);
    }

    #[test]
    fn test_read_traceid() {
        let mut engine = TracingTagEngine::new(
            (150202 as i64).into(),
            Box::new("/tmp".to_string()),
            init_tracing_schema(),
        );
        println!("Init result is:{:?}", engine.init());
        let (reader, index) = engine.reader().unwrap();
        let query_parser =
            QueryParser::for_index(index, vec![engine.schema.get_field(TRACE_ID).unwrap()]);
        let searcher = reader.searcher();
        let query = query_parser
            .parse_query("d9d3c657-41c9-494a-8269-8422f2d107e5")
            .unwrap();
        let search_res = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        println!("search res is:{:?}", search_res);

        for (_score, doc_address) in search_res {
            let retrieved_doc = searcher.doc(doc_address).unwrap();
            println!("search result is: {:?}", retrieved_doc);
        }
    }
}
