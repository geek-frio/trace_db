use std::collections::HashMap;

use skproto::tracing::SegmentData;
use tantivy::collector::Count;
use tantivy::directory::MmapDirectory;
use tantivy::error::TantivyError;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::{
    Field, FieldValue, IndexRecordOption, Schema, Value, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{doc, Directory, Document, Index, IndexReader, IndexWriter};

pub trait TagSearch {
    fn query_any(&self);
}

pub struct TagWriteEngine {
    dir: &'static str,
    // tag data gps (Every 15 minutes of a day, we create a new directory)
    addr: u64,
    index_writer: Option<IndexWriter>,
    index: Option<Index>,
    field_map: HashMap<TagField, Field>,
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

impl TagWriteEngine {
    pub fn new(addr: u64, dir: &'static str) -> TagWriteEngine {
        TagWriteEngine {
            dir,
            addr,
            index_writer: None,
            field_map: HashMap::new(),
            index: None,
        }
    }

    pub fn init(&mut self) -> Result<(), TagEngineError> {
        let mut schema_builder = Schema::builder();
        self.field_map.insert(
            TagField::Zone,
            schema_builder.add_text_field("zone", STRING),
        );
        self.field_map.insert(
            TagField::ApiId,
            schema_builder.add_i64_field("api_id", INDEXED),
        );
        self.field_map.insert(
            TagField::Service,
            schema_builder.add_text_field("service", TEXT),
        );
        self.field_map.insert(
            TagField::Biztime,
            schema_builder.add_u64_field("biztime", STORED),
        );
        self.field_map.insert(
            TagField::TraceId,
            schema_builder.add_text_field("trace_id", STRING | STORED),
        );
        self.field_map.insert(
            TagField::SegId,
            schema_builder.add_text_field("seg_id", STRING),
        );
        self.field_map.insert(
            TagField::Payload,
            schema_builder.add_text_field("payload", STRING),
        );
        let schema = schema_builder.build();
        // TODO: check if it is an outdated directory
        // create directory
        let path = &format!("{}/{}", self.dir, self.addr);
        // Create index directory
        let result = std::fs::create_dir_all(path);
        if result.is_err() {
            return Err(TagEngineError::IndexDirCreateFailed);
        }
        // TODO: check open operation valid
        let dir = MmapDirectory::open(path).unwrap();
        let index = Index::open_or_create(dir, schema).unwrap();
        self.index_writer = Some(index.writer(100_100_000).unwrap());
        self.index = Some(index);
        return Ok(());
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
            self.field_map.get(&TagField::TraceId).unwrap().clone(),
            Value::Str(data.trace_id.clone()),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::Zone).unwrap().clone(),
            Value::Str(data.zone.clone()),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::SegId).unwrap().clone(),
            Value::Str(data.seg_id.clone()),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::ApiId).unwrap().clone(),
            Value::I64(data.api_id as i64),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::Biztime).unwrap().clone(),
            Value::I64(data.biz_timestamp as i64),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::Service).unwrap().clone(),
            Value::Str(data.ser_key.clone()),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::Payload).unwrap().clone(),
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

    pub fn get_field(&self, key: TagField) -> Field {
        self.field_map.get(&key).unwrap().clone()
    }
}

mod tests {

    use tantivy::{chrono::Local, collector::TopDocs, query::QueryParser, DocAddress, Score};

    use super::*;
    use crate::{
        com::util::*,
        test::gen::{_gen_data_binary, _gen_tag},
    };

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
        let mut engine = TagWriteEngine::new(123, "/tmp/tantivy_records");

        println!("{:?}", engine.init());

        let (reader, index) = engine.reader().unwrap();
        let query_parser = QueryParser::for_index(index, vec![engine.get_field(TagField::ApiId)]);
        let searcher = reader.searcher();

        for _ in 0..10 {
            for j in 0..100 {
                let now = Local::now();
                let mut record = SegmentData::new();
                let uuid = uuid::Uuid::new_v4();
                record.set_api_id(j);
                record.set_biz_timestamp(now.timestamp_nanos() as u64);
                record.set_zone(_gen_tag(3, 3, 'a'));
                record.set_seg_id(uuid.to_string());
                record.set_trace_id(uuid.to_string());
                record.set_ser_key(_gen_tag(20, 3, 'e'));
                record.set_payload(_gen_data_binary());
                engine.add_record(&record);
            }
            println!("flushed result is:{:?}", engine.flush());
            let query = query_parser.parse_query("1").unwrap();
            let top_docs: Vec<(Score, DocAddress)> =
                searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
            println!("Top docs length is:{}", top_docs.len());
            println!("Top docs result is:{:?}", top_docs);
        }
    }
}
