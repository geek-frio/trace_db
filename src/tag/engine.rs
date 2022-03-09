use std::collections::HashMap;

use skproto::tracing::SegmentData;
use tantivy::collector::Count;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::{
    Field, FieldValue, IndexRecordOption, Schema, Value, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{doc, Directory, Document, Index, IndexWriter};

pub struct TagWriteEngine {
    dir: &'static str,
    // tag data gps (Every 15 minutes of a day, we create a new directory)
    addr: u64,
    index_writer: Option<IndexWriter>,
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

pub enum TagEngineErrorCode {
    DirOpenFailed,
    WriterCreateFailed,
}

impl TagWriteEngine {
    pub fn new(addr: u64, dir: &'static str) -> TagWriteEngine {
        TagWriteEngine {
            dir,
            addr,
            index_writer: None,
            field_map: HashMap::new(),
        }
    }

    pub fn init(&mut self) -> Result<(), TagEngineErrorCode> {
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
        // TODO: check open operation valid
        let dir = MmapDirectory::open(path).unwrap();
        let index = Index::open_or_create(dir, schema).unwrap();
        self.index_writer = Some(index.writer(100_100_000).unwrap());
        return Ok(());
    }

    pub fn add_record(&self, data: SegmentData) -> u64 {
        let document = self.create_doc(data);
        if let Some(writer) = self.index_writer.as_ref() {
            return writer.add_document(document);
        }
        // Index writer is null, 0 return instead
        0
    }

    pub fn create_doc(&self, data: SegmentData) -> Document {
        let mut doc = Document::new();
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::TraceId).unwrap().clone(),
            Value::Str(data.trace_id),
        ));
        // TODO: segment id need to be added
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::Zone).unwrap().clone(),
            Value::Str(data.zone),
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
            Value::Str(data.ser_key),
        ));
        doc.add(FieldValue::new(
            self.field_map.get(&TagField::Payload).unwrap().clone(),
            Value::Str(data.payload),
        ));
        doc
    }

    pub fn commit() {}
}
