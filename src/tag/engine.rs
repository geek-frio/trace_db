use crate::com::index::MailKeyAddress;
use fail::fail_point;
use lazy_static::__Deref;
use skproto::tracing::SegmentData;
use tantivy::directory::MmapDirectory;
use tantivy::schema::*;
use tantivy::{Document, Index, IndexReader, IndexWriter};
use tracing::error;

use super::schema::{API_ID, BIZTIME, PAYLOAD, SEGID, SERVICE, TRACE_ID, TRACING_SCHEMA, ZONE};

pub struct TracingTagEngine {
    index_writer: IndexWriter,
    index: Index,
}

impl TracingTagEngine {
    pub fn get_index_writer(&self) -> &IndexWriter {
        &self.index_writer
    }
    pub fn get_mut_index_writer(&mut self) -> &mut IndexWriter {
        &mut self.index_writer
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum TagEngineError {
    #[error("Tantivy commit error, e: {0:?}")]
    RecordsCommitError(String),
    #[error("Tantivy writer is not init, tracing tag engine should be called init first")]
    WriterNotInit,
    #[error("Tantivy reader is not init, tracing tag engine should be called init first")]
    IndexNotExist,
    #[error("tanivy read error, {0:?}")]
    Other(String),
    #[error("Tracing tag engine init failed, create index dir failed!")]
    IndexDirCreateFailed,
    #[error("Mailbox receiver is dropped")]
    ReceiverDroppped,
}

impl TracingTagEngine {
    pub fn new(addr: MailKeyAddress, data_dir: &str) -> Result<TracingTagEngine, TagEngineError> {
        let res_index = Self::index_dir_create(addr, data_dir);

        match res_index {
            Ok(index) => {
                let res_writer = index.writer(100_100_000);
                match res_writer {
                    Ok(writer) => Ok(TracingTagEngine {
                        index_writer: writer,
                        index,
                    }),
                    Err(e) => {
                        error!("Index writer create failed!e:{:?}", e);
                        Err(TagEngineError::IndexDirCreateFailed)
                    }
                }
            }
            Err(e) => {
                error!("Create index directory failed!e:{:?}", e);
                Err(TagEngineError::IndexDirCreateFailed)
            }
        }
    }

    #[cfg(test)]
    pub fn new_for_test() -> Result<TracingTagEngine, TagEngineError> {
        let res_index = Self::index_ram_dir_create();

        match res_index {
            Ok(index) => {
                let res_writer = index.writer(100_100_000);
                match res_writer {
                    Ok(writer) => Ok(TracingTagEngine {
                        index_writer: writer,
                        index,
                    }),
                    Err(e) => {
                        error!("Index writer create failed!e:{:?}", e);
                        Err(TagEngineError::IndexDirCreateFailed)
                    }
                }
            }
            Err(e) => {
                error!("Create index directory failed!e:{:?}", e);
                Err(TagEngineError::IndexDirCreateFailed)
            }
        }
    }

    #[cfg(test)]
    fn index_ram_dir_create() -> Result<Index, anyhow::Error> {
        let schema = (&TRACING_SCHEMA).deref().clone();
        Ok(Index::create_in_ram(schema.clone()))
    }

    fn index_dir_create(addr: MailKeyAddress, data_dir: &str) -> Result<Index, anyhow::Error> {
        let path = addr.get_idx_path(data_dir);

        std::fs::create_dir_all(path.as_path())?;

        let dir = MmapDirectory::open(path.as_path())?;
        let schema = (&TRACING_SCHEMA).deref().clone();

        Ok(Index::open_or_create(dir, schema.clone())?)
    }

    pub fn add_record(&self, data: &SegmentData) -> u64 {
        let document = self.create_doc(data);
        self.index_writer.add_document(document);
        0
    }

    fn create_doc(&self, data: &SegmentData) -> Document {
        let mut doc = Document::new();
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(TRACE_ID).unwrap(),
            Value::Str(data.trace_id.clone()),
        ));
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(ZONE).unwrap(),
            Value::Str(data.zone.clone()),
        ));
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(SEGID).unwrap(),
            Value::Str(data.seg_id.clone()),
        ));
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(API_ID).unwrap(),
            Value::I64(data.api_id as i64),
        ));
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(BIZTIME).unwrap(),
            Value::I64(data.biz_timestamp as i64),
        ));
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(SERVICE).unwrap(),
            Value::Str(data.ser_key.clone()),
        ));
        doc.add(FieldValue::new(
            TRACING_SCHEMA.get_field(PAYLOAD).unwrap(),
            Value::Str(data.payload.clone()),
        ));
        doc
    }

    pub fn flush(&mut self) -> Result<u64, TagEngineError> {
        fail_point!("read-dir");
        tracing::info!("flush is called!");
        let res = self.index_writer.commit();
        fail::fail_point!("flush-err", |_| {
            tracing::info!("dfasfdasfasfdsfsafdasfasfdasfdsfsaf");
            Err(TagEngineError::RecordsCommitError(
                "failed to flush".to_string(),
            ))
        });
        match res {
            Ok(commit_idx) => return Ok(commit_idx),
            Err(e) => return Err(TagEngineError::RecordsCommitError(e.to_string())),
        }
    }

    pub fn reader(&self) -> Result<(IndexReader, &Index), TagEngineError> {
        self.index
            .reader()
            .map(|r| (r, &self.index))
            .map_err(|e| TagEngineError::Other(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::com::gen::{_gen_data_binary, _gen_tag};
    // use crate::com::index::*;
    // use std::sync::Once;
    // use tantivy::{chrono::Local, collector::TopDocs, query::QueryParser};

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
        // let mut engine = TracingTagEngine::new(
        //     123i64.with_index_addr().unwrap(),
        //     Box::new("/tmp/tantivy_records".to_string()),
        //     init_tracing_schema(),
        // );

        // println!("{:?}", engine.init());

        // // let (reader, index) = engine.reader().unwrap();
        // // let query_parser = QueryParser::for_index(index, vec![engine.get_field(TagField::ApiId)]);
        // // let searcher = reader.searcher();

        // let init: Once = Once::new();
        // let mut captured_val = String::new();
        // for i in 0..10 {
        //     for j in 0..100 {
        //         let now = Local::now();
        //         let mut record = SegmentData::new();
        //         let uuid = uuid::Uuid::new_v4();
        //         record.set_api_id(j);
        //         record.set_biz_timestamp(now.timestamp_nanos() as u64);
        //         record.set_zone(_gen_tag(3, 3, 'a'));
        //         record.set_seg_id(uuid.to_string());
        //         record.set_trace_id(uuid.to_string());
        //         init.call_once(|| {
        //             captured_val.push_str(&uuid.to_string());
        //             println!("i:{}; j:{}; uuid:{}", i, j, uuid.to_string());
        //         });
        //         record.set_ser_key(_gen_tag(20, 3, 'e'));
        //         record.set_payload(_gen_data_binary());
        //         engine.add_record(&record);
        //     }
        //     let e = &mut engine;
        //     println!("flushed result is:{:?}", e.flush());
        // }

        // let (reader, index) = engine.reader().unwrap();
        // let query_parser =
        //     QueryParser::for_index(index, vec![engine.schema.get_field(TRACE_ID).unwrap()]);
        // let searcher = reader.searcher();
        // let query = query_parser.parse_query(&captured_val).unwrap();
        // let search_res = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        // println!("Search result is:{:?}", search_res);
        // println!("captured value:{}", captured_val);
    }

    #[test]
    fn test_read_traceid() {
        // let mut engine = TracingTagEngine::new(
        //     (150202 as i64).with_index_addr().unwrap(),
        //     Box::new("/tmp".to_string()),
        //     init_tracing_schema(),
        // );
        // println!("Init result is:{:?}", engine.init());
        // let (reader, index) = engine.reader().unwrap();
        // let query_parser =
        //     QueryParser::for_index(index, vec![engine.schema.get_field(TRACE_ID).unwrap()]);
        // let searcher = reader.searcher();
        // let query = query_parser
        //     .parse_query("d9d3c657-41c9-494a-8269-8422f2d107e5")
        //     .unwrap();
        // let search_res = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        // println!("search res is:{:?}", search_res);

        // for (_score, doc_address) in search_res {
        //     let retrieved_doc = searcher.doc(doc_address).unwrap();
        //     println!("search result is: {:?}", retrieved_doc);
        // }
    }
}
