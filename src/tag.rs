use tantivy::collector::Count;
use rand::Rng;
use super::gen as local_gen;
use tantivy::{doc, Index, Document};
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, TEXT, STORED, STRING, INDEXED};

pub fn test_search() {
    let mut schema_builder= Schema::builder();
    let zone_field = schema_builder.add_text_field("zone", STRING);
    let service_field = schema_builder.add_text_field("service", TEXT);
    let api_id_field = schema_builder.add_i64_field("api_id", INDEXED);
    let uuid_field= schema_builder.add_text_field("uuid", STRING|STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_dir("/home/frio/skytagindex", schema).unwrap();
    let index_writer = index.writer(100_000_000).unwrap();

    let mut thread_rng = rand::thread_rng();
    for _ in 0..500 {
        let mut doc = Document::new();
        doc.add_text(zone_field, local_gen::gen_tag(3, 5));
        doc.add_text(service_field, local_gen::gen_tag(10, 3));
        let api_id: i64 = thread_rng.gen_range(0..1000);
        doc.add_i64(api_id_field, api_id);
        let uuid = uuid::Uuid::new_v4();
        doc.add_text(uuid_field, uuid.to_string());
        index_writer.add_document(doc);
    }


}

mod tests {
    #[test]
    fn test_normal_search() {}
}
