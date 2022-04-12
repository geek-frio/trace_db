pub mod engine;
pub mod fsm;
pub mod search;
#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::test::gen as local_gen;
    use rand::Rng;
    use tantivy::schema::{Schema, INDEXED, STORED, STRING, TEXT};
    use tantivy::{Document, Index};

    #[test]
    fn test_open_reopen() {}
    #[test]
    fn test_normal_search() {}

    #[test]
    pub fn test_search() {
        let mut schema_builder = Schema::builder();
        let zone_field = schema_builder.add_text_field("zone", STRING);
        let service_field = schema_builder.add_text_field("service", TEXT);
        let api_id_field = schema_builder.add_i64_field("api_id", INDEXED);
        let uuid_field = schema_builder.add_text_field("uuid", STRING | STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_dir("/home/frio/skytagindex", schema).unwrap();
        let mut index_writer = index.writer(100_000_000).unwrap();

        let mut thread_rng = rand::thread_rng();

        let current = Instant::now();

        let batch_size = 500000;
        let mut commit_num = 0;
        for i in 0..batch_size {
            let mut doc = Document::new();
            let zone = local_gen::_gen_tag(3, 20, 'a');
            doc.add_text(zone_field, zone.clone());

            let service = local_gen::_gen_tag(30, 25, 'e');
            doc.add_text(service_field, service.clone());

            let api_id: i64 = thread_rng.gen_range(0..10000);
            doc.add_i64(api_id_field, api_id);

            let uuid = uuid::Uuid::new_v4();
            doc.add_text(uuid_field, uuid.to_string());
            index_writer.add_document(doc);
            if i % 50000 == 1 {
                println!(
                    "zone:{}, service:{}, api_id:{}, uuid:{}",
                    zone, service, api_id, uuid
                );
                commit_num += 1;
                println!("进行多次commit, 第{}次", commit_num);
                index_writer.commit().unwrap();
            }
        }
        index_writer.commit().unwrap();
        println!(
            "Elapse: {}ms, batch size is:{}",
            current.elapsed().as_millis(),
            batch_size
        );
        println!(
            "Average speed is:{}",
            batch_size / current.elapsed().as_secs()
        );
    }
}
