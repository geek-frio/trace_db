use std::cmp::Ordering;
use tantivy::Document;
use tantivy::Score;

pub trait RemoteClient {
    fn query_docs(
        &self,
        query: &'static str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Score, Document)>, ()>;
}
pub struct DistSearchManager<T> {
    remotes: Vec<T>,
}

impl<T: RemoteClient> DistSearchManager<T> {
    pub fn search(
        &self,
        query: &'static str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Score, Document)>, ()> {
        // Collect query result data from different remote client
        let res: Vec<Result<Vec<(Score, Document)>, ()>> = self
            .remotes
            .iter()
            .map(|client| {
                // TODO: handle ERROR and return
                let res = client.query_docs(query, offset, limit);
                res
            })
            .collect();
        let mut merged_docs = res
            .into_iter()
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .flatten()
            .collect::<Vec<(Score, Document)>>();
        // Sort data by score, only get the top ${limit} documents
        merged_docs.sort_by(|a, b| {
            if a.0 < b.0 {
                return Ordering::Greater;
            } else if a.0 == b.0 {
                return Ordering::Equal;
            } else {
                return Ordering::Less;
            }
        });
        let _ = merged_docs.split_off(limit);
        Ok(merged_docs)
    }
}
