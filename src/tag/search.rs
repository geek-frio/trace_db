use crossbeam_channel::unbounded;
use grpcio::ChannelBuilder;
use grpcio::Environment;
use std::cmp::Ordering;
use std::sync::atomic::AtomicPtr;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tantivy::Document;
use tantivy::Score;
use tokio::sync::watch;
pub type ConfigChangeCallback = Box<dyn FnOnce(Vec<String>) + Send>;

trait ConfigWatcher {
    fn watch(&self, cb: ConfigChangeCallback);
}
// Global Single Instance
struct SearchBuilder<T> {
    searcher: AtomicPtr<DistSearchManager<T>>,
}

impl<T: Clone> SearchBuilder<T> {
    fn new<W: ConfigWatcher>(&self, watcher: W) -> SearchBuilder<T> {
        let (s, r) = unbounded();

        let cb = thread::spawn(move || {
            let cb = Box::new(|v| {
                v.into_iter().map(|addr| {
                    let env = Environment::new(3);
                    let channel = ChannelBuilder::new(Arc::new(env)).connect(addr);
                });
            });
            watcher.watch(cb)
        });
        r.recv();
    }
}

pub trait RemoteClient {
    fn query_docs(
        &self,
        query: &'static str,
        // Vec<(MonthDay, BucketIdx(In one day every 15 minutes, there is a bucket for storing data))
        seg_range: Vec<(i32, i32)>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Score, Document)>, ()>;
}
#[derive(Clone)]
pub struct DistSearchManager<T> {
    remotes: Arc<Vec<T>>,
}

impl<T: RemoteClient> DistSearchManager<T> {
    pub fn new(remotes: Arc<Vec<T>>) -> Self {
        DistSearchManager { remotes }
    }

    pub fn search(
        &self,
        query: &'static str,
        seg_range: Vec<(i32, i32)>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Score, Document)>, ()> {
        // Collect query result data from different remote client
        let res: Vec<Result<Vec<(Score, Document)>, ()>> = self
            .remotes
            .iter()
            .map(|client| {
                // TODO: handle ERROR and return
                let res = client.query_docs(query, seg_range.clone(), offset, limit);
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
