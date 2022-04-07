use anyhow::Error as AnyError;
use crossbeam_channel::unbounded;
use crossbeam_channel::Sender;
use protobuf::RepeatedField;
use redis::Client as RedisClient;
use skproto::tracing::SegRange;
use skproto::tracing::SkyQueryParam;
use skproto::tracing::SkyTracingClient;
use std::cmp::Ordering;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use tantivy::Document;
use tantivy::Score;
use tantivy_common::BinarySerializable;

use crate::com::redis::RedisTTLSet;

trait ConfigWatcher<T>: Send {
    fn watch<F>(&self, cb: F, sender: Sender<Vec<T>>)
    where
        F: FnOnce(Vec<String>) -> Vec<T> + Send + 'static + Copy;
}
// Global Single Instance
#[derive(Clone)]
struct SearchBuilder<T> {
    searcher: Arc<Mutex<DistSearchManager<T>>>,
    redis_client: RedisClient,
}

impl<T: Sync + Send + Clone + 'static + RemoteClient> SearchBuilder<T> {
    fn new_init<
        F: FnOnce(Vec<String>) -> Vec<T> + Send + 'static + Copy,
        W: ConfigWatcher<T> + 'static,
    >(
        watcher: W,
        cb: F,
        redis_client: RedisClient,
    ) -> Result<SearchBuilder<T>, AnyError> {
        // Regist new address
        let ttl_set = RedisTTLSet { ttl: 5 };
        let mut conn = redis_client.get_connection()?;
        // TODO generate
        ttl_set.push(&mut conn, "127.0.0.1:9000")?;
        let (s, r) = unbounded::<Vec<T>>();
        // Wait for client init connection ready
        watcher.watch(cb, s);
        let clients = r.recv()?;
        let mutex = Mutex::new(DistSearchManager::new(Arc::new(clients)));
        let searcher = Arc::new(mutex);
        let builder = SearchBuilder {
            searcher: searcher.clone(),
            redis_client,
        };
        // Task for receiving client change events
        thread::spawn(move || loop {
            let clients = r.recv();
            if let Ok(clients) = clients {
                if let Ok(mut s) = searcher.lock() {
                    *s = DistSearchManager::new(Arc::new(clients));
                }
            }
        });
        return Ok(builder);
        // Spawn receiver to get watcher events
    }

    pub fn get_searcher(&self) -> Result<DistSearchManager<T>, ()> {
        match self.searcher.lock() {
            Ok(s) => return Ok(s.clone()),
            Err(_) => return Err(()),
        }
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
    ) -> Result<Vec<ScoreDocument>, ()>;
}

pub struct ScoreDocument {
    pub score: Score,
    pub doc: Document,
}

impl RemoteClient for SkyTracingClient {
    fn query_docs(
        &self,
        query: &'static str,
        // Vec<(MonthDay, BucketIdx(In one day every 15 minutes, there is a bucket for storing data))
        seg_range: Vec<(i32, i32)>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ScoreDocument>, ()> {
        let mut param = SkyQueryParam::new();
        let mut ranges = RepeatedField::new();

        for seg in seg_range {
            let wrap: SegRangeWrap = seg.into();
            ranges.push(wrap.0);
        }

        param.set_limit(limit as i32);
        param.set_offset(offset as i32);
        param.set_query(query.to_string());
        param.set_seg_range(ranges);

        let a = self
            .query_sky_segments(&param)
            .map(|v| {
                let score_doc = v.score_doc;
                let res: Vec<ScoreDocument> = score_doc
                    .into_iter()
                    .map(|score_doc| {
                        let doc = score_doc.doc;
                        let mut cursor = Cursor::new(doc);
                        ScoreDocument {
                            score: score_doc.score,
                            doc: <Document as BinarySerializable>::deserialize(&mut cursor)
                                .unwrap(),
                        }
                    })
                    .collect();
                res
            })
            .map_err(|_| ());
        a
    }
}

struct SegRangeWrap(SegRange);

impl From<(i32, i32)> for SegRangeWrap {
    fn from(t: (i32, i32)) -> Self {
        let mut s = SegRange::new();
        s.set_month_day(t.0);
        s.set_bucket(t.1);
        SegRangeWrap(s)
    }
}

#[derive(Clone)]
pub struct DistSearchManager<T> {
    pub remotes: Arc<Vec<T>>,
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
    ) -> Result<Vec<ScoreDocument>, ()> {
        // Collect query result data from different remote client
        let res: Vec<Result<Vec<ScoreDocument>, ()>> = self
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
            .collect::<Vec<ScoreDocument>>();
        // Sort data by score, only get the top ${limit} documents
        merged_docs.sort_by(|a, b| {
            if a.score < b.score {
                return Ordering::Greater;
            } else if a.score == b.score {
                return Ordering::Equal;
            } else {
                return Ordering::Less;
            }
        });
        let _ = merged_docs.split_off(limit);
        Ok(merged_docs)
    }
}

struct AddrsConfigWatcher {
    redis_client: RedisClient,
}

impl AddrsConfigWatcher {
    fn new(client: RedisClient) -> Result<AddrsConfigWatcher, ()> {
        match client.get_connection() {
            Ok(c) => {
                return Ok(AddrsConfigWatcher {
                    redis_client: client,
                })
            }
            Err(_) => return Err(()),
        }
    }
}

impl<T> ConfigWatcher<T> for AddrsConfigWatcher
where
    T: Sync + Send + 'static + Clone,
{
    fn watch<F>(&self, cb: F, sender: Sender<Vec<T>>)
    where
        F: FnOnce(Vec<String>) -> Vec<T> + Send + 'static + Copy,
    {
        // Logic to pull addrs
        let redis_client = self.redis_client.clone();
        let redis_ttl = RedisTTLSet { ttl: 5 };
        thread::spawn(move || {
            let mut last: Vec<String> = Vec::new();
            loop {
                let conn = redis_client.get_connection();
                if let Ok(mut c) = conn {
                    let res = redis_ttl.query_all(&mut c);
                    match res {
                        Ok(records) => {
                            let addrs = records
                                .into_iter()
                                .map(|r| r.sub_key)
                                .collect::<Vec<String>>();

                            if last != addrs {
                                last = addrs.clone();
                                let clients = cb(addrs);
                                let _ = sender.send(clients);
                            }
                            sleep(Duration::from_secs(5));
                        }
                        Err(_) => continue,
                    }
                } else {
                    println!("Get connection failed!Continue to next loop");
                    continue;
                }
            }
        });
    }
}

mod tests {
    use super::*;
    use anyhow::Error as AnyError;
    use grpcio::{ChannelBuilder, Environment};

    #[test]
    fn test_xxx() -> Result<(), AnyError> {
        let client = redis::Client::open("redis://127.0.0.1:6379")?;
        let addrs_watcher = AddrsConfigWatcher {
            redis_client: client.clone(),
        };
        let builder = SearchBuilder::<SkyTracingClient>::new_init(
            addrs_watcher,
            |v: Vec<String>| {
                let mut clients = Vec::new();
                for addr in v {
                    let env = Environment::new(3);
                    // TODO: config change
                    let channel = ChannelBuilder::new(Arc::new(env)).connect(addr.as_str());
                    let client = SkyTracingClient::new(channel);
                    clients.push(client);
                }
                clients
            },
            client,
        );
        Ok(())
    }
}
