use skproto::tracing::SegRange;
use skproto::tracing::SkyQueryParam;
use skproto::tracing::SkyTracingClient;
use std::cmp::Ordering;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use tantivy::Document;
use tantivy::Score;
use tantivy_common::BinarySerializable;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::com::index::IndexAddr;
use crate::TOKIO_RUN;

// Global Single Instance
#[derive(Clone)]
pub struct Searcher<T> {
    searcher: Arc<Mutex<DistSearchManager<T>>>,
}

impl<T: Sync + Send + Clone + 'static + RemoteClient> Searcher<T> {
    pub fn new(mut clients_chg: UnboundedReceiver<Vec<T>>) -> Searcher<T>
    where
        T: Send + 'static,
    {
        let clients = Vec::<T>::new();
        let searcher = Arc::new(Mutex::new(DistSearchManager::new(Arc::new(clients))));

        let builder = Searcher {
            searcher: searcher.clone(),
        };
        // start watcher task to watch clients add/delete event
        TOKIO_RUN.spawn(async move {
            let clients = clients_chg.recv().await;
            if let Some(clients) = clients {
                if let Ok(mut s) = searcher.lock() {
                    *s = DistSearchManager::new(Arc::new(clients));
                }
            }
        });
        builder
    }

    pub fn get_searcher(&self) -> Result<DistSearchManager<T>, ()> {
        match self.searcher.lock() {
            Ok(s) => return Ok(s.clone()),
            Err(_) => return Err(()),
        }
    }
}

pub trait RemoteClient {
    fn query_docs<'a>(
        &self,
        query: &'a str,
        // Vec<(MonthDay, BucketIdx(In one day every 15 minutes, there is a bucket for storing data))
        addr: IndexAddr,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ScoreDocument>, ()>;
}

pub struct ScoreDocument {
    pub score: Score,
    pub doc: Document,
}

impl RemoteClient for SkyTracingClient {
    fn query_docs<'a>(
        &self,
        query: &'a str,
        addr: IndexAddr,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ScoreDocument>, ()> {
        let mut param = SkyQueryParam::new();
        let mut range = SegRange::new();
        range.set_addr(addr);
        param.set_limit(limit as i32);
        param.set_offset(offset as i32);
        param.set_query(query.to_string());
        param.set_seg_range(range);

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

#[derive(Clone)]
pub struct DistSearchManager<T> {
    pub remotes: Arc<Vec<T>>,
}

impl<T: RemoteClient> DistSearchManager<T> {
    pub fn new(remotes: Arc<Vec<T>>) -> Self {
        DistSearchManager { remotes }
    }

    pub fn search<'a>(
        &self,
        query: &'a str,
        addr: IndexAddr,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ScoreDocument>, ()> {
        // Collect query result data from different remote client
        let res: Vec<Result<Vec<ScoreDocument>, ()>> = self
            .remotes
            .iter()
            .map(|client| {
                // TODO: handle ERROR and return
                let res = client.query_docs(query, addr, offset, limit);
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

// pub struct AddrsConfigWatcher {
//     redis_client: RedisClient,
//     config: Arc<GlobalConfig>,
// }

// impl AddrsConfigWatcher {
//     pub fn new(client: RedisClient, config: Arc<GlobalConfig>) -> AddrsConfigWatcher {
//         AddrsConfigWatcher {
//             redis_client: client,
//             config,
//         }
//     }
// }

// impl<T> ConfigWatcher<T> for AddrsConfigWatcher
// where
//     T: Sync + Send + 'static + Clone,
// {
//     fn watch<F>(&self, cb: F, sender: Sender<Vec<T>>)
//     where
//         F: FnOnce(Vec<String>) -> Vec<T> + Send + 'static + Copy,
//     {
//         // Logic to pull addrs
//         let redis_client = self.redis_client.clone();
//         let redis_ttl = RedisTTLSet { ttl: 5 };
//         let redis_addr = self.config.redis_addr.clone();
//         let grpc_port = self.config.grpc_port;
//         thread::spawn(move || {
//             let mut last: Vec<String> = Vec::new();
//             let mut conn = redis_client.get_connection();
//             loop {
//                 if let Ok(mut c) = conn.as_mut() {
//                     let _ = redis_ttl.push(&mut c, &redis_addr);
//                     let res = redis_ttl.query_all(&mut c);
//                     match res {
//                         Ok(records) => {
//                             let addrs = records
//                                 .into_iter()
//                                 .map(|r| format!("{}:{}", r.sub_key, grpc_port).to_string())
//                                 .collect::<Vec<String>>();

//                             if last != addrs {
//                                 last = addrs.clone();
//                                 let clients = cb(addrs);
//                                 let _ = sender.send(clients);
//                             }
//                             sleep(Duration::from_secs(5));
//                         }
//                         Err(_) => continue,
//                     }
//                 } else {
//                     println!("Get connection failed!Continue to next loop");
//                     continue;
//                 }
//             }
//         });
//     }
// }

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::com::config::ConfigManager;
    use anyhow::Error as AnyError;
    // use grpcio::{ChannelBuilder, Environment};

    #[test]
    fn test_xxx() -> Result<(), AnyError> {
        // let client = redis::Client::open("redis://127.0.0.1:6379")?;
        // let config = Arc::new(ConfigManager::load("/tmp/skdb_test.yaml".into()));
        // let addrs_watcher = AddrsConfigWatcher {
        //     redis_client: client.clone(),
        //     config: config.clone(),
        // };
        // let _ = SearchBuilder::<SkyTracingClient>::new_init(
        //     addrs_watcher,
        //     |v: Vec<String>| {
        //         let mut clients = Vec::new();
        //         for addr in v {
        //             let env = Environment::new(3);
        //             // TODO: config change
        //             let channel = ChannelBuilder::new(Arc::new(env)).connect(addr.as_str());
        //             let client = SkyTracingClient::new(channel);
        //             clients.push(client);
        //         }
        //         clients
        //     },
        //     client,
        //     config,
        // );
        // let search_builder = SearchBuilder::new();
        Ok(())
    }
}
