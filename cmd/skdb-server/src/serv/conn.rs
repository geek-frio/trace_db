use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use std::sync::Mutex;

pub struct ConnManager {
    connId: AtomicI32,
    conns: Arc<Mutex<HashMap<i32, ()>>>,
}

impl ConnManager {
    pub fn new() -> ConnManager {
        ConnManager {
            conns: Arc::new(Mutex::new(HashMap::new())),
            connId: AtomicI32::new(1),
        }
    }

    pub fn gen_new_conn_id(&self) -> i32 {
        let id = self.connId.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.conns.lock().unwrap();
        guard.insert(id, ());
        id
    }
}
