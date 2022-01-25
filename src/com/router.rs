use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, sync::atomic::AtomicUsize};

use super::fsm::{Fsm, FsmScheduler};
use super::mail::BasicMailbox;

struct NormalMailMap<N: Fsm> {
    map: HashMap<u64, BasicMailbox<N>>,
    alive_cnt: Arc<AtomicUsize>,
}

enum CheckDoResult<T> {
    NotExist,
    Invalid,
    Valid(T),
}

pub struct Router<N: Fsm, S> {
    normals: Arc<Mutex<NormalMailMap<N>>>,
    pub(crate) normal_scheduler: S,
    state_cnt: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl<N: Fsm, S> Router<N, S>
where
    S: FsmScheduler<F = N> + Clone,
{
    pub(crate) fn new(normal_scheduler: S, state_cnt: Arc<AtomicUsize>) -> Router<N, S> {
        Router {
            normals: Arc::new(Mutex::new(NormalMailMap {
                map: HashMap::default(),
                alive_cnt: Arc::default(),
            })),
            normal_scheduler,
            state_cnt,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub fn register(&self, addr: u64, mailbox: BasicMailbox<N>) {}
}
