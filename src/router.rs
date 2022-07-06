use crate::com::index::{IndexAddr, MailKeyAddress};
use crate::com::mail::BasicMailbox;
use crate::com::util::{CountTracker, LruCache};
use crate::fsm::Fsm;
use crate::sched::FsmScheduler;
use anyhow::Error as AnyError;
use crossbeam_channel::TrySendError;
use std::cell::RefCell;
use std::collections::hash_map::Iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tracing::info;

pub trait RouteMsg<L, N: Fsm> {
    type Addr;

    fn route_msg(&self, addr: Self::Addr, msg: N::Message) -> Either<L, N::Message>;

    fn register(&self, addr: IndexAddr, mailbox: BasicMailbox<N>);

    fn register_all(&self, mailboxes: Vec<(IndexAddr, BasicMailbox<N>)>);
}

pub struct NormalMailMap<N: Fsm> {
    map: HashMap<IndexAddr, BasicMailbox<N>>,
    alive_cnt: Arc<AtomicUsize>,
}

enum CheckDoResult<T> {
    NotExist,
    Invalid,
    Valid(T),
}

pub struct Router<N: Fsm, S> {
    pub normals: Arc<Mutex<NormalMailMap<N>>>,
    caches: RefCell<LruCache<IndexAddr, BasicMailbox<N>, CountTracker>>,
    pub(crate) normal_scheduler: S,
    state_cnt: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    pub fn as_ref(&self) -> Either<&L, &R> {
        match self {
            Either::Left(ref l) => Either::Left(l),
            Either::Right(ref r) => Either::Right(r),
        }
    }

    pub fn as_mut(&mut self) -> Either<&mut L, &mut R> {
        match self {
            Either::Left(ref mut l) => Either::Left(l),
            Either::Right(ref mut r) => Either::Right(r),
        }
    }

    pub fn left(self) -> Option<L> {
        match self {
            Either::Left(l) => Some(l),
            _ => None,
        }
    }

    pub fn right(self) -> Option<R> {
        match self {
            Either::Right(r) => Some(r),
            _ => None,
        }
    }
}

impl<N: Fsm, S: FsmScheduler<F = N> + Clone> RouteMsg<Result<(), TrySendError<N::Message>>, N>
    for Router<N, S>
{
    type Addr = MailKeyAddress;

    fn route_msg(
        &self,
        addr: Self::Addr,
        msg: N::Message,
    ) -> Either<Result<(), TrySendError<N::Message>>, N::Message> {
        let mut msg = Some(msg);
        let res = self.check_do(addr.into(), |mailbox| {
            let m = msg.take().unwrap();
            match mailbox.send(m, &self.normal_scheduler) {
                Ok(()) => Some(()),
                Err(send_err) => {
                    msg = Some(send_err.0);
                    None
                }
            }
        });
        match res {
            CheckDoResult::NotExist => Either::Right(msg.unwrap()),
            CheckDoResult::Invalid => Either::Left(Err(TrySendError::Disconnected(msg.unwrap()))),
            CheckDoResult::Valid(_) => Either::Left(Ok(())),
        }
    }

    fn register(&self, addr: IndexAddr, mailbox: BasicMailbox<N>) {
        info!("Has inserted a new mailbox, addr is:{}", addr);
        let mut normals = self.normals.lock().unwrap();
        if let Some(mailbox) = normals.map.insert(addr, mailbox) {
            mailbox.close();
        }
        normals
            .alive_cnt
            .store(normals.map.len(), Ordering::Relaxed);
    }

    fn register_all(&self, mailboxes: Vec<(IndexAddr, BasicMailbox<N>)>) {
        let mut normals = self.normals.lock().unwrap();
        normals.map.reserve(mailboxes.len());
        for (addr, mailbox) in mailboxes {
            if let Some(m) = normals.map.insert(addr, mailbox) {
                m.close();
            }
        }
        normals
            .alive_cnt
            .store(normals.map.len(), Ordering::Relaxed);
    }
}

impl<N: Fsm, S> Router<N, S>
where
    S: FsmScheduler<F = N> + Clone,
{
    pub fn new(normal_scheduler: S, state_cnt: Arc<AtomicUsize>) -> Router<N, S> {
        Router {
            normals: Arc::new(Mutex::new(NormalMailMap {
                map: HashMap::default(),
                alive_cnt: Arc::default(),
            })),
            caches: RefCell::new(LruCache::with_capacity_and_sample(1024, 7)),
            normal_scheduler,
            state_cnt,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    fn check_do<F, R>(&self, addr: IndexAddr, mut f: F) -> CheckDoResult<R>
    where
        F: FnMut(&BasicMailbox<N>) -> Option<R>,
    {
        let mut caches = self.caches.borrow_mut();
        // 缓存命中，走缓存获取
        if let Some(mailbox) = caches.get(&addr) {
            if let Some(r) = f(mailbox) {
                return CheckDoResult::Valid(r);
            }
        }
        let (cnt, mailbox) = {
            let mut boxes = self.normals.lock().unwrap();
            let cnt = boxes.map.len();

            let b = match boxes.map.get_mut(&addr) {
                Some(mailbox) => mailbox.clone(),
                None => {
                    drop(boxes);
                    return CheckDoResult::NotExist;
                }
            };
            (cnt, b)
        };
        if cnt > caches.capacity() || cnt < caches.capacity() / 2 {
            caches.resize(cnt);
        }

        let res = f(&mailbox);
        match res {
            Some(r) => {
                caches.insert(addr, mailbox);
                CheckDoResult::Valid(r)
            }
            None => CheckDoResult::Invalid,
        }
    }

    pub fn notify_all_idle_mailbox(&self) -> Result<(), AnyError> {
        let res = self.normals.lock();
        match res {
            Ok(r) => {
                let iter = r.iter();
                for (_, mail) in iter {
                    mail.notify(&self.normal_scheduler);
                }
                Ok(())
            }
            Err(e) => Err(AnyError::msg(format!("lock has been poisoned, e:{:?}", e))),
        }
    }

    pub fn close(&self, addr: IndexAddr) {
        self.caches.borrow_mut().remove(&addr);
        let mut mailboxes = self.normals.lock().unwrap();
        if let Some(mb) = mailboxes.map.remove(&addr) {
            mb.close();
        }
        mailboxes
            .alive_cnt
            .store(mailboxes.map.len(), Ordering::Relaxed);
    }

    pub fn clear_cache(&self) {
        self.caches.borrow_mut().clear();
    }

    pub fn state_cnt(&self) -> &Arc<AtomicUsize> {
        &self.state_cnt
    }

    pub fn alive_cnt(&self) -> Arc<AtomicUsize> {
        self.normals.lock().unwrap().alive_cnt.clone()
    }
}

impl<N: Fsm, S: Clone> Clone for Router<N, S> {
    fn clone(&self) -> Router<N, S> {
        Router {
            normals: self.normals.clone(),
            caches: RefCell::new(LruCache::with_capacity_and_sample(1024, 7)),
            normal_scheduler: self.normal_scheduler.clone(),
            shutdown: self.shutdown.clone(),
            state_cnt: self.state_cnt.clone(),
        }
    }
}

impl<N: Fsm> NormalMailMap<N> {
    fn iter(&self) -> Iter<IndexAddr, BasicMailbox<N>> {
        self.map.iter()
    }
}
