use crate::com::index::{IndexAddr, MailKeyAddress};
use crate::com::mail::BasicMailbox;
use crate::com::util::{CountTracker, LruCache};
use crate::conf::GlobalConfig;
use crate::fsm::Fsm;
use crate::sched::{FsmScheduler, NormalScheduler};
use crate::tag::engine::{TagEngineError, TracingTagEngine};
use crate::tag::fsm::TagFsm;
use anyhow::Error as AnyError;
use crossbeam_channel::Sender;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::Iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tracing::info;

pub struct RouteErr<Msg>(pub Msg, pub TagEngineError);

pub trait RouteMsg<N: Fsm> {
    type Addr;

    fn route_msg(
        &self,
        addr: Self::Addr,
        msg: N::Message,
        f: fn(MailKeyAddress, &str) -> Result<(N, Sender<N::Message>), TagEngineError>,
    ) -> Result<(), RouteErr<N::Message>>;

    fn register(&self, addr: IndexAddr, mailbox: BasicMailbox<N>);

    fn register_all(&self, mailboxes: Vec<(IndexAddr, BasicMailbox<N>)>);
}

pub struct NormalMailMap<N: Fsm> {
    map: HashMap<IndexAddr, BasicMailbox<N>>,
    alive_cnt: Arc<AtomicUsize>,
}

pub struct Router<N: Fsm, S> {
    pub normals: Arc<Mutex<NormalMailMap<N>>>,
    caches: RefCell<LruCache<IndexAddr, BasicMailbox<N>, CountTracker>>,
    pub(crate) normal_scheduler: S,
    state_cnt: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    conf: Arc<GlobalConfig>,
}

impl Router<TagFsm, NormalScheduler<TagFsm>> {
    pub fn create_tag_fsm(
        addr: MailKeyAddress,
        dir: &str,
    ) -> Result<(TagFsm, crossbeam::channel::Sender<<TagFsm as Fsm>::Message>), TagEngineError>
    {
        let engine = TracingTagEngine::new(addr, dir)?;
        let (s, r) = crossbeam_channel::unbounded();
        Ok((TagFsm::new(r, None, engine), s))
    }
}

impl<N, S> Router<N, S>
where
    N: Fsm,
    S: FsmScheduler<F = N> + Clone,
{
    fn create_mailbox(&self, fsm: N, fsm_sender: Sender<N::Message>) -> BasicMailbox<N> {
        let state_cnt = Arc::new(AtomicUsize::new(0));
        let mailbox = BasicMailbox::new(fsm_sender, Box::new(fsm), state_cnt);

        let fsm = mailbox.take_fsm();
        if let Some(mut f) = fsm {
            f.set_mailbox(Cow::Borrowed(&mailbox));
            mailbox.release(f);
        }

        mailbox
    }

    fn send_msg(
        &self,
        mailbox: &BasicMailbox<N>,
        msg: N::Message,
    ) -> Result<(), RouteErr<N::Message>> {
        match mailbox.send(msg, &self.normal_scheduler) {
            Ok(()) => Ok(()),
            Err(e) => Err(RouteErr(e.0, TagEngineError::ReceiverDroppped)),
        }
    }
}

impl<N: Fsm, S: FsmScheduler<F = N> + Clone> RouteMsg<N> for Router<N, S> {
    type Addr = MailKeyAddress;

    fn route_msg(
        &self,
        addr: Self::Addr,
        msg: N::Message,
        create_fsm: fn(MailKeyAddress, &str) -> Result<(N, Sender<N::Message>), TagEngineError>,
    ) -> Result<(), RouteErr<N::Message>> {
        info!("Start to retrieve mailbox");
        let mailbox = self.retrive_mailbox(addr.into());
        match mailbox {
            Some(mailbox) => self.send_msg(mailbox, msg),
            None => {
                let res = create_fsm(addr.clone(), &self.conf.index_dir);
                if let Err(e) = res {
                    return Err(RouteErr(msg, e));
                }

                let (fsm, fsm_sender) = res.unwrap();
                let mailbox = self.create_mailbox(fsm, fsm_sender);

                let res_send = self.send_msg(&mailbox, msg);
                self.register(addr.into(), mailbox);

                res_send
            }
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
    pub fn new(
        normal_scheduler: S,
        state_cnt: Arc<AtomicUsize>,
        conf: Arc<GlobalConfig>,
    ) -> Router<N, S> {
        Router {
            normals: Arc::new(Mutex::new(NormalMailMap {
                map: HashMap::default(),
                alive_cnt: Arc::default(),
            })),
            caches: RefCell::new(LruCache::with_capacity_and_sample(1024, 7)),
            normal_scheduler,
            state_cnt,
            shutdown: Arc::new(AtomicBool::new(false)),
            conf,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub(crate) fn retrive_mailbox(&self, addr: IndexAddr) -> Option<&BasicMailbox<N>> {
        // FixMe: this used RefMut::leak, it's a nightly feature
        //  the reason to use leak is that we cannot leak &BasicMailBox using RefMut
        //  compiler will throw error
        let basic_box = self.get_mailbox_from_ref(&addr);

        if basic_box.is_some() {
            return basic_box;
        }

        let (cnt, mailbox) = {
            let mut boxes = self.normals.lock().unwrap();
            let cnt = boxes.map.len();

            let b = match boxes.map.get_mut(&addr) {
                Some(mailbox) => mailbox.clone(),
                None => {
                    return None;
                }
            };
            (cnt, b)
        };

        if cnt > self.caches.borrow().capacity() || cnt < self.caches.borrow().capacity() / 2 {
            self.caches.borrow_mut().resize(cnt);
        }

        self.caches.borrow_mut().insert(addr, mailbox);
        self.get_mailbox_from_ref(&addr)
    }

    fn get_mailbox_from_ref(&self, addr: &IndexAddr) -> Option<&BasicMailbox<N>> {
        let caches_mut = self.caches.borrow_mut();
        std::cell::RefMut::leak(caches_mut).get(addr.into())
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
            conf: self.conf.clone(),
        }
    }
}

impl<N: Fsm> NormalMailMap<N> {
    fn iter(&self) -> Iter<IndexAddr, BasicMailbox<N>> {
        self.map.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::Router;
    use crate::{
        log::init_console_logger, router::RouteMsg, sched::NormalScheduler, tag::fsm::TagFsm,
    };
    use std::sync::{atomic::AtomicUsize, Arc};

    struct Init {
        router: Router<TagFsm, NormalScheduler<TagFsm>>,
        fsm: TagFsm,
    }

    fn setup() -> Router<TagFsm, NormalScheduler<TagFsm>> {
        init_console_logger();

        let (send, _recv) = crossbeam_channel::unbounded();
        let fsm_sche = NormalScheduler { sender: send };
        let atomic = AtomicUsize::new(1);

        let router = Router::new(fsm_sche, Arc::new(atomic), Arc::new(Default::default()));

        router
    }

    #[test]
    fn test_router_basics() {
        let router = setup();

        let res_mailbox = router.retrive_mailbox(1234);
        assert!(res_mailbox.is_none());

        router.create_mailbox(fsm, fsm_sender)
    }
}
