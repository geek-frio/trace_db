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
use protobuf::wire_format::Tag;
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::hash_map::Iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, sync::atomic::AtomicUsize};
use tracing::info;

#[derive(Debug)]
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
    pub caches: RefCell<LruCache<IndexAddr, BasicMailbox<N>, CountTracker>>,
    pub(crate) normal_scheduler: S,
    state_cnt: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    pub conf: Arc<GlobalConfig>,
}

impl Router<TagFsm, NormalScheduler<TagFsm>> {
    pub fn create_tag_fsm(
        addr: MailKeyAddress,
        dir: &str,
    ) -> Result<(TagFsm, crossbeam::channel::Sender<<TagFsm as Fsm>::Message>), TagEngineError>
    {
        let engine = TracingTagEngine::new(addr, dir)?;
        let (s, r) = crossbeam_channel::unbounded();
        Ok((TagFsm::new(r, None, engine, 5000), s))
    }

    pub fn create_tag_fsm_with_size(
        addr: MailKeyAddress,
        dir: &str,
        batch_size: usize,
    ) -> Result<(TagFsm, crossbeam::channel::Sender<<TagFsm as Fsm>::Message>), TagEngineError>
    {
        let engine = TracingTagEngine::new(addr, dir)?;
        let (s, r) = crossbeam_channel::unbounded();
        Ok((TagFsm::new(r, None, engine, batch_size), s))
    }

    #[cfg(test)]
    pub fn create_test_tag_fsm_with_size(
        batch_size: usize,
    ) -> Result<(TagFsm, crossbeam::channel::Sender<<TagFsm as Fsm>::Message>), TagEngineError>
    {
        let engine = TracingTagEngine::new_for_test()?;
        let (s, r) = crossbeam_channel::unbounded();
        Ok((TagFsm::new(r, None, engine, batch_size), s))
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
}

impl<N: Fsm, S: FsmScheduler<F = N> + Clone> RouteMsg<N> for Router<N, S> {
    type Addr = MailKeyAddress;

    fn route_msg(
        &self,
        addr: Self::Addr,
        msg: N::Message,
        create_fsm: fn(MailKeyAddress, &str) -> Result<(N, Sender<N::Message>), TagEngineError>,
    ) -> Result<(), RouteErr<N::Message>> {
        let addr_num = addr.convert_to_index_addr_key();

        let stat = self.send_msg(addr_num, msg, &self.normal_scheduler);

        match stat {
            SendStat::SendSuccess => Ok(()),
            SendStat::MailboxNotExist(msg) | SendStat::MailboxSendErr(msg) => {
                let res = create_fsm(addr, &self.conf.index_dir);

                if let Err(e) = res {
                    tracing::warn!("Create fsm failed, will not regist this mailbox, e:{:?}", e);
                    return Err(RouteErr(msg, e));
                }

                let (fsm, fsm_sender) = res.unwrap();
                let mailbox = self.create_mailbox(fsm, fsm_sender);

                self.register(addr.convert_to_index_addr_key(), mailbox);

                let stat = self.send_msg(addr_num, msg, &self.normal_scheduler);
                match stat {
                    SendStat::SendSuccess => Ok(()),
                    SendStat::MailboxNotExist(msg) | SendStat::MailboxSendErr(msg) => {
                        Err(RouteErr(
                            msg,
                            TagEngineError::Other("Mailbox & TagFsm create failed".to_string()),
                        ))
                    }
                }
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

pub enum SendStat<Msg> {
    SendSuccess,
    MailboxNotExist(Msg),
    MailboxSendErr(Msg),
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

    pub(crate) fn send_msg(&self, addr: IndexAddr, msg: N::Message, s: &S) -> SendStat<N::Message> {
        let mut caches_ref = self.caches.borrow_mut();

        // Try cache map get
        let mailbox = caches_ref.get(&addr);
        if let Some(mailbox) = mailbox {
            let send_res = mailbox.send(msg, s);
            return match send_res {
                Ok(_) => SendStat::SendSuccess,
                Err(msg) => {
                    caches_ref.remove(&addr);

                    let mut boxes = self.normals.lock().unwrap();
                    boxes.map.remove(&addr);

                    SendStat::MailboxSendErr(msg.0)
                }
            };
        }

        let (cnt, mailbox) = {
            let mut boxes = self.normals.lock().unwrap();
            let cnt = boxes.map.len();

            let b = match boxes.map.get_mut(&addr) {
                Some(mailbox) => mailbox.clone(),
                None => {
                    tracing::warn!("Not find addr:{} related mailbox", addr);
                    return SendStat::MailboxNotExist(msg);
                }
            };
            (cnt, b)
        };

        // Resize Cache Map
        if cnt > caches_ref.capacity() || cnt < caches_ref.capacity() / 2 {
            caches_ref.resize(cnt);
        }

        let res_send = mailbox.send(msg, s);
        if let Err(e) = res_send {
            let mut boxes = self.normals.lock().unwrap();
            boxes.map.remove(&addr);

            return SendStat::MailboxSendErr(e.0);
        }
        caches_ref.insert(addr, mailbox);

        SendStat::SendSuccess
    }

    // pub(crate) fn send_msg(
    //     &self,
    //     addr: IndexAddr,
    //     msg: N::Message,
    //     s: &S,
    // ) -> Result<(), N::Message> {
    //     let mut caches_ref = self.caches.borrow_mut();

    //     let mailbox = caches_ref.get(&addr);
    //     if let Some(mailbox) = mailbox {
    //         let res_send = mailbox.send(msg, s);
    //         return match res_send {
    //             Ok(_) => Ok(()),
    //             Err(e) => {
    //                 tracing::warn!("1: Mailbox send failed! e:{:?}", e);
    //                 Err(e.0)
    //             }
    //         };
    //     }

    //     let (cnt, mailbox) = {
    //         let mut boxes = self.normals.lock().unwrap();
    //         let cnt = boxes.map.len();

    //         let b = match boxes.map.get_mut(&addr) {
    //             Some(mailbox) => mailbox.clone(),
    //             None => {
    //                 tracing::warn!("Not find addr:{} related mailbox", addr);
    //                 return Err(msg);
    //             }
    //         };
    //         (cnt, b)
    //     };

    //     if cnt > caches_ref.capacity() || cnt < caches_ref.capacity() / 2 {
    //         caches_ref.resize(cnt);
    //     }

    //     let res_send = mailbox.send(msg, s);
    //     if let Err(e) = res_send {
    //         return Err(e.0);
    //     }

    //     caches_ref.insert(addr, mailbox);
    //     Ok(())
    // }

    pub fn notify_all_idle_mailbox(&self) -> Result<(), AnyError> {
        let res = self.normals.lock();
        match res {
            Ok(r) => {
                let iter = r.iter();
                for (_, mail) in iter {
                    mail.tick(&self.normal_scheduler);
                }
                Ok(())
            }
            Err(e) => Err(AnyError::msg(format!("lock has been poisoned, e:{:?}", e))),
        }
    }

    pub fn remove_expired_mailbox(&mut self) -> Result<Vec<BasicMailbox<N>>, anyhow::Error> {
        let res = self.normals.lock();
        let bound_start = MailKeyAddress::get_not_expired_bound_start()
            .parse::<i64>()
            .unwrap();

        match res {
            Ok(mut r) => {
                let iter = r.iter();

                let mut ids = Vec::new();
                for (id, _) in iter {
                    if *id < bound_start {
                        ids.push(*id);
                    }
                }

                Ok(ids
                    .iter()
                    .map(move |id| {
                        let mailbox = r.map.remove(id);
                        mailbox
                    })
                    .filter(|m| m.is_some())
                    .map(|m| m.unwrap())
                    .collect::<Vec<_>>())
            }
            Err(e) => Err(AnyError::msg(format!("lock has been poisoned, e:{:?}", e))),
        }
    }

    pub fn close(&self, addr: IndexAddr) {
        self.caches.borrow_mut().remove(&addr);
        let mut mailboxes = self.normals.lock().unwrap();

        mailboxes.map.iter().for_each(|(k, _)| {
            tracing::trace!("key is:{}", k);
        });

        tracing::trace!("map length:{}", mailboxes.map.len());
        if let Some(mb) = mailboxes.map.remove(&addr) {
            tracing::trace!("Remove success!");
            mb.close();
        }

        tracing::trace!("after remove, length is:{}", mailboxes.map.len());
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
    use tracing::trace;

    use super::Router;
    use crate::{
        batch::FsmTypes,
        com::{
            index::ConvertIndexAddr,
            test_util::{gen_expired_mailkeyadd, gen_segcallback, gen_valid_mailkeyadd},
        },
        log::init_console_logger,
        router::{RouteMsg, SendStat},
        sched::NormalScheduler,
        tag::fsm::{FsmExecutor, TagFsm},
    };
    use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};

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

        let msg = gen_segcallback(1, 1);

        let res = router.route_msg(1234u64.with_index_addr(), msg.0, Router::create_tag_fsm);

        match res {
            Ok(_) => {
                assert!(router.alive_cnt().load(Ordering::Relaxed) == 1);
            }
            Err(_) => {
                panic!();
            }
        }
        router.close(1234i64.with_index_addr().convert_to_index_addr_key());

        assert_eq!(0, router.alive_cnt().load(Ordering::Relaxed));

        // Test regist
        let mail_addr = gen_valid_mailkeyadd();
        let (tag_fsm, fsm_sender) = Router::create_tag_fsm(mail_addr.clone(), "/tmp").unwrap();
        let mailbox = router.create_mailbox(tag_fsm, fsm_sender);

        router.register(mail_addr.convert_to_index_addr_key(), mailbox);

        trace!(
            "router caches ref count:{}",
            router.caches.try_borrow_mut().is_ok()
        );

        let (s, r) = crossbeam_channel::unbounded::<FsmTypes<TagFsm>>();
        let fsm_sche = NormalScheduler { sender: s };

        let msg = gen_segcallback(1, 1);
        let res_send = router.send_msg(mail_addr.convert_to_index_addr_key(), msg.0, &fsm_sche);

        match res_send {
            SendStat::SendSuccess => {}
            _ => {
                unreachable!()
            }
        }

        let res_fsm = r.recv();
        assert!(res_fsm.is_ok());

        let fsm_types = res_fsm.unwrap();
        match fsm_types {
            FsmTypes::Empty => {
                panic!();
            }
            FsmTypes::Normal(box_fsm) => {
                assert!(box_fsm.remain_msgs() == 1);
            }
        }

        router.close(mail_addr.convert_to_index_addr_key());
        assert_eq!(0, router.alive_cnt().load(Ordering::Relaxed));
    }

    #[test]
    fn test_regist_all() {
        let router = setup();

        let mut v = Vec::new();
        for _ in 0..3 {
            let m1 = gen_valid_mailkeyadd();
            let (fsm1, sender1) = Router::create_tag_fsm(m1.clone(), "/tmp").unwrap();
            let box1 = router.create_mailbox(fsm1, sender1);

            v.push((m1.convert_to_index_addr_key(), box1));
        }
        router.register_all(v);

        assert_eq!(router.alive_cnt().load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_remove_expired_mailboxes() {
        let mut router = setup();

        // Create 3 valid Fsm
        for _ in 0..3 {
            let m1 = gen_valid_mailkeyadd();
            let (fsm1, sender1) = Router::create_tag_fsm(m1.clone(), "/tmp").unwrap();
            let box1 = router.create_mailbox(fsm1, sender1);

            router.register(m1.convert_to_index_addr_key(), box1);
        }

        // Create 4 expired Fsm
        for _ in 0..4 {
            let m1 = gen_expired_mailkeyadd();
            let (fsm1, sender1) = Router::create_tag_fsm(m1.clone(), "/tmp").unwrap();
            let box1 = router.create_mailbox(fsm1, sender1);

            router.register(m1.convert_to_index_addr_key(), box1);
        }

        let res = router.remove_expired_mailbox();
        match res {
            Ok(v) => {
                assert_eq!(4, v.len());
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }
}
