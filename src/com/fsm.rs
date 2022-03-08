use skproto::tracing::SegmentData;

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use super::mail::BasicMailbox;
use super::sched::FsmScheduler;
use core::panic;
use core::ptr;
use core::sync::atomic::Ordering;
use std::borrow::Cow;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

const NOTIFYSTATE_NOTIFIED: usize = 0;
const NOTIFYSTATE_IDLE: usize = 1;
const NOTIFYSTATE_DROP: usize = 2;

impl<Owner: Fsm> Clone for BasicMailbox<Owner> {
    fn clone(&self) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

pub struct FsmState<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
    state_cnt: Arc<AtomicUsize>,
}

impl<N: Fsm> FsmState<N> {
    pub fn new(data: Box<N>, state_cnt: Arc<AtomicUsize>) -> FsmState<N> {
        state_cnt.fetch_add(1, Ordering::Relaxed);
        FsmState {
            status: AtomicUsize::new(NOTIFYSTATE_IDLE),
            data: AtomicPtr::new(Box::into_raw(data)),
            state_cnt,
        }
    }

    pub fn notify<S: FsmScheduler<F = N>>(&self, s: &S) {
        match self.take_fsm() {
            None => {}
            Some(n) => s.schedule(n),
        }
    }

    pub fn take_fsm(&self) -> Option<Box<N>> {
        let res = self.status.compare_exchange(
            NOTIFYSTATE_IDLE,
            NOTIFYSTATE_NOTIFIED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if res.is_err() {
            return None;
        }

        let p = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_raw(p) })
        } else {
            panic!("inconsistent status and data, something should be wrong.");
        }
    }

    pub fn release(&self, fsm: Box<N>) {
        let previous = self.data.swap(Box::into_raw(fsm), Ordering::AcqRel);
        if previous.is_null() {
            let res = self.status.compare_exchange(
                NOTIFYSTATE_NOTIFIED,
                NOTIFYSTATE_IDLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            match res {
                Ok(_) => return,
                Err(NOTIFYSTATE_DROP) => {
                    let ptr = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe {
                        Box::from_raw(ptr);
                    }
                    return;
                }
                Err(s) => {
                    panic!("previous_status is not correct:{}", s)
                }
            };
        }
        panic!("should not be here! previous state is not null!");
    }

    pub fn clear(&self) {
        match self.status.swap(NOTIFYSTATE_DROP, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED | NOTIFYSTATE_DROP => return,
            _ => {}
        }

        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }
    }
}

impl<N> Drop for FsmState<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Ordering::AcqRel);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
            self.state_cnt.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub trait Fsm {
    type Message: Send;

    fn is_stopped(&self) -> bool;

    fn set_mailbox(&mut self, _mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized;

    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized;
}

// For tag fsm 处理
pub struct TagFsm;

impl Fsm for TagFsm {
    type Message = SegmentData;

    fn is_stopped(&self) -> bool {
        todo!()
    }

    fn set_mailbox(&mut self, _mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        todo!()
    }

    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        todo!()
    }
}
