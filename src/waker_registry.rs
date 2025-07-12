use crate::collections::LockedQueue;
use crate::locked_waker::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Context;

#[enum_dispatch(RegistryTrait)]
pub enum RegistrySender {
    Single(RegistrySingle),
    Multi(RegistryMultiSender),
    Dummy(RegistryDummy),
}

#[enum_dispatch(RegistryTrait)]
pub enum RegistryRecv {
    Single(RegistrySingle),
    Multi(RegistryMultiRecv),
    Dummy(RegistryDummy),
}

#[enum_dispatch]
pub trait RegistryTrait {
    fn is_empty(&self) -> bool;

    /// For async context
    fn reg_async(&self, _ctx: &mut Context, _o_waker: &mut Option<LockedWaker>) -> bool;

    /// For thread context
    fn reg_blocking(&self, _waker: &LockedWaker) -> bool;

    fn clear_wakers(&self, _seq: u64);

    fn cancel_waker(&self, _waker: LockedWaker);

    fn fire(&self);

    fn close(&self);

    /// return waker queue size
    fn get_size(&self) -> usize;
}

/// RegistryDummy is for unbounded channel tx, which is never blocked
pub struct RegistryDummy();

impl RegistryDummy {
    #[inline(always)]
    pub fn new() -> Self {
        Self()
    }
}

impl RegistryTrait for RegistryDummy {
    #[inline(always)]
    fn is_empty(&self) -> bool {
        true
    }

    #[inline(always)]
    fn reg_async(&self, _ctx: &mut Context, _o_waker: &mut Option<LockedWaker>) -> bool {
        unreachable!();
    }

    #[inline(always)]
    fn reg_blocking(&self, _waker: &LockedWaker) -> bool {
        unreachable!();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {}

    #[inline(always)]
    fn cancel_waker(&self, _waker: LockedWaker) {}

    #[inline(always)]
    fn fire(&self) {}

    #[inline(always)]
    fn close(&self) {}

    /// return waker queue size
    #[inline(always)]
    fn get_size(&self) -> usize {
        0
    }
}

pub struct RegistrySingle {
    cell: WakerCell,
}

impl RegistrySingle {
    #[inline(always)]
    pub fn new() -> Self {
        Self { cell: WakerCell::new() }
    }
}

impl RegistryTrait for RegistrySingle {
    #[inline(always)]
    fn is_empty(&self) -> bool {
        !self.cell.exists()
    }

    /// return is_skip
    #[inline(always)]
    fn reg_async(&self, ctx: &mut Context, o_waker: &mut Option<LockedWaker>) -> bool {
        let waker = {
            if o_waker.is_none() {
                o_waker.replace(LockedWaker::new_async(ctx));
                o_waker.as_ref().unwrap()
            } else {
                let _waker = o_waker.as_ref().unwrap();
                if !_waker.is_waked() {
                    // No need to reg again, since waker is not consumed
                    return true;
                }
                _waker
            }
        };
        self.cell.put(waker.weak());
        false
    }

    #[inline(always)]
    fn reg_blocking(&self, waker: &LockedWaker) -> bool {
        self.cell.put(waker.weak());
        true
    }

    #[inline(always)]
    fn cancel_waker(&self, _waker: LockedWaker) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn fire(&self) {
        self.cell.wake();
    }

    #[inline(always)]
    fn close(&self) {
        self.fire();
    }

    /// return waker queue size
    #[inline(always)]
    fn get_size(&self) -> usize {
        if self.cell.exists() {
            1
        } else {
            0
        }
    }
}

struct RegistryMultiSenderInner {
    queue: VecDeque<LockedWakerRef>,
    seq: u64,
}

pub struct RegistryMultiSender {
    checking: AtomicBool,
    // 0 is invalid for seq
    is_empty: AtomicBool,
    inner: Mutex<RegistryMultiSenderInner>,
}

impl RegistryMultiSender {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RegistryMultiSenderInner {
                queue: VecDeque::with_capacity(32),
                seq: 0,
            }),
            checking: AtomicBool::new(false),
            is_empty: AtomicBool::new(true),
        }
    }

    #[inline]
    fn push(&self, waker: &LockedWaker) -> bool {
        let weak = waker.weak();
        let mut guard = self.inner.lock();
        let mut seq = guard.seq.wrapping_add(1);
        if seq == 0 {
            seq = seq.wrapping_add(1);
        }
        guard.seq = seq;
        waker.set_seq(seq);
        if guard.queue.is_empty() {
            self.is_empty.store(false, Ordering::Release);
            guard.queue.push_back(weak);
            true
        } else {
            guard.queue.push_back(weak);
            false
        }
    }
}

impl RegistryTrait for RegistryMultiSender {
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.is_empty.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn reg_async(&self, ctx: &mut Context, o_waker: &mut Option<LockedWaker>) -> bool {
        let waker = {
            if o_waker.is_none() {
                o_waker.replace(LockedWaker::new_async(ctx));
                o_waker.as_ref().unwrap()
            } else {
                let _waker = o_waker.as_ref().unwrap();
                if !_waker.is_waked() {
                    // No need to reg again, since waker is not consumed
                    return true;
                }
                _waker
            }
        };
        self.push(waker);
        false
    }

    #[inline(always)]
    fn reg_blocking(&self, waker: &LockedWaker) -> bool {
        self.push(waker)
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: LockedWaker) {
        // Just abandon and leave it to fire() to clean it
        waker.cancel();
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: u64) {
        if self.checking.swap(true, Ordering::SeqCst) {
            // Other thread is cleaning
            return;
        }
        let mut guard = self.inner.lock();
        while let Some(waker_ref) = guard.queue.pop_front() {
            if waker_ref.try_to_clear(seq) {
                // we do not known push back may have concurrent problem
                break;
            }
        }
        if guard.queue.is_empty() {
            self.is_empty.store(true, Ordering::Release);
        }
        self.checking.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn fire(&self) {
        if self.is_empty.load(Ordering::Acquire) {
            return;
        }
        let mut guard = self.inner.lock();
        while let Some(item) = guard.queue.pop_front() {
            if !item.wake() {
                continue;
            }
            if guard.queue.is_empty() {
                break;
            }
            return;
        }
        self.is_empty.store(true, Ordering::Release);
    }

    #[inline(always)]
    fn close(&self) {
        let mut guard = self.inner.lock();
        while let Some(waker) = guard.queue.pop_front() {
            waker.wake();
        }
        self.is_empty.store(true, Ordering::Release);
    }

    /// return waker queue size
    #[inline(always)]
    fn get_size(&self) -> usize {
        let guard = self.inner.lock();
        guard.queue.len()
    }
}

pub struct RegistryMultiRecv {
    queue: LockedQueue<LockedWakerRef>,
    seq: AtomicU64,
    checking: AtomicBool,
}

impl RegistryMultiRecv {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            queue: LockedQueue::new(32),
            seq: AtomicU64::new(0),
            checking: AtomicBool::new(false),
        }
    }
}

impl RegistryTrait for RegistryMultiRecv {
    #[inline(always)]
    fn is_empty(&self) -> bool {
        unreachable!();
    }

    #[inline(always)]
    fn reg_async(&self, ctx: &mut Context, o_waker: &mut Option<LockedWaker>) -> bool {
        let waker = {
            if o_waker.is_none() {
                o_waker.replace(LockedWaker::new_async(ctx));
                o_waker.as_ref().unwrap()
            } else {
                let _waker = o_waker.as_ref().unwrap();
                if !_waker.is_waked() {
                    // No need to reg again, since waker is not consumed
                    return true;
                }
                _waker
            }
        };
        waker.set_seq(self.seq.fetch_add(1, Ordering::SeqCst));
        self.queue.push(waker.weak());
        false
    }

    #[inline(always)]
    fn reg_blocking(&self, waker: &LockedWaker) -> bool {
        self.queue.push(waker.weak());
        true
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: LockedWaker) {
        // Just abandon and leave it to fire() to clean it
        waker.cancel();
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: u64) {
        if self.checking.swap(true, Ordering::SeqCst) {
            // Other thread is cleaning
            return;
        }
        while let Some(waker_ref) = self.queue.pop() {
            if waker_ref.try_to_clear(seq) {
                // we do not known push back may have concurrent problem
                break;
            }
        }
        self.checking.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn fire(&self) {
        while let Some(waker) = self.queue.pop() {
            if waker.wake() {
                return;
            }
        }
    }

    #[inline(always)]
    fn close(&self) {
        while let Some(waker) = self.queue.pop() {
            waker.wake();
        }
    }

    /// return waker queue size
    #[inline(always)]
    fn get_size(&self) -> usize {
        self.queue.len()
    }
}
