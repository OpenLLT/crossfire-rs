use crate::collections::LockedQueue;
use crate::locked_waker::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[enum_dispatch(RegistryTrait)]
pub enum Registry {
    Single(RegistrySingle),
    Multi(RegistryMulti),
    Dummy(RegistryDummy),
}

#[enum_dispatch]
pub trait RegistryTrait {
    /// For async context
    fn reg_waker(&self, waker: &LockedWaker);

    fn clear_wakers(&self, _seq: u64);

    fn cancel_waker(&self, _waker: &LockedWaker);

    fn fire(&self);

    fn close(&self);

    /// return waker queue size
    fn len(&self) -> usize;
}

/// RegistryDummy is for unbounded channel tx, which is never blocked
pub struct RegistryDummy();

impl RegistryDummy {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Dummy(RegistryDummy())
    }
}

impl RegistryTrait for RegistryDummy {
    #[inline(always)]
    fn reg_waker(&self, _waker: &LockedWaker) {
        unreachable!();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {}

    #[inline(always)]
    fn cancel_waker(&self, _waker: &LockedWaker) {}

    #[inline(always)]
    fn fire(&self) {}

    #[inline(always)]
    fn close(&self) {}

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }
}

pub struct RegistrySingle {
    cell: WakerCell,
}

impl RegistrySingle {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Single(Self { cell: WakerCell::new() })
    }
}

impl RegistryTrait for RegistrySingle {
    /// return is_skip
    #[inline(always)]
    fn reg_waker(&self, waker: &LockedWaker) {
        self.cell.put(waker.weak());
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: &LockedWaker) {
        // Got to be it, because only one single thread.
        // Although it's ok to ignore it, next time will be overwritten,
        // but miri is not happy about it
        if waker.abandon() >= WakerState::WAKED as u8 {
            return;
        }
        self.cell.clear();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn fire(&self) {
        if let Some(waker) = self.cell.pop() {
            if waker.wake() {
                return;
            }
        }
    }

    #[inline(always)]
    fn close(&self) {
        self.fire();
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }
}

pub struct RegistryMulti {
    queue: LockedQueue<LockedWakerRef>,
    seq: AtomicU64,
    checking: AtomicBool,
}

impl RegistryMulti {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Multi(Self {
            queue: LockedQueue::new(32),
            seq: AtomicU64::new(0),
            checking: AtomicBool::new(false),
        })
    }
}

impl RegistryTrait for RegistryMulti {
    #[inline(always)]
    fn reg_waker(&self, waker: &LockedWaker) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        waker.set_seq(seq);
        self.queue.push(waker.weak());
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: &LockedWaker) {
        if waker.abandon() >= WakerState::WAKED as u8 {
            return;
        }
        let seq = waker.get_seq();
        if let Some(waker_ref) = self.queue.pop() {
            waker_ref.try_to_clear(seq);
            // Just abandon and leave it to fire() to clean it.
            // At most try one.
        }
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: u64) {
        if self.checking.swap(true, Ordering::Acquire) {
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
        let seq = self.seq.load(Ordering::SeqCst).wrapping_sub(1);
        while let Some(weak) = self.queue.pop() {
            if let Some(waker) = weak.upgrade() {
                if waker.wake() {
                    return;
                }
                // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                // Because some waker (issued by sink / stream) might be INIT all the time,
                // prevent to dead loop situation when they are wake up and re-register again.
                if waker.get_seq() >= seq {
                    return;
                }
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
    fn len(&self) -> usize {
        self.queue.len()
    }
}
