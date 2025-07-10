use crate::locked_waker::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Context;

#[enum_dispatch(RegistryTrait)]
pub enum Registry {
    Single(RegistrySingle),
    Multi(RegistryMulti),
    Dummy(RegistryDummy),
}

#[enum_dispatch]
pub trait RegistryTrait {
    fn get_control_seq(&self) -> u64;

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
    pub fn new() -> Registry {
        Registry::Dummy(RegistryDummy())
    }
}

impl RegistryTrait for RegistryDummy {
    #[inline(always)]
    fn reg_async(&self, _ctx: &mut Context, _o_waker: &mut Option<LockedWaker>) -> bool {
        unreachable!();
    }

    #[inline(always)]
    fn reg_blocking(&self, _waker: &LockedWaker) -> bool {
        unreachable!();
    }

    #[inline(always)]
    fn get_control_seq(&self) -> u64 {
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
    pub fn new() -> Registry {
        Registry::Single(Self { cell: WakerCell::new() })
    }
}

impl RegistryTrait for RegistrySingle {
    #[inline(always)]
    fn get_control_seq(&self) -> u64 {
        unreachable!();
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

struct RegistryMultiInner {
    queue: VecDeque<LockedWakerRef>,
    seq: u64,
}

pub struct RegistryMulti {
    checking: AtomicBool,
    // 0 is invalid for seq
    control_seq: AtomicU64,
    inner: Mutex<RegistryMultiInner>,
}

impl RegistryMulti {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Multi(Self {
            inner: Mutex::new(RegistryMultiInner { queue: VecDeque::with_capacity(32), seq: 0 }),
            checking: AtomicBool::new(false),
            control_seq: AtomicU64::new(0),
        })
    }
}

impl RegistryMulti {
    #[inline]
    fn push(&self, waker: &LockedWaker) -> bool {
        let weak = waker.weak();
        let mut guard = self.inner.lock();
        guard.seq += 1;
        if guard.seq == 0 {
            guard.seq += 1;
        }
        waker.set_seq(guard.seq);
        self.control_seq.store(guard.seq, Ordering::Release);
        let is_first = guard.queue.len() == 0;
        guard.queue.push_back(weak);
        return is_first;
    }
}

impl RegistryTrait for RegistryMulti {
    #[inline(always)]
    fn get_control_seq(&self) -> u64 {
        self.control_seq.load(Ordering::Acquire)
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
        self.checking.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn fire(&self) {
        if self.control_seq.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut guard = self.inner.lock();
        while let Some(item) = guard.queue.pop_front() {
            if item.wake() {
                return;
            }
        }
        self.control_seq.store(0, Ordering::Release);
    }

    #[inline(always)]
    fn close(&self) {
        let mut guard = self.inner.lock();
        while let Some(waker) = guard.queue.pop_front() {
            waker.wake();
        }
        self.control_seq.store(0, Ordering::Release);
    }

    /// return waker queue size
    #[inline(always)]
    fn get_size(&self) -> usize {
        let guard = self.inner.lock();
        guard.queue.len()
    }
}
