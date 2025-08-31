use crate::collections::LockedQueue;
use crate::locked_waker::*;
use crate::trace_log;
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
    fn reg_waker(&self, _waker: &LockedWaker, _tag: &str);

    fn clear_wakers(&self, _seq: u64, _tag: &str);

    fn cancel_waker(&self, _waker: &LockedWaker, _tag: &str);

    fn fire(&self, _tag: &str);

    fn close(&self, _tag: &str);

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
    fn reg_waker(&self, _waker: &LockedWaker, _tag: &str) {
        unreachable!();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64, _tag: &str) {}

    #[inline(always)]
    fn cancel_waker(&self, _waker: &LockedWaker, _tag: &str) {}

    #[inline(always)]
    fn fire(&self, _tag: &str) {}

    #[inline(always)]
    fn close(&self, _tag: &str) {}

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }
}

pub struct RegistrySingle {
    cell: WakerCell,
    #[cfg(feature = "deadlock_debug")]
    seq: AtomicU64,
}

impl RegistrySingle {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Single(Self {
            cell: WakerCell::new(),
            #[cfg(feature = "deadlock_debug")]
            seq: AtomicU64::new(0),
        })
    }
}

impl RegistryTrait for RegistrySingle {
    /// return is_skip
    #[inline(always)]
    fn reg_waker(&self, waker: &LockedWaker, _tag: &str) {
        #[cfg(feature = "deadlock_debug")]
        {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            waker.set_seq(seq);
        }
        self.cell.put(waker.weak());
        trace_log!("{}: reg {:?}", _tag, waker);
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: &LockedWaker, _tag: &str) {
        // Got to be it, because only one single thread.
        // Although it's ok to ignore it, next time will be overwritten,
        // but miri is not happy about it
        if waker.abandon() >= WakerState::WAKED as u8 {
            return;
        }
        let _r = self.cell.clear();
        trace_log!("{}: cancel_waker {:?}", _tag, _r);
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64, _tag: &str) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn fire(&self, _tag: &str) {
        if let Some(waker) = self.cell.pop() {
            let _old_state = waker.wake();
            trace_log!("wake {} {:?} {}", _tag, waker, _old_state);
        }
    }

    #[inline(always)]
    fn close(&self, tag: &str) {
        self.fire(tag);
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
    fn reg_waker(&self, waker: &LockedWaker, _tag: &str) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        waker.set_seq(seq);
        self.queue.push(waker.weak());
        trace_log!("{}: reg {:?}", _tag, waker);
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: &LockedWaker, _tag: &str) {
        if waker.abandon() >= WakerState::WAKED as u8 {
            return;
        }
        let seq = waker.get_seq();
        if let Some(waker_ref) = self.queue.pop() {
            waker_ref.try_to_clear(seq);
            trace_log!("{}: canceled {:?}", _tag, waker);
            // Just abandon and leave it to fire() to clean it.
            // At most try one.
        } else {
            trace_log!("{}: nothing to cancel", _tag);
        }
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: u64, _tag: &str) {
        if self.checking.swap(true, Ordering::Acquire) {
            // Other thread is cleaning
            return;
        }
        while let Some(waker_ref) = self.queue.pop() {
            trace_log!("{}: clear {:?}", _tag, waker_ref);
            if waker_ref.try_to_clear(seq) {
                // we do not known push back may have concurrent problem
                break;
            }
        }
        self.checking.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn fire(&self, _tag: &str) {
        let seq = self.seq.load(Ordering::SeqCst).wrapping_sub(1);
        while let Some(weak) = self.queue.pop() {
            if let Some(waker) = weak.upgrade() {
                let old_state = waker.wake();
                trace_log!("{}: wake {:?} {}", _tag, waker, old_state);
                if old_state == WakerState::WAITING as u8 {
                    return;
                }
                // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                // Because some waker (issued by sink / stream) might be INIT all the time,
                // prevent to dead loop situation when they are wake up and re-register again.
                if waker.get_seq() >= seq {
                    trace_log!("{}: stopped at seq {}", _tag, seq);
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn close(&self, _tag: &str) {
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
