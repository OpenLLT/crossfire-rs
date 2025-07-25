use crate::collections::WeakCell;
use crate::locked_waker::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;

pub enum RegistrySender<T> {
    Single(RegistrySingle<SendWaker<T>>),
    Multi(RegistryMulti<SendWaker<T>>),
    Dummy(RegistryDummy<SendWaker<T>>),
}

impl<T> RegistrySender<T> {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        match self {
            RegistrySender::Single(inner) => inner.is_empty(),
            RegistrySender::Multi(inner) => inner.is_empty(),
            RegistrySender::Dummy(_) => true,
        }
    }

    /// For async context
    #[inline(always)]
    pub fn reg_async(&self, waker: &SendWaker<T>) -> Result<(), u8> {
        let state = waker.get_state();
        if state == WakerState::WAKED as u8 {
            waker.set_state(WakerState::WAITING);
        } else {
            // Might be WAITING
            return Err(state);
        }
        match self {
            RegistrySender::Single(inner) => inner.reg_async(waker),
            RegistrySender::Multi(inner) => inner.reg_async(waker),
            RegistrySender::Dummy(_) => {}
        }
        Ok(())
    }

    /// For thread context
    #[inline(always)]
    pub fn reg_blocking(&self, waker: &SendWaker<T>) -> Result<(), u8> {
        let state = waker.get_state();
        if state == WakerState::WAKED as u8 {
            waker.set_state(WakerState::WAITING);
        } else {
            // Might be WAITING
            return Err(state);
        }
        match self {
            RegistrySender::Single(inner) => inner.reg_blocking(waker),
            RegistrySender::Multi(inner) => inner.reg_blocking(waker),
            RegistrySender::Dummy(_) => {}
        }
        Ok(())
    }

    #[inline(always)]
    pub fn clear_wakers(&self, seq: usize) {
        match self {
            RegistrySender::Single(inner) => inner.clear_wakers(seq),
            RegistrySender::Multi(inner) => inner.clear_wakers(seq),
            RegistrySender::Dummy(_) => {}
        }
    }

    #[inline(always)]
    pub fn cancel_waker(&self) {
        match self {
            RegistrySender::Single(inner) => inner.cancel_waker(),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<SendWaker<T>> {
        match self {
            RegistrySender::Single(inner) => inner.pop(),
            RegistrySender::Multi(inner) => inner.pop(),
            RegistrySender::Dummy(_) => None,
        }
    }

    /// return waker queue size
    pub fn len(&self) -> usize {
        match self {
            RegistrySender::Single(inner) => inner.len(),
            RegistrySender::Multi(inner) => inner.len(),
            RegistrySender::Dummy(_) => 0,
        }
    }
}

pub enum RegistryRecv {
    Single(RegistrySingle<RecvWaker>),
    Multi(RegistryMulti<RecvWaker>),
}

impl RegistryRecv {
    /// For async context
    #[inline(always)]
    pub fn reg_async(&self, waker: &RecvWaker) -> Result<(), u8> {
        let state = waker.get_state();
        if state == WakerState::WAKED as u8 {
            waker.set_state(WakerState::INIT);
        } else {
            // Might be WAITING
            return Err(state);
        }
        match self {
            RegistryRecv::Single(inner) => inner.reg_async(waker),
            RegistryRecv::Multi(inner) => inner.reg_async(waker),
        }
        Ok(())
    }

    /// For thread context
    #[inline(always)]
    pub fn reg_blocking(&self, waker: &RecvWaker) -> Result<(), u8> {
        let state = waker.get_state();
        if state == WakerState::WAKED as u8 {
            waker.set_state(WakerState::INIT);
        } else {
            // Might be WAITING
            return Err(state);
        }
        match self {
            RegistryRecv::Single(inner) => inner.reg_blocking(waker),
            RegistryRecv::Multi(inner) => inner.reg_blocking(waker),
        }
        Ok(())
    }

    #[inline(always)]
    pub fn clear_wakers(&self, seq: usize) {
        match self {
            RegistryRecv::Single(inner) => inner.clear_wakers(seq),
            RegistryRecv::Multi(inner) => inner.clear_wakers(seq),
        }
    }

    #[inline(always)]
    pub fn cancel_waker(&self) {
        match self {
            RegistryRecv::Single(inner) => inner.cancel_waker(),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<RecvWaker> {
        match self {
            RegistryRecv::Single(inner) => inner.pop(),
            RegistryRecv::Multi(inner) => inner.pop(),
        }
    }

    /// return waker queue size
    pub fn len(&self) -> usize {
        match self {
            RegistryRecv::Single(inner) => inner.len(),
            RegistryRecv::Multi(inner) => inner.len(),
        }
    }
}

/// RegistryDummy is for unbounded channel tx, which is never blocked
pub struct RegistryDummy<W: WakerTrait>(PhantomData<W>);

impl<W: WakerTrait> RegistryDummy<W> {
    #[inline(always)]
    pub fn new() -> Self {
        Self(Default::default())
    }
}

pub struct RegistrySingle<W: WakerTrait> {
    cell: WeakCell<W::Inner>,
}

impl<W: WakerTrait> RegistrySingle<W> {
    #[inline(always)]
    pub fn new() -> Self {
        Self { cell: WeakCell::new() }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        !self.cell.exists()
    }

    /// return is_skip
    #[inline(always)]
    fn reg_async(&self, waker: &W) {
        self.cell.put(waker.weak());
    }

    #[inline(always)]
    fn reg_blocking(&self, waker: &W) {
        self.cell.put(waker.weak());
    }

    #[inline(always)]
    fn cancel_waker(&self) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: usize) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn pop(&self) -> Option<W> {
        if let Some(w) = self.cell.pop() {
            Some(W::from_arc(w))
        } else {
            None
        }
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        if self.cell.exists() {
            1
        } else {
            0
        }
    }
}

struct RegistryMultiInner<W: WakerTrait> {
    queue: VecDeque<Weak<W::Inner>>,
    seq: usize,
}

pub struct RegistryMulti<W: WakerTrait> {
    checking: AtomicBool,
    is_empty: AtomicBool,
    inner: Mutex<RegistryMultiInner<W>>,
}

impl<W: WakerTrait> RegistryMulti<W> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RegistryMultiInner { queue: VecDeque::with_capacity(32), seq: 0 }),
            checking: AtomicBool::new(false),
            is_empty: AtomicBool::new(true),
        }
    }

    #[inline(always)]
    fn push(&self, waker: &W) {
        let weak = waker.weak();
        let mut guard = self.inner.lock();
        let seq = guard.seq.wrapping_add(1);
        guard.seq = seq;
        waker.set_seq(seq);
        if self.is_empty.load(Ordering::Relaxed) {
            self.is_empty.store(false, Ordering::Release);
            guard.queue.push_back(weak);
        } else {
            guard.queue.push_back(weak);
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.is_empty.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn reg_async(&self, waker: &W) {
        self.push(&waker);
    }

    #[inline(always)]
    fn reg_blocking(&self, waker: &W) {
        self.push(waker);
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: usize) {
        if self.checking.swap(true, Ordering::SeqCst) {
            // Other thread is cleaning
            return;
        }
        let mut guard = self.inner.lock();
        while let Some(weak) = guard.queue.pop_front() {
            if W::try_to_clear(weak, seq) {
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
    fn pop(&self) -> Option<W> {
        if self.is_empty.load(Ordering::Acquire) {
            return None;
        }
        let mut guard = self.inner.lock();
        while let Some(weak) = guard.queue.pop_front() {
            if guard.queue.is_empty() {
                self.is_empty.store(true, Ordering::Release);
            }
            if let Some(waker) = weak.upgrade() {
                return Some(W::from_arc(waker));
            }
        }
        self.is_empty.store(true, Ordering::Release);
        return None;
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        let guard = self.inner.lock();
        guard.queue.len()
    }
}

/*
#[cfg(test)]
mod tests {

    use super::*;
    use crate::locked_waker::RecvWaker;
    #[test]
    fn test_registry_multi() {
        let reg = RegistryMulti::new();

        // test push
        let waker1 = RecvWaker::new_blocking();
        assert_eq!(reg.is_empty(), true);
        reg.reg_blocking(&waker1);
        assert!(waker1.get_seq() > 0);
        assert_eq!(reg.is_empty(), false);
        assert_eq!(reg.len(), 1);
        assert_eq!(waker1.is_waked(), false);

        let waker2 = RecvWaker::new_blocking();
        reg.reg_blocking(&waker2);
        assert_eq!(reg.len(), 2);
        assert_eq!(waker2.get_seq(), waker1.get_seq() + 1);
        assert_eq!(waker2.is_waked(), false);

        // test fire
        reg.fire();
        assert_eq!(waker1.is_waked(), true);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.is_empty(), false);
        reg.fire();
        assert_eq!(waker2.is_waked(), true);
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);

        // test seq

        let waker3 = RecvWaker::new_blocking();
        reg.reg_blocking(&waker3);
        let waker4 = RecvWaker::new_blocking();
        reg.reg_blocking(&waker4);
        for _ in 0..10 {
            let _waker = RecvWaker::new_blocking();
            reg.reg_blocking(&_waker);
        }
        assert_eq!(reg.len(), 12);
        assert_eq!(waker4.abandon(), false);
        reg.clear_wakers(waker4.get_seq());
        assert_eq!(reg.len(), 10);
        assert!(waker3.is_waked());
        assert!(waker4.is_waked());

        // test close
        assert_eq!(reg.is_empty(), false);
        reg.close();
        assert_eq!(reg.len(), 0);
    }
}
*/
