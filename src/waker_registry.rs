use crate::channel::ChannelShared;
use crate::collections::WeakCell;
use crate::locked_waker::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub enum RegistrySender<T> {
    Single(RegistrySingle<*mut T>),
    Multi(RegistryMulti<*mut T>),
    Dummy,
}

impl<T> RegistrySender<T> {
    #[inline(always)]
    pub fn new_single() -> Self {
        Self::Single(RegistrySingle::<*mut T>::new())
    }

    #[inline(always)]
    pub fn new_multi() -> Self {
        Self::Multi(RegistryMulti::<*mut T>::new())
    }

    #[inline(always)]
    pub fn use_direct_copy(&self, channel: &ChannelShared<T>) -> bool {
        match self {
            RegistrySender::Multi(inner) => {
                if channel.congest.load(Ordering::Relaxed) > 0 {
                    return true;
                }
                return !inner.is_empty();
            }
            RegistrySender::Single(_) => true,
            RegistrySender::Dummy => false,
        }
    }

    #[inline(always)]
    pub fn reg_waker(&self, waker: &SendWaker<T>) {
        debug_assert_eq!(waker.get_state(), WakerState::Init as u8);
        // Clear the ptr in waker if it want to re-register
        match self {
            RegistrySender::Multi(inner) => inner.reg_waker(waker),
            RegistrySender::Single(inner) => inner.reg_waker(waker),
            _ => {}
        }
    }

    /// Cancel outdated wakers until me, make sure it does not accumulate
    #[inline(always)]
    pub fn clear_wakers(&self, waker: &SendWaker<T>) {
        match self {
            RegistrySender::Single(inner) => inner.cancel_waker(),
            RegistrySender::Multi(inner) => inner.clear_wakers(waker, false),
            _ => {}
        }
    }

    /// Cancel one outdated waker, make sure it does not accumulate
    #[inline(always)]
    pub fn cancel_waker(&self, waker: &SendWaker<T>) {
        match self {
            RegistrySender::Multi(inner) => inner.clear_wakers(waker, true),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn fire(&self, shared: &ChannelShared<T>) -> WakeResult {
        match self {
            RegistrySender::Multi(inner) => {
                return inner.fire(|waker| shared.on_recv_try_send(waker));
            }
            RegistrySender::Single(inner) => {
                if let Some(waker) = inner.pop() {
                    return shared.on_recv_try_send(&waker);
                }
            }
            _ => {}
        }
        return WakeResult::Next;
    }

    #[inline(always)]
    pub fn close(&self) {
        match self {
            RegistrySender::Single(inner) => inner.close(),
            RegistrySender::Multi(inner) => inner.close(),
            _ => {}
        }
    }

    /// return waker queue size
    pub fn len(&self) -> usize {
        match self {
            RegistrySender::Single(inner) => inner.len(),
            RegistrySender::Multi(inner) => inner.len(),
            RegistrySender::Dummy => 0,
        }
    }
}

pub enum RegistryRecv {
    Single(RegistrySingle<()>),
    Multi(RegistryMulti<()>),
}

impl RegistryRecv {
    #[inline(always)]
    pub fn new_single() -> Self {
        Self::Single(RegistrySingle::<()>::new())
    }

    #[inline(always)]
    pub fn new_multi() -> Self {
        Self::Multi(RegistryMulti::<()>::new())
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        match self {
            RegistryRecv::Single(inner) => inner.is_empty(),
            RegistryRecv::Multi(inner) => inner.is_empty(),
        }
    }

    #[inline(always)]
    pub fn reg_waker(&self, waker: &RecvWaker) {
        debug_assert_eq!(waker.get_state(), WakerState::Init as u8);
        match self {
            RegistryRecv::Multi(inner) => inner.reg_waker(waker),
            RegistryRecv::Single(inner) => inner.reg_waker(waker),
        }
    }

    #[inline(always)]
    pub fn fire(&self) {
        match self {
            RegistryRecv::Multi(inner) => {
                inner.fire(|waker| waker.wake());
            }
            RegistryRecv::Single(inner) => {
                if let Some(waker) = inner.pop() {
                    let _ = waker.wake();
                }
            }
        }
    }

    /// cancel outdated wakers until me, make sure it does not accumulate
    #[inline(always)]
    pub fn clear_wakers(&self, waker: &RecvWaker) {
        match self {
            RegistryRecv::Single(inner) => inner.cancel_waker(),
            RegistryRecv::Multi(inner) => inner.clear_wakers(waker, false),
        }
    }

    /// cancel one outdated waker, make sure it does not accumulate
    #[inline(always)]
    pub fn cancel_waker(&self, waker: &RecvWaker) {
        match self {
            RegistryRecv::Multi(inner) => inner.clear_wakers(waker, true),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn close(&self) {
        match self {
            RegistryRecv::Single(inner) => inner.close(),
            RegistryRecv::Multi(inner) => inner.close(),
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

pub struct RegistrySingle<P> {
    cell: WeakCell<WakerInner<P>>,
}

impl<P> RegistrySingle<P> {
    #[inline(always)]
    pub fn new() -> Self {
        Self { cell: WeakCell::new() }
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn is_empty(&self) -> bool {
        !self.cell.exists()
    }

    /// return is_skip
    #[inline(always)]
    fn reg_waker(&self, waker: &ChannelWaker<P>) {
        self.cell.put(waker.weak());
    }

    #[inline(always)]
    fn cancel_waker(&self) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn pop(&self) -> Option<Arc<WakerInner<P>>> {
        if let Some(inner) = self.cell.pop() {
            Some(inner)
        } else {
            None
        }
    }

    fn close(&self) {
        if let Some(inner) = self.cell.pop() {
            let _ = inner.close_wake();
        }
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }
}

struct RegistryMultiInner<P> {
    queue: VecDeque<Weak<WakerInner<P>>>,
}

pub struct RegistryMulti<P> {
    is_empty: AtomicBool,
    inner: Mutex<RegistryMultiInner<P>>,
    seq: AtomicUsize,
}

impl<P> RegistryMulti<P> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RegistryMultiInner { queue: VecDeque::with_capacity(32) }),
            is_empty: AtomicBool::new(true),
            seq: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.is_empty.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn reg_waker(&self, waker: &ChannelWaker<P>) {
        let weak = waker.weak();
        let mut guard = self.inner.lock();
        let seq = self.seq.fetch_add(1, Ordering::Release);
        waker.set_seq(seq);
        if guard.queue.is_empty() {
            self.is_empty.store(false, Ordering::SeqCst);
        }
        guard.queue.push_back(weak);
    }

    #[inline(always)]
    fn pop(&self) -> Option<ChannelWaker<P>> {
        if self.is_empty.load(Ordering::SeqCst) {
            return None;
        }
        let mut guard = self.inner.lock();
        let mut waker = None;
        loop {
            if let Some(weak) = guard.queue.pop_front() {
                if let Some(inner) = weak.upgrade() {
                    waker = Some(ChannelWaker::from_arc(inner));
                    break;
                }
            } else {
                break;
            }
        }
        if guard.queue.is_empty() {
            self.is_empty.store(true, Ordering::SeqCst);
        }
        return waker;
    }

    #[inline(always)]
    fn fire<F>(&self, handle: F) -> WakeResult
    where
        F: Fn(&ChannelWaker<P>) -> WakeResult,
    {
        if let Some(waker) = self.pop() {
            let r = handle(&waker);
            if r != WakeResult::Next {
                return r;
            }
            let seq = self.seq.load(Ordering::SeqCst);
            while let Some(waker) = self.pop() {
                let r = handle(&waker);
                if r != WakeResult::Next {
                    return r;
                }
                // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                // Because some waker (issued by sink / stream) might be INIT all the time,
                // prevent to dead loop situation when they are wake up and re-register again.
                if waker.get_seq().wrapping_add(1) >= seq {
                    return WakeResult::Next;
                }
            }
        }
        WakeResult::Next
    }

    /// Call when waker is cancelled
    #[inline(always)]
    fn clear_wakers(&self, old_waker: &ChannelWaker<P>, oneshot: bool) {
        if self.is_empty.load(Ordering::SeqCst) {
            return;
        }
        let old_seq = old_waker.get_seq();
        macro_rules! process {
            ($guard: expr, $weak: expr) => {{
                if let Some(waker) = $weak.upgrade() {
                    let _seq = waker.get_seq();
                    if _seq == old_seq {
                        true
                    } else {
                        // There might be later waker cancel due to success sending before commit_waiting.
                        // While earlier waker is still waiting.
                        let state = waker.get_state();
                        if state == WakerState::Init as u8 {
                            let _ = waker.wake();
                            if oneshot {
                                true
                            } else if _seq > old_seq {
                                true
                            } else {
                                false
                            }
                        } else if state == WakerState::Waiting as u8 {
                            $guard.queue.push_front($weak);
                            return;
                        } else {
                            false
                        }
                    }
                } else {
                    false
                }
            }};
        }
        let mut guard = self.inner.lock();
        if let Some(weak) = guard.queue.pop_front() {
            if process!(guard, weak) {
                if guard.queue.is_empty() {
                    self.is_empty.store(true, Ordering::SeqCst);
                }
                return;
            }
            loop {
                if let Some(weak) = guard.queue.pop_front() {
                    if process!(guard, weak) {
                        if guard.queue.is_empty() {
                            self.is_empty.store(true, Ordering::SeqCst);
                        }
                        return;
                    }
                } else {
                    self.is_empty.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn close(&self) {
        if self.is_empty.load(Ordering::SeqCst) {
            return;
        }
        let mut guard = self.inner.lock();
        while let Some(weak) = guard.queue.pop_front() {
            if let Some(waker) = weak.upgrade() {
                waker.close_wake();
            }
        }
        self.is_empty.store(true, Ordering::SeqCst);
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        let guard = self.inner.lock();
        guard.queue.len()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::locked_waker::RecvWaker;
    #[test]
    fn test_registry_multi() {
        let reg = RegistryMulti::new();

        // test push
        let waker1 = RecvWaker::new_blocking(());
        assert_eq!(reg.is_empty(), true);
        waker1.set_state(WakerState::Init);
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        assert_eq!(waker1.get_seq(), 0);
        assert_eq!(reg.is_empty(), false);
        assert_eq!(reg.len(), 1);

        let waker2 = RecvWaker::new_blocking(());
        reg.reg_waker(&waker2);
        waker2.set_state(WakerState::Waiting);
        assert_eq!(waker2.get_seq(), 1);
        assert_eq!(reg.len(), 2);
        assert_eq!(waker2.get_seq(), waker1.get_seq() + 1);
        assert_eq!(waker2.get_state(), WakerState::Waiting as u8);

        if let Some(w) = reg.pop() {
            assert!(w.wake() == WakeResult::Next);
        }
        assert_eq!(waker1.get_state(), WakerState::Waked as u8);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.is_empty(), false);
        if let Some(w) = reg.pop() {
            assert!(w.wake() == WakeResult::Waked);
        }
        assert_eq!(waker2.get_state(), WakerState::Waked as u8);
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);

        // test seq

        let waker3 = RecvWaker::new_blocking(());
        reg.reg_waker(&waker3);
        waker3.set_state(WakerState::Waiting);
        assert_eq!(waker3.get_state(), WakerState::Waiting as u8);
        let waker4 = RecvWaker::new_blocking(());
        reg.reg_waker(&waker4); // Init
        assert_eq!(waker4.get_state(), WakerState::Init as u8);
        let num_workers = reg.len();
        // Because waker3 not waked up, waker4 is not clear
        reg.clear_wakers(&waker4, false);
        assert_eq!(reg.len(), num_workers);
        for _ in 0..10 {
            let _waker = RecvWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        let num_workers = reg.len();
        assert_eq!(reg.len(), num_workers);
        waker3.set_state(WakerState::Init);
        //        assert!(waker4.abandon().is_ok());
        println!("clear waker4 oneshot seq {}", waker4.get_seq());
        reg.clear_wakers(&waker4, true); // oneshot only clear waker3
        assert_eq!(reg.len(), num_workers - 1);
        assert!(waker3.get_state() >= WakerState::Waked as u8);
        assert_eq!(waker4.get_state(), WakerState::Init as u8);
        let waker5 = RecvWaker::new_blocking(());
        reg.reg_waker(&waker5);
        println!("clear waker5 seq={}", waker5.get_seq());
        reg.clear_wakers(&waker5, false); // clear waker4, waker5
        assert_eq!(reg.len(), 0);

        println!("test close");
        for _ in 0..10 {
            let _waker = RecvWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        assert_eq!(reg.is_empty(), false);
        reg.close();
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);
    }
}
