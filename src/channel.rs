use crate::backoff::*;
use crate::crossbeam::array_queue::ArrayQueue;
pub use crate::crossbeam::err::*;
pub use crate::locked_waker::*;
pub use crate::waker_registry::*;
use crossbeam_queue::SegQueue;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::time::{Duration, Instant};

pub(crate) enum Channel<T> {
    List(SegQueue<T>),
    Array(ArrayQueue<T>),
}

impl<T> Channel<T> {
    #[inline(always)]
    pub fn new_list() -> Self {
        Self::List(SegQueue::new())
    }

    #[inline(always)]
    pub fn new_array(bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        assert!(bound > 0);
        Self::Array(ArrayQueue::new(bound))
    }

    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::List(s) => s.len(),
            Self::Array(s) => s.len(),
        }
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        match self {
            Self::Array(s) => Some(s.capacity()),
            Self::List(_) => None,
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        match self {
            Self::List(s) => s.is_empty(),
            Self::Array(s) => s.is_empty(),
        }
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        match self {
            Self::Array(s) => s.is_full(),
            Self::List(_) => false,
        }
    }
}

pub struct ChannelShared<T> {
    closed: AtomicBool,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    congest: AtomicIsize,
    inner: Channel<T>,
    pub(crate) senders: RegistrySender<T>,
    pub(crate) recvs: RegistryRecv,
    bound_size: Option<u32>,
}

impl<T> ChannelShared<T> {
    pub(crate) fn new(
        inner: Channel<T>, senders: RegistrySender<T>, recvs: RegistryRecv,
    ) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            congest: AtomicIsize::new(0),
            senders,
            recvs,
            bound_size: if let Some(bound) = inner.capacity() { Some(bound as u32) } else { None },
            inner,
        })
    }

    #[inline(always)]
    pub(crate) fn try_recv(&self) -> Option<T> {
        match &self.inner {
            Channel::List(inner) => {
                return inner.pop();
            }
            Channel::Array(inner) => {
                return inner.pop();
            }
        }
    }

    #[inline(always)]
    pub(crate) fn is_zero(&self) -> bool {
        self.bound_size == Some(0)
    }

    /// The number of messages in the channel at the moment.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// The capacity of the channel, return None for unbounded channel.
    #[inline(always)]
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    /// Whether channel is empty at the moment
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Whether the channel is full at the moment
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Return true if all the senders or receivers are dropped
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Get the count of alive senders
    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.tx_count.load(Ordering::Acquire) as usize
    }

    /// Get the count of alive receivers
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    pub(crate) fn is_congest(&self) -> bool {
        self.congest.load(Ordering::Relaxed) > 0
    }

    /// Just for debugging purpose, to monitor queue size
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        let _ = self.tx_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.congest.fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        let _ = self.rx_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.congest.fetch_sub(1, Ordering::Release);
    }

    /// Call when tx drop
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        let _ = self.congest.fetch_sub(1, Ordering::Release);
        if self.tx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::SeqCst);
            self._close_all();
        }
    }

    /// Call when rx drop
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        let _ = self.congest.fetch_add(1, Ordering::Release);
        if self.rx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::SeqCst);
            self._close_all();
        }
    }

    #[inline(always)]
    fn _close_all(&self) {
        self.senders.close();
        self.recvs.close();
    }

    /// Register waker for current rx
    #[inline(always)]
    pub(crate) fn reg_recv(&self, o_waker: &RecvWaker) {
        self.recvs.reg_waker(o_waker)
    }

    #[inline(always)]
    pub(crate) fn send(&self, item: &MaybeUninit<T>) -> bool {
        match &self.inner {
            Channel::Array(inner) => {
                return unsafe { inner.push_with_ptr(item.as_ptr()) };
            }
            Channel::List(inner) => {
                inner.push(unsafe { item.assume_init_read() });
                return true;
            }
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn try_send_oneshot(&self, item: &MaybeUninit<T>) -> Option<bool> {
        match &self.inner {
            Channel::Array(inner) => {
                return unsafe { inner.try_push_oneshot(item.as_ptr()) };
            }
            Channel::List(_inner) => {
                unreachable!();
            }
        }
    }

    /// When waker exists and reused for async_tx, might need to check_waker every poll(),
    /// in case of spurious waked up by runtime.
    #[inline]
    pub(crate) fn sender_try_again_async(
        &self, waker: SendWaker<T>, ctx: &mut Context,
    ) -> (u8, Option<SendWaker<T>>) {
        // NOTE: it's possible be WakerState::INIT for AsyncSink::poll_send()
        // Async context does not use direct copy, so there won't be COPY state.
        if self.is_disconnected() {
            match waker.change_state_smaller_eq(WakerState::Waiting, WakerState::Closed) {
                Ok(_) => {
                    return (WakerState::Closed as u8, None);
                }
                Err(state) => {
                    // Since all rx has been drop, not possible to by Copy,
                    return (state, None);
                }
            }
        }
        let mut state = waker.get_state();
        // return pending need to check waker, avoid spurious wake
        let will_wake = waker.will_wake(ctx);
        let backoff_conf: BackoffConfig;
        if will_wake {
            // might be due to select!
            if state != WakerState::Copy as u8 {
                return (state, Some(waker));
            }
            backoff_conf = BackoffConfig { spin_limit: SPIN_LIMIT, limit: DEFAULT_LIMIT };
        } else {
            // spurious wake because no other future can be run,
            // might be async<->blocking, let's yield to other thread
            // to save CPU resource.
            backoff_conf = BackoffConfig { spin_limit: 2, limit: MAX_LIMIT };
        }
        let mut backoff = Backoff::new(backoff_conf);
        while state <= WakerState::Waked as u8 {
            if state != WakerState::Copy as u8 && backoff.is_completed() {
                if will_wake {
                    return (state, Some(waker));
                } else {
                    // The waker could not be used anymore
                    match waker.change_state_smaller_eq(WakerState::Waiting, WakerState::Waked) {
                        Ok(_) => {
                            // This is rare case for idle select with spurious wake
                            self.senders.cancel_waker(&waker);
                            return (WakerState::Waked as u8, None);
                        }
                        Err(state) => {
                            if state == WakerState::Copy as u8 {
                                // reset to continue;
                                backoff.reset();
                                continue;
                            }
                            return (state, None);
                        }
                    }
                }
            }
            backoff.snooze();
            state = waker.get_state();
        }
        return (state, None);
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    #[inline]
    pub(crate) fn sender_reg_and_try(
        &self, item: &mut MaybeUninit<T>, waker: SendWaker<T>, sink: bool,
    ) -> (u8, Option<SendWaker<T>>) {
        self.senders.reg_waker(&waker);
        // Not allow Spurious wake and enter this function again;
        if let Some(res) = self.try_send_oneshot(item) {
            if res {
                waker.set_state(WakerState::Done);
                self.senders.cancel_waker(&waker);
                self.on_send();
                return (WakerState::Done as u8, Some(waker));
            } else {
                if sink {
                    if self.is_disconnected() {
                        return (WakerState::Closed as u8, None);
                    } else {
                        // outside logic only regconize Waiting
                        return (WakerState::Waiting as u8, Some(waker));
                    }
                } else {
                    let state = waker.commit_waiting();
                    // let on_recv do it's job,
                    // is_disconnected == true means no receivers
                    if self.is_disconnected() {
                        return (WakerState::Closed as u8, None);
                    } else {
                        return (state, Some(waker));
                    }
                }
            }
        } else {
            match waker.try_change_state(WakerState::Init, WakerState::Waked) {
                Ok(_) => {
                    self.senders.cancel_waker(&waker);
                    // Unlikely to be disconnected,
                    // might be in queue, should not use again
                    // Retry send outside
                    return (WakerState::Waked as u8, None);
                }
                Err(state) => {
                    // changed by waker, might be: COPY, Waked, or Done.
                    return (state, Some(waker));
                }
            }
        }
    }

    /// Prevent Copy state enter
    #[inline(always)]
    pub(crate) fn sender_snooze(&self, waker: &SendWaker<T>, backoff: &mut Backoff) -> u8 {
        backoff.reset();
        loop {
            backoff.snooze();
            let state = waker.get_state();
            if state >= WakerState::Waked as u8 {
                return state;
            } else if state == WakerState::Waiting as u8 && backoff.is_completed() {
                return state;
            }
        }
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        self.recvs.fire();
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        if WakeResult::Sent == self.senders.fire(self) {
            self.on_send();
        }
    }

    #[inline(always)]
    pub(crate) fn on_recv_try_send(&self, waker: &WakerInner<*mut T>) -> WakeResult {
        match waker.wake_or_copy() {
            Ok(r) => return r,
            Err(p) => {
                // There won't be direct copy when timeout and async,
                // so it's safe to proceed without a COPY state, saving an atomic OP
                if let Channel::Array(inner) = &self.inner {
                    if unsafe { inner.push_with_ptr(p) } {
                        waker.set_state(WakerState::Done);
                        waker._wake_nolock();
                        return WakeResult::Sent;
                    } else {
                        waker.set_state(WakerState::Waked);
                        waker._wake_nolock();
                        return WakeResult::PushBack;
                    }
                } else {
                    unreachable!();
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn recv_waker_cancel(&self, waker: &RecvWaker) {
        if waker.cancel() {
            self.recvs.cancel_waker(&waker);
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already Done.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, waker: SendWaker<T>) -> bool {
        let state = waker.abandon();
        if state == WakerState::Closed as u8 {
            self.senders.clear_wakers(waker.get_seq());
            return true;
        } else if state == WakerState::Done as u8 {
            return false;
        } else {
            debug_assert_eq!(state, WakerState::Waked as u8);
            // We are waked, but give up sending, should notify another sender for safety
            self.on_recv();
            return true;
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, waker: RecvWaker) -> bool {
        let state = waker.abandon();
        if state == WakerState::Closed as u8 {
            self.recvs.clear_wakers(waker.get_seq());
            return true;
        } else if state == WakerState::Done as u8 {
            return false;
        } else {
            debug_assert_eq!(state, WakerState::Waked as u8);
            // We are waked, but give up receiving, should notify another receiver for safety
            self.on_send();
            return true;
        }
    }

    /// On timeout, clear dead wakers on receiver queue
    #[inline(always)]
    pub(crate) fn clear_recv_wakers(&self, seq: usize) {
        self.recvs.clear_wakers(seq);
    }

    #[inline]
    pub(crate) fn detect_async_backoff_tx(&self) -> u16 {
        // Async parameter is determine by runtime,
        // like tokio you might have multiple runtime. So the result should stored in
        // sender and receivers, not in the ChannelShared
        #[cfg(feature = "tokio")]
        {
            use tokio::runtime::Handle;
            if Handle::current().metrics().num_workers() <= 1 {
                return 0;
            }
        }
        if self.bound_size > Some(0) && self.bound_size <= Some(2) {
            return 6;
        } else {
            return 1;
        }
    }

    #[inline]
    pub(crate) fn detect_async_backoff_rx(&self) -> u16 {
        // Async parameter is determine by runtime,
        // like tokio you might have multiple runtime. So the result should stored in
        // sender and receivers, not in the ChannelShared
        #[cfg(feature = "tokio")]
        {
            use tokio::runtime::Handle;
            if Handle::current().metrics().num_workers() <= 1 {
                return 0;
            }
        }
        if self.bound_size > Some(0) && self.bound_size <= Some(2) {
            return 6;
        } else {
            return 1;
        }
    }
}

/// On timed out, returns Err(())
#[inline(always)]
pub fn check_timeout(deadline: Option<Instant>) -> Result<Option<Duration>, ()> {
    if let Some(end) = deadline {
        let now = Instant::now();
        if now < end {
            return Ok(Some(end - now));
        } else {
            return Err(());
        }
    }
    Ok(None)
}
