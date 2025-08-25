use crate::backoff::*;
use crate::collections::ArcCell;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::transmute;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc, Weak,
};
use std::task::*;
use std::thread;

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum WakerState {
    Init = 0, // A temporary state, https://github.com/frostyplanet/crossfire-rs/issues/22
    Waiting = 1,
    //Copy = 2, // Omit due to skipping direct copy on async or with deadline
    Waked = 3,
    Done = 4,
    Closed = 5, // Channel closed, or timeout cancellation
}

#[derive(PartialEq, Debug)]
pub enum WakeResult {
    Sent,
    Next,
    PushBack,
    Waked,
}

/// Although removing direct copy feature of the payload pointer is not used,
/// leave it to unbuffer channel in the future
pub struct ChannelWaker<P>(Arc<WakerInner<P>>);

impl<P> fmt::Debug for ChannelWaker<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "waker({} state={})", self.get_seq(), self.get_state())
    }
}

impl<P> Deref for ChannelWaker<P> {
    type Target = WakerInner<P>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<P> ChannelWaker<P> {
    #[inline(always)]
    pub fn new_async(ctx: &Context, payload: P) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            state: AtomicU8::new(WakerState::Init as u8),
            waker: UnsafeCell::new(WakerType::Async(ctx.waker().clone())),
            payload: UnsafeCell::new(payload),
        }))
    }

    #[inline(always)]
    pub fn new_blocking(payload: P) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            state: AtomicU8::new(WakerState::Init as u8),
            waker: UnsafeCell::new(WakerType::Blocking(thread::current())),
            payload: UnsafeCell::new(payload),
        }))
    }
}

impl<P> ChannelWaker<P> {
    #[inline(always)]
    pub fn from_arc(inner: Arc<WakerInner<P>>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    pub fn to_arc(self) -> Arc<WakerInner<P>> {
        self.0
    }

    #[inline(always)]
    pub fn weak(&self) -> Weak<WakerInner<P>> {
        Arc::downgrade(&self.0)
    }
}

pub type RecvWaker = ChannelWaker<()>;

pub type SendWaker<T> = ChannelWaker<*mut T>;

enum WakerType {
    Async(Waker),
    Blocking(thread::Thread),
}

pub struct WakerInner<P> {
    state: AtomicU8,
    seq: AtomicUsize,
    waker: UnsafeCell<WakerType>,
    #[allow(dead_code)]
    payload: UnsafeCell<P>,
}

unsafe impl<P> Send for WakerInner<P> {}
unsafe impl<P> Sync for WakerInner<P> {}

impl<P> WakerInner<P> {
    #[inline(always)]
    fn get_waker(&self) -> &WakerType {
        unsafe { transmute(self.waker.get()) }
    }

    #[inline(always)]
    fn get_waker_mut(&self) -> &mut WakerType {
        unsafe { transmute(self.waker.get()) }
    }

    #[inline(always)]
    fn get_payload_mut(&self) -> &mut P {
        unsafe { transmute(self.payload.get()) }
    }

    #[inline(always)]
    pub fn reset(&self, payload: P) {
        self.state.store(WakerState::Init as u8, Ordering::Release);
        *self.get_payload_mut() = payload;
    }

    #[inline(always)]
    pub fn get_seq(&self) -> usize {
        self.seq.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_seq(&self, seq: usize) {
        self.seq.store(seq, Ordering::Relaxed);
    }

    #[inline(always)]
    fn update_thread_handle(&self) {
        let _waker = self.get_waker_mut();
        *_waker = WakerType::Blocking(thread::current());
    }

    #[inline(always)]
    pub fn commit_waiting(&self) -> u8 {
        if let Err(s) = self.try_change_state(WakerState::Init, WakerState::Waiting) {
            return s;
        } else {
            return WakerState::Waiting as u8;
        }
    }

    /// Return true if the canceled waker is not woken
    #[inline(always)]
    pub fn cancel(&self) -> bool {
        self.state.swap(WakerState::Waked as u8, Ordering::SeqCst) < WakerState::Waked as u8
    }

    #[inline(always)]
    pub fn try_change_state(&self, cur: WakerState, new_state: WakerState) -> Result<(), u8> {
        if let Err(s) = self.state.compare_exchange(
            cur as u8,
            new_state as u8,
            Ordering::SeqCst,
            Ordering::Acquire,
        ) {
            return Err(s);
        }
        return Ok(());
    }

    #[inline(always)]
    pub fn set_state(&self, state: WakerState) -> u8 {
        let _state = state as u8;
        #[cfg(test)]
        {
            if _state != WakerState::Closed as u8 {
                let __state = self.get_state();
                assert!(__state <= WakerState::Waked as u8, "unexpected state: {}", __state);
            }
        }
        self.state.store(_state, Ordering::Release);
        return _state;
    }

    /// Return current status,
    /// Closed: might be channel closed, or future successfully cancelled, the future should drop message; try to clear its waker.
    /// Done: the message actually sent, nothing to DO
    /// Waked: the future should drop message, and waked another counterpart.
    #[inline(always)]
    pub fn abandon(&self) -> u8 {
        // it will content with close() and on_recv()
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            match self.state.compare_exchange(
                WakerState::Waiting as u8,
                WakerState::Closed as u8,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return WakerState::Closed as u8,
                Err(s) => {
                    if s >= WakerState::Waked as u8 {
                        return s;
                    }
                    if s == WakerState::Init as u8 {
                        // Not expect to call abandon in Init state, just defensive against
                        // a dead loop
                        return s;
                    }
                } // If Copying, will wait until complete
            }
            backoff.snooze();
        }
    }

    #[inline(always)]
    pub fn close_wake(&self) {
        // should have lock because it will content with abandon()
        if self.change_state_smaller_eq(WakerState::Waiting, WakerState::Closed).is_ok() {
            self._wake_nolock();
        }
        return;
    }

    // Return Ok(pre_state), otherwise return Err(current_state)
    #[inline(always)]
    pub fn change_state_smaller_eq(
        &self, condition: WakerState, target: WakerState,
    ) -> Result<u8, u8> {
        debug_assert!((condition as u8) < (target as u8));
        // Save one load()
        let mut state = condition as u8;
        loop {
            match self.state.compare_exchange_weak(
                state,
                target as u8,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(state);
                }
                Err(s) => {
                    if s > condition as u8 {
                        return Err(s);
                    }
                    state = s;
                }
            }
        }
    }

    #[inline(always)]
    pub fn get_state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn get_state_strict(&self) -> u8 {
        self.state.load(Ordering::SeqCst)
    }

    /// Assume no lock
    #[inline(always)]
    pub fn wake(&self) -> WakeResult {
        match self.change_state_smaller_eq(WakerState::Waiting, WakerState::Waked) {
            Ok(state) => {
                self._wake_nolock();
                if state == WakerState::Waiting as u8 {
                    return WakeResult::Waked;
                } else {
                    return WakeResult::Next;
                }
            }
            Err(_) => return WakeResult::Next,
        }
    }

    pub fn will_wake(&self, ctx: &mut Context) -> bool {
        // ref: https://github.com/frostyplanet/crossfire-rs/issues/14
        // https://docs.rs/tokio/latest/tokio/runtime/index.html#:~:text=Normally%2C%20tasks%20are%20scheduled%20only,is%20called%20a%20spurious%20wakeup
        // There might be situation like spurious wakeup, poll() again under no waking up ever
        // happened, waker still exists in registry but cannot be used to wake the current future.
        let o_waker = self.get_waker();
        if let WakerType::Async(_waker) = o_waker {
            return _waker.will_wake(ctx.waker());
        } else {
            unreachable!();
        }
    }

    /// no lock version
    #[inline(always)]
    pub fn check_waker_nolock(&self, ctx: &mut Context) {
        let o_waker = self.get_waker_mut();
        if let WakerType::Async(_waker) = o_waker {
            if !_waker.will_wake(ctx.waker()) {
                *o_waker = WakerType::Async(ctx.waker().clone());
            }
        } else {
            unreachable!();
        }
    }

    // Assume have lock
    #[inline(always)]
    pub fn _wake_nolock(&self) {
        match self.get_waker() {
            WakerType::Async(w) => w.wake_by_ref(),
            WakerType::Blocking(th) => th.unpark(),
        }
    }
}

impl<T> WakerInner<*mut T> {
    #[inline(always)]
    fn get_payload(&self) -> *mut T {
        *self.get_payload_mut()
    }

    #[inline(always)]
    pub fn wake_or_copy(&self) -> Result<WakeResult, *mut T> {
        let p = self.get_payload();
        if p == std::ptr::null_mut() {
            return Ok(self.wake());
        }
        let mut state = self.state.load(Ordering::SeqCst);
        loop {
            if state >= WakerState::Waked as u8 {
                return Ok(WakeResult::Next);
            } else if state == WakerState::Waiting as u8 {
                return Err(p);
            } else {
                match self.state.compare_exchange_weak(
                    WakerState::Init as u8,
                    WakerState::Waked as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self._wake_nolock();
                        return Ok(WakeResult::Next);
                    }
                    Err(s) => {
                        state = s;
                    }
                }
            }
        }
    }
}

pub struct WakerCache<P: Copy>(ArcCell<WakerInner<P>>);

impl<P: Copy> WakerCache<P> {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self(ArcCell::new())
    }

    #[inline(always)]
    pub fn new_blocking(&self, payload: P) -> ChannelWaker<P> {
        if let Some(inner) = self.0.pop() {
            inner.update_thread_handle();
            inner.reset(payload);
            return ChannelWaker::<P>::from_arc(inner);
        }
        return ChannelWaker::new_blocking(payload);
    }

    #[inline(always)]
    pub(crate) fn push(&self, waker: ChannelWaker<P>) {
        debug_assert!(waker.get_state() >= WakerState::Waked as u8);
        let a = waker.to_arc();
        if Arc::weak_count(&a) == 0 && Arc::strong_count(&a) == 1 {
            self.0.try_put(a);
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        !self.0.exists()
    }
}
