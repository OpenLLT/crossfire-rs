use crate::collections::ArcCell;
use std::cell::UnsafeCell;
use std::mem::transmute;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
    Arc, Weak,
};
use std::task::*;
use std::thread;

#[repr(u8)]
pub enum WakerState {
    WAITING = 0,
    WAKED = 1,
    DONE = 2,
    CLOSED = 3, // Channel closed, or timeout cancellation
}

pub trait WakerTrait: Deref<Target = Self::Inner> {
    type Inner;

    type Payload;

    fn from_arc(inner: Arc<Self::Inner>) -> Self;

    fn to_arc(self) -> Arc<Self::Inner>;

    fn update_blocking_payload(inner: &Arc<Self::Inner>, payload: Self::Payload);

    fn new_async(ctx: &Context, payload: Self::Payload) -> Self;

    fn new_blocking(payload: Self::Payload) -> Self;

    fn get_seq(&self) -> usize;

    fn set_seq(&self, seq: usize);

    fn get_state(&self) -> u8;

    fn weak(&self) -> Weak<Self::Inner>;

    /// return true to stop; return false to continue the search.
    fn try_to_clear(weak: Weak<Self::Inner>, seq: usize) -> bool;
}

pub struct SendWaker<T>(Arc<WakerInner<AtomicPtr<T>>>);

impl<T> Deref for SendWaker<T> {
    type Target = WakerInner<AtomicPtr<T>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T> WakerTrait for SendWaker<T> {
    type Inner = WakerInner<AtomicPtr<T>>;

    type Payload = *mut T;

    #[inline(always)]
    fn from_arc(inner: Arc<Self::Inner>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    fn to_arc(self) -> Arc<Self::Inner> {
        self.0
    }

    #[inline(always)]
    fn new_async(ctx: &Context, payload: Self::Payload) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Async(ctx.waker().clone())),
            payload: AtomicPtr::new(payload),
        }))
    }

    #[inline(always)]
    fn new_blocking(payload: Self::Payload) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Blocking(thread::current())),
            payload: AtomicPtr::new(payload),
        }))
    }

    #[inline(always)]
    fn update_blocking_payload(inner: &Arc<Self::Inner>, payload: Self::Payload) {
        inner.state.store(WakerState::WAKED as u8, Ordering::Release);
        inner.payload.store(payload, Ordering::Release);
        inner.update_thread_handle();
    }

    #[inline(always)]
    fn get_seq(&self) -> usize {
        self.0.seq.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn set_seq(&self, seq: usize) {
        self.0.seq.store(seq, Ordering::Release);
    }

    #[inline(always)]
    fn get_state(&self) -> u8 {
        self.0.state.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn weak(&self) -> Weak<Self::Inner> {
        Arc::downgrade(&self.0)
    }

    /// return true to stop; return false to continue the search.
    #[inline(always)]
    fn try_to_clear(weak: Weak<Self::Inner>, seq: usize) -> bool {
        if let Some(inner) = weak.upgrade() {
            let _seq = inner.seq.load(Ordering::Acquire);
            if _seq == seq {
                // It's my waker, stopped
                return true;
            }
            inner.wake_simple();
            return _seq > seq;
        }
        return false;
    }
}

pub struct RecvWaker(Arc<WakerInner<()>>);

impl Deref for RecvWaker {
    type Target = WakerInner<()>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl WakerTrait for RecvWaker {
    type Inner = WakerInner<()>;

    type Payload = ();

    #[inline(always)]
    fn from_arc(inner: Arc<Self::Inner>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    fn to_arc(self) -> Arc<Self::Inner> {
        self.0
    }

    #[inline(always)]
    fn new_async(ctx: &Context, payload: Self::Payload) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Async(ctx.waker().clone())),
            payload,
        }))
    }

    #[inline(always)]
    fn new_blocking(payload: Self::Payload) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Blocking(thread::current())),
            payload,
        }))
    }

    #[inline(always)]
    fn update_blocking_payload(inner: &Arc<Self::Inner>, _payload: Self::Payload) {
        inner.state.store(WakerState::WAKED as u8, Ordering::Release);
        inner.update_thread_handle();
    }

    #[inline(always)]
    fn get_seq(&self) -> usize {
        self.0.seq.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn set_seq(&self, seq: usize) {
        self.0.seq.store(seq, Ordering::Release);
    }

    #[inline(always)]
    fn get_state(&self) -> u8 {
        self.0.state.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn weak(&self) -> Weak<Self::Inner> {
        Arc::downgrade(&self.0)
    }

    /// return true to stop; return false to continue the search.
    #[inline(always)]
    fn try_to_clear(weak: Weak<Self::Inner>, seq: usize) -> bool {
        if let Some(inner) = weak.upgrade() {
            let _seq = inner.seq.load(Ordering::Acquire);
            if _seq == seq {
                // It's my waker, stopped
                return true;
            }
            inner.wake_simple();
            return _seq > seq;
        }
        return false;
    }
}

enum WakerType {
    Async(Waker),
    Blocking(thread::Thread),
}

pub struct WakerInner<P> {
    state: AtomicU8,
    locked: AtomicBool,
    seq: AtomicUsize,
    waker: UnsafeCell<WakerType>,
    pub payload: P,
}

pub struct WakerInnerGuard<'a, P>(&'a WakerInner<P>);

impl<'a, P> Drop for WakerInnerGuard<'a, P> {
    fn drop(&mut self) {
        self.0.unlock();
    }
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
    fn update_thread_handle(&self) {
        let _waker = self.get_waker_mut();
        *_waker = WakerType::Blocking(thread::current());
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
    pub fn get_state_relaxed(&self) -> u8 {
        self.state.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_state(&self, state: WakerState) {
        #[cfg(test)]
        {
            let state = self.state.load(Ordering::Acquire);
            assert!(state <= WakerState::WAKED as u8, "unexpected state: {}", state);
        }
        self.state.store(state as u8, Ordering::Release);
    }

    /// Return current status,
    /// CLOSED: might be channel closed, or future successfully cancelled, the future should drop message; try to clear its waker.
    /// DONE: the message actually sent, nothing to DO
    /// WAKED: the future should drop message, and waked another counterpart.
    #[inline(always)]
    pub fn abandon(&self) -> u8 {
        match self.state.compare_exchange(
            WakerState::WAITING as u8,
            WakerState::CLOSED as u8,
            Ordering::SeqCst,
            Ordering::Acquire,
        ) {
            Ok(_) => return WakerState::CLOSED as u8,
            Err(s) => {
                return s;
            }
        }
    }

    #[inline(always)]
    pub fn is_waked(&self) -> bool {
        self.state.load(Ordering::Acquire) >= WakerState::WAKED as u8
    }

    #[inline(always)]
    pub fn close(&self) {
        loop {
            match self.try_change_state(WakerState::WAITING, WakerState::CLOSED) {
                Ok(_) => {
                    self._wake(false);
                    return;
                }
                Err(s) => {
                    if s >= WakerState::WAKED as u8 {
                        return;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub fn wake_simple(&self) -> bool {
        if let WakerType::Blocking(t) = self.get_waker() {
            if self.try_change_state(WakerState::WAITING, WakerState::WAKED).is_ok() {
                t.unpark();
                return true;
            } else {
                return false;
            }
        } else {
            // Async will try to update waker
            match self.try_change_state(WakerState::WAITING, WakerState::WAKED) {
                Ok(_) => {
                    self._wake(false);
                    return true;
                }
                Err(_) => {
                    return false;
                }
            }
        }
    }

    #[inline(always)]
    pub fn try_lock_weak<'a>(&'a self) -> Option<WakerInnerGuard<'a, P>> {
        if self
            .locked
            .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return Some(WakerInnerGuard(self));
        }
        None
    }

    #[inline(always)]
    pub fn try_lock<'a>(&'a self) -> Option<WakerInnerGuard<'a, P>> {
        if self.locked.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
            return Some(WakerInnerGuard(self));
        }
        None
    }

    #[inline(always)]
    fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }

    #[inline(always)]
    pub fn check_waker(&self, ctx: &mut Context, have_lock: bool) {
        // ref: https://github.com/frostyplanet/crossfire-rs/issues/14
        // https://docs.rs/tokio/latest/tokio/runtime/index.html#:~:text=Normally%2C%20tasks%20are%20scheduled%20only,is%20called%20a%20spurious%20wakeup
        // There might be situation like spurious wakeup, poll() again under no fire() ever
        // happened, waker still exists but cannot be used to wake the current future.
        // Since there's no lock inside fire(), to avoid race, can not update the content but to put a new one.
        if have_lock {
            let o_waker = self.get_waker_mut();
            if let WakerType::Async(_waker) = o_waker {
                if !_waker.will_wake(ctx.waker()) {
                    *o_waker = WakerType::Async(ctx.waker().clone());
                }
            } else {
                unreachable!();
            }
            return;
        }
        loop {
            if let Some(_guard) = self.try_lock_weak() {
                let o_waker = self.get_waker_mut();
                if let WakerType::Async(_waker) = o_waker {
                    if !_waker.will_wake(ctx.waker()) {
                        *o_waker = WakerType::Async(ctx.waker().clone());
                    }
                } else {
                    unreachable!();
                }
                return;
            } else {
                std::hint::spin_loop();
            }
        }
    }

    #[inline(always)]
    pub fn _wake(&self, have_lock: bool) {
        match self.get_waker() {
            WakerType::Async(w) => {
                if have_lock {
                    w.wake_by_ref();
                    return;
                } else {
                    loop {
                        if let Some(_guard) = self.try_lock_weak() {
                            if let WakerType::Async(waker) = self.get_waker() {
                                waker.wake_by_ref();
                            }
                            return;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
            WakerType::Blocking(th) => th.unpark(),
        }
    }
}

pub struct WakerCache<T: WakerTrait>(ArcCell<T::Inner>);

impl<T: WakerTrait> WakerCache<T> {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self(ArcCell::new())
    }

    #[inline(always)]
    pub(crate) fn new_blocking(&self, payload: T::Payload) -> T {
        if let Some(inner) = self.0.pop() {
            T::update_blocking_payload(&inner, payload);
            return T::from_arc(inner);
        }
        return T::new_blocking(payload);
    }

    #[inline(always)]
    pub(crate) fn push(&self, waker: T) {
        if waker.get_state() < WakerState::WAKED as u8 {
            return;
        }
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
