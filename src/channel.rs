pub use super::waker_registry::*;
use crate::backoff::*;
use crate::crossbeam::array_queue::ArrayQueue;
pub use crate::crossbeam::err::*;
pub use crate::locked_waker::*;
use crossbeam_queue::SegQueue;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use std::mem;
use std::num::NonZeroUsize;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::thread;
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
        Self::Array(ArrayQueue::new(bound))
    }

    #[inline(always)]
    fn get_bound(&self) -> Option<usize> {
        match self {
            Self::List(_) => None,
            Self::Array(s) => Some(s.capacity()),
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::List(s) => s.len(),
            Self::Array(s) => s.len(),
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
    inner: Channel<T>,
    pub(crate) senders: RegistrySender<T>,
    pub(crate) recvs: RegistryRecv,
    pub(crate) bound_size: Option<usize>,
    backoff_tx: AtomicU32,
    backoff_rx: AtomicU32,
    pub(crate) tx_control: AtomicBool,
    lock: Mutex<()>,
}

impl<T: Send + 'static> ChannelShared<T> {
    pub fn try_send(&self, item: &mem::MaybeUninit<T>) -> Result<(), ()> {
        match &self.inner {
            Channel::List(inner) => {
                inner.push(unsafe { item.assume_init_read() });
                return Ok(());
            }
            Channel::Array(inner) => {
                if let Err(()) = unsafe { inner.push_with_ptr(item.as_ptr()) } {
                    return Err(());
                } else {
                    return Ok(());
                }
            }
        }
    }
}

impl<T> ChannelShared<T> {
    pub(crate) fn new(
        inner: Channel<T>, senders: RegistrySender<T>, recvs: RegistryRecv,
    ) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            senders,
            recvs,
            bound_size: inner.get_bound(),
            inner,
            backoff_tx: AtomicU32::new(BackoffConfig::default().to_u32()),
            backoff_rx: AtomicU32::new(BackoffConfig::default().to_u32()),
            tx_control: AtomicBool::new(false),
            lock: Mutex::new(()),
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

    /// The number of messages in the channel at the moment.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
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
        self.closed.load(Ordering::Acquire)
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

    /// Just for debugging purpose, to monitor queue size
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    fn auto_config(&self) {
        let _guard = self.lock.lock();
        let congest = if self.bound_size.is_none() {
            false
        } else {
            self.tx_count.load(Ordering::Acquire) > self.rx_count.load(Ordering::Acquire)
        };
        self.tx_control.store(congest, Ordering::Release);
        let _config = determine_backoff(&self.backoff_tx, &self.tx_count, &self.rx_count);
        let _config = determine_backoff(&self.backoff_rx, &self.rx_count, &self.tx_count);
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        let _ = self.tx_count.fetch_add(1, Ordering::SeqCst);
        self.auto_config();
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        let _ = self.rx_count.fetch_add(1, Ordering::SeqCst);
        self.auto_config();
    }

    #[inline(always)]
    pub(crate) fn get_backoff_tx(&self) -> BackoffConfig {
        load_backoff(&self.backoff_tx)
    }

    #[inline(always)]
    pub(crate) fn get_backoff_rx(&self) -> BackoffConfig {
        load_backoff(&self.backoff_rx)
    }

    /// Call when tx drop
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        if self.tx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self._close_all();
        }
    }

    /// Call when rx drop
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        if self.rx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self._close_all();
        }
    }

    #[inline(always)]
    fn _close_all(&self) {
        while let Some(waker) = self.recvs.pop() {
            waker.close();
        }
        while let Some(waker) = self.senders.pop() {
            waker.close();
        }
    }

    /// Register waker for current tx
    #[inline(always)]
    pub(crate) fn reg_send_async(&self, waker: &SendWaker<T>) -> Result<(), u8> {
        self.senders.reg_async(waker)
    }

    /// Register waker for current rx
    #[inline(always)]
    pub(crate) fn reg_recv_async(&self, o_waker: &RecvWaker) -> Result<(), u8> {
        self.recvs.reg_async(o_waker)
    }

    #[inline(always)]
    pub(crate) fn reg_send_blocking(&self, waker: &SendWaker<T>) -> Result<(), u8> {
        self.senders.reg_blocking(waker)
    }

    #[inline(always)]
    pub(crate) fn reg_recv_blocking(&self, waker: &RecvWaker) -> Result<(), u8> {
        self.recvs.reg_blocking(waker)
    }

    /// Return is_waked
    #[inline]
    pub(crate) fn on_recv_try_send(&self, waker: &SendWaker<T>) -> bool {
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            if let Some(_guard) = waker.try_lock() {
                let state = waker.get_state();
                if state >= WakerState::WAKED as u8 {
                    return false;
                }
                // the receiver no need to check disconnect,
                // its impossible if there's live waker
                // Check the state again, during locked, no one allowed to change the status
                let p = waker.payload.load(Ordering::Acquire);
                debug_assert!(p != ptr::null_mut());
                if let Channel::Array(inner) = &self.inner {
                    if unsafe { inner.push_with_ptr(p) }.is_ok() {
                        waker.set_state(WakerState::DONE);
                        waker._wake_nolock();
                        drop(_guard);
                        self.on_send();
                        return true;
                    } else {
                        // still full
                        // Let the sender to re-register
                        waker.set_state(WakerState::WAKED);
                        waker._wake_nolock();
                        // TODO optimise
                        return true; // Do not try another
                    }
                } else {
                    unreachable!();
                }
            } else {
                let state = waker.get_state();
                if state >= WakerState::WAKED as u8 {
                    return false;
                }
                backoff.snooze();
                // The sender is checking itself, we cannot wakeup up otherwise state is wrong
            }
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    #[inline]
    pub(crate) fn sender_try_again(
        &self, waker: &SendWaker<T>, ctx: Option<&mut Context>, fastpath: bool,
        backoff_conf: BackoffConfig,
    ) -> u8 {
        if self.is_disconnected() {
            // check disconnect in case dead lock on rx drop.
            if let Err(s) = waker.try_change_state(WakerState::WAITING, WakerState::CLOSED) {
                return s;
            } else {
                return WakerState::CLOSED as u8;
            }
        }
        macro_rules! try_send {
            ($guard: expr, $state: expr) => {{
                if $state >= WakerState::DONE as u8 {
                    return $state;
                }
                // Check the state again, during locked, no one allowed to change the status
                let p = waker.payload.load(Ordering::Acquire);
                debug_assert!(p != ptr::null_mut());
                if let Channel::Array(inner) = &self.inner {
                    if unsafe { inner.push_with_ptr(p) }.is_ok() {
                        waker.set_state(WakerState::DONE);
                        drop($guard);
                        if $state == WakerState::WAITING as u8 {
                            self.senders.cancel_waker();
                        }
                        self.on_send();
                        return WakerState::DONE as u8;
                    }
                    // still full
                } else {
                    unreachable!();
                }
            }};
        }
        let mut backoff = Backoff::new(backoff_conf);
        if let Some(_ctx) = ctx {
            // Assume WAITING,  must check_waker
            loop {
                if let Some(guard) = waker.try_lock_weak() {
                    let state = waker.get_state();
                    try_send!(guard, state);
                    waker._check_waker_nolock(_ctx);
                    return state; // might be WAITING or WAKED
                }
                backoff.snooze();
            }
        } else {
            if !fastpath {
                backoff.snooze();
                let state = waker.get_state();
                if state >= WakerState::WAKED as u8 {
                    return state;
                }
            }
            if !self.is_full() {
                {
                    if let Some(_guard) = waker.try_lock() {
                        let state = waker.get_state();
                        try_send!(_guard, state);
                        // still full
                    }
                }
            }
            // As sender, we do not contend the lock with on_recv, backoff and peak the state
            let mut state = waker.get_state_relaxed();
            while state < WakerState::WAKED as u8 {
                // NOTE: Normally async does not snooze
                if backoff.is_completed() && waker.is_locked() == false {
                    // Check lock state, if there's no receiver, should not spin forever.
                    // If already see by receiver, but we should ensure the waker is ok.
                    // When overloaded, we'd better park.
                    //
                    return state;
                }
                backoff.snooze();
                state = waker.get_state();
            }
            return state;
        }
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        while let Some(waker) = self.recvs.pop() {
            if waker.wake_simple() {
                return;
            }
        }
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        while let Some(waker) = self.senders.pop() {
            if self.on_recv_try_send(&waker) {
                return;
            }
        }
    }

    #[inline(always)]
    pub(crate) fn recv_waker_done(&self, waker: &RecvWaker) {
        if waker.get_state() == WakerState::WAITING as u8 {
            waker.set_state(WakerState::DONE);
            self.recvs.cancel_waker();
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already DONE.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, waker: SendWaker<T>) -> bool {
        let state = waker.abandon();
        if state == WakerState::CLOSED as u8 {
            self.senders.clear_wakers(waker.get_seq());
            return true;
        } else if state == WakerState::DONE as u8 {
            return false;
        } else {
            debug_assert_eq!(state, WakerState::WAKED as u8);
            // We are waked, but give up sending, should notify another sender for safety
            self.on_recv();
            return true;
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, waker: RecvWaker) -> bool {
        let state = waker.abandon();
        if state == WakerState::CLOSED as u8 {
            self.recvs.clear_wakers(waker.get_seq());
            return true;
        } else if state == WakerState::DONE as u8 {
            return false;
        } else {
            debug_assert_eq!(state, WakerState::WAKED as u8);
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
    pub fn detect_async_backoff_tx(&self) -> i8 {
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
    pub fn detect_async_backoff_rx(&self) -> i8 {
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
            return 5;
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

#[inline(always)]
pub(crate) fn determine_backoff(
    global: &AtomicU32, my_count: &AtomicUsize, other_count: &AtomicUsize,
) -> BackoffConfig {
    let avail =
        usize::from(thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap()));
    loop {
        let cur = global.load(Ordering::Acquire);
        let cur_count = my_count.load(Ordering::Acquire);
        let other = other_count.load(Ordering::Acquire);
        let mut limit = 6;
        let mut spin_limit = 6;
        let total = cur_count + other;
        if total > avail + 1 {
            limit = 4;
            spin_limit = 2;
        } else if total >= avail {
            limit = 5;
            spin_limit = 4;
        } else if cur_count == other {
            // 1x1 2x2
            spin_limit = 7;
            limit = 7;
        }
        if cur_count > (other << 2) {
            // 8x1
            // They are out numbered, yield more cpu resource to them.
            spin_limit = 0;
        } else if cur_count << 2 < other && (cur_count << 1) < avail {
            // 1x4, 1x8
            // We are out numbered, always spinning
            spin_limit = limit;
        }
        if avail <= 1 {
            spin_limit = 0;
        }
        let config = BackoffConfig { spin_limit, limit };
        let c = config.to_u32();
        if global.compare_exchange(cur, c, Ordering::SeqCst, Ordering::Acquire).is_ok() {
            return config;
        }
    }
}

#[inline(always)]
fn load_backoff(global: &AtomicU32) -> BackoffConfig {
    BackoffConfig::from_u32(global.load(Ordering::Relaxed))
}

#[allow(dead_code)]
#[derive(Default)]
pub struct ChannelStats {
    tx_try: AtomicUsize,
    tx_poll: AtomicUsize,
    tx_done: AtomicUsize,
    rx_try: AtomicUsize,
    rx_poll: AtomicUsize,
    rx_done: AtomicUsize,
}

lazy_static! {
    static ref STATS: ChannelStats = Default::default();
}

#[cfg(feature = "profile")]
impl ChannelStats {
    pub fn to_string() -> String {
        let mut tx_try = STATS.tx_try.load(Ordering::Acquire) as f64;
        let mut tx_poll = STATS.tx_poll.load(Ordering::Acquire) as f64;
        let tx_done = STATS.tx_done.load(Ordering::Acquire);
        let mut rx_try = STATS.rx_try.load(Ordering::Acquire) as f64;
        let mut rx_poll = STATS.rx_poll.load(Ordering::Acquire) as f64;
        let rx_done = STATS.rx_done.load(Ordering::Acquire);
        if tx_done > 0 {
            tx_try /= tx_poll;
            tx_poll /= tx_done as f64;
        }
        if rx_done > 0 {
            rx_try /= rx_poll;
            rx_poll /= rx_done as f64;
        }
        format!(
            "tx:[avg(try={}, poll={}) op={}], rx[avg(try={}, poll={}), op={}]",
            tx_try, tx_poll, tx_done, rx_try, rx_poll, rx_done,
        )
        .to_string()
    }

    pub fn clear() {
        STATS.tx_try.store(0, Ordering::Release);
        STATS.rx_try.store(0, Ordering::Release);
        STATS.tx_poll.store(0, Ordering::Release);
        STATS.rx_poll.store(0, Ordering::Release);
        STATS.tx_done.store(0, Ordering::Release);
        STATS.rx_done.store(0, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn tx_poll(retry: usize) {
        STATS.tx_try.fetch_add(retry, Ordering::SeqCst);
        STATS.tx_poll.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn rx_poll(retry: usize) {
        STATS.rx_try.fetch_add(retry, Ordering::SeqCst);
        STATS.rx_poll.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn tx_done() {
        STATS.tx_done.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn rx_done() {
        STATS.rx_done.fetch_add(1, Ordering::SeqCst);
    }
}

#[macro_export(local_inner_macros)]
macro_rules! rx_stats {
    ($try: expr, $done: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::rx_poll($try);
            if $done {
                ChannelStats::rx_done();
            }
        }
    };
    ($try: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::rx_poll($try);
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! tx_stats {
    ($try: expr, $done: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::tx_poll($try);
            if $done {
                ChannelStats::tx_done();
            }
        }
    };
    ($try: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::tx_poll($try);
        }
    };
}
