pub use super::waker_registry::*;
use crate::crossbeam::array_queue::ArrayQueue;
pub use crate::crossbeam::err::*;
pub use crate::locked_waker::*;
use crossbeam_queue::SegQueue;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::time::Instant;

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
    tx_count: AtomicU64,
    rx_count: AtomicU64,
    inner: Channel<T>,
    pub(crate) senders: Registry,
    pub(crate) recvs: Registry,
    pub(crate) bound_size: Option<usize>,
}

impl<T: Send + 'static> ChannelShared<T> {
    pub fn try_send(&self, item: &mem::MaybeUninit<T>) -> Result<(), ()> {
        match &self.inner {
            Channel::List(inner) => {
                inner.push(unsafe { item.assume_init_read() });
                return Ok(());
            }
            Channel::Array(inner) => {
                if let Err(()) = unsafe { inner.push_uninit_ref(item) } {
                    return Err(());
                } else {
                    return Ok(());
                }
            }
        }
    }
}

impl<T> ChannelShared<T> {
    pub(crate) fn new(inner: Channel<T>, senders: Registry, recvs: Registry) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            tx_count: AtomicU64::new(1),
            rx_count: AtomicU64::new(1),
            senders,
            recvs,
            bound_size: inner.capacity(),
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
    pub(crate) fn add_tx(&self) {
        let _ = self.tx_count.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        let _ = self.rx_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Call when tx drop
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        if self.tx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self.recvs.close();
        }
    }

    /// Call when rx drop
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        if self.rx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self.senders.close();
        }
    }

    /// Register waker for current rx
    #[inline(always)]
    pub(crate) fn reg_recv_async(
        &self, ctx: &mut Context, o_waker: &mut Option<LockedWaker>,
    ) -> bool {
        self.recvs.reg_async(ctx, o_waker)
    }

    /// Register waker for current tx
    #[inline(always)]
    pub(crate) fn reg_send_async(
        &self, ctx: &mut Context, o_waker: &mut Option<LockedWaker>,
    ) -> bool {
        self.senders.reg_async(ctx, o_waker)
    }

    #[inline(always)]
    pub(crate) fn reg_send_blocking(&self, waker: &LockedWaker) {
        self.senders.reg_blocking(waker);
    }

    #[inline(always)]
    pub(crate) fn reg_recv_blocking(&self, waker: &LockedWaker) {
        self.recvs.reg_blocking(waker);
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        self.recvs.fire()
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        self.senders.fire()
    }

    #[inline(always)]
    pub(crate) fn cancel_recv_waker(&self, waker: LockedWaker) {
        self.recvs.cancel_waker(&waker);
    }

    #[inline(always)]
    pub(crate) fn cancel_send_waker(&self, waker: LockedWaker) {
        self.senders.cancel_waker(&waker);
    }

    /// On timeout, clear dead wakers on sender queue
    #[inline(always)]
    pub(crate) fn clear_send_wakers(&self, seq: u64) {
        self.senders.clear_wakers(seq);
    }

    /// On timeout, clear dead wakers on receiver queue
    #[inline(always)]
    pub(crate) fn clear_recv_wakers(&self, seq: u64) {
        self.recvs.clear_wakers(seq);
    }
}

/// If timed out, returns false
#[inline]
pub fn wait_timeout(deadline: Option<Instant>) -> bool {
    if let Some(end) = deadline {
        let now = Instant::now();
        if now < end {
            let dur = end - now;
            std::thread::park_timeout(dur);
            true
        } else {
            false
        }
    } else {
        std::thread::park();
        true
    }
}
