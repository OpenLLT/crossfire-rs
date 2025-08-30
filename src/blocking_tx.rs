use crate::backoff::*;
use crate::{channel::*, AsyncTx, MAsyncTx};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Single producer (sender) that works in blocking context.
///
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// **NOTE: Tx is not Clone, nor Sync.**
/// If you need concurrent access, use [MTx](crate::MTx) instead.
///
/// Tx has Send marker, can be moved to other thread.
/// The following code is OK:
///
/// ``` rust
/// use crossfire::*;
/// let (tx, rx) = spsc::bounded_blocking::<usize>(100);
/// std::thread::spawn(move || {
///     let _ = tx.send(1);
/// });
/// drop(rx);
/// ```
///
/// Because Tx does not have Sync marker, using `Arc<Tx>` will lose Send marker.
///
/// For your safety, the following code **should not compile**:
///
/// ``` compile_fail
/// use crossfire::*;
/// use std::sync::Arc;
/// let (tx, rx) = spsc::bounded_blocking::<usize>(100);
/// let tx = Arc::new(tx);
/// std::thread::spawn(move || {
///     let _ = tx.send(1);
/// });
/// drop(rx);
/// ```
pub struct Tx<T> {
    pub(crate) shared: Arc<ChannelShared<T>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    waker_cache: WakerCache<*mut T>,
}

unsafe impl<T: Send> Send for Tx<T> {}

impl<T> fmt::Debug for Tx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tx")
    }
}

impl<T> fmt::Display for Tx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tx")
    }
}

impl<T> Drop for Tx<T> {
    fn drop(&mut self) {
        self.shared.close_tx();
    }
}

impl<T> From<AsyncTx<T>> for Tx<T> {
    fn from(value: AsyncTx<T>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<T: Send + 'static> Tx<T> {
    #[inline(always)]
    pub(crate) fn _send_bounded(
        &self, mut item: MaybeUninit<T>, deadline: Option<Instant>,
    ) -> Result<(), SendTimeoutError<T>> {
        let shared = &self.shared;
        let large = shared.large;
        let backoff_cfg = BackoffConfig::default().spin(2).limit(7);
        let mut backoff = Backoff::new(backoff_cfg);
        let direct_copy = deadline.is_none() && shared.sender_direct_copy();
        if large {
            backoff.set_step(2);
        }
        loop {
            let r = if large { backoff.yield_now() } else { backoff.spin() };
            if direct_copy && large {
                match shared.try_send_oneshot(item.as_ptr()) {
                    Some(false) => break,
                    None => {
                        if r {
                            break;
                        }
                        continue;
                    }
                    _ => {
                        shared.on_send();
                        std::thread::yield_now();
                        return Ok(());
                    }
                }
            } else {
                if false == shared.send(&item) {
                    if r {
                        break;
                    }
                    continue;
                }
                shared.on_send();
                return Ok(());
            }
        }
        let direct_copy_ptr: *mut T =
            if direct_copy { item.as_mut_ptr() } else { std::ptr::null_mut() };

        let mut state: u8;
        let mut o_waker: Option<SendWaker<T>> = None;
        macro_rules! return_ok {
            () => {
                if let Some(waker) = o_waker.take() {
                    self.waker_cache.push(waker);
                }
                if shared.is_full() {
                    // It's for 8x1, 16x1.
                    std::thread::yield_now();
                }
                return Ok(())
            };
        }
        loop {
            let waker = if let Some(w) = o_waker.take() {
                w.set_state(WakerState::Init);
                w
            } else {
                self.waker_cache.new_blocking(direct_copy_ptr)
            };
            // For nx1 (more likely congest), need to reset backoff
            // to allow more yield to receivers.
            // For nxn (the backoff is already complete), wait a little bit.
            (state, o_waker) = shared.sender_reg_and_try(&mut item, waker, false);
            while state < WakerState::Waked as u8 {
                if direct_copy_ptr != std::ptr::null_mut() {
                    state = shared.sender_snooze(o_waker.as_ref().unwrap(), &mut backoff);
                }
                if state == WakerState::Waiting as u8 {
                    match check_timeout(deadline) {
                        Ok(None) => {
                            std::thread::park();
                        }
                        Ok(Some(dur)) => {
                            std::thread::park_timeout(dur);
                        }
                        Err(_) => {
                            if shared.abandon_send_waker(o_waker.take().unwrap()) {
                                return Err(SendTimeoutError::Timeout(unsafe {
                                    item.assume_init_read()
                                }));
                            } else {
                                // NOTE: Unlikely since we disable direct copy with deadline
                                // state is WakerState::Done
                                return Ok(());
                            }
                        }
                    }
                    state = o_waker.as_ref().unwrap().get_state();
                }
            }
            if state == WakerState::Done as u8 {
                return_ok!();
            } else if state == WakerState::Waked as u8 {
                backoff.reset();
                loop {
                    if shared.send(&item) {
                        shared.on_send();
                        return_ok!();
                    }
                    if backoff.is_completed() {
                        break;
                    }
                    backoff.snooze();
                }
            } else if state == WakerState::Closed as u8 {
                return Err(SendTimeoutError::Disconnected(unsafe { item.assume_init_read() }));
            }
        }
    }

    /// Send message. Will block when channel is full.
    ///
    /// Returns `Ok(())` on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    ///
    #[inline]
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let shared = &self.shared;
        if shared.is_disconnected() {
            return Err(SendError(item));
        }
        match &shared.inner {
            Channel::Array(inner) => {
                let mut _item = MaybeUninit::new(item);
                if unsafe { inner.push_with_ptr(_item.as_ptr()) } {
                    shared.on_send();
                    return Ok(());
                }
                match self._send_bounded(_item, None) {
                    Ok(_) => return Ok(()),
                    Err(SendTimeoutError::Disconnected(e)) => Err(SendError(e)),
                    Err(SendTimeoutError::Timeout(_)) => unreachable!(),
                }
            }
            Channel::List(inner) => {
                inner.push(item);
                shared.on_send();
                return Ok(());
            }
        }
    }

    /// Try to send message, non-blocking
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([TrySendError::Full]) on channel full for bounded channel.
    ///
    /// Returns Err([TrySendError::Disconnected]) when all Rx dropped.
    #[inline]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let shared = &self.shared;
        if shared.is_disconnected() {
            return Err(TrySendError::Disconnected(item));
        }
        let _item = MaybeUninit::new(item);
        if shared.send(&_item) {
            shared.on_send();
            return Ok(());
        } else {
            return Err(TrySendError::Full(unsafe { _item.assume_init_read() }));
        }
    }

    /// Waits for a message to be sent into the channel, but only for a limited time.
    /// Will block when channel is full.
    ///
    /// The behavior is atomic, either message sent successfully or returned on error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) when the the operation timed out.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) when all Rx dropped.
    #[inline]
    pub fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        let shared = &self.shared;
        if shared.is_disconnected() {
            return Err(SendTimeoutError::Disconnected(item));
        }
        match &shared.inner {
            Channel::Array(inner) => match Instant::now().checked_add(timeout) {
                None => self.try_send(item).map_err(|e| match e {
                    TrySendError::Disconnected(t) => SendTimeoutError::Disconnected(t),
                    TrySendError::Full(t) => SendTimeoutError::Timeout(t),
                }),
                Some(deadline) => {
                    let mut _item = MaybeUninit::new(item);
                    if unsafe { inner.push_with_ptr(_item.as_ptr()) } {
                        shared.on_send();
                        return Ok(());
                    }
                    match self._send_bounded(_item, Some(deadline)) {
                        Ok(_) => return Ok(()),
                        Err(e) => return Err(e),
                    }
                }
            },
            Channel::List(inner) => {
                inner.push(item);
                shared.on_send();
                return Ok(());
            }
        }
    }
}

impl<T> Tx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self { shared, waker_cache: WakerCache::new(), _phan: Default::default() }
    }
}

/// Multi-producer (sender) that works in blocking context.
///
/// Inherits [`Tx<T>`] and implements [Clone].
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// You can use `into()` to convert it to `Tx<T>`.
pub struct MTx<T>(pub(crate) Tx<T>);

impl<T> fmt::Debug for MTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MTx")
    }
}

impl<T> fmt::Display for MTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MTx")
    }
}

impl<T> From<MTx<T>> for Tx<T> {
    fn from(tx: MTx<T>) -> Self {
        tx.0
    }
}

impl<T> From<MAsyncTx<T>> for MTx<T> {
    fn from(value: MAsyncTx<T>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

unsafe impl<T: Send> Sync for MTx<T> {}

impl<T> MTx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self(Tx::new(shared))
    }
}

impl<T> Clone for MTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_tx();
        Self(Tx::new(inner.shared.clone()))
    }
}

impl<T> Deref for MTx<T> {
    type Target = Tx<T>;

    /// inherit all the functions of [Tx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// For writing generic code with MTx & Tx
pub trait BlockingTxTrait<T: Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>> + Sized
{
    /// Send message. Will block when channel is full.
    ///
    /// Returns `Ok(())` on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    fn send(&self, _item: T) -> Result<(), SendError<T>>;

    /// Try to send message, non-blocking
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([TrySendError::Full]) on channel full for bounded channel.
    ///
    /// Returns Err([TrySendError::Disconnected]) when all Rx dropped.
    fn try_send(&self, _item: T) -> Result<(), TrySendError<T>>;

    /// Waits for a message to be sent into the channel, but only for a limited time.
    /// Will block when channel is empty.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) when the message could not be sent because the channel is full and the operation timed out.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) when all Rx dropped.
    fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>>;

    /// The number of messages in the channel at the moment
    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// The capacity of the channel, return None for unbounded channel.
    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        self.as_ref().capacity()
    }

    /// Whether channel is empty at the moment
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    /// Whether the channel is full at the moment
    #[inline(always)]
    fn is_full(&self) -> bool {
        self.as_ref().is_full()
    }

    /// Return true if the other side has closed
    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        self.as_ref().is_disconnected()
    }

    fn clone_to_vec(self, count: usize) -> Vec<Self>;
}

impl<T: Send + 'static> BlockingTxTrait<T> for Tx<T> {
    #[inline(always)]
    fn clone_to_vec(self, _count: usize) -> Vec<Self> {
        assert_eq!(_count, 1);
        vec![self]
    }

    #[inline(always)]
    fn send(&self, item: T) -> Result<(), SendError<T>> {
        Tx::send(self, item)
    }

    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        Tx::try_send(self, item)
    }

    #[inline(always)]
    fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        Tx::send_timeout(&self, item, timeout)
    }
}

impl<T: Send + 'static> BlockingTxTrait<T> for MTx<T> {
    #[inline(always)]
    fn clone_to_vec(self, count: usize) -> Vec<Self> {
        let mut v = Vec::with_capacity(count);
        for _ in 0..count - 1 {
            v.push(self.clone());
        }
        v.push(self);
        v
    }

    #[inline(always)]
    fn send(&self, item: T) -> Result<(), SendError<T>> {
        self.0.send(item)
    }

    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(item)
    }

    #[inline(always)]
    fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        self.0.send_timeout(item, timeout)
    }
}

impl<T> Deref for Tx<T> {
    type Target = ChannelShared<T>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for Tx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for MTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
