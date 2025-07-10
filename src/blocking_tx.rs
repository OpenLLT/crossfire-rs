use crate::backoff::Backoff;
use crate::{channel::*, tx_stats};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Single producer (sender) that works in blocking context.
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
    waker_cache: WakerCache,
}

unsafe impl<T: Send> Send for Tx<T> {}

impl<T> fmt::Debug for Tx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tx")
    }
}

impl<T> Drop for Tx<T> {
    fn drop(&mut self) {
        self.shared.close_tx();
    }
}

impl<T: Send + 'static> Tx<T> {
    #[inline(always)]
    fn _try_send(shared: &ChannelShared<T>, item: T) -> Result<(), T> {
        let _item = MaybeUninit::new(item);
        match shared.try_send(&_item) {
            Err(()) => {
                return Err(unsafe { _item.assume_init_read() });
            }
            Ok(_) => {
                shared.on_send();
                return Ok(());
            }
        }
    }

    #[inline(always)]
    pub(crate) fn _send_blocking(
        shared: &ChannelShared<T>, item: T, deadline: Option<Instant>,
        waker_cache: Option<&WakerCache>,
    ) -> Result<(), SendTimeoutError<T>> {
        if shared.get_rx_count() == 0 {
            return Err(SendTimeoutError::Disconnected(item));
        }
        if let Some(bound_size) = shared.bound_size {
            if bound_size == 0 {
                todo!();
            } else {
                let _item = MaybeUninit::new(item);
                if shared.try_send(&_item).is_ok() {
                    shared.on_send();
                    tx_stats!(1, true);
                    return Ok(());
                }
                let waker = WakerCache::new_blocking(waker_cache);
                debug_assert!(waker.is_waked());
                let mut backoff = Backoff::new(6);
                backoff.snooze();
                loop {
                    loop {
                        if shared.try_send(&_item).is_ok() {
                            shared.on_send();
                            WakerCache::push(waker_cache, waker);
                            tx_stats!(_i, true);
                            return Ok(());
                        }
                        if backoff.is_completed() {
                            break;
                        }
                        backoff.snooze();
                    }
                    shared.reg_send_blocking(&waker);
                    if shared.get_rx_count() == 0 {
                        waker.cancel();
                        return Err(SendTimeoutError::Disconnected(unsafe {
                            _item.assume_init_read()
                        }));
                    }
                    if !shared.is_full() {
                        continue;
                    }
                    tx_stats!(backoff.step());
                    backoff.reset();
                    if !wait_timeout(deadline) {
                        if waker.abandon() {
                            // We are waked, but give up sending, should notify another sender for safety
                            shared.on_recv();
                        } else {
                            shared.clear_send_wakers(waker.get_seq());
                        }
                        return Err(SendTimeoutError::Timeout(unsafe { _item.assume_init_read() }));
                    }
                }
            }
        } else {
            // unbounded
            match Self::_try_send(shared, item) {
                Ok(_) => return Ok(()),
                Err(_) => unreachable!(),
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
        Self::_send_blocking(&self.shared, item, None, Some(&self.waker_cache)).map_err(|err| {
            match err {
                SendTimeoutError::Disconnected(msg) => SendError(msg),
                SendTimeoutError::Timeout(_) => unreachable!(),
            }
        })
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
        if self.shared.bound_size == Some(0) {
            todo!();
        }
        if let Err(t) = Self::_try_send(&self.shared, item) {
            if self.shared.get_rx_count() == 0 {
                return Err(TrySendError::Disconnected(t));
            }
            return Err(TrySendError::Full(t));
        } else {
            Ok(())
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
        match Instant::now().checked_add(timeout) {
            Some(deadline) => {
                Self::_send_blocking(&self.shared, item, Some(deadline), Some(&self.waker_cache))
            }
            None => self.try_send(item).map_err(|e| match e {
                TrySendError::Disconnected(t) => SendTimeoutError::Disconnected(t),
                TrySendError::Full(t) => SendTimeoutError::Timeout(t),
            }),
        }
    }
}

impl<T> Tx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self { shared, waker_cache: WakerCache::new(), _phan: Default::default() }
    }

    /// Probe possible messages in the channel (not accurate)
    #[inline]
    pub fn len(&self) -> usize {
        self.shared.len()
    }

    /// Whether there's message in the channel (not accurate)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }
}

/// Multi-producer (sender) that works in blocking context.
///
/// Inherits [`Tx<T>`] and implements [Clone].
///
/// You can use `into()` to convert it to `Tx<T>`.
pub struct MTx<T> {
    pub(crate) inner: Tx<T>,
    waker_cache: WakerCache,
}

unsafe impl<T: Send> Sync for MTx<T> {}

impl<T> MTx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self { inner: Tx::new(shared), waker_cache: WakerCache::new() }
    }
}

impl<T: Send + 'static> MTx<T> {
    #[inline(always)]
    pub(crate) fn _send_blocking(
        &self, item: T, deadline: Option<Instant>, waker_cache: Option<&WakerCache>,
    ) -> Result<(), SendTimeoutError<T>> {
        let shared = &self.shared;
        if shared.get_rx_count() == 0 {
            return Err(SendTimeoutError::Disconnected(item));
        }
        if let Some(bound_size) = shared.bound_size {
            if bound_size == 0 {
                todo!();
            } else {
                let _item = MaybeUninit::new(item);
                if shared.get_tx_control_seq() == 0 {
                    if shared.try_send(&_item).is_ok() {
                        shared.on_send();
                        tx_stats!(1, true);
                        return Ok(());
                    }
                }
                let waker = WakerCache::new_blocking(waker_cache);
                debug_assert!(waker.is_waked());
                let mut backoff = Backoff::new(6);
                let mut control_seq = shared.reg_send_blocking(&waker);
                let seq = waker.get_seq();
                if control_seq == seq {
                    backoff.snooze()
                } else {
                    let mut dis = seq.wrapping_sub(control_seq);
                    if dis > 5 {
                        dis = 5;
                    }
                    for _ in 0..1 << dis {
                        std::hint::spin_loop();
                    }
                }
                let mut init = true;
                loop {
                    loop {
                        if init && control_seq.wrapping_add(5) < seq {
                            if shared.is_full() {
                                break;
                            }
                        }
                        if shared.try_send(&_item).is_ok() {
                            shared.on_send();
                            WakerCache::push(waker_cache, waker);
                            tx_stats!(backoff.step(), true);
                            return Ok(());
                        }
                        if backoff.is_completed() {
                            break;
                        }
                        backoff.snooze();
                    }
                    if init {
                        init = false;
                        backoff.set_limit(2);
                    } else {
                        shared.reg_send_blocking(&waker);
                        if shared.get_rx_count() == 0 {
                            waker.cancel();
                            return Err(SendTimeoutError::Disconnected(unsafe {
                                _item.assume_init_read()
                            }));
                        }
                        if !shared.is_full() {
                            continue;
                        }
                    }
                    backoff.reset();
                    tx_stats!(backoff.step());
                    if !wait_timeout(deadline) {
                        if waker.abandon() {
                            // We are waked, but give up sending, should notify another sender for safety
                            shared.on_recv();
                        } else {
                            shared.clear_send_wakers(waker.get_seq());
                        }
                        return Err(SendTimeoutError::Timeout(unsafe { _item.assume_init_read() }));
                    }
                    control_seq = shared.get_tx_control_seq();
                }
            }
        } else {
            // unbounded
            match Tx::_try_send(shared, item) {
                Ok(_) => return Ok(()),
                Err(_) => unreachable!(),
            }
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
        match Instant::now().checked_add(timeout) {
            Some(deadline) => {
                Self::_send_blocking(&self, item, Some(deadline), Some(&self.waker_cache))
            }
            None => self.try_send(item).map_err(|e| match e {
                TrySendError::Disconnected(t) => SendTimeoutError::Disconnected(t),
                TrySendError::Full(t) => SendTimeoutError::Timeout(t),
            }),
        }
    }
}

impl<T: Unpin> Clone for MTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.inner;
        inner.shared.add_tx();
        Self::new(inner.shared.clone())
    }
}

impl<T> Deref for MTx<T> {
    type Target = Tx<T>;

    /// inherit all the functions of [Tx]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for MTx<T> {
    /// inherit all the functions of [Tx]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// For writing generic code with MTx & Tx
pub trait BlockingTxTrait<T: Send + 'static>: Send + 'static {
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

    /// Probe possible messages in the channel (not accurate)
    fn len(&self) -> usize;

    /// Whether there's message in the channel (not accurate)
    fn is_empty(&self) -> bool;
}

impl<T: Send + 'static> BlockingTxTrait<T> for Tx<T> {
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

    #[inline(always)]
    fn len(&self) -> usize {
        Tx::len(self)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        Tx::is_empty(self)
    }
}

impl<T: Send + 'static> BlockingTxTrait<T> for MTx<T> {
    #[inline(always)]
    fn send(&self, item: T) -> Result<(), SendError<T>> {
        MTx::_send_blocking(&self, item, None, Some(&self.waker_cache)).map_err(|err| match err {
            SendTimeoutError::Disconnected(msg) => SendError(msg),
            SendTimeoutError::Timeout(_) => unreachable!(),
        })
    }

    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(item)
    }

    #[inline(always)]
    fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        MTx::send_timeout(self, item, timeout)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
