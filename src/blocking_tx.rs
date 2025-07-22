use crate::{channel::*, AsyncTx, MAsyncTx};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::sync::{atomic::Ordering, Arc};
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
    waker_cache: WakerCache<SendWaker<T>>,
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
        &self, item: T, fastpath: bool, deadline: Option<Instant>,
    ) -> Result<(), SendTimeoutError<T>> {
        let shared = &self.shared;
        if shared.is_disconnected() {
            return Err(SendTimeoutError::Disconnected(item));
        }
        if let Some(bound_size) = shared.bound_size {
            if bound_size == 0 {
                todo!();
            } else {
                let mut _item = MaybeUninit::new(item);
                if fastpath {
                    if shared.try_send(&_item).is_ok() {
                        shared.on_send();
                        return Ok(());
                    }
                }
                let waker = self.waker_cache.new_blocking(_item.as_mut_ptr());

                macro_rules! process_state {
                    ($state: expr) => {
                        if $state == WakerState::DONE as u8 {
                            self.waker_cache.push(waker);
                            return Ok(());
                        } else if $state == WakerState::CLOSED as u8 {
                            return Err(SendTimeoutError::Disconnected(unsafe {
                                _item.assume_init_read()
                            }));
                        }
                    };
                }
                debug_assert!(waker.is_waked());
                let mut state;
                let backoff = shared.get_backoff_tx();
                loop {
                    match shared.reg_send_blocking(&waker) {
                        Ok(()) => {
                            state = shared.sender_try_again(&waker, None, fastpath, backoff);
                            if state == WakerState::WAITING as u8 {
                                if let Ok(time_left) = check_timeout(deadline) {
                                    if let Some(dur) = time_left {
                                        std::thread::park_timeout(dur);
                                    } else {
                                        std::thread::park();
                                    }
                                    state = waker.get_state();
                                } else {
                                    if shared.abandon_send_waker(waker) {
                                        return Err(SendTimeoutError::Timeout(unsafe {
                                            _item.assume_init_read()
                                        }));
                                    } else {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        Err(s) => {
                            state = s;
                        }
                    }
                    process_state!(state);
                    if state == WakerState::WAKED as u8 {
                        if shared.try_send(&_item).is_ok() {
                            shared.on_send();
                            self.waker_cache.push(waker);
                            return Ok(());
                        }
                    } else {
                        // Waited due to park_timeout or spurious waked
                        let state = shared.sender_try_again(&waker, None, fastpath, backoff);
                        process_state!(state);
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
        match self._send_blocking(item, true, None) {
            Ok(_) => return Ok(()),
            Err(SendTimeoutError::Disconnected(e)) => Err(SendError(e)),
            Err(SendTimeoutError::Timeout(_)) => unreachable!(),
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
        if shared.bound_size == Some(0) {
            todo!();
        }
        if let Err(t) = Self::_try_send(shared, item) {
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
            Some(deadline) => self._send_blocking(item, true, Some(deadline)),
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

impl<T: Send + 'static> MTx<T> {
    /// Send message. Will block when channel is full.
    ///
    /// Returns `Ok(())` on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    ///
    #[inline]
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let shared = &self.shared;
        let fastpath = !shared.tx_control.load(Ordering::Relaxed) || shared.senders.is_empty();
        match self._send_blocking(item, fastpath, None) {
            Ok(_) => return Ok(()),
            Err(SendTimeoutError::Disconnected(e)) => Err(SendError(e)),
            Err(SendTimeoutError::Timeout(_)) => unreachable!(),
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
                let shared = &self.shared;
                let fastpath =
                    !shared.tx_control.load(Ordering::Relaxed) || shared.senders.is_empty();
                self._send_blocking(item, fastpath, Some(deadline))
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
        let inner = &self.0;
        inner.shared.add_tx();
        Self(Tx::new(inner.shared.clone()))
    }
}

impl<T> Deref for MTx<T> {
    type Target = Tx<T>;

    /// inherit all the functions of [Tx]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for MTx<T> {
    /// inherit all the functions of [Tx]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// For writing generic code with MTx & Tx
pub trait BlockingTxTrait<T: Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>>
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
}

impl<T: Send + 'static> BlockingTxTrait<T> for MTx<T> {
    #[inline(always)]
    fn send(&self, item: T) -> Result<(), SendError<T>> {
        MTx::send(self, item)
    }

    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(item)
    }

    #[inline(always)]
    fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        MTx::send_timeout(self, item, timeout)
    }
}

impl<T> Deref for Tx<T> {
    type Target = ChannelShared<T>;
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for Tx<T> {
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for MTx<T> {
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
