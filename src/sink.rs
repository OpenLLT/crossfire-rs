use crate::channel::*;
use crate::TrySendError;
use crate::{AsyncTx, MAsyncTx};
use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::task::*;

/// This is for you to write custom future with poll_send(ctx)
pub struct AsyncSink<T> {
    tx: AsyncTx<T>,
    waker: Option<SendWaker<T>>,
}

impl<T> fmt::Debug for AsyncSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncSink")
    }
}

impl<T> fmt::Display for AsyncSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncSink")
    }
}

impl<T> AsyncSink<T> {
    #[inline]
    pub fn new(tx: AsyncTx<T>) -> Self {
        Self { tx, waker: None }
    }
}

impl<T> Deref for AsyncSink<T> {
    type Target = AsyncTx<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<T: Unpin + Send + 'static> From<AsyncTx<T>> for AsyncSink<T> {
    #[inline]
    fn from(tx: AsyncTx<T>) -> Self {
        tx.into_sink()
    }
}

impl<T: Unpin + Send + 'static> From<MAsyncTx<T>> for AsyncSink<T> {
    #[inline]
    fn from(tx: MAsyncTx<T>) -> Self {
        tx.into_sink()
    }
}

impl<T: Send + Unpin + 'static> AsyncSink<T> {
    /// poll_send() will try to send message.
    /// On channel full, will register notification for the next poll.
    ///
    /// # Behavior
    ///
    /// The polling behavior is different from [SendFuture](crate::SendFuture).
    /// Because waker is not exposed to user, you cannot to delicate operation to
    /// the waker (compared to the `Drop` handler in `SendFuture`).
    /// To make sure no deadlock happen on cancellation,
    /// WakerState will be `INIT` after registered (and will not convert to `WAITING`).
    /// The receivers will wake up all `INIT` state wakers,
    /// until it find a normal pending sender in `WAITING` state.
    ///
    /// # Return value:
    ///
    /// Returns `Ok(())` on message sent.
    ///
    /// Returns Err([crate::TrySendError::Full]) for Poll::Pending case.
    /// The next time channel is not full, your future will be waked again,
    /// should continue calling poll_send() to send message.
    /// If you want to cancel, just don't call poll_send() again and there's no side-effect,
    /// others always have chances to send message.
    ///
    /// Returns Err([crate::TrySendError::Disconnected]) when all Rx dropped.
    #[inline]
    pub fn poll_send(&mut self, ctx: &mut Context, item: T) -> Result<(), TrySendError<T>> {
        let mut _item = MaybeUninit::new(item);
        let shared = &self.tx.shared;
        if shared.send(&_item) {
            shared.on_send();
            return Ok(());
        }
        match self.tx.poll_send(ctx, &mut _item, &mut self.waker, true) {
            Poll::Ready(Ok(())) => Ok(()),
            Poll::Ready(Err(())) => Err(TrySendError::Disconnected(unsafe { _item.assume_init() })),
            Poll::Pending => Err(TrySendError::Full(unsafe { _item.assume_init() })),
        }
    }
}

impl<T> Drop for AsyncSink<T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            self.tx.shared.abandon_send_waker(waker);
        }
    }
}
