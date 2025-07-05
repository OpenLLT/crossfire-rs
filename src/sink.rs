use crate::async_tx::AsyncTx;
use crate::locked_waker::LockedWaker;
use crate::TrySendError;
use std::fmt;
use std::mem::MaybeUninit;
use std::task::*;

/// This is for you to write custom future with poll_send(ctx)
pub struct AsyncSink<T> {
    tx: AsyncTx<T>,
    waker: Option<LockedWaker>,
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

impl<T: Send + Unpin + 'static> AsyncSink<T> {
    /// poll_send() will try to send message, if not successful, will register notification for
    /// the next poll.
    ///
    /// Returns `Ok(())` on message sent.
    ///
    /// Returns Err([TrySendError::Full]) for Poll::Pending case.
    ///
    /// Returns Err([TrySendError::Disconnected]) when all Rx dropped.
    #[inline]
    pub fn poll_send(&mut self, ctx: &mut Context, item: T) -> Result<(), TrySendError<T>> {
        let _item = MaybeUninit::new(item);
        match self.tx.poll_send(ctx, &_item, &mut self.waker) {
            Poll::Ready(Ok(())) => Ok(()),
            Poll::Ready(Err(())) => Err(TrySendError::Disconnected(unsafe { _item.assume_init() })),
            Poll::Pending => Err(TrySendError::Full(unsafe { _item.assume_init() })),
        }
    }
}

impl<T> Drop for AsyncSink<T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            self.tx.shared.clear_recv_wakers(waker.get_seq());
        }
    }
}
