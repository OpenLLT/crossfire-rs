use crate::channel::*;
use crate::{AsyncRx, MAsyncRx};
use futures::stream;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::task::*;

/// Constructed by [AsyncRx::into_stream()](crate::AsyncRx::into_stream())
///
/// Implemented futures::stream::Stream;
pub struct AsyncStream<T> {
    rx: AsyncRx<T>,
    waker: Option<RecvWaker>,
    ended: bool,
}

impl<T> fmt::Debug for AsyncStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncStream")
    }
}

impl<T> fmt::Display for AsyncStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncStream")
    }
}

impl<T> AsyncStream<T>
where
    T: Unpin + Send + 'static,
{
    #[inline(always)]
    pub fn new(rx: AsyncRx<T>) -> Self {
        Self { rx, waker: None, ended: false }
    }

    /// poll_item() will try to receive message.
    /// On channel empty, will register notification for the next poll.
    ///
    /// # Behavior
    ///
    /// The polling behavior is different from [ReceiveFuture](crate::ReceiveFuture).
    /// Because waker is not exposed to user, you cannot to delicate operation to
    /// the waker (compared to the `Drop` handler in `ReceiveFuture`).
    /// To make sure no deadlock happen on cancellation,
    /// WakerState will be `INIT` after registered (and will not convert to `WAITING`).
    /// The senders will wake up all `INIT` state wakers,
    /// until it find a normal pending receiver in `WAITING` state.
    ///
    /// # Return Value:
    ///
    /// Returns `Ok(T)` on successful.
    ///
    /// Return Err([crate::TryRecvError::Empty]) for Poll::Pending case.
    /// The next time channel is not empty, your future will be waked again,
    /// should continue calling poll_item() to receive message.
    /// If you want to cancel, just don't call poll_item(), others always have chances
    /// to receive messages.
    ///
    /// Return Err([crate::TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    #[inline]
    pub fn poll_item(&mut self, ctx: &mut Context) -> Poll<Option<T>> {
        match self.rx.poll_item(ctx, &mut self.waker, true) {
            Ok(item) => Poll::Ready(Some(item)),
            Err(e) => {
                if e.is_empty() {
                    return Poll::Pending;
                }
                self.ended = true;
                return Poll::Ready(None);
            }
        }
    }
}

impl<T> Deref for AsyncStream<T> {
    type Target = AsyncRx<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl<T> stream::Stream for AsyncStream<T>
where
    T: Unpin + Send + 'static,
{
    type Item = T;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut _self = self.get_mut();
        if _self.ended {
            return Poll::Ready(None);
        }
        match _self.rx.poll_item(ctx, &mut _self.waker, false) {
            Ok(item) => Poll::Ready(Some(item)),
            Err(e) => {
                if e.is_empty() {
                    return Poll::Pending;
                }
                _self.ended = true;
                return Poll::Ready(None);
            }
        }
    }
}

impl<T> stream::FusedStream for AsyncStream<T>
where
    T: Unpin + Send + 'static,
{
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<T> Drop for AsyncStream<T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            self.rx.shared.clear_recv_wakers(waker.get_seq());
        }
    }
}

impl<T: Unpin + Send + 'static> From<AsyncRx<T>> for AsyncStream<T> {
    #[inline]
    fn from(rx: AsyncRx<T>) -> Self {
        rx.into_stream()
    }
}

impl<T: Unpin + Send + 'static> From<MAsyncRx<T>> for AsyncStream<T> {
    #[inline]
    fn from(rx: MAsyncRx<T>) -> Self {
        rx.into_stream()
    }
}
