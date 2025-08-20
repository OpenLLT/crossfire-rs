use crate::backoff::*;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Spinlock<T> {
    lock: AtomicBool,
    inner: UnsafeCell<T>,
}

unsafe impl<T> Send for Spinlock<T> {}
unsafe impl<T> Sync for Spinlock<T> {}

pub struct SpinlockGuard<'a, T> {
    inner: &'a Spinlock<T>,
    _phan: PhantomData<*mut ()>,
}

impl<'a, T> Deref for SpinlockGuard<'a, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { transmute(self.inner.inner.get()) }
    }
}

impl<'a, T> DerefMut for SpinlockGuard<'a, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { transmute(self.inner.inner.get()) }
    }
}

impl<'a, T> Drop for SpinlockGuard<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.inner.lock.store(false, Ordering::Release);
    }
}

macro_rules! try_lock {
    ($self: expr) => {
        $self.lock.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
    };
}

impl<T> Spinlock<T> {
    #[inline(always)]
    pub fn new(inner: T) -> Self {
        Self { lock: AtomicBool::new(false), inner: UnsafeCell::new(inner) }
    }

    #[inline(always)]
    fn guard<'a>(&'a self) -> SpinlockGuard<'a, T> {
        return SpinlockGuard { inner: self, _phan: Default::default() };
    }

    #[inline(always)]
    pub fn lock<'a>(&'a self) -> SpinlockGuard<'a, T> {
        if try_lock!(self).is_ok() {
            return self.guard();
        }
        let mut backoff = Backoff::new(BackoffConfig::default().limit(SPIN_LIMIT + 1));
        loop {
            backoff.snooze();
            if try_lock!(self).is_ok() {
                return self.guard();
            }
        }
    }

    #[inline(always)]
    pub fn lock_condition<'a>(&'a self, skip: &AtomicBool) -> Option<SpinlockGuard<'a, T>> {
        if try_lock!(self).is_ok() {
            return Some(self.guard());
        }
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            backoff.snooze();
            if try_lock!(self).is_ok() {
                return Some(self.guard());
            }
            if skip.load(Ordering::SeqCst) {
                return None;
            }
        }
    }
}
