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

impl<T> Spinlock<T> {
    #[inline(always)]
    pub fn new(inner: T) -> Self {
        Self { lock: AtomicBool::new(false), inner: UnsafeCell::new(inner) }
    }

    #[inline]
    #[cold]
    fn lock_slow(&self) {
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            backoff.snooze();
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    #[inline(always)]
    pub fn lock<'a>(&'a self) -> SpinlockGuard<'a, T> {
        if self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return SpinlockGuard { inner: self, _phan: Default::default() };
        }
        self.lock_slow();
        SpinlockGuard { inner: self, _phan: Default::default() }
    }
}
