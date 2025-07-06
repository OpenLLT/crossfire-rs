use parking_lot::Mutex;
use std::collections::VecDeque;
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, Ordering},
    Arc, Weak,
};

pub struct ArcCell<T> {
    ptr: AtomicPtr<T>,
}

unsafe impl<T> Send for ArcCell<T> {}
unsafe impl<T> Sync for ArcCell<T> {}

impl<T> ArcCell<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Self { ptr: AtomicPtr::new(ptr::null_mut()) }
    }

    #[inline(always)]
    pub fn exists(&self) -> bool {
        self.ptr.load(Ordering::Acquire) != ptr::null_mut()
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<Arc<T>> {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            return Some(unsafe { Arc::from_raw(ptr) });
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn clear(&self) {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            // Convert into Weak and drop
            let _ = unsafe { Arc::from_raw(ptr) };
        }
    }

    #[inline(always)]
    pub fn try_put(&self, item: Arc<T>) {
        let item_ptr = Arc::into_raw(item) as *mut T;
        match self.ptr.compare_exchange(
            ptr::null_mut(),
            item_ptr,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {}
            Err(_) => {
                let _ = unsafe { Arc::from_raw(item_ptr) };
            }
        }
    }
}

pub struct WeakCell<T> {
    ptr: AtomicPtr<T>,
}

unsafe impl<T> Send for WeakCell<T> {}
unsafe impl<T> Sync for WeakCell<T> {}

impl<T> WeakCell<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Self { ptr: AtomicPtr::new(ptr::null_mut()) }
    }

    #[inline(always)]
    pub fn exists(&self) -> bool {
        self.ptr.load(Ordering::Acquire) != ptr::null_mut()
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<Arc<T>> {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            return unsafe { Weak::from_raw(ptr) }.upgrade();
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            // Convert into Weak and drop
            let _ = unsafe { Weak::from_raw(ptr) };
        }
    }

    #[inline(always)]
    pub fn put(&self, item: Weak<T>) {
        let old_ptr = self.ptr.swap(item.into_raw() as *mut T, Ordering::SeqCst);
        if old_ptr != ptr::null_mut() {
            let _ = unsafe { Weak::from_raw(old_ptr) };
        }
    }
}

pub struct LockedQueue<T> {
    empty: AtomicBool,
    queue: Mutex<VecDeque<T>>,
}

impl<T> LockedQueue<T> {
    #[inline]
    pub fn new(cap: usize) -> Self {
        Self { empty: AtomicBool::new(true), queue: Mutex::new(VecDeque::with_capacity(cap)) }
    }

    #[inline(always)]
    pub fn push(&self, msg: T) {
        let mut guard = self.queue.lock();
        let _ = self.empty.compare_exchange_weak(true, false, Ordering::SeqCst, Ordering::Relaxed);
        guard.push_back(msg);
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        if self.empty.load(Ordering::Acquire) {
            return None;
        }
        let mut guard = self.queue.lock();
        if let Some(item) = guard.pop_front() {
            Some(item)
        } else {
            self.empty.store(true, Ordering::Release);
            None
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let guard = self.queue.lock();
        guard.len()
    }
}
