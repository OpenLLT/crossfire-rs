use criterion::*;
use crossbeam_queue::{ArrayQueue, SegQueue};
use crossfire::inner::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Weak,
};
use std::thread;
use std::time::Duration;

const TEN_MILLION: usize = 10000000;

struct Foo {
    _inner: usize,
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
        if guard.is_empty() {
            self.empty.store(false, Ordering::SeqCst);
        }
        guard.push_back(msg);
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        if self.empty.load(Ordering::SeqCst) {
            return None;
        }
        let mut guard = self.queue.lock();
        if let Some(item) = guard.pop_front() {
            if guard.len() == 0 {
                self.empty.store(true, Ordering::SeqCst);
            }
            Some(item)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let guard = self.queue.lock();
        guard.len()
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn exists(&self) -> bool {
        !self.empty.load(Ordering::Acquire)
    }
}

pub struct SpinQueue<T> {
    queue: Spinlock<VecDeque<T>>,
    empty: AtomicBool,
}

unsafe impl<T> Send for SpinQueue<T> {}
unsafe impl<T> Sync for SpinQueue<T> {}

impl<T> SpinQueue<T> {
    #[inline(always)]
    pub fn new(cap: usize) -> Self {
        Self {
            empty: AtomicBool::new(true),
            queue: Spinlock::new(VecDeque::with_capacity(cap), BackoffConfig::default()),
        }
    }

    #[inline(always)]
    pub fn push(&self, msg: T) {
        let mut guard = self.queue.lock();
        if guard.is_empty() {
            self.empty.store(true, Ordering::SeqCst);
        }
        guard.push_back(msg);
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        if self.empty.load(Ordering::SeqCst) {
            return None;
        }
        let mut guard = self.queue.lock();
        if let Some(item) = guard.pop_front() {
            if guard.is_empty() {
                self.empty.store(true, Ordering::SeqCst)
            }
            return Some(item);
        }
        return None;
    }
}

fn _bench_spin_queue(count: usize) {
    let queue = Arc::new(SpinQueue::<Weak<Foo>>::new(10));
    let mut th_s = Vec::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..count {
        let _queue = queue.clone();
        let _counter = counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < TEN_MILLION {
                if let Some(weak) = _queue.pop() {
                    let _ = weak.upgrade();
                }
            } else {
                break;
            }
        }));
    }
    th_s.push(thread::spawn(move || {
        for _ in 0..TEN_MILLION {
            let foo = Arc::new(Foo { _inner: 1 });
            queue.push(Arc::downgrade(&foo));
        }
    }));
    for th in th_s {
        let _ = th.join();
    }
}

fn _bench_locked_queue(count: usize) {
    let queue = Arc::new(LockedQueue::<Weak<Foo>>::new(10));
    let mut th_s = Vec::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..count {
        let _queue = queue.clone();
        let _counter = counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < TEN_MILLION {
                if let Some(weak) = _queue.pop() {
                    let _ = weak.upgrade();
                }
            } else {
                break;
            }
        }));
    }
    th_s.push(thread::spawn(move || {
        for _ in 0..TEN_MILLION {
            let foo = Arc::new(Foo { _inner: 1 });
            queue.push(Arc::downgrade(&foo));
        }
    }));
    for th in th_s {
        let _ = th.join();
    }
}

fn _bench_array_queue(count: usize) {
    let queue = Arc::new(ArrayQueue::<Weak<Foo>>::new(1));
    let mut th_s = Vec::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..count {
        let _queue = queue.clone();
        let _counter = counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < TEN_MILLION {
                if let Some(weak) = _queue.pop() {
                    let _ = weak.upgrade();
                }
            } else {
                break;
            }
        }));
    }
    th_s.push(thread::spawn(move || {
        for _ in 0..TEN_MILLION {
            let foo = Arc::new(Foo { _inner: 1 });
            queue.force_push(Arc::downgrade(&foo));
        }
    }));
    for th in th_s {
        let _ = th.join();
    }
}

fn _bench_seg_queue(count: usize) {
    let queue = Arc::new(SegQueue::<Weak<Foo>>::new());
    let mut th_s = Vec::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..count {
        let _queue = queue.clone();
        let _counter = counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < TEN_MILLION {
                if let Some(weak) = _queue.pop() {
                    let _ = weak.upgrade();
                }
            } else {
                break;
            }
        }));
    }
    th_s.push(thread::spawn(move || {
        for _ in 0..TEN_MILLION {
            let foo = Arc::new(Foo { _inner: 1 });
            queue.push(Arc::downgrade(&foo));
        }
    }));
    for th in th_s {
        let _ = th.join();
    }
}

fn _bench_weak_cell(count: usize) {
    let cell = Arc::new(WeakCell::<Foo>::new());
    let mut th_s = Vec::new();
    let counter = Arc::new(AtomicUsize::new(0));
    for _ in 0..count {
        let _cell = cell.clone();
        let _counter = counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < TEN_MILLION {
                let _ = _cell.pop();
            } else {
                break;
            }
        }));
    }
    th_s.push(thread::spawn(move || {
        for _ in 0..TEN_MILLION {
            let foo = Arc::new(Foo { _inner: 1 });
            cell.put(Arc::downgrade(&foo));
        }
    }));
    for th in th_s {
        let _ = th.join();
    }
}

fn _bench_empty(c: &mut Criterion) {
    let mut group = c.benchmark_group("empty");
    group.significance_level(0.1).sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("weak_cell", |b| {
        b.iter(|| {
            let cell = WeakCell::<Foo>::new();
            for _ in 0..TEN_MILLION {
                let _ = cell.pop();
            }
        })
    });
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("spin VecDeque", |b| {
        b.iter(|| {
            let queue = SpinQueue::<Foo>::new(10);
            for _ in 0..TEN_MILLION {
                let _ = queue.pop();
            }
        })
    });
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("locked VecDeque", |b| {
        b.iter(|| {
            let queue = LockedQueue::<Foo>::new(10);
            for _ in 0..TEN_MILLION {
                let _ = queue.pop();
            }
        })
    });

    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("array_queue", |b| {
        b.iter(|| {
            let queue = ArrayQueue::<Foo>::new(1);
            for _ in 0..TEN_MILLION {
                let _ = queue.pop();
            }
        })
    });
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("seg_queue", |b| {
        b.iter(|| {
            let queue = SegQueue::<Foo>::new();
            for _ in 0..TEN_MILLION {
                let _ = queue.pop();
            }
        })
    });
}

fn _bench_sequence(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequence");
    group.significance_level(0.1).sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("weak_cell", |b| {
        b.iter(|| {
            let cell = WeakCell::<Foo>::new();
            for _ in 0..TEN_MILLION {
                let foo = Arc::new(Foo { _inner: 1 });
                cell.put(Arc::downgrade(&foo));
                let _ = cell.pop();
            }
        })
    });
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("spin VecDeque", |b| {
        b.iter(|| {
            let queue = SpinQueue::new(10);
            for _ in 0..TEN_MILLION {
                let foo = Arc::new(Foo { _inner: 1 });
                let _ = queue.push(Arc::downgrade(&foo));
                if let Some(w) = queue.pop() {
                    let _ = w.upgrade();
                }
            }
        })
    });
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("locked VecDeque", |b| {
        b.iter(|| {
            let queue = LockedQueue::new(10);
            for _ in 0..TEN_MILLION {
                let foo = Arc::new(Foo { _inner: 1 });
                let _ = queue.push(Arc::downgrade(&foo));
                if let Some(w) = queue.pop() {
                    let _ = w.upgrade();
                }
            }
        })
    });

    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("array_queue", |b| {
        b.iter(|| {
            let queue = ArrayQueue::<Weak<Foo>>::new(1);
            for _ in 0..TEN_MILLION {
                let foo = Arc::new(Foo { _inner: 1 });
                let _ = queue.push(Arc::downgrade(&foo));
                if let Some(w) = queue.pop() {
                    let _ = w.upgrade();
                }
            }
        })
    });
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("seg_queue", |b| {
        b.iter(|| {
            let queue = SegQueue::<Weak<Foo>>::new();
            for _ in 0..TEN_MILLION {
                let foo = Arc::new(Foo { _inner: 1 });
                let _ = queue.push(Arc::downgrade(&foo));
                if let Some(w) = queue.pop() {
                    let _ = w.upgrade();
                }
            }
        })
    });
}

fn _bench_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("threads");
    group.significance_level(0.1).sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for input in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(TEN_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("weak_cell", input), &input, |b, i| {
            b.iter(|| _bench_weak_cell(*i))
        });
        group.throughput(Throughput::Elements(TEN_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("spin VecDeque", input), &input, |b, i| {
            b.iter(|| _bench_spin_queue(*i))
        });
        group.throughput(Throughput::Elements(TEN_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("locked VecDeque", input), &input, |b, i| {
            b.iter(|| _bench_locked_queue(*i))
        });
        group.throughput(Throughput::Elements(TEN_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("array_queue", input), &input, |b, i| {
            b.iter(|| _bench_array_queue(*i))
        });
        group.throughput(Throughput::Elements(TEN_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("seg_queue", input), &input, |b, i| {
            b.iter(|| _bench_seg_queue(*i))
        });
    }
}

fn _bench_waker_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("waker_cache");
    group.significance_level(0.1).sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(TEN_MILLION as u64));
    group.bench_function("cache", |b| {
        b.iter(|| {
            let cache = WakerCache::<RecvWaker>::new();
            for _ in 0..TEN_MILLION {
                let waker = cache.new_blocking();
                waker.set_state(WakerState::WAKED);
                cache.push(waker);
            }
        })
    });
    group.bench_function("alloc", |b| {
        b.iter(|| {
            for _ in 0..TEN_MILLION {
                let _waker = RecvWaker::new_blocking();
                _waker.set_state(WakerState::WAKED);
            }
        })
    });
    group.finish();
}

criterion_group!(benches, _bench_empty, _bench_sequence, _bench_threads, _bench_waker_cache);
criterion_main!(benches);
