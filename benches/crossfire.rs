use criterion::*;
use crossfire::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

mod common;
use common::*;

macro_rules! bench_bounded_blocking {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr) => {
        bench_bounded_blocking!($group, $name, $tx, $rx, $new, $size, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}_{}", $name, $size).to_string(), &param),
            &param,
            |b, i| {
                b.iter(move || {
                    let (tx, rx) = $new($size);
                    _crossfire_blocking(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    );
                })
            },
        );
    };
}

macro_rules! bench_unbounded_blocking {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr) => {
        bench_unbounded_blocking!($group, $name, $tx, $rx, $new, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}", $name).to_string(), &param),
            &param,
            |b, i| {
                b.iter(move || {
                    let (tx, rx) = $new();
                    _crossfire_blocking(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    );
                })
            },
        );
    };
}

macro_rules! bench_bounded_async {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr) => {
        bench_bounded_async!($group, $name, $tx, $rx, $new, $size, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}_{}", $name, $size).to_string(), &param),
            &param,
            |b, i| {
                b.to_async(get_runtime()).iter(async || {
                    let (tx, rx) = $new($size);
                    _crossfire_bounded_async(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    )
                    .await;
                })
            },
        );
    };
}

macro_rules! bench_unbounded_async {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr) => {
        bench_unbounded_async!($group, $name, $tx, $rx, $new, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}", $name).to_string(), &param),
            &param,
            |b, i| {
                b.to_async(get_runtime()).iter(async || {
                    let (tx, rx) = $new();
                    _crossfire_blocking_async(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    )
                    .await;
                })
            },
        );
    };
}

fn _crossfire_blocking<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let send_counter = Arc::new(AtomicUsize::new(0));
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let mut th_s = Vec::new();
    for _tx in txs {
        let _send_counter = send_counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _send_counter.fetch_add(1, Ordering::SeqCst);
            if i < msg_count {
                _tx.send(i).expect("send");
            } else {
                break;
            }
        }));
    }
    let rx_count = rxs.len();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
        let _recv_counter = recv_counter.clone();
        th_s.push(thread::spawn(move || loop {
            match _rx.recv() {
                Ok(_) => {
                    let _ = _recv_counter.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    break;
                }
            }
        }));
    }
    let rx = rxs.pop().unwrap();
    loop {
        match rx.recv() {
            Ok(_) => {
                let _ = recv_counter.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_s {
        let _ = th.join();
    }
    assert!(send_counter.load(Ordering::Acquire) >= msg_count);
    assert!(recv_counter.load(Ordering::Acquire) >= msg_count);
}

async fn _crossfire_blocking_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut sender_th_s = Vec::new();
    for tx in txs {
        let _counter = counter.clone();
        sender_th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < msg_count {
                if let Err(e) = tx.send(i) {
                    panic!("send error: {:?}", e);
                }
            } else {
                break;
            }
        }));
    }
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let rx_count = rxs.len();
    let mut recv_th_s = Vec::new();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
        let _recv_counter = recv_counter.clone();
        recv_th_s.push(tokio::spawn(async move {
            loop {
                match _rx.recv().await {
                    Ok(_) => {
                        let _ = _recv_counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }));
    }
    let rx = rxs.pop().unwrap();
    loop {
        match rx.recv().await {
            Ok(_) => {
                let _ = recv_counter.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in recv_th_s {
        let _ = th.await;
    }
    for th in sender_th_s {
        let _ = th.join();
    }
    assert!(counter.load(Ordering::Acquire) >= msg_count);
    assert!(recv_counter.load(Ordering::Acquire) >= msg_count);
}

async fn _crossfire_bounded_async<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut th_s = Vec::new();
    for tx in txs {
        let _counter = counter.clone();
        th_s.push(tokio::spawn(async move {
            loop {
                let i = _counter.fetch_add(1, Ordering::SeqCst);
                if i < msg_count {
                    if let Err(e) = tx.send(i).await {
                        panic!("send error: {:?}", e);
                    }
                } else {
                    break;
                }
            }
        }));
    }
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let rx_count = rxs.len();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
        let _recv_counter = recv_counter.clone();
        th_s.push(tokio::spawn(async move {
            loop {
                match _rx.recv().await {
                    Ok(_) => {
                        let _ = _recv_counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }));
    }
    let rx = rxs.pop().unwrap();
    loop {
        match rx.recv().await {
            Ok(_) => {
                let _ = recv_counter.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_s {
        let _ = th.await;
    }
    assert!(counter.load(Ordering::Acquire) >= msg_count);
    assert!(recv_counter.load(Ordering::Acquire) >= msg_count);
}

fn crossfire_bounded_1_blocking_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_1_1");
    bench_bounded_blocking!(group, "spsc", 1, 1, spsc::bounded_blocking, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_blocking!(group, "mpsc", 1, 1, mpsc::bounded_blocking, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_blocking!(group, "mpmc", 1, 1, mpmc::bounded_blocking, 1, TEN_THOUSAND, 10, 100);
    group.finish();
}

fn crossfire_bounded_1_blocking_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_n_1");
    for tx_count in [1, 2, 4, 8, 16] {
        bench_bounded_blocking!(
            group,
            "mpsc",
            tx_count,
            1,
            mpsc::bounded_blocking,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    for tx_count in [1, 2, 4, 8, 16] {
        bench_bounded_blocking!(
            group,
            "mpmc",
            tx_count,
            1,
            mpmc::bounded_blocking,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_1_blocking_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_n_n");
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_bounded_blocking!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_blocking,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_100_blocking_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_1_1");
    bench_bounded_blocking!(group, "spsc", 1, 1, spsc::bounded_blocking, 100, ONE_MILLION);
    bench_bounded_blocking!(group, "mpsc", 1, 1, mpsc::bounded_blocking, 100, ONE_MILLION);
    bench_bounded_blocking!(group, "mpmc", 1, 1, mpmc::bounded_blocking, 100, ONE_MILLION);
    group.finish();
}

fn crossfire_bounded_100_blocking_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_n_1");
    for tx_count in [1, 2, 4, 8, 16] {
        bench_bounded_blocking!(
            group,
            "mpsc",
            tx_count,
            1,
            mpsc::bounded_blocking,
            100,
            ONE_MILLION
        );
    }
    for tx_count in [1, 2, 4, 8, 16] {
        bench_bounded_blocking!(
            group,
            "mpmc",
            tx_count,
            1,
            mpmc::bounded_blocking,
            100,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_bounded_100_blocking_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_n_n");
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_bounded_blocking!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_blocking,
            100,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_bounded_1_async_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_1_async_1_1");
    bench_bounded_async!(group, "spsc", 1, 1, spsc::bounded_async, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_async!(group, "mpsc", 1, 1, mpsc::bounded_async, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_async!(group, "mpmc", 1, 1, mpmc::bounded_async, 1, TEN_THOUSAND, 10, 100);
    group.finish();
}

fn crossfire_bounded_1_async_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_1_async_n_1");
    for tx_count in [2, 4, 8, 16] {
        bench_bounded_async!(
            group,
            "mpsc",
            tx_count,
            1,
            mpsc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    for tx_count in [2, 4, 8, 16] {
        bench_bounded_async!(
            group,
            "mpmc",
            tx_count,
            1,
            mpmc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_1_async_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_1_async_n_n");
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_bounded_async!(
            group,
            "mpsc",
            input.0,
            input.1,
            mpsc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_bounded_async!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_100_async_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_100_async_1_1");
    bench_bounded_async!(group, "spsc", 1, 1, spsc::bounded_async, 100, ONE_MILLION);
    bench_bounded_async!(group, "mpsc", 1, 1, mpsc::bounded_async, 100, ONE_MILLION);
    bench_bounded_async!(group, "mpmc", 1, 1, mpmc::bounded_async, 100, ONE_MILLION);
    group.finish();
}

fn crossfire_bounded_100_async_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_100_async_n_1");
    for tx_count in [1, 2, 4, 8, 16] {
        bench_bounded_async!(group, "mpsc", tx_count, 1, mpsc::bounded_async, 100, ONE_MILLION);
    }

    for tx_count in [1, 2, 4, 8, 16] {
        bench_bounded_async!(group, "mpmc", tx_count, 1, mpmc::bounded_async, 100, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_bounded_100_async_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_bounded_100_async_n_n");
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_bounded_async!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_async,
            100,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_unbounded_blocking_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_1_1");
    bench_unbounded_blocking!(group, "spsc", 1, 1, spsc::unbounded_blocking, ONE_MILLION);
    bench_unbounded_blocking!(group, "mpsc", 1, 1, mpsc::unbounded_blocking, ONE_MILLION);
    bench_unbounded_blocking!(group, "mpmc", 1, 1, mpmc::unbounded_blocking, ONE_MILLION);
    group.finish();
}

fn crossfire_unbounded_blocking_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_n_1");
    for input in [1, 2, 4, 8, 16] {
        bench_unbounded_blocking!(group, "mpsc", input, 1, mpsc::unbounded_blocking, ONE_MILLION);
    }
    for input in [1, 2, 4, 8, 16] {
        bench_unbounded_blocking!(group, "mpmc", input, 1, mpmc::unbounded_blocking, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_unbounded_blocking_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_n_n");
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_unbounded_blocking!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::unbounded_blocking,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_unbounded_async_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_unbounded_async_1_1");
    bench_unbounded_async!(group, "spsc", 1, 1, spsc::unbounded_async, ONE_MILLION);
    bench_unbounded_async!(group, "mpsc", 1, 1, mpsc::unbounded_async, ONE_MILLION);
    bench_unbounded_async!(group, "mpmc", 1, 1, mpmc::unbounded_async, ONE_MILLION);
    group.finish();
}

fn crossfire_unbounded_async_mpsc(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_unbounded_async_n_1");
    for input in [1, 2, 4, 8, 16] {
        bench_unbounded_async!(group, "mpsc", input, 1, mpsc::unbounded_async, ONE_MILLION);
    }
    for input in [1, 2, 4, 8, 16] {
        bench_unbounded_async!(group, "mpmc", input, 1, mpmc::unbounded_async, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_unbounded_async_mpmc(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("crossfire_unbounded_async_n_n");
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        bench_unbounded_async!(group, "mpmc", input.0, input.1, mpmc::unbounded_async, ONE_MILLION);
    }
    group.finish();
}

criterion_group!(
    benches,
    crossfire_bounded_1_blocking_1_1,
    crossfire_bounded_1_blocking_n_1,
    crossfire_bounded_1_blocking_n_n,
    crossfire_bounded_100_blocking_1_1,
    crossfire_bounded_100_blocking_n_1,
    crossfire_bounded_100_blocking_n_n,
    crossfire_unbounded_blocking_1_1,
    crossfire_unbounded_blocking_n_1,
    crossfire_unbounded_blocking_n_n,
    crossfire_bounded_1_async_1_1,
    crossfire_bounded_1_async_n_1,
    crossfire_bounded_1_async_n_n,
    crossfire_bounded_100_async_1_1,
    crossfire_bounded_100_async_n_1,
    crossfire_bounded_100_async_n_n,
    crossfire_unbounded_async_1_1,
    crossfire_unbounded_async_mpsc,
    crossfire_unbounded_async_mpmc,
);
criterion_main!(benches);
