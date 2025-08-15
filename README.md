# Crossfire

[![Build Status](https://github.com/frostyplanet/crossfire-rs/workflows/Rust/badge.svg)](
https://github.com/frostyplanet/crossfire-rs/actions)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](
https://github.com/qignstor/crossfire-rs#license)
[![Cargo](https://img.shields.io/crates/v/crossfire.svg)](
https://crates.io/crates/crossfire)
[![Documentation](https://docs.rs/crossfire/badge.svg)](
https://docs.rs/crossfire)
[![Rust 1.36+](https://img.shields.io/badge/rust-1.36+-lightgray.svg)](
https://www.rust-lang.org)

High-performance lockless spsc/mpsc/mpmc channels.

It supports async context, or communicates between async-blocking context.

Low level is based on crossbeam-queue.

For the concept, please refer to [wiki](https://github.com/frostyplanet/crossfire-rs/wiki).

## Versions history

V1.0 has been released and used in production since 2022.12. Heavily tested on X86_64 and ARM.

V2.0 (first release on 2025.6), refactored the codebase and API. By removing generic types of ChannelShared object in sender and receiver,
it's easier to remember and code.

v2.1 (first release on 2025.8), replaced underlaying crossbeam-channel with crossbeam-queue, bringing massive performance
boost, for both async context and blocking context.


## Performance

Being a lockless channel, crossfire outperform other async capability channel.
And thanks to a lighter notification mechanism, in blocking context some cases are even
better than original crossbeam-channel,

Also being a lockless channel, the algorithm rely on spinning and yielding. Spinning is good on
multi core system, but not friendly to single core system (like virtual machine). So we provide a function
`detect_backoff_cfg()` to detect the running platform. Call it on your code initialization
section, will get 2x performance boost, by replacing the backoff action with `std::thread::yield_now`.

Benchmark is written in criterion framework. You can run benchmark by:

```
cargo bench --bench crossfire
```

More benchmark data is on [wiki](https://github.com/frostyplanet/crossfire-rs/wiki/benchmark-v2.0.14-2025%E2%80%9008%E2%80%9003). Here are some of the results:

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.0.14-2025-08-03/mpsc_size_100_async.png" alt="mpsc bounded size 100 async context">

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.0.14-2025-08-03/mpmc_size_100_async.png" alt="mpmc bounded size 100 async context">

<img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.0.14-2025-08-03/mpmc_unbounded_async.png" alt="mpmc unbounded async context">


## APIs

### modules and functions

There are 3 modules: [spsc](https://docs.rs/crossfire/latest/crossfire/spsc/index.html), [mpsc](https://docs.rs/crossfire/latest/crossfire/mpsc/index.html), [mpmc](https://docs.rs/crossfire/latest/crossfire/mpmc/index.html), providing functions to allocate different types of channels.

The SP or SC interface, only for non-concurrent operation, it's more memory efficient than MP or MC implementations, and sometimes slightly faster.

The return types in these 3 modules are different:

* mpmc::bounded_blocking() : (tx blocking, rx blocking)

* mpmc::bounded_async() :  (tx async, rx async)

* mpmc::bounded_tx_async_rx_blocking() : (tx async, rx blocking)

* mpmc::bounded_tx_blocking_rx_async() : (tx blocking, rx async)

* mpmc::unbounded_blocking() : (tx non-blocking, rx blocking)

* mpmc::unbounded_async() : (tx non-blocking, rx async)


> **NOTE** :  For bounded channel, 0 size case is not supported yet. (Temporary rewrite as 1 size).

### Types

<table align="center" cellpadding="30">
<tr> <th rowspan="2"> Context </th><th colspan="2" align="center"> Sender (Producer) </th> <th colspan="2" align="center"> Receiver (Consumer) </th> </tr>
<tr> <td> Single </td> <td> Multiple </td><td> Single </td><td> Multiple </td></tr>
<tr><td rowspan="2"> <b>Blocking</b> </td>
<td colspan="2" align="center"> BlockingTxTrait </td>
<td colspan="2" align="center"> BlockingRxTrait </td></tr>
<tr>
<td align="center">Tx </td>
<td align="center">MTx</td>
<td align="center">Rx</td>
<td align="center">MRx</td> </tr>

<tr><td rowspan="2"><b>Async</b></td>
<td colspan="2" align="center">AsyncTxTrait</td>
<td colspan="2" align="center">AsyncRxTrait</td></tr>
<tr>
<td>AsyncTx</td>
<td>MAsyncTx</td>
<td>AsyncRx</td>
<td>MAsyncRx</td></tr>

</table>

For SP / SC version `AsyncTx`, `AsyncRx`, `Tx`, `Rx`, is not `Clone`, and without `Sync`,
Although can be moved to other thread, but not allowed to use send/recv while in Arc.
(Refer to the compile_fail examples in type document).

The benifit using SP / SC API is completely lockless for waker registration, in exchange for some performance boost.

Sender/receiver can use `From` trait to convert between blocking context and async context
conterparts.

### Error types

Error types are the same with crossbeam-channel:  `TrySendError`, `SendError`, `TryRecvError`, `RecvError`

### Feature flags

 - `tokio`: Enable send_timeout, recv_timeout API for async context, based on `tokio`. And will
 detect the right backoff strategy for the type of runtime (multi-threaded / current-thread).

- `async_std`: Enable send_timeout, recv_timeout API for async context, base on `async-std`.

### Async compatibility

Tested on tokio-1.x and async-std-1.x, crossfire is run time agnostic.

The following senario is considered:

* The `AsyncTx::send()`and `AsyncRx:recv()` operation is **cancellation-safe** in async context,
you can safely use select! macro and timeout() function in tokio/futures in combination with recv()
 On cancellation, [SendFuture] and [RecvFuture] will trigget drop(), which will cleanup the state of waker,
make sure no mem-leak and deadlock.
But you cannot know the true result from SendFuture, since it's dropped
upon cancellation, thus we suggest use `AsyncTx::send_timeout()` instead.

* When feature "tokio" or "async_std" enable, we also provide two additional functions:

- `AsyncTx::send_timeout()`, which will return the message failed to sent in
[SendTimeoutError], we guarantee the result is atomic.

- `AsyncRx::recv_timeout()`, we guarantee the result is atomic.

* Between blocking context and async context, and between different async runtime instances.

Be aware that being the only runable future in tokio runtime might result to busy polling.
(future is polled before message arrive). If this situation is detected, we will yield the thread to OS to
reduce meaning-less cpu consumption.

* The async waker footprint.

While using multi-producer and multi-consumer scenario, there's small memory overhead to pass along `Weak`
reference of wakers.
Because we aim to be lockless, when the sending/receiving futures are cancelled (like tokio::time::timeout()),
might trigger immediate cleanup if try-lock is successful, otherwise will rely on lazy cleanup.
(This won't be an issue because weak wakers will be consumed by actual message send and recv).
On idle-select scenario, like notification for close, waker will be reuse as much as possible
if poll() return pending.

## Usage

Cargo.toml:
```toml
[dependencies]
crossfire = "2.0"
```

# Example with tokio::select!

```rust
extern crate crossfire;
use crossfire::*;
#[macro_use]
extern crate tokio;
use tokio::time::{sleep, interval, Duration};

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::bounded_async::<i32>(100);
    for _ in 0..10 {
        let _tx = tx.clone();
        tokio::spawn(async move {
            for i in 0i32..10 {
                let _ = _tx.send(i).await;
                sleep(Duration::from_millis(100)).await;
                println!("sent {}", i);
            }
        });
    }
    drop(tx);
    let mut inv = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = inv.tick() =>{
                println!("tick");
            }
            r = rx.recv() => {
                if let Ok(_i) = r {
                    println!("recv {}", _i);
                } else {
                    println!("rx closed");
                    break;
                }
            }
        }
    }
}
```
