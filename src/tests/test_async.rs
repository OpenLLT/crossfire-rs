use super::common::*;
use crate::{sink::*, stream::*, *};
use captains_log::{logfn, *};
use futures::stream::{FusedStream, StreamExt};
use rstest::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::*;
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    let _ = recipe::env_logger("LOG_FILE", "LOG_LEVEL").build().expect("log setup");
    //    let _ = recipe::ring_file("/tmp/ring.log", 512*1024*1024, Level::Debug, signal_consts::SIGHUP).build().expect("log_setup");
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(mpsc::bounded_async(1))]
#[case(mpmc::bounded_async(1))]
fn test_basic_bounded_empty_full_drop_rx<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    assert_eq!(tx.capacity(), Some(1));
    assert_eq!(rx.capacity(), Some(1));
    tx.try_send(1).expect("Ok");
    assert!(tx.is_full());
    assert!(rx.is_full());
    assert!(!tx.is_empty());
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(rx);
    assert_eq!(tx.is_disconnected(), true);
    assert_eq!(tx.as_ref().get_rx_count(), 0);
    assert_eq!(tx.as_ref().get_tx_count(), 1);
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(mpsc::bounded_async(1))]
#[case(mpmc::bounded_async(1))]
fn test_basic_bounded_empty_full_drop_tx<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    assert_eq!(tx.capacity(), Some(1));
    assert_eq!(rx.capacity(), Some(1));
    tx.try_send(1).expect("Ok");
    assert!(tx.is_full());
    assert!(rx.is_full());
    assert!(!tx.is_empty());
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(tx);
    assert_eq!(rx.is_disconnected(), true);
    assert_eq!(rx.as_ref().get_tx_count(), 0);
    assert_eq!(rx.as_ref().get_rx_count(), 1);
}

#[logfn]
#[rstest]
fn test_basic_compile_bounded_empty_full() {
    let (tx, rx) = mpmc::bounded_async::<usize>(1);
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    tx.try_send(1).expect("ok");
    assert!(tx.is_full());
    assert!(!tx.is_empty());
    assert!(rx.is_full());
    assert_eq!(tx.get_tx_count(), 1);
    assert_eq!(rx.get_tx_count(), 1);
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(rx);
    assert_eq!(tx.is_disconnected(), true);
}

#[logfn]
#[rstest]
fn test_sync() {
    use futures::FutureExt;
    runtime_block_on!(async move {
        let (tx, rx) = spsc::bounded_async::<usize>(100);
        //  Example1: should fail to compile with Arc
        //    let tx = Arc::new(tx);
        async_spawn!(async move {
            let _ = tx.send(2).await;
        });
        drop(rx);

        let (tx, rx) = mpsc::bounded_async::<usize>(100);
        //  example2: should fail to compile with Arc
        //    let rx = Arc::new(rx);
        async_spawn!(async move {
            let _ = rx.recv().await;
        });
        drop(tx);

        let (tx, rx) = mpsc::bounded_blocking::<usize>(100);
        ////  example3: should fail to compile with Arc
        //    let rx = Arc::new(rx);
        std::thread::spawn(move || {
            let _ = rx.recv();
        });
        drop(tx);

        let (tx, rx) = spsc::bounded_blocking::<usize>(100);
        ////  example4: should fail to compile after Arc
        //   let tx = Arc::new(tx);
        std::thread::spawn(move || {
            let _ = tx.send(1);
        });
        drop(rx);

        let (tx, rx) = mpmc::bounded_blocking::<usize>(100);
        // MRx can put in Arc
        let rx = Arc::new(rx);
        std::thread::spawn(move || {
            let _ = rx.try_recv();
        });
        // MTx can put in Arc
        let tx = Arc::new(tx);
        std::thread::spawn(move || {
            let _ = tx.try_send(1);
        });

        let (tx, rx) = spsc::bounded_async::<usize>(100);
        let th = async_spawn!(async move {
            let mut i = 0;
            loop {
                sleep(Duration::from_secs(1)).await;
                i += 1;
                if let Err(_) = tx.send(i).await {
                    println!("rx dropped");
                    return;
                }
            }
        });
        'LOOP: for _ in 0..10 {
            futures::select! {
                _ = sleep(Duration::from_millis(500)).fuse() =>{
                    println!("tick");
                },
                r = rx.recv().fuse() => {
                    match r {
                        Ok(item)=>{
                            println!("recv {}", item);
                        }
                        Err(e)=>{
                            println!("tx dropped {:?}", e);
                            break 'LOOP;
                        }
                    }
                }
            }
        }
        drop(rx);
        let _ = th.await;
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<usize>(100))]
#[case(mpsc::bounded_async::<usize>(100))]
#[case(mpmc::bounded_async::<usize>(100))]
fn test_basic_bounded_rx_drop<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let tx = {
            let (tx, _rx) = channel;
            tx.send(1).await.expect("ok");
            tx.send(2).await.expect("ok");
            tx.send(3).await.expect("ok");
            tx
        };
        {
            info!("try to send after rx dropped");
            assert_eq!(tx.send(4).await.unwrap_err(), SendError(4));
            drop(tx);
            info!("dropped tx");
        }
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async::<usize>())]
#[case(mpsc::unbounded_async::<usize>())]
#[case(mpmc::unbounded_async::<usize>())]
fn test_basic_unbounded_rx_drop<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let tx = {
            let (tx, _rx) = channel;
            tx.send(1).expect("ok");
            tx.send(2).expect("ok");
            tx.send(3).expect("ok");
            tx
        };
        {
            info!("try to send after rx dropped");
            assert_eq!(tx.send(4).unwrap_err(), SendError(4));
            drop(tx);
            info!("dropped tx");
        }
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<i32>(10))]
#[case(mpsc::bounded_async::<i32>(10))]
#[case(mpmc::bounded_async::<i32>(10))]
fn test_basic_bounded_1_thread<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let rx_res = rx.try_recv();
        assert!(rx_res.is_err());
        assert!(rx_res.unwrap_err().is_empty());
        for i in 0i32..10 {
            let tx_res = tx.try_send(i);
            assert!(tx_res.is_ok());
        }
        let tx_res = tx.try_send(11);
        assert!(tx_res.is_err());
        assert!(tx_res.unwrap_err().is_full());

        let th = async_spawn!(async move {
            for i in 0i32..12 {
                match rx.recv().await {
                    Ok(j) => {
                        debug!("recv {}", i);
                        assert_eq!(i, j);
                    }
                    Err(e) => {
                        panic!("error {}", e);
                    }
                }
            }
            let res = rx.recv().await;
            assert!(res.is_err());
            debug!("rx close");
        });
        assert!(tx.send(10).await.is_ok());
        sleep(Duration::from_secs(1)).await;
        assert!(tx.send(11).await.is_ok());
        drop(tx);
        let _ = th.await;
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
fn test_basic_unbounded_1_thread<T: BlockingTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert_eq!(tx.capacity(), None);
    assert_eq!(rx.capacity(), None);
    runtime_block_on!(async move {
        let rx_res = rx.try_recv();
        assert!(rx_res.is_err());
        assert!(rx_res.unwrap_err().is_empty());
        for i in 0i32..10 {
            let tx_res = tx.try_send(i);
            assert!(tx_res.is_ok());
        }

        let th = async_spawn!(async move {
            for i in 0i32..12 {
                match rx.recv().await {
                    Ok(j) => {
                        debug!("recv {}", i);
                        assert_eq!(i, j);
                    }
                    Err(e) => {
                        panic!("error {}", e);
                    }
                }
            }
            let res = rx.recv().await;
            assert!(res.is_err());
            debug!("rx close");
        });
        assert!(tx.send(10).is_ok());
        sleep(Duration::from_secs(1)).await;
        assert!(tx.send(11).is_ok());
        drop(tx);
        let _ = th.await;
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
fn test_basic_unbounded_idle_select<T: BlockingTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (_tx, rx) = channel;

    use futures::{pin_mut, select, FutureExt};
    runtime_block_on!(async move {
        let mut c = rx.recv().fuse();
        for _ in 0..1000 {
            {
                let f = sleep(Duration::from_millis(1)).fuse();
                pin_mut!(f);
                select! {
                    _ = f => {
                        let (_tx_wakers, _rx_wakers) = rx.as_ref().get_wakers_count();
                        debug!("waker tx {} rx {}", _tx_wakers, _rx_wakers);
                    },
                    _ = c => {
                        unreachable!()
                    },
                }
            }
        }
        let (tx_wakers, rx_wakers) = rx.as_ref().get_wakers_count();
        assert_eq!(tx_wakers, 0);
        info!("waker rx {}", rx_wakers);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<i32>(10))]
#[case(mpsc::bounded_async::<i32>(10))]
#[case(mpmc::bounded_async::<i32>(10))]
fn test_basic_bounded_recv_after_sender_close<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let total_msg_count = 5;
    for i in 0..total_msg_count {
        let _ = tx.try_send(i).expect("send ok");
    }
    drop(tx);

    runtime_block_on!(async move {
        // NOTE: 5 < 10
        let mut recv_msg_count = 0;
        loop {
            match rx.recv().await {
                Ok(_) => {
                    recv_msg_count += 1;
                }
                Err(_) => {
                    break;
                }
            }
        }
        assert_eq!(recv_msg_count, total_msg_count);
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
fn test_basic_unbounded_recv_after_sender_close<T: BlockingTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let total_msg_count = 500;
    for i in 0..total_msg_count {
        let _ = tx.send(i).expect("send ok");
    }
    drop(tx);
    runtime_block_on!(async move {
        let mut recv_msg_count = 0;
        loop {
            match rx.recv().await {
                Ok(_) => {
                    recv_msg_count += 1;
                }
                Err(_) => {
                    break;
                }
            }
        }
        assert_eq!(recv_msg_count, total_msg_count);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<i32>(100))]
#[case(mpsc::bounded_async::<i32>(100))]
#[case(mpmc::bounded_async::<i32>(100))]
fn test_basic_timeout_recv_async_waker<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    #[cfg(feature = "tokio")]
    {
        let (tx, rx) = channel;
        runtime_block_on!(async move {
            for _ in 0..1000 {
                assert!(tokio::time::timeout(Duration::from_millis(1), rx.recv()).await.is_err());
            }
            let (tx_wakers, rx_wakers) = rx.as_ref().get_wakers_count();
            println!("wakers: {}, {}", tx_wakers, rx_wakers);
            assert!(tx_wakers <= 1);
            assert!(rx_wakers <= 1);
            sleep(Duration::from_secs(1)).await;
            let _ = tx.send(1).await;
            assert_eq!(rx.recv().await.unwrap(), 1);
            let (tx_wakers, rx_wakers) = rx.as_ref().get_wakers_count();
            println!("wakers: {}, {}", tx_wakers, rx_wakers);
            assert!(tx_wakers <= 1);
            assert!(rx_wakers <= 1);
        });
    }
    #[cfg(not(feature = "tokio"))]
    {
        println!("skipped")
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
fn test_basic_unbounded_recv_timeout_async<T: BlockingTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        runtime_block_on!(async move {
            let th = async_spawn!(async move {
                sleep(Duration::from_millis(200)).await;
                let _ = tx.send(1);
            });
            assert_eq!(
                rx.recv_timeout(Duration::from_millis(1)).await.unwrap_err(),
                RecvTimeoutError::Timeout
            );
            let _ = th.await;
            let (tx_wakers, rx_wakers) = rx.as_ref().get_wakers_count();
            println!("wakers: {}, {}", tx_wakers, rx_wakers);
            assert_eq!(tx_wakers, 0);
            assert_eq!(rx_wakers, 0);
            assert_eq!(rx.recv_timeout(Duration::from_millis(2)).await.unwrap(), 1);
        });
    }
    #[cfg(not(any(feature = "tokio", feature = "async_std")))]
    {
        println!("skipped");
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<i32>(10))]
#[case(mpsc::bounded_async::<i32>(10))]
#[case(mpmc::bounded_async::<i32>(10))]
fn test_basic_send_timeout_async<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        for i in 0..10 {
            assert!(tx.try_send(i).is_ok());
        }

        runtime_block_on!(async move {
            assert_eq!(
                tx.send_timeout(11, Duration::from_millis(1)).await.unwrap_err(),
                SendTimeoutError::Timeout(11)
            );
            let th = async_spawn!(async move {
                loop {
                    sleep(Duration::from_millis(2)).await;
                    if let Err(_) = rx.recv().await {
                        println!("tx dropped");
                        break;
                    }
                }
            });
            let mut try_times = 0;
            loop {
                try_times += 1;
                match tx.send_timeout(11, Duration::from_millis(1)).await {
                    Ok(_) => {
                        println!("send ok after {} tries", try_times);
                        break;
                    }
                    Err(SendTimeoutError::Timeout(msg)) => {
                        println!("timeout");
                        assert_eq!(msg, 11);
                    }
                    Err(SendTimeoutError::Disconnected(_)) => {
                        unreachable!();
                    }
                }
            }
            let (tx_wakers, rx_wakers) = tx.as_ref().get_wakers_count();
            println!("wakers: {}, {}", tx_wakers, rx_wakers);
            assert_eq!(tx_wakers, 0);
            assert_eq!(rx_wakers, 0);
            drop(tx);
            let _ = th.await;
        });
    }
    #[cfg(not(any(feature = "tokio", feature = "async_std")))]
    {
        println!("skipped");
    }
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<i32>(1))]
fn test_pressure_bounded_timeout_async(
    setup_log: (), #[case] _channel: (MAsyncTx<i32>, MAsyncRx<i32>),
) {
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    {
        use parking_lot::Mutex;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicI32;
        let (tx, rx) = _channel;

        runtime_block_on!(async move {
            assert_eq!(
                rx.recv_timeout(Duration::from_millis(1)).await.unwrap_err(),
                RecvTimeoutError::Timeout
            );
            let (tx_wakers, rx_wakers) = rx.as_ref().get_wakers_count();
            println!("wakers: {}, {}", tx_wakers, rx_wakers);
            assert_eq!(tx_wakers, 0);
            assert_eq!(rx_wakers, 0);
            const ROUND: i32 = 50000;

            let send_counter = Arc::new(AtomicI32::new(0));
            let recv_counter = Arc::new(AtomicI32::new(0));
            let send_timeout_counter = Arc::new(AtomicUsize::new(0));
            let recv_timeout_counter = Arc::new(AtomicUsize::new(0));
            let recv_map = Arc::new(Mutex::new(HashMap::new()));

            let mut th_s = Vec::new();
            for thread_id in 0..3 {
                let _send_counter = send_counter.clone();
                let _send_timeout_counter = send_timeout_counter.clone();
                let _recv_map = recv_map.clone();
                let _tx = tx.clone();
                th_s.push(async_spawn!(async move {
                    // randomize start up
                    sleep(Duration::from_millis(thread_id & 3)).await;
                    loop {
                        let i = _send_counter.fetch_add(1, Ordering::SeqCst);
                        if i >= ROUND {
                            return;
                        }
                        {
                            let mut guard = _recv_map.lock();
                            guard.insert(i, ());
                        }
                        if i & 2 == 0 {
                            sleep(Duration::from_millis(3)).await;
                        } else {
                            sleep(Duration::from_millis(1)).await;
                        }
                        loop {
                            match _tx.send_timeout(i, Duration::from_millis(1)).await {
                                Ok(_) => break,
                                Err(SendTimeoutError::Timeout(_i)) => {
                                    _send_timeout_counter.fetch_add(1, Ordering::SeqCst);
                                    assert_eq!(_i, i);
                                }
                                Err(SendTimeoutError::Disconnected(_)) => {
                                    unreachable!();
                                }
                            }
                        }
                    }
                }));
            }
            for _thread_id in 0..2 {
                let _rx = rx.clone();
                let _recv_map = recv_map.clone();
                let _recv_counter = recv_counter.clone();
                let _recv_timeout_counter = recv_timeout_counter.clone();
                th_s.push(async_spawn!(async move {
                    let mut step: usize = 0;
                    loop {
                        step += 1;
                        let timeout = if step & 2 == 0 { 1 } else { 2 };
                        if step & 2 > 0 {
                            sleep(Duration::from_millis(1)).await;
                        }
                        match _rx.recv_timeout(Duration::from_millis(timeout)).await {
                            Ok(item) => {
                                _recv_counter.fetch_add(1, Ordering::SeqCst);
                                {
                                    let mut guard = _recv_map.lock();
                                    guard.remove(&item);
                                }
                            }
                            Err(RecvTimeoutError::Timeout) => {
                                _recv_timeout_counter.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(RecvTimeoutError::Disconnected) => {
                                return;
                            }
                        }
                    }
                }));
            }
            drop(tx);
            drop(rx);
            for th in th_s {
                let _ = th.await;
            }
            {
                let guard = recv_map.lock();
                assert!(guard.is_empty());
            }
            assert_eq!(ROUND, recv_counter.load(Ordering::Acquire));
            println!("send timeout count: {}", send_timeout_counter.load(Ordering::Acquire));
            println!("recv timeout count: {}", recv_timeout_counter.load(Ordering::Acquire));
        });
    }
    #[cfg(not(any(feature = "tokio", feature = "async_std")))]
    {
        println!("skipped");
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<usize>(1))]
#[case(spsc::bounded_async::<usize>(10))]
#[case(spsc::bounded_async::<usize>(100))]
#[case(spsc::bounded_async::<usize>(300))]
#[case(mpsc::bounded_async::<usize>(1))]
#[case(mpsc::bounded_async::<usize>(10))]
#[case(mpsc::bounded_async::<usize>(100))]
#[case(mpsc::bounded_async::<usize>(300))]
#[case(mpmc::bounded_async::<usize>(1))]
#[case(mpmc::bounded_async::<usize>(10))]
#[case(mpmc::bounded_async::<usize>(100))]
#[case(mpmc::bounded_async::<usize>(300))]
fn test_pressure_bounded_async_1_1<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 10000;
    runtime_block_on!(async move {
        let _round = round;
        let th = async_spawn!(async move {
            for i in 0.._round {
                if let Err(e) = tx.send(i).await {
                    panic!("{:?}", e);
                }
            }
            debug!("tx exit");
        });
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        let _ = th.await;
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round);
    });
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_async::<usize>(1), 10)]
#[case(mpsc::bounded_async::<usize>(1), 100)]
#[case(mpsc::bounded_async::<usize>(1), 300)]
#[case(mpsc::bounded_async::<usize>(10), 10)]
#[case(mpsc::bounded_async::<usize>(10), 100)]
#[case(mpsc::bounded_async::<usize>(10), 300)]
#[case(mpsc::bounded_async::<usize>(100), 10)]
#[case(mpsc::bounded_async::<usize>(100), 100)]
#[case(mpsc::bounded_async::<usize>(100), 300)]
#[case(mpmc::bounded_async::<usize>(1), 10)]
#[case(mpmc::bounded_async::<usize>(1), 100)]
#[case(mpmc::bounded_async::<usize>(1), 300)]
#[case(mpmc::bounded_async::<usize>(10), 10)]
#[case(mpmc::bounded_async::<usize>(10), 100)]
#[case(mpmc::bounded_async::<usize>(10), 300)]
#[case(mpmc::bounded_async::<usize>(100), 10)]
#[case(mpmc::bounded_async::<usize>(100), 100)]
#[case(mpmc::bounded_async::<usize>(100), 300)]
fn test_pressure_bounded_async_multi_1<R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;

    runtime_block_on!(async move {
        let counter = Arc::new(AtomicUsize::new(0));
        let round: usize = 10000;
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            let _round = round;
            th_s.push(async_spawn!(async move {
                for i in 0.._round {
                    match _tx.send(i).await {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                debug!("tx {} exit", _tx_i);
            }));
        }
        drop(tx);
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        for th in th_s {
            let _ = th.await;
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round * tx_count);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<usize>(1), 100, 10)]
#[case(mpmc::bounded_async::<usize>(1), 10, 100)]
#[case(mpmc::bounded_async::<usize>(1), 300, 300)]
#[case(mpmc::bounded_async::<usize>(10), 10, 10)]
#[case(mpmc::bounded_async::<usize>(10), 100, 10)]
#[case(mpmc::bounded_async::<usize>(10), 10, 100)]
#[case(mpmc::bounded_async::<usize>(10), 300, 300)]
#[case(mpmc::bounded_async::<usize>(100), 10, 10)]
#[case(mpmc::bounded_async::<usize>(100), 100, 10)]
#[case(mpmc::bounded_async::<usize>(100), 10, 100)]
#[case(mpmc::bounded_async::<usize>(100), 300, 300)]
fn test_pressure_bounded_async_multi(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MAsyncRx<usize>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let counter = Arc::new(AtomicUsize::new(0));
        let round: usize = 10000;
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            let _round = round;
            th_s.push(async_spawn!(async move {
                for i in 0.._round {
                    match _tx.send(i).await {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                debug!("tx {} exit", _tx_i);
            }));
        }
        for _rx_i in 0..rx_count {
            let _rx = rx.clone();
            let _counter = counter.clone();
            th_s.push(async_spawn!(async move {
                'A: loop {
                    match _rx.recv().await {
                        Ok(_i) => {
                            _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                            debug!("recv {} {}", _rx_i, _i);
                        }
                        Err(_) => break 'A,
                    }
                }
                debug!("rx {} exit", _rx_i);
            }));
        }
        drop(tx);
        drop(rx);
        for th in th_s {
            let _ = th.await;
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round * tx_count);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<usize>(1))]
#[case(mpmc::bounded_async::<usize>(10))]
#[case(mpmc::bounded_async::<usize>(100))]
fn test_pressure_bounded_mixed_async_blocking_conversion(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MAsyncRx<usize>),
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let counter = Arc::new(AtomicUsize::new(0));
        let round: usize = 10000;
        let mut th_s = Vec::new();
        let mut co_s = Vec::new();
        let _tx: MTx<usize> = tx.clone().into();
        th_s.push(thread::spawn(move || {
            for i in 0..round {
                match _tx.send(i) {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            debug!("tx blocking exit");
        }));
        co_s.push(async_spawn!(async move {
            for i in 0..round {
                match tx.send(i).await {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            debug!("tx async exit");
        }));
        let _rx: MRx<usize> = rx.clone().into();
        let _counter = counter.clone();
        th_s.push(thread::spawn(move || {
            'A: loop {
                match _rx.recv() {
                    Ok(_i) => {
                        _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                        debug!("recv blocking {}", _i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx blocking exit");
        }));

        let _counter = counter.clone();
        co_s.push(async_spawn!(async move {
            'A: loop {
                match rx.recv().await {
                    Ok(_i) => {
                        _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                        debug!("recv async {}", _i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx async exit");
        }));
        for th in co_s {
            let _ = th.await;
        }
        for th in th_s {
            let _ = th.join();
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round * 2);
    });
}

#[test]
fn test_conversion() {
    use crate::stream::AsyncStream;
    let (mtx, mrx) = mpmc::bounded_async(1);
    let _tx: AsyncTx<usize> = mtx.into();
    let _rx: AsyncRx<usize> = mrx.into();
    let (_mtx, rx) = mpsc::bounded_async(1);
    let _stream: AsyncStream<usize> = rx.into(); // AsyncRx -> AsyncStream
    let (_mtx, mrx) = mpmc::bounded_async(1);
    let _stream: AsyncStream<usize> = mrx.into(); // AsyncRx -> AsyncStream
}

struct SpuriousTx {
    sink: AsyncSink<usize>,
    normal: bool,
    step: usize,
}

impl Future for SpuriousTx {
    type Output = Result<usize, usize>;

    fn poll(self: Pin<&mut Self>, ctx: &mut std::task::Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        if !_self.normal && _self.step > 0 {
            return Poll::Ready(Err(_self.step));
        }
        match _self.sink.poll_send(ctx, _self.step) {
            Ok(_) => {
                let res = _self.step;
                _self.step += 1;
                return Poll::Ready(Ok(res));
            }
            Err(TrySendError::Disconnected(_)) => {
                return Poll::Ready(Err(_self.step));
            }
            Err(TrySendError::Full(_)) => {
                _self.step += 1;
                return Poll::Pending;
            }
        }
    }
}

struct SpuriousRx {
    stream: AsyncStream<usize>,
    normal: bool,
    step: usize,
}

impl Future for SpuriousRx {
    type Output = Result<usize, usize>;

    fn poll(self: Pin<&mut Self>, ctx: &mut std::task::Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        if !_self.normal && _self.step > 0 {
            return Poll::Ready(Err(_self.step));
        }
        match _self.stream.poll_item(ctx) {
            Poll::Ready(Some(item)) => {
                _self.step += 1;
                return Poll::Ready(Ok(item));
            }
            Poll::Ready(None) => {
                return Poll::Ready(Err(_self.step));
            }
            Poll::Pending => {
                _self.step += 1;
                return Poll::Pending;
            }
        }
    }
}

#[logfn]
#[rstest]
fn test_spurious_sink(setup_log: ()) {
    #[cfg(feature = "tokio")]
    {
        let (tx, rx) = mpmc::bounded_async::<usize>(1);

        async fn spawn_tx(tx: MAsyncTx<usize>, normal: bool) {
            let sink = tx.into_sink();
            let _tx = SpuriousTx { sink, normal, step: 0 };
            if normal {
                assert_eq!(_tx.await.expect("send ok"), 1);
            } else {
                if let Ok(Err(step)) = tokio::time::timeout(Duration::from_secs(5), _tx).await {
                    assert_eq!(step, 1);
                } else {
                    unreachable!();
                }
            }
        }
        runtime_block_on!(async move {
            tx.send(0).await.expect("send");
            let _tx = tx.clone();
            let mut th_s = Vec::new();
            println!("spawn spurious");
            // Make sure its the first
            th_s.push(tokio::spawn(async move { spawn_tx(_tx, false).await }));
            sleep(Duration::from_secs(1)).await;
            let _tx = tx.clone();
            println!("spawn normal");
            th_s.push(tokio::spawn(async move { spawn_tx(_tx, true).await }));
            sleep(Duration::from_secs(1)).await;
            println!("recv 1 to make the 2 senders waked");
            assert_eq!(rx.recv().await.expect("recv"), 0);
            for th in th_s {
                let _ = th.await.expect("join ok");
            }
        });
    }
}

#[logfn]
#[rstest]
fn test_spurious_stream(setup_log: ()) {
    #[cfg(feature = "tokio")]
    {
        let (tx, rx) = mpmc::bounded_async::<usize>(1);

        async fn spawn_rx(rx: MAsyncRx<usize>, normal: bool) {
            let stream = rx.into_stream();
            let _rx = SpuriousRx { stream, normal, step: 0 };
            if normal {
                assert_eq!(_rx.await.expect("recv ok"), 1);
            } else {
                if let Ok(Err(step)) = tokio::time::timeout(Duration::from_secs(10), _rx).await {
                    assert_eq!(step, 1);
                } else {
                    unreachable!();
                }
            }
        }
        runtime_block_on!(async move {
            let _rx = rx.clone();
            let mut th_s = Vec::new();
            println!("spawn spurious");
            // Make sure its the first
            th_s.push(tokio::spawn(async move { spawn_rx(_rx, false).await }));
            sleep(Duration::from_millis(500)).await;
            let _rx = rx.clone();
            println!("spawn normal");
            th_s.push(tokio::spawn(async move { spawn_rx(_rx, true).await }));
            sleep(Duration::from_secs(1)).await;
            println!("send");
            tx.send(1).await.expect("send");
            sleep(Duration::from_secs(2)).await;
            for th in th_s {
                let _ = th.await.expect("join ok");
            }
        });
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<i32>(1))]
#[case(spsc::bounded_async::<i32>(2))]
#[case(mpsc::bounded_async::<i32>(1))]
#[case(mpsc::bounded_async::<i32>(2))]
#[case(mpmc::bounded_async::<i32>(1))]
#[case(mpmc::bounded_async::<i32>(2))]
fn test_basic_into_stream_1_1<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let total_message = 100;
        let (tx, rx) = channel;
        async_spawn!(async move {
            println!("sender thread send {} message start", total_message);
            for i in 0i32..total_message {
                let _ = tx.send(i).await;
                // println!("send {}", i);
            }
            println!("sender thread send {} message end", total_message);
        });
        let mut s: AsyncStream<i32> = rx.into();

        for _i in 0..total_message {
            assert_eq!(s.next().await, Some(_i));
        }
        assert_eq!(s.next().await, None);
        assert!(s.is_terminated())
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<i32>(1), 2)]
#[case(mpmc::bounded_async::<i32>(2), 4)]
#[case(mpmc::bounded_async::<i32>(2), 10)]
#[case(mpmc::bounded_async::<i32>(10), 3)]
#[case(mpmc::bounded_async::<i32>(10), 30)]
#[case(mpmc::bounded_async::<i32>(100), 2)]
#[case(mpmc::bounded_async::<i32>(100), 4)]
#[case(mpmc::bounded_async::<i32>(100), 50)]
fn test_pressure_stream_multi(
    setup_log: (), #[case] channel: (MAsyncTx<i32>, MAsyncRx<i32>), #[case] rx_count: usize,
) {
    runtime_block_on!(async move {
        let total_message = 100000;
        let (tx, rx) = channel;
        let mut th_s = Vec::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for rx_i in 0..rx_count {
            let _rx = rx.clone();
            let _counter = counter.clone();
            th_s.push(async_spawn!(async move {
                let mut stream = _rx.into_stream();
                while let Some(_item) = stream.next().await {
                    _counter.fetch_add(1, Ordering::SeqCst);
                }
                debug!("rx {} exit", rx_i);
            }));
        }
        drop(rx);
        for i in 0..total_message {
            tx.send(i).await.expect("send");
        }
        drop(tx);
        for th in th_s {
            let _ = th.await;
        }
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<i32>(1), 2)]
#[case(mpmc::bounded_async::<i32>(2), 4)]
#[case(mpmc::bounded_async::<i32>(2), 10)]
#[case(mpmc::bounded_async::<i32>(10), 3)]
#[case(mpmc::bounded_async::<i32>(10), 30)]
#[case(mpmc::bounded_async::<i32>(100), 2)]
#[case(mpmc::bounded_async::<i32>(100), 4)]
#[case(mpmc::bounded_async::<i32>(100), 50)]
fn test_pressure_stream_multi_idle(
    setup_log: (), #[case] channel: (MAsyncTx<i32>, MAsyncRx<i32>), #[case] rx_count: usize,
) {
    runtime_block_on!(async move {
        let total_message = 5000;
        let (tx, rx) = channel;
        let mut th_s = Vec::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for rx_i in 0..rx_count {
            let _rx = rx.clone();
            let _counter = counter.clone();
            th_s.push(async_spawn!(async move {
                let mut stream = _rx.into_stream();
                while let Some(_item) = stream.next().await {
                    _counter.fetch_add(1, Ordering::SeqCst);
                }
                debug!("rx {} exit", rx_i);
            }));
        }
        drop(rx);
        for i in 0..total_message {
            tx.send(i).await.expect("send");
            sleep(Duration::from_millis(10)).await;
        }
        drop(tx);
        for th in th_s {
            let _ = th.await;
        }
    });
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_async::<SmallMsg>(1))]
#[case(spsc::bounded_async::<SmallMsg>(10))]
#[case(mpsc::bounded_async::<SmallMsg>(1))]
#[case(mpsc::bounded_async::<SmallMsg>(10))]
#[case(mpmc::bounded_async::<SmallMsg>(1))]
#[case(mpmc::bounded_async::<SmallMsg>(10))]
fn test_drop_small_msg<T: AsyncTxTrait<SmallMsg>, R: AsyncRxTrait<SmallMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<SmallMsg>());
    _test_drop_msg(channel);
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_async::<LargeMsg>(1))]
#[case(spsc::bounded_async::<LargeMsg>(10))]
#[case(mpsc::bounded_async::<LargeMsg>(1))]
#[case(mpsc::bounded_async::<LargeMsg>(10))]
#[case(mpmc::bounded_async::<LargeMsg>(1))]
#[case(mpmc::bounded_async::<LargeMsg>(10))]
fn test_drop_large_msg<T: AsyncTxTrait<LargeMsg>, R: AsyncRxTrait<LargeMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<LargeMsg>());
    _test_drop_msg(channel);
}

fn _test_drop_msg<M: TestDropMsg, T: AsyncTxTrait<M>, R: AsyncRxTrait<M>>(channel: (T, R)) {
    let (tx, rx) = channel;
    reset_drop_counter();
    runtime_block_on!(async move {
        let cap = tx.capacity().unwrap();
        let mut ids = cap;
        for i in 0..ids {
            let msg = M::new(i);
            assert!(tx.try_send(msg).is_ok());
        }
        assert_eq!(get_drop_counter(), 0);
        let msg = M::new(ids);
        if let Err(TrySendError::Full(_msg)) = tx.try_send(msg) {
            assert_eq!(_msg.get_value(), ids);
            assert_eq!(get_drop_counter(), 0);
            drop(_msg);
            assert_eq!(get_drop_counter(), 1);
        } else {
            unreachable!();
        }
        let th = async_spawn!(async move {
            let _msg = rx.recv().await.expect("recv");
            assert_eq!(_msg.get_value(), 0);
            drop(_msg);
            sleep(Duration::from_millis(20)).await;
        });
        let msg = M::new(ids);
        tx.send(msg).await.expect("send");
        ids += 1;
        let _ = th.await;
        sleep(Duration::from_millis(20)).await;
        assert_eq!(get_drop_counter(), 2);
        let msg = M::new(ids);
        if let Err(TrySendError::Disconnected(_msg)) = tx.try_send(msg) {
            assert_eq!(_msg.get_value(), ids);
        } else {
            unreachable!();
        }
        ids += 1;
        let msg = M::new(ids);
        if let Err(SendError(_msg)) = tx.send(msg).await {
            assert_eq!(_msg.get_value(), ids);
        } else {
            unreachable!();
        }
        assert_eq!(get_drop_counter(), 4);
        ids += 1;
        drop(tx);
        sleep(Duration::from_millis(20)).await;
        // every thing dropped inside the channel
        assert_eq!(get_drop_counter(), ids + 1); // ids begins at 0
        assert_eq!(get_drop_counter(), 4 + cap);
    });
}
