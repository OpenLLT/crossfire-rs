use super::common::*;
use crate::*;
use captains_log::{logfn, *};
use rstest::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

#[fixture]
fn setup_log() {
    let _ = recipe::env_logger("LOG_FILE", "LOG_LEVEL").build().expect("log setup");
    //    let _ = recipe::ring_file("/tmp/ring.log", 512*1024*1024, Level::Debug, signal_consts::SIGHUP).build().expect("log_setup");
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(1))]
fn test_basic_bounded_empty_full_drop_rx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    // Just don't want to run duplicately in the workflow
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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
        assert_eq!(tx.try_send(2).unwrap_err(), TrySendError::Disconnected(2));
        assert_eq!(tx.send(2).unwrap_err(), SendError(2));
        let start = Instant::now();
        assert_eq!(
            tx.send_timeout(3, Duration::from_secs(1)).unwrap_err(),
            SendTimeoutError::Disconnected(3)
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(1))]
fn test_basic_bounded_empty_full_drop_tx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert_eq!(rx.recv().unwrap_err(), RecvError);
        let start = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap_err(),
            RecvTimeoutError::Disconnected
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_blocking())]
#[case(mpsc::unbounded_blocking())]
#[case(mpmc::unbounded_blocking())]
fn test_basic_unbounded_empty_drop_rx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        assert!(tx.is_empty());
        assert!(rx.is_empty());
        assert_eq!(tx.capacity(), None);
        assert_eq!(rx.capacity(), None);
        tx.try_send(1).expect("Ok");
        assert!(!tx.is_empty());
        assert_eq!(tx.is_disconnected(), false);
        assert_eq!(rx.is_disconnected(), false);
        drop(rx);
        assert_eq!(tx.is_disconnected(), true);
        assert_eq!(tx.as_ref().get_rx_count(), 0);
        assert_eq!(tx.as_ref().get_tx_count(), 1);
        assert_eq!(tx.try_send(2).unwrap_err(), TrySendError::Disconnected(2));
        assert_eq!(tx.send(2).unwrap_err(), SendError(2));
        let start = Instant::now();
        assert_eq!(
            tx.send_timeout(3, Duration::from_secs(1)).unwrap_err(),
            SendTimeoutError::Disconnected(3)
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_blocking())]
#[case(mpsc::unbounded_blocking())]
#[case(mpmc::unbounded_blocking())]
fn test_basic_unbounded_empty_drop_tx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        assert!(tx.is_empty());
        assert!(rx.is_empty());
        tx.try_send(1).expect("Ok");
        assert!(!tx.is_empty());
        assert_eq!(tx.is_disconnected(), false);
        assert_eq!(rx.is_disconnected(), false);
        drop(tx);
        assert_eq!(rx.is_disconnected(), true);
        assert_eq!(rx.as_ref().get_tx_count(), 0);
        assert_eq!(rx.as_ref().get_rx_count(), 1);
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert_eq!(rx.recv().unwrap_err(), RecvError);
        let start = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap_err(),
            RecvTimeoutError::Disconnected
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<i32>(10))]
#[case(mpsc::bounded_blocking::<i32>(10))]
#[case(mpmc::bounded_blocking::<i32>(10))]
fn test_basic_bounded_1_thread<T: BlockingTxTrait<i32>, R: BlockingRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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

        let th = thread::spawn(move || {
            for i in 0i32..12 {
                match rx.recv() {
                    Ok(j) => {
                        debug!("recv {}", i);
                        assert_eq!(i, j);
                    }
                    Err(e) => {
                        panic!("error {}", e);
                    }
                }
            }
            let res = rx.recv();
            assert!(res.is_err());
            debug!("rx close");
        });
        assert!(tx.send(10).is_ok());
        sleep(Duration::from_secs(1));
        assert!(tx.send(11).is_ok());
        drop(tx);
        let _ = th.join();
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_blocking::<i32>())]
#[case(mpsc::unbounded_blocking::<i32>())]
#[case(mpmc::unbounded_blocking::<i32>())]
fn test_basic_unbounded_1_thread<T: BlockingTxTrait<i32>, R: BlockingRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        let rx_res = rx.try_recv();
        assert!(rx_res.is_err());
        assert!(rx_res.unwrap_err().is_empty());
        for i in 0i32..10 {
            let tx_res = tx.try_send(i);
            assert!(tx_res.is_ok());
        }

        let th = thread::spawn(move || {
            for i in 0i32..12 {
                match rx.recv() {
                    Ok(j) => {
                        debug!("recv {}", i);
                        assert_eq!(i, j);
                    }
                    Err(e) => {
                        panic!("error {}", e);
                    }
                }
            }
            let res = rx.recv();
            assert!(res.is_err());
            debug!("rx close");
        });
        assert!(tx.send(10).is_ok());
        sleep(Duration::from_secs(1));
        assert!(tx.send(11).is_ok());
        drop(tx);
        let _ = th.join();
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<i32>(10))]
#[case(mpsc::bounded_blocking::<i32>(10))]
#[case(mpmc::bounded_blocking::<i32>(10))]
#[case(spsc::unbounded_blocking::<i32>())]
#[case(mpsc::unbounded_blocking::<i32>())]
#[case(mpmc::unbounded_blocking::<i32>())]
fn test_basic_recv_after_sender_close<T: BlockingTxTrait<i32>, R: BlockingRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        let total_msg_count = 5;
        for i in 0..total_msg_count {
            let _ = tx.try_send(i).expect("send ok");
        }
        drop(tx);

        // NOTE: 5 < 10
        let mut recv_msg_count = 0;
        loop {
            match rx.recv() {
                Ok(_) => {
                    recv_msg_count += 1;
                }
                Err(_) => {
                    break;
                }
            }
        }
        assert_eq!(recv_msg_count, total_msg_count);
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<usize>(1))]
#[case(spsc::bounded_blocking::<usize>(10))]
#[case(spsc::bounded_blocking::<usize>(100))]
#[case(spsc::bounded_blocking::<usize>(300))]
#[case(mpsc::bounded_blocking::<usize>(1))]
#[case(mpsc::bounded_blocking::<usize>(10))]
#[case(mpsc::bounded_blocking::<usize>(100))]
#[case(mpsc::bounded_blocking::<usize>(300))]
#[case(mpmc::bounded_blocking::<usize>(1))]
#[case(mpmc::bounded_blocking::<usize>(10))]
#[case(mpmc::bounded_blocking::<usize>(100))]
#[case(mpmc::bounded_blocking::<usize>(300))]
fn test_pressure_bounded_blocking_1_1<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;

        let counter = Arc::new(AtomicUsize::new(0));
        let round: usize = 1000000;
        let _round = round;
        let th = thread::spawn(move || {
            for i in 0.._round {
                if let Err(e) = tx.send(i) {
                    panic!("{:?}", e);
                }
            }
            debug!("tx exit");
        });
        'A: loop {
            match rx.recv() {
                Ok(_i) => {
                    counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        let _ = th.join();
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round);
    }
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_blocking::<usize>(1), 3)]
#[case(mpsc::bounded_blocking::<usize>(1), 5)]
#[case(mpsc::bounded_blocking::<usize>(1), 10)]
#[case(mpsc::bounded_blocking::<usize>(1), 16)]
#[case(mpsc::bounded_blocking::<usize>(10), 4)]
#[case(mpsc::bounded_blocking::<usize>(10), 7)]
#[case(mpsc::bounded_blocking::<usize>(10), 12)]
#[case(mpsc::bounded_blocking::<usize>(100), 3)]
#[case(mpsc::bounded_blocking::<usize>(100), 9)]
#[case(mpsc::bounded_blocking::<usize>(100), 13)]
#[case(mpmc::bounded_blocking::<usize>(1), 2)]
#[case(mpmc::bounded_blocking::<usize>(1), 5)]
#[case(mpmc::bounded_blocking::<usize>(1), 15)]
#[case(mpmc::bounded_blocking::<usize>(10), 3)]
#[case(mpmc::bounded_blocking::<usize>(10), 7)]
#[case(mpmc::bounded_blocking::<usize>(10), 16)]
#[case(mpmc::bounded_blocking::<usize>(100), 2)]
#[case(mpmc::bounded_blocking::<usize>(100), 8)]
#[case(mpmc::bounded_blocking::<usize>(100), 16)]
fn test_pressure_bounded_blocking_multi_1<R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (MTx<usize>, R), #[case] tx_count: usize,
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;

        let counter = Arc::new(AtomicUsize::new(0));
        let round: usize = 1000000;
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            let _round = round;
            th_s.push(thread::spawn(move || {
                for i in 0.._round {
                    match _tx.send(i) {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                debug!("tx {} exit", _tx_i);
            }));
        }
        drop(tx);
        'A: loop {
            match rx.recv() {
                Ok(_i) => {
                    counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        for th in th_s {
            let _ = th.join();
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round * tx_count);
    }
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_blocking::<usize>(1), 2, 2)]
#[case(mpmc::bounded_blocking::<usize>(1), 16, 2)]
#[case(mpmc::bounded_blocking::<usize>(1), 2, 16)]
#[case(mpmc::bounded_blocking::<usize>(10), 2, 2)]
#[case(mpmc::bounded_blocking::<usize>(10), 13, 2)]
#[case(mpmc::bounded_blocking::<usize>(10), 3, 10)]
#[case(mpmc::bounded_blocking::<usize>(100), 3, 3)]
#[case(mpmc::bounded_blocking::<usize>(100), 8, 3)]
#[case(mpmc::bounded_blocking::<usize>(100), 3, 8)]
#[case(mpmc::bounded_blocking::<usize>(100), 5, 5)]
fn test_pressure_bounded_blocking_multi(
    setup_log: (), #[case] _channel: (MTx<usize>, MRx<usize>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        let counter = Arc::new(AtomicUsize::new(0));
        let round: usize = 1000000;
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            let _round = round;
            th_s.push(thread::spawn(move || {
                for i in 0.._round {
                    match _tx.send(i) {
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
            th_s.push(thread::spawn(move || {
                'A: loop {
                    match _rx.recv() {
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
            let _ = th.join();
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round * tx_count);
    }
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_blocking::<i32>(1))]
fn test_pressure_bounded_timeout_blocking(setup_log: (), #[case] _channel: (MTx<i32>, MRx<i32>)) {
    #[cfg(not(feature = "async_std"))]
    {
        use parking_lot::Mutex;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicI32;
        let (tx, rx) = _channel;

        assert_eq!(
            rx.recv_timeout(Duration::from_millis(1)).unwrap_err(),
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
            th_s.push(thread::spawn(move || {
                // randomize start up
                sleep(Duration::from_millis(thread_id & 3));
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
                        sleep(Duration::from_millis(3));
                    } else {
                        sleep(Duration::from_millis(1));
                    }
                    loop {
                        match _tx.send_timeout(i, Duration::from_millis(1)) {
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
            th_s.push(thread::spawn(move || {
                let mut step: usize = 0;
                loop {
                    step += 1;
                    let timeout = if step & 2 == 0 { 1 } else { 2 };
                    if step & 2 > 0 {
                        sleep(Duration::from_millis(1));
                    }
                    match _rx.recv_timeout(Duration::from_millis(timeout)) {
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
            let _ = th.join();
        }
        {
            let guard = recv_map.lock();
            assert!(guard.is_empty());
        }
        assert_eq!(ROUND, recv_counter.load(Ordering::Acquire));
        println!("send timeout count: {}", send_timeout_counter.load(Ordering::Acquire));
        println!("recv timeout count: {}", recv_timeout_counter.load(Ordering::Acquire));
    }
}

#[test]
fn test_conversion() {
    let (mtx, mrx) = mpmc::bounded_blocking::<usize>(1);
    let _tx: Tx<usize> = mtx.into();
    let _rx: Rx<usize> = mrx.into();
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<SmallMsg>(1))]
#[case(spsc::bounded_blocking::<SmallMsg>(10))]
#[case(mpsc::bounded_blocking::<SmallMsg>(1))]
#[case(mpsc::bounded_blocking::<SmallMsg>(10))]
#[case(mpmc::bounded_blocking::<SmallMsg>(1))]
#[case(mpmc::bounded_blocking::<SmallMsg>(10))]
fn test_drop_small_msg<T: BlockingTxTrait<SmallMsg>, R: BlockingRxTrait<SmallMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<SmallMsg>());
    _test_drop_msg(channel);
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<LargeMsg>(1))]
#[case(spsc::bounded_blocking::<LargeMsg>(10))]
#[case(mpsc::bounded_blocking::<LargeMsg>(1))]
#[case(mpsc::bounded_blocking::<LargeMsg>(10))]
#[case(mpmc::bounded_blocking::<LargeMsg>(1))]
#[case(mpmc::bounded_blocking::<LargeMsg>(10))]
fn test_drop_large_msg<T: BlockingTxTrait<LargeMsg>, R: BlockingRxTrait<LargeMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<LargeMsg>());
    _test_drop_msg(channel);
}

fn _test_drop_msg<M: TestDropMsg, T: BlockingTxTrait<M>, R: BlockingRxTrait<M>>(channel: (T, R)) {
    let (tx, rx) = channel;
    reset_drop_counter();
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
    let th = thread::spawn(move || {
        let _msg = rx.recv().expect("recv");
        assert_eq!(_msg.get_value(), 0);
        drop(_msg);
        sleep(Duration::from_millis(50));
    });
    let msg = M::new(ids);
    tx.send(msg).expect("send");
    ids += 1;
    let _ = th.join();
    sleep(Duration::from_millis(20));
    assert_eq!(get_drop_counter(), 2);
    let msg = M::new(ids);
    if let Err(TrySendError::Disconnected(_msg)) = tx.try_send(msg) {
        assert_eq!(_msg.get_value(), ids);
    } else {
        unreachable!();
    }
    ids += 1;
    let msg = M::new(ids);
    if let Err(SendError(_msg)) = tx.send(msg) {
        assert_eq!(_msg.get_value(), ids);
    } else {
        unreachable!();
    }
    assert_eq!(get_drop_counter(), 4);
    ids += 1;
    drop(tx);
    sleep(Duration::from_millis(50));
    // every thing dropped inside the channel
    assert_eq!(get_drop_counter(), ids + 1); // ids begins at 0
    assert_eq!(get_drop_counter(), 4 + cap);
}
