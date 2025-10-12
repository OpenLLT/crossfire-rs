use super::common::*;
use crate::{sink::*, stream::*, *};
use captains_log::{logfn, *};
use futures::stream::{FusedStream, StreamExt};
use rstest::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::*;
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<usize>(1))]
fn test_pressure_bounded_mixed_async_blocking_conversion(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MAsyncRx<usize>),
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let mut recv_counter = 0;
        let mut th_tx = Vec::new();
        let mut th_rx = Vec::new();
        let mut co_tx = Vec::new();
        let mut co_rx = Vec::new();
        let _tx: MTx<usize> = tx.clone().into();
        th_tx.push(thread::spawn(move || {
            for i in 0..ROUND {
                match _tx.send(i) {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            debug!("tx blocking exit");
        }));
        co_tx.push(async_spawn!(async move {
            for i in 0..ROUND {
                match tx.send(i).await {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            debug!("tx{:?} async exit", tokio_task_id!());
        }));
        let _rx: MRx<usize> = rx.clone().into();
        th_rx.push(thread::spawn(move || {
            let mut count: usize = 0;
            'A: loop {
                match _rx.recv() {
                    Ok(_i) => {
                        count += 1;
                        trace!("recv blocking {}", _i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx blocking exit");
            count
        }));

        co_rx.push(async_spawn!(async move {
            let mut count: usize = 0;
            'A: loop {
                match rx.recv().await {
                    Ok(_i) => {
                        count += 1;
                        trace!("recv async {}", _i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx{:?} async exit", tokio_task_id!());
            count
        }));
        for th in co_tx {
            let _ = async_join_result!(th);
        }
        for th in th_tx {
            let _ = th.join().unwrap();
        }
        for th in co_rx {
            recv_counter += async_join_result!(th);
        }
        for th in th_rx {
            recv_counter += th.join().unwrap();
        }
        assert_eq!(recv_counter, ROUND * 2);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async::<usize>(1))]
#[case(spsc::bounded_async::<usize>(2))]
#[case(mpsc::bounded_async::<usize>(1))]
#[case(mpsc::bounded_async::<usize>(2))]
#[case(mpmc::bounded_async::<usize>(1))]
#[case(mpmc::bounded_async::<usize>(2))]
fn test_basic_into_stream_1_1<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let total_message = 100;
        let (tx, rx) = channel;
        let th = async_spawn!(async move {
            println!("sender thread send {} message start", total_message);
            for i in 0usize..total_message {
                let _ = tx.send(i).await;
                // println!("send {}", i);
            }
            println!("sender thread send {} message end", total_message);
        });
        let mut s: AsyncStream<usize> = rx.into();

        for _i in 0..total_message {
            assert_eq!(s.next().await, Some(_i));
        }
        assert_eq!(s.next().await, None);
        assert!(s.is_terminated());
        async_join_result!(th);
    });
}
