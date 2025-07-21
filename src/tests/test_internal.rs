use crate::backoff::*;
use crate::channel::*;
use rstest::*;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

#[rstest]
fn test_backoff_determine() {
    fn _test(tx_count: usize, rx_count: usize) {
        let global = AtomicU32::new(BackoffConfig::default().to_u32());
        let tx = AtomicUsize::new(tx_count);
        let rx = AtomicUsize::new(rx_count);
        let config = determine_backoff(&global, &tx, &rx);
        let _config = BackoffConfig::from_u32(global.load(Ordering::Relaxed));
        assert_eq!(config.spin_limit, _config.spin_limit);
        assert_eq!(config.limit, _config.limit);
        println!("tx {} rx {} config {:?}", tx_count, rx_count, config);
    }

    _test(1, 1);
    _test(2, 1);
    _test(4, 1);
    _test(8, 1);
    _test(1, 4);
    _test(1, 8);
    _test(16, 1);
    _test(2, 2);
    _test(4, 4);
    _test(8, 8);
}
