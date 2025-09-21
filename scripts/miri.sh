#!/bin/bash
# -Zmiri-no-short-fd-operations is to prevent short write perform by miri, which breaks to atomic appending in log
# -Zmiri-permissive-provenance is to disable warning about parking_lot

# By default log is off, if you need to enable, pass the option with the script: --features trace_log

MIRIFLAGS="$MIRIFLAGS -Zmiri-disable-isolation -Zmiri-no-short-fd-operations -Zmiri-backtrace=full -Zmiri-permissive-provenance"
export MIRIFLAGS
RUSTFLAGS="--cfg tokio_unstable" RUST_BACKTRACE=1 cargo +nightly miri test $@ -- --no-capture --test-threads=1
