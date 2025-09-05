#MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-backtrace=full -Zmiri-deterministic-concurrency -Zmiri-tree-borrows -Zmiri-strict-provenance " cargo +nightly miri test -- --no-capture --test-threads=1
MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-no-short-fd-operations -Zmiri-backtrace=full" cargo +nightly miri test $@ --features deadlock_debug -- --no-capture --test-threads=1
