# -Zmiri-no-short-fd-operations is to prevent short write perform by miri, which breaks to atomic appending in log
MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-no-short-fd-operations -Zmiri-backtrace=full" cargo +nightly miri test $@ --features trace_log -- --no-capture --test-threads=1
