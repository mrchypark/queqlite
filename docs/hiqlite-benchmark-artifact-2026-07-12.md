# Hiqlite Remote Benchmark Artifact (2026-07-12)

Source commit: `c8316c53799c509990475ea8e2aa2ef8679e070e`

Common configuration:

```text
Apple M3, 8 cores, 24 GiB RAM, macOS 26.3, Rust 1.95.0
3 native server processes, 1 remote client, loopback, TLS disabled
release + LTO + jemalloc
HQL_WAL_SIZE=8388608
HQL_LOGS_UNTIL_SNAPSHOT=10000
HQL_CACHE_STORAGE_DISK=false
```

Command:

```sh
bench-bin remote -c 4 -r 100000 \
  -n 127.0.0.1:18101 \
  -n 127.0.0.1:18102 \
  -n 127.0.0.1:18103 \
  -s '<redacted>'
```

## interval_200

```text
>>> 100000 single INSERTs with concurrency 4 took:
10209 ms -> 9795 inserts / s
```

All three SQLite replicas contained exactly 100,000 `_bench` rows. The three
Raft log indices were `100014`.

Server process samples, combined:

```text
average CPU: 368.2% (100% = one logical core)
peak CPU: 394.6%
average RSS: 121.3 MiB
sum of per-node peak RSS: 366.0 MiB
```

## interval_1000

The same command was run with `HQL_LOG_SYNC=interval_1000` and fresh data
directories.

```text
>>> 100000 single INSERTs with concurrency 4 took:
12772 ms -> 7829 inserts / s
```

All three SQLite replicas contained exactly 100,000 `_bench` rows. CPU and RSS
were not sampled for this run.

The official runner does not emit per-request percentiles. It was terminated
after the single-INSERT phase so later transaction and cache phases did not
alter the SQL result.
