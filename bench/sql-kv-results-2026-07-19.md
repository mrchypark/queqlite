# SQL/KV 3-voter diagnostic benchmark — 2026-07-19

This report intentionally excludes Graph. It records the SQL and KV benchmark
matrix requested after the Graph benchmark work was stopped.

## Scope and provenance

- Benchmark: `rhiza-profile`, schema v3, direct `NodeRuntime` API
- Topology: one in-process QuePaxa node with three file-backed Recorder voters
- Durability: Recorder file `fsync`, local qlog, and materializer apply
- Commit: `5fc083a1843bfdf254ad3cb7c83a9a0d2e5be6f0` with a dirty worktree
- Binary SHA-256:
  `c5a0ee6c643540eb5135111974a11975432aa7f83fab5544f94cd57cd1ad7e8f`
- Read cells: 40,000 measured operations, 4,000 warmup operations, three runs
- Runtime write cells: 102,400 measured operations, 10,240 warmup operations
- SQL writes: public batch 256; five c1 runs and seven c4 runs
- KV writes: public maximum batch 64; five c1 runs and seven c4 runs
- Median is the order statistic. IQR uses Tukey hinges with the median excluded.

The original matrix's 77 raw JSON reports and process snapshots were independently
audited before another shell's explicit cleanup command removed both repository
`target/` directories. Those original values are recoverable from the audit and
terminal record, but their raw artifacts are no longer available for publication.
The host also had substantial unrelated load (`syspolicyd`, `trustd`, XProtect,
Xcode builds, Time Machine, and intermittent Virtualization work). Treat all
throughput values as diagnostic, not release evidence.

## Reads

Every read run completed 40,000/40,000 operations with zero errors. Both local
and ReadBarrier reads created zero qlog entries. ReadBarrier is a read-only 2/3
quorum fence; it does not append a replicated no-op.

| Profile | Consistency | c1 ops/s | c4 ops/s | c16 ops/s |
| --- | --- | ---: | ---: | ---: |
| SQL | local | 70,411.62 | 57,609.72 | 58,809.17 |
| SQL | ReadBarrier | 7,843.66 | 31,368.50 | 37,890.53 |
| KV | local | 490,210.84 | 591,469.53 | 495,774.30 |
| KV | ReadBarrier | 10,182.73 | 39,983.03 | 143,766.40 |

ReadBarrier/local throughput was 11.14% / 54.45% / 64.43% for SQL and
2.08% / 6.76% / 29.00% for KV at c1/c4/c16. The fixed quorum round trip is
dominant at c1 and amortizes under concurrency. Local KV was especially noisy:
its three-run range relative to the median was 19.96% at c1, 11.08% at c4, and
37.45% at c16. ReadBarrier was much more repeatable, but the host load still
prevents publication-quality conclusions.

## Durable runtime writes

All retained write runs completed 102,400/102,400 logical operations with zero
errors and zero failed batches.

| Profile | Concurrency | Median ops/s | IQR / median | Qlog entries | Logical ops/qlog | Verdict |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| SQL b256 | c1 | 7,205.26 | 2.22% | 400 | 256.00 | structural and stability pass |
| SQL b256 | c4 | 15,779.91 | 3.02% | median 109 | 939.45 | reject: all runs missed qlog 100–102 gate |
| KV b64 | c1 | 2,141.40 | 4.16% | 1,600 | 64.00 | structural and stability pass |
| KV b64 | c4 | 2,092.55 | 5.04% | 1,600 | 64.00 | reject: marginally missed 5% stability gate |

SQL c4 produced qlog counts 103, 106, 108, 109, 109, 109, and 111. Counts,
successes, profiler sample counts, and all 102,400 profiler members were correct,
with no dropped samples. The 500 microsecond group-drain window did not reliably
form the intended roughly 100 physical groups under the observed scheduler load.
The diagnostic median is nevertheless 2.74x the user-supplied Hiqlite c4 baseline
of 5,760 INSERT/s and closely matches the earlier controlled Rhiza median of
15,823.98 ops/s.

This first KV matrix predates the internal KV group-commit queue. Its c4
**2,092.55 ops/s**, 1,600-qlog result is retained below as the explicit
pre-change baseline.

## KV group-commit follow-up

The post-change raw artifact is
`target/rhiza-bench/kv-group-commit/20260719T122120/`. Its release binary is
recorded in `binary.sha256` as:

```text
a1f34866955b638371db4e0852f04d382425d22a0c5247aced5b828009c4db76  bench/target/release/rhiza-profile
```

The public typed cap remains 64. Direct runtime and embedded
`mutate_kv`/`mutate_kv_batch` calls now enter a FIFO queue bounded to 64 calls
and 32 MiB of pending canonical bytes. The queue waits up to 500µs and drains
at most 1,024 members per active group. Internal replication uses KV batch wire
command version 3 and the redb materializer fingerprint domain v2. The 512 KiB
qlog-command ceiling remains: a large flattened group is split into the largest
ordered fitting prefixes. HTTP writes keep their existing writer queue and use
direct KV batch execution without entering this queue a second time.

Each run submitted 102,400 logical writes as 1,600 public 64-member batch calls.
All runs completed 102,400 successes, 1,600 successful batch calls, zero failed
batches, and zero errors.

| Concurrency | Runs | Median ops/s | IQR / median | Qlog entries | Median logical ops/qlog |
| ---: | ---: | ---: | ---: | ---: | ---: |
| c1 | 5 | 2,008.81 | 11.09% | 1,600 | 64.00 |
| c4 | 7 | 10,738.69 | 7.24% | 401–402 | 254.73 |

Compared with the pre-change c4 median of 2,092.55 ops/s, the new diagnostic
median is **+413.19%**. It is **5.35x** the post-change c1 median. The qlog count
fell from 1,600 to 401–402 at c4, matching four concurrent 64-member calls per
physical group apart from scheduling boundaries. The structural and qlog gate
therefore passes.

The throughput stability gate fails. Both IQRs exceed 5%, and the paired system
snapshots show sustained Dory VM load plus intermittent `syspolicyd` and macOS
Storage extension activity. Preserve 10,738.69 as a diagnostic median, not
release evidence; rerun on an idle host before publishing throughput or the
413.19% delta. Graph was not run and is excluded from this artifact.

## Direct SQL QWAL ceiling

This layer excludes consensus and qlog append. It measures QWAL preparation,
envelope construction, and SQLite materializer apply, so it must not be compared
directly with Hiqlite.

| Batch | Median logical ops/s | IQR / median | Verdict |
| ---: | ---: | ---: | --- |
| 256 | 14,260.07 | 3.01% | structural and stability pass |
| 512 | 20,636.73 | 5.71% | reject: stability gate |
| 1,024 | 27,811.35 | 17.80% | reject: stability gate |

The rising ceiling confirms that larger physical SQL groups amortize QWAL and
SQLite durability work. The high b512/b1024 variance tracks the unrelated host
load rather than a correctness failure: every QWAL run completed all operations,
all 100 batch calls, and reported zero errors.

## Conclusion

The correctness contracts passed: no errors, no stale-read qlog writes, durable
three-voter writes, exact batch accounting, and complete SQL phase profiling.
The performance conclusion is narrower. SQL c4 remains around 15.8k logical
ops/s and exceeds the supplied Hiqlite number. KV group commit now passes its
structural gate: c4 reduced 102,400 writes from 1,600 qlog entries to 401–402 and
raised the diagnostic median from 2,092.55 to 10,738.69 ops/s. The Dory VM and
macOS background load make that throughput unstable, so a clean idle-host rerun
is still required before promoting it or the +413.19% delta to release evidence.

## Capped-debounce and KV 256 follow-up

The fixed collection deadline above missed calls that arrived just after the
collector started under scheduler pressure. SQL and KV now collect until one
500µs quiet period has elapsed since the latest arrival, capped at 2ms with the
default window. KV receipt preflight and post-commit lookup each use one redb
read transaction per physical group, and the public KV batch cap is now 256.
These are clean-install breaking changes; the replicated KV wire and materializer
fingerprint remain version 3/domain v2.

On the same loaded Apple M3 host, SQL b256 c4 produced 101–104 qlog entries over
102,400 writes. Five runs had median **12,215.74 logical ops/s** and IQR/median
**4.1%**. The paired pre-change profiled run produced 8,865.98 ops/s and 136 qlog
entries; the first post-change profiled run produced 12,496.18 ops/s and 101
entries. Treat the delta as paired diagnostic evidence because unrelated host
load remained high.

KV b64 c4 improved to a five-run median of **13,320.73 logical ops/s**, with
401–403 qlog entries and IQR/median **3.6%**. KV b256 c4 produced a five-run
median of **34,106.47 logical ops/s**, 101–102 qlog entries, and IQR/median
**2.75%**. This exceeds the supplied Hiqlite 5,760 INSERT/s number by 5.92x,
but it measures 256-member logical batches and is not a single-INSERT comparison.

## Strict single-write comparison

With batch size 1 and c4, Rhiza completed **76.60 SQL writes/s** and **135.44 KV
writes/s** in a noisy diagnostic run, grouping about four writes per durable
slot. A SQL phase profile measured p50 1.86ms QWAL prepare, 6.40ms Recorder
quorum, 3.90ms local qlog sync, and 9.92ms materializer apply. The QuePaxa fast
path already decided in its first Recorder round; there is no removable leader
round trip in this result.

For comparison, upstream Hiqlite 0.14.0 at current local `main`, three local
networked nodes, c4, 10,000 single INSERTs, and `HQL_LOG_SYNC=immediate` reported
**9,319 INSERT/s**. This is still not an equal power-loss boundary on macOS:
Rhiza's Rust file sync calls map to `F_FULLFSYNC`, while Hiqlite's immediate WAL
path uses synchronous mmap flush. Hiqlite's documented `interval_*` and
`immediate_async` modes acknowledge without waiting for the equivalent flush and
must not be compared with Rhiza strict ACK. See the upstream
[LogSync tuning documentation](https://sebadob.github.io/rauthy/config/tuning.html).

The single-call saturation curve confirms that available queue depth, rather
than a leader handoff, controls amortization. SQL/KV batch-1 throughput was
585.59/982.33 ops/s at c16 (14.99 logical ops/qlog) and
2,209.78/3,799.55 ops/s at c64 (62.5 logical ops/qlog). Matching the batched
throughput at c4 would require weakening ACK durability or inventing requests
that are not outstanding; neither is a valid transparent optimization.

## Final read/write regression after no-quorum fixes

The final direct-runtime regression used the rebuilt release benchmark after
the dedicated read-fence worker lane and quorum failure handling changes. The
host was still unsuitable for publication: `syspolicyd` consumed roughly
140% CPU and the Dory VM remained active. Every retained run nevertheless had
zero errors; every read had zero qlog entries.

Three-run read medians:

| Profile | Consistency | c1 ops/s | c4 ops/s | c16 ops/s |
| --- | --- | ---: | ---: | ---: |
| SQL | local | 56,740.90 | 48,612.80 | 50,279.06 |
| SQL | ReadBarrier | 7,348.38 | 21,693.37 | 25,946.84 |
| KV | local | 198,665.38 | 268,142.19 | 353,641.18 |
| KV | ReadBarrier | 9,601.89 | 36,844.70 | 126,421.02 |

Five-run b256/c4 write medians were **15,306.69 SQL ops/s** and
**35,286.07 KV ops/s**. All runs completed 102,400/102,400 writes and 400/400
public batch calls with zero failures. SQL used 101–102 qlog entries; KV used
100–101, or approximately 1,004–1,024 logical operations per durable entry.
The loaded-host IQR/median was 5.84% for SQL and 19.55% for KV, so the values
remain diagnostic. Structurally, both group commit paths filled their 1,024
member physical cap and preserved exact accounting.
