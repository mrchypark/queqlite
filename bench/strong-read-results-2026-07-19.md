# Strong-read local diagnostic — 2026-07-19

This is local diagnostic evidence for Rhiza's read-only quorum fence and its
local-read control. Disk space had been cleared before the run, but the host was
not proven idle. The ignored raw artifact is
`target/rhiza-bench/strong-read-clean/20260719T040900/`; `summary.json` contains
the medians and the 54 per-run JSON files preserve the raw measurements.

These numbers are not publishable release or multi-host evidence. Later host
inspection found an orphan Virtualization VM and substantial background
`syspolicyd`/`trustd` activity. That later load does not invalidate each JSON
sample, but it prevents claiming a controlled idle-host environment.

## Fixed conditions

- revision: `5fc083a1843bfdf254ad3cb7c83a9a0d2e5be6f0` plus uncommitted optimization work
- host: Apple M3, 8 logical CPUs, macOS 26.3, arm64
- layer: direct `NodeRuntime` API, one blocking thread per benchmark worker
- profiles/workloads: SQL `get`, KV `get`, Graph `document-get`
- topology: one process, three file-backed `RecorderRpc` voters, 2/3 read quorum
- keyspace/value: 256 keys, 64-byte configured value
- warmup/measurement: c1 2,000/20,000; c4 4,000/40,000; c16 8,000/80,000
- every cell: three independent runs with a fresh data directory
- excluded: HTTP, serialization, node-to-node sockets, remote checkpoint, multi-host behavior

## Three-run throughput medians

Values are successful logical reads per second, rounded to the nearest whole
operation. Every one of the 18 cells has three runs, **0 errors**, and **0 QLog
entries**. The zero QLog delta confirms that the read-only fence common path did
not append a Noop.

| Profile/workload | c | Local ops/s | ReadBarrier ops/s |
| --- | ---: | ---: | ---: |
| SQL `get` | 1 | 63,814 | 7,801 |
| SQL `get` | 4 | 58,318 | 29,440 |
| SQL `get` | 16 | 57,889 | 36,050 |
| KV `get` | 1 | 360,942 | 10,405 |
| KV `get` | 4 | 727,622 | 45,250 |
| KV `get` | 16 | 647,819 | 161,051 |
| Graph `document-get` | 1 | 1,652 | 1,163 |
| Graph `document-get` | 4 | 4,191 | 1,663 |
| Graph `document-get` | 16 | 3,981 | 2,144 |

## Interpretation

SQL and KV show the intended fence shape: the c1 quorum round costs throughput,
then the generation-coalesced read-only fence amortizes that cost strongly at c4
and c16 without writing QLog entries. Local reads remain the lower-latency choice
when stale reads are acceptable; `ReadBarrier` pays a quorum fence to bind the
subsequent local snapshot to an agreed context.

Graph is different. Its local `document-get` ceiling is already only 1.7k–4.2k
ops/s, and the read-barrier result stays within 1.2k–2.1k ops/s. SQL and KV fence
throughput is far higher under the same voter topology, so the dominant Graph
cost is the LadybugDB/document-projection query backend rather than the
read-only quorum protocol. Further Graph gains require backend/query-path work;
fence-only tuning cannot lift throughput beyond the local backend ceiling.

## Reproduction

Use the same operation counts for each profile, concurrency, and consistency:

```sh
bench/target/release/rhiza-profile \
  --profile PROFILE --workload WORKLOAD --layer runtime \
  --operations OPERATIONS --warmup WARMUP --concurrency CONCURRENCY \
  --batch-size 1 --value-bytes 64 --consistency local

bench/target/release/rhiza-profile \
  --profile PROFILE --workload WORKLOAD --layer runtime \
  --operations OPERATIONS --warmup WARMUP --concurrency CONCURRENCY \
  --batch-size 1 --value-bytes 64 --consistency read_barrier
```

For a publishable comparison, rerun on a verified idle host and capture process,
disk, thermal, source, and binary manifests before and after the complete matrix.

## SQL/KV post-fix regression

After separating ReadBarrier from long control RPCs and bounding no-quorum
record attempts, SQL and KV were rerun three times per local/ReadBarrier and
c1/c4/c16 cell. All 36 runs completed with zero errors and zero qlog entries.

| Profile | Consistency | c1 ops/s | c4 ops/s | c16 ops/s |
| --- | --- | ---: | ---: | ---: |
| SQL | local | 56,740.90 | 48,612.80 | 50,279.06 |
| SQL | ReadBarrier | 7,348.38 | 21,693.37 | 25,946.84 |
| KV | local | 198,665.38 | 268,142.19 | 353,641.18 |
| KV | ReadBarrier | 9,601.89 | 36,844.70 | 126,421.02 |

The host was more heavily loaded than the original run (`syspolicyd` around
140% CPU plus an active Dory VM), so absolute deltas are not attributed to the
code change. The regression proves the intended structure: local reads remain
quorum-free, ReadBarrier remains read-only, and the added failure isolation did
not create qlog traffic or correctness failures.

## ReadBarrier versus Hiqlite `query_consistent`

Hiqlite routes `query_consistent` to the Raft leader, calls OpenRaft
`ensure_linearizable()`, and then executes the query against that node's local
SQLite read pool. Its own client documentation calls out the leader-only route,
network round trips, and owned-row allocation. Rhiza ReadBarrier instead asks
the current fixed voter set for a context-bound 2/3 read fence, then validates
the local snapshot against the returned anchor. It does not append a no-op or
require a distinguished leader.

The tradeoff is explicit:

- `local` is fastest and remains available on the lone F2 survivor, but may be
  stale and is not a linearizable read.
- Rhiza ReadBarrier avoids leader routing and leader read concentration,
  coalesces concurrent readers, and generates zero qlog entries. It pays one
  quorum fence per generation and must fail closed without 2/3 voters.
- Hiqlite `query_consistent` uses the mature Raft linearizable-read path but is
  leader-bound, so redirects/leader availability and leader load are part of
  the latency path.

The implementations are semantically comparable as strong reads, but they are
not identical protocols. A fair throughput comparison must use the same
network topology, query shape, result ownership/serialization, and concurrency;
the direct-runtime Rhiza numbers above exclude HTTP and remote client costs.
