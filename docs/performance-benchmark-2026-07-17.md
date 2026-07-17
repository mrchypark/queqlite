# rhiza performance benchmark — 2026-07-17

This report records the clean-revision benchmark rerun requested after the
query, batching, mutex, and Recorder durability work. It separates embedded
database costs from loopback transport costs. None of these results is a
multi-host or production benchmark.

## Provenance and scope

- Source revision: `073ce3473655c13094527d3b53c4b24751016994`
- Git state during every measured run: clean
- Host: Apple M3, macOS 26.3, rustc 1.95.0
- Database payload: 128 bytes, 256 bounded keys
- Database repetitions: three order-rotated runs; median reported
- Write cell: warmup 100, measured 500, concurrency 1
- Read cells: warmup 100, measured 10,000, concurrency 1 and 8
- Writes include in-process QuePaxa, three file-backed Recorder voters with
  local fsync, qlog, and materialization.
- Reads use local consistency. HTTP, node-to-node networking, consensus read
  barriers, and remote checkpoints are excluded.

The checked-in `rhiza-profile` harness uses the public embedded API. Every
measured write changes the value while preserving the same payload length
across SQL, graph, and KV.

## SQL, graph, and KV results

Latency is in microseconds.

| Profile | Workload | Concurrency | Median ops/s | p50 | p95 | p99 | p99.9 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQL | write | 1 | 22.88 | 43,653 | 52,053 | 56,127 | 62,951 |
| Graph | write | 1 | 18.60 | 52,962 | 64,494 | 80,688 | 145,856 |
| KV | write | 1 | 21.98 | 45,053 | 56,233 | 62,975 | 125,480 |
| SQL | get | 1 | 43,988 | 20 | 30 | 46 | 79 |
| Graph | get through Cypher | 1 | 1,706 | 564 | 673 | 924 | 2,760 |
| KV | get | 1 | 55,804 | 13 | 34 | 68 | 125 |
| SQL | get | 8 | 47,148 | 146 | 304 | 526 | 1,694 |
| Graph | get through Cypher | 8 | 2,494 | 2,422 | 5,895 | 18,436 | 97,399 |
| KV | get | 8 | 120,650 | 41 | 180 | 320 | 564 |

All 180,000 declared-comparison point reads and 4,500 measured writes completed
without an error. Because the order-rotated KV c=1 samples varied substantially,
a separate three-run confirmation produced 88,397, 96,872, and 89,054 ops/s.
The confirmation median is 89,054 ops/s with p50 9 us, p99 29 us, and no
errors. The primary order-rotated table remains the declared comparison set;
the confirmation is evidence that system load affected the lower samples.

### Direction versus the preserved 2026-07-15 baseline

The prior baseline came from dirty revision `06b0860`; its raw artifacts and
harness were not preserved. The following comparison is therefore directional,
not release evidence.

| Metric | Prior | Current | Change |
| --- | ---: | ---: | ---: |
| SQL write ops/s | 12.42 | 22.88 | +84.2% |
| Graph write ops/s | 11.18 | 18.60 | +66.4% |
| KV write ops/s | 12.02 | 21.98 | +82.9% |
| SQL c=1 get ops/s | 52,581 | 43,988 | -16.3% |
| Graph c=1 get ops/s | 1,425 | 1,706 | +19.7% |
| KV c=1 get ops/s | 312,083 | 89,054 confirmation | -71.5% |

The Recorder durability reduction is reflected in the large write-throughput
increase across all three profiles. Graph point reads also improved. SQL reads
show a modest regression signal. KV reads remain fast in absolute latency, but
the repeatable gap from the preserved baseline is too large to dismiss and
should be profiled through the public `RhizaHandle::get_kv` path before a
release claim is made.

Graph c=8 improved 76.2% over the pre-mutex 1,416 ops/s diagnostic, but is 7.5%
below the later 2,697 ops/s diagnostic. Its p99/p99.9 tail remains much larger
than c=1, so the mutex issue is substantially improved rather than eliminated.

## Node transport results

The official equal-TLS runner completed three independent, order-rotated runs.
Every raw run used 4,096 warmup and 40,000 measured operations for each of two
payload sizes and three concurrency levels. Its aggregate report states:

- `diagnostic_valid: true`
- `comparison_valid: true`
- `production_valid: false`
- zero validation errors and no comparison blockers
- clean, consistent Git provenance across all runs
- TLS 1.3 server authentication and ALPN observed; no measured handshakes

Selected 128-byte medians:

| Candidate | c=1 ops/s / p99 us | c=8 ops/s / p99 us | c=64 ops/s / p99 us |
| --- | ---: | ---: | ---: |
| HTTPS/JSON | 15,707 / 135.8 | 38,100 / 520.5 | 46,281 / 7,868.4 |
| Quinn lane | 25,099 / 57.5 | 40,476 / 622.7 | 84,486 / 1,397.0 |
| TLS TCP/Postcard | 35,475 / 43.3 | 96,895 / 154.5 | 119,798 / 870.7 |

Across all six 128-byte/4-KiB and c=1/8/64 cells, TLS TCP/Postcard versus
HTTPS/JSON achieved:

- 2.736x geometric-mean throughput
- 4.368x geometric-mean p99 improvement

TLS TCP/Postcard also beat Quinn lane in all six cells, with 1.916x
geometric-mean throughput and 2.391x geometric-mean p99 improvement.

A separate mixed plaintext/TLS diagnostic found plaintext TCP/Postcard versus
HTTP/JSON at 2.465x geometric-mean throughput and 3.658x p99 improvement. TLS
TCP/Postcard versus plaintext TCP/Postcard was within noise: 0.989x throughput
and approximately 2.5% worse p99 in aggregate. TLS is therefore not a material
performance gate on this host.

## Decision

1. Keep the Recorder durability changes: the clean local evidence shows a
   66–84% write-throughput gain across every storage profile.
2. Keep `tcp-tls-postcard` as the leading Recorder RPC candidate. Do not promote
   it to the default from loopback evidence alone.
3. Do not adopt Quinn for the current LAN Recorder workload. It did not win a
   measured equal-TLS cell.
4. Do not disable TLS for performance reasons. Its measured cost relative to
   plaintext was negligible compared with run-to-run variance.
5. Profile and optimize KV point reads, and inspect SQL point-read overhead,
   before publishing a general performance claim.
6. Continue to treat the transport decision as `HOLD` until the existing
   physical two-host, fault/reconnect, resource, checkpoint equality, and
   30-minute soak gates pass.

## Artifacts and environment limitation

Local raw artifacts and SHA-256 manifests are under ignored paths:

- `target/rhiza-bench/profile-073ce34-20260717T114031/`
- `target/rhiza-bench/transport-073ce34-20260717T115220/`
- `target/rhiza-bench/tls-073ce34-20260717T115627/`

Vind was not run. The active Kubernetes context was a production-like GKE
cluster, no disposable vind/vcluster existed, the Docker VM had only 5 CPUs and
4 GiB, and the available `rhiza:dev` image predated the measured commit. Running
there would have been unsafe and non-reproducible. A publishable cluster rerun
requires a dedicated kubeconfig, a fresh image labeled with the exact revision,
at least the documented 8 CPU/24 GiB host resources, and Graph/KV cluster
workload support in addition to the existing SQL runner.
