# rhiza Node RPC Stack Diagnostic Benchmark

Date: 2026-07-15

Status: two local diagnostic artifacts combined after the Quinn lane warm-state
fix. This result does **not** approve a production transport migration.

Update: this report remains an immutable account of the original diagnostic.
The current opt-in runtime candidate is plaintext `tcp-postcard`, restricted to
one trusted Kubernetes-cluster boundary. Historical candidate labels and TLS
comparisons below are intentionally unchanged.

This run executes the repository-owned
[`rhiza-transport` harness](../bench/src/bin/rhiza-transport.rs). See the
[`bench` usage notes](../bench/README.md#node-transport-microbenchmark), the
[RPC stack research](node-rpc-stack-research-2026-07-15.md), and the earlier
[transport shootout](node-transport-research-2026-07-15.md) for the decision
context and production acceptance gates.

## Artifact and exact run conditions

- Primary artifact: `/private/tmp/rhiza-rpc-stack-fixed-run1.json`
- Primary SHA-256:
  `acc3f2459701db19f061803cf9d5ebe6b30a333945d00cece964ff682dfd9b0b`
- Primary scope: the five non-lane candidates in this report. Its six
  `quinn-lane-persistent-worker` performance rows are superseded and must not be
  used.
- Supplemental corrected-lane artifact:
  `/private/tmp/rhiza-rpc-stack-quinn-lane-fixed-run1.json`
- Supplemental SHA-256:
  `b5be1df20ce0ff2111d45b2eaf5e419c001f8fe5876850e3eb9efa99eb23ba50`
- Supplemental scope: the six Quinn lane rows only, after opening one lane per
  worker and reusing that same lane across warm-up and measurement.
- Report schema: `1`
- Primary generated at: epoch `1784091329.693591`
  (`2026-07-15T04:55:29Z`, truncated to whole seconds for the ISO rendering)
- Supplemental generated at: epoch `1784091688.386214`
- Harness validity flag: `true` in both artifacts
- Combined metrics: 36 (30 primary non-lane plus 6 supplemental lane); total
  errors: 0
- Warm-up: 4,096 operations per metric
- Measurement: 60,000 operations per metric
- Payloads: 128 B and 4,096 B
- Concurrency: 1, 8, and 64
- Call timeout: 2 seconds
- Host: `127.0.0.1` loopback; clients and servers in one process
- Candidate order: fixed in the CLI/default order shown below
- Server reuse: one HTTP, TCP, and QUIC server set across all metrics. HTTP
  reused its reqwest pool, TCP reused one warmed connection per worker, and all
  QUIC candidates reused one connection.
- Included in latency: request construction and encoding, loopback transport,
  server decode and semantic validation, SHA-256 response construction,
  response decoding, and response validation including request identity.
- Excluded: QuePaxa quorum work, persistence, fsync, materialization, remote
  networking, and resource profiling.
- Security: HTTP and TCP were plaintext. QUIC used TLS server authentication.
  No candidate used mTLS.

Exact captured environment:

| Field | Value |
|---|---|
| Git commit | `06b0860b8a8272d7fa62a498367995587d3b95cc` |
| Git dirty | `true` |
| Rust | `rustc 1.95.0 (59807616e 2026-04-14) (Homebrew)` |
| OS | `Darwin 10.nate.com 25.3.0 Darwin Kernel Version 25.3.0: Wed Jan 28 20:53:31 PST 2026; root:xnu-12377.81.4~5/RELEASE_ARM64_T8122 arm64` |
| CPU | `Apple M3` |

The machine battery fell from approximately 7% to 5% while the run executed,
and the host was under high load. That is a material uncontrolled condition.

## Encoded message sizes

These are application-encoded request and response sizes. TCP and QUIC rows
include their four-byte length prefix; HTTP headers and TCP/IP/QUIC/TLS packet
overhead are not included.

| Payload B | Candidate | Request B | Response B |
|---:|---|---:|---:|
| 128 | `http-json` | 696 | 202 |
| 128 | `http-postcard` | 197 | 68 |
| 128 | `http-prost` | 201 | 72 |
| 128 | `tcp-postcard-persistent-worker` | 201 | 72 |
| 128 | `quinn-rpc-stream` | 202 | 73 |
| 128 | `quinn-lane-persistent-worker` | 202 | 73 |
| 4,096 | `http-json` | 16,568 | 203 |
| 4,096 | `http-postcard` | 4,165 | 68 |
| 4,096 | `http-prost` | 4,169 | 72 |
| 4,096 | `tcp-postcard-persistent-worker` | 4,169 | 72 |
| 4,096 | `quinn-rpc-stream` | 4,170 | 73 |
| 4,096 | `quinn-lane-persistent-worker` | 4,170 | 73 |

These sizes come from the final corrected harness smoke using each transport's
representative measurement sequence, not the superseded zero-sequence size
calculation in the primary artifact. JSON expanded the 128-byte request to 696
bytes versus 197–201 bytes for binary HTTP. At the 4,096-byte payload it
produced 16,568 bytes versus 4,165–4,169 bytes.

## Full result matrix

Throughput is rounded to the nearest whole operation per second. p99 is in
microseconds and is reproduced from the artifact. “Winner” means highest
throughput within that payload/concurrency cell, not production selection.

| Payload B | C | Candidate | ops/s | p99 us | Cell result |
|---:|---:|---|---:|---:|---|
| 128 | 1 | `http-json` | 15,160 | 169.583 | |
| 128 | 1 | `http-postcard` | 18,924 | 93.125 | |
| 128 | 1 | `http-prost` | 19,246 | 109.875 | |
| 128 | 1 | `tcp-postcard-persistent-worker` | 34,731 | 42.25 | **winner** |
| 128 | 1 | `quinn-rpc-stream` | 11,522 | 388.583 | |
| 128 | 1 | `quinn-lane-persistent-worker` | 24,485 | 67.833 | |
| 128 | 8 | `http-json` | 22,289 | 1,446.5 | |
| 128 | 8 | `http-postcard` | 38,063 | 538.292 | |
| 128 | 8 | `http-prost` | 49,978 | 354.25 | |
| 128 | 8 | `tcp-postcard-persistent-worker` | 112,791 | 125.708 | **winner** |
| 128 | 8 | `quinn-rpc-stream` | 31,340 | 653.75 | |
| 128 | 8 | `quinn-lane-persistent-worker` | 36,313 | 1,006.042 | |
| 128 | 64 | `http-json` | 47,358 | 2,612.125 | |
| 128 | 64 | `http-postcard` | 54,030 | 2,395.667 | |
| 128 | 64 | `http-prost` | 58,085 | 1,626.791 | |
| 128 | 64 | `tcp-postcard-persistent-worker` | 113,681 | 1,074.833 | **winner** |
| 128 | 64 | `quinn-rpc-stream` | 67,359 | 1,653.375 | |
| 128 | 64 | `quinn-lane-persistent-worker` | 72,785 | 2,278.958 | |
| 4,096 | 1 | `http-json` | 5,227 | 484.125 | |
| 4,096 | 1 | `http-postcard` | 9,710 | 260.042 | |
| 4,096 | 1 | `http-prost` | 11,394 | 181.208 | |
| 4,096 | 1 | `tcp-postcard-persistent-worker` | 15,683 | 85.333 | **winner** |
| 4,096 | 1 | `quinn-rpc-stream` | 9,313 | 187.458 | |
| 4,096 | 1 | `quinn-lane-persistent-worker` | 9,887 | 206.541 | |
| 4,096 | 8 | `http-json` | 18,550 | 748.209 | |
| 4,096 | 8 | `http-postcard` | 12,179 | 6,462.0 | |
| 4,096 | 8 | `http-prost` | 32,076 | 713.0 | |
| 4,096 | 8 | `tcp-postcard-persistent-worker` | 48,728 | 370.667 | **winner** |
| 4,096 | 8 | `quinn-rpc-stream` | 16,245 | 1,513.292 | |
| 4,096 | 8 | `quinn-lane-persistent-worker` | 21,225 | 959.625 | |
| 4,096 | 64 | `http-json` | 23,217 | 6,476.708 | |
| 4,096 | 64 | `http-postcard` | 40,948 | 3,110.291 | |
| 4,096 | 64 | `http-prost` | 42,785 | 2,980.709 | |
| 4,096 | 64 | `tcp-postcard-persistent-worker` | 68,429 | 1,595.583 | **winner** |
| 4,096 | 64 | `quinn-rpc-stream` | 25,489 | 5,613.125 | |
| 4,096 | 64 | `quinn-lane-persistent-worker` | 26,671 | 5,652.459 | |

## Diagnostic conclusions

1. Persistent TCP/Postcard had the highest throughput in all six
   payload/concurrency cells and also the lowest p99 in all six cells in this
   run. This justifies a controlled repeat, not adoption.
2. Binary HTTP substantially reduced encoded bytes. Prost had higher throughput
   than Postcard in all six cells, but Postcard had the lower p99 at 128 B,
   concurrency 1. The 4 KiB/concurrency-8 Postcard result regressed below JSON
   and reached 6,462 us p99, a conspicuous signal of run noise, scheduling, or a
   codec-specific issue that needs profiling and repetition.
3. The corrected persistent lane beat stream-per-RPC throughput at 128 B for
   concurrency 1, 8, and 64. It improved p99 only at concurrency 1; p99 was
   worse at concurrency 8 and 64. At 4 KiB the lane also had higher throughput
   in all three cells, while p99 improved only at concurrency 8. The latency
   result is therefore mixed, and the combined diagnostic still does not select
   a universal Quinn topology.
4. The result supports the research ladder: repeat codec isolation first, then
   compare persistent TCP and both Quinn topologies under equal TLS, physical
   hosts, loss, reconnect, and durable QuePaxa workloads.

## Limitations and decision

This is a combination of a fixed-order primary run and a later lane-only
supplemental run, not one counterbalanced run. Both used a dirty working tree,
one process, loopback traffic, shared servers, low battery, and high host load.
HTTP/TCP were plaintext while QUIC paid TLS costs. There was no mTLS, physical network,
QuePaxa consensus, quorum-early completion, qlog/fsync, database
materialization, packet loss, RTT injection, reconnect, certificate rotation,
slow minority, CPU/op, RSS, or soak test. The harness-level
`valid_for_comparison=true` means all 36 cells completed with validated responses
and no recorded error; it does not satisfy the production promotion gates.

Therefore no transport or codec is promoted to production from this artifact.
The existing HTTP/JSON peer transport remains the control until repeated,
counterbalanced, two-host and end-to-end durable tests clear the acceptance
gates in the research documents.
