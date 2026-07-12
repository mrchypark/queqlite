# Queqlite Failover and Throughput Verification

Date: 2026-07-12

## Scope

This change implements four related improvements without making Queqlite depend
on Kubernetes:

1. Separate peer and client Services, preferred-first multi-endpoint client
   fallback/hedging, and a preferred-proposer deletion benchmark.
2. Command piggybacking on typed QuePaxa Record messages and quorum-early
   `ConfigChange` proof installation.
3. A bounded writer microbatch (maximum 8 requests, 500 microseconds by
   default) for KV and arbitrary SQL writes.
4. An append-only qlog open segment, SQLite WAL/NORMAL cache mode, and a
   renewable cached OSS publisher session with grouped flushes.

RustFS is used only as the local S3-compatible OSS simulator. No RustFS failure
was injected.

## Correctness boundaries

- Endpoint order preserves the preferred node first. Later endpoints are used
  only for fallback or hedge attempts, and every attempt reuses the exact
  request body and `request_id`.
- A recorder validates the piggybacked command against the complete accepted
  value, persists it, and only then acknowledges ISR progress.
- An ordinary SQL/KV FastPath decision returns after one phase-0 recorder
  quorum; it does not wait for a second quorum to install the decision proof.
  `ConfigChange` decisions still install the proof on a quorum before return.
  A slow minority is not on either response critical path.
- A microbatch is one consensus entry and one SQLite transaction. Each member
  keeps an independent persistent idempotency record and SQL result, including
  `RETURNING` rows.
- If one SQL request cannot be prepared with the others, requests fall back to
  independent execution instead of failing unrelated clients.
- SQLite is a rebuildable local cache. WAL/NORMAL is safe because the local
  qlog remains the replay source, while OSS remains the disaster-recovery
  source according to the selected durability mode.
- Publisher state is cached only as an optimization. Manifest CAS remains the
  authority, and stale sessions reload after a precondition conflict.

## Performance

Workload: one parameterized INSERT per request, concurrency 4, periodic OSS
durability at 1 second, RustFS metering enabled, resource sampling disabled.

| Run | Duration | Success/error | Writes/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| Previous periodic baseline | 20 s | 3,251 / 0 | 162.55 | 25.6 ms | 51.2 ms | 51.2 ms |
| Optimized preferred endpoint | 30 s | 8,578 / 0 | 285.93 | 12.8 ms | 25.6 ms | 102.4 ms |
| Three endpoints, before preferred deletion | 10 s | 1,738 / 0 | 173.80 | 25.6 ms | 102.4 ms | 102.4 ms |
| Three endpoints, preferred deleted | 20.00 s | 3,515 / 0 | 175.71 | 25.6 ms | 102.4 ms | 102.4 ms |
| Final image, preferred-first multi-endpoint | 30 s | 9,143 / 0 | 304.77 | 12.8 ms | 51.2 ms | 102.4 ms |

The final image used three endpoints in preferred-first order; it did not
deliberately distribute requests across non-preferred proposers. Its fault
windows were:

| Window | Duration | Success/error | Writes/s |
|---|---:|---:|---:|
| Before preferred pod deletion | 10 s | 3,174 / 0 | 317.4 |
| During deletion/restore | 20.009 s | 5,969 / 0 | 298.316 |

Throughput during the deletion/restore window was about 6.0% below the
pre-deletion window, with zero request errors. The fault command succeeded in
34.846 seconds. The measurement ended with the fault window, so this run has no
separate after-recovery throughput window and does not isolate protocol cost
from routing, restoration, or other system overhead.

These measurements compare two Queqlite configurations and do **not** establish
Raft-equivalent performance. The paper-conformance and benchmark-comparability
boundaries are documented in
[`quepaxa-paper-conformance-2026-07-12.md`](quepaxa-paper-conformance-2026-07-12.md).

Checkpoint drain required no wait: qlog and checkpoint both ended at index 5193
with the exact same hash. This establishes agreement for this run, not a general
recovery-time or performance claim.

Artifacts:

- Final preferred-first deletion run: `target/queqlite-bench/20260712-113825-51624`
- Normal: `target/queqlite-bench/20260712-034534-80063`
- Preferred deletion, final: `target/queqlite-bench/20260712-035609-39970`
- Longer preferred deletion sample: `target/queqlite-bench/20260712-034903-7352`

## OSS usage and cost

The final preferred-first deletion run used 5,259 metered OSS requests and
retained 29,855,178 bytes in 286 objects.

For historical context, the earlier optimized normal run used 3,997 metered OSS
calls for 10,053 warmup and measured writes and retained 29.50 MB in 292 objects.
Its checked-in request-rate estimate was $1.01 per million writes before retained
storage and network egress, versus the previous $4.53 estimate. Those cost
figures were not recomputed from the final artifact.

The final access log included transient PUT 412 responses from expected CAS
races and PUT 502 responses that were retried successfully. These were
observations, not injected RustFS failures; all client writes succeeded and the
final checkpoint drain matched the qlog index and hash exactly.

## Verification

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `cargo test --manifest-path bench/Cargo.toml`
- `cargo clippy --manifest-path bench/Cargo.toml --all-targets -- -D warnings`
- `shellcheck scripts/*.sh`
- `scripts/check-deploy.sh`
- release Docker image build
- vind/RustFS normal and preferred-proposer deletion benchmarks
