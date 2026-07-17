# rhiza-quepaxa

`rhiza-quepaxa` is the transport-independent QuePaxa consensus engine used
by rhiza. It provides the proposer, recorder state machine, decision proofs,
fixed and stoppable membership transitions, and a file-backed recorder for
embedding and tests. It does not depend on SQLite, HTTP, Tokio, object storage,
or Kubernetes.

The crate deliberately re-exports the `rhiza-core` model types used by its
public API. Applications normally need only this crate to construct commands,
drive consensus, and inspect decisions.

```rust
use rhiza_quepaxa::{Command, CommandKind, Consensus, ThreeNodeConsensus};

let base = std::env::temp_dir().join(format!("rhiza-quepaxa-readme-{}", std::process::id()));
let _ = std::fs::remove_dir_all(&base);
let roots = [base.join("n1"), base.join("n2"), base.join("n3")];
let consensus = ThreeNodeConsensus::new("cluster", "n1", 1, 1, roots)?;
let entry = consensus.propose(Command::new(
    CommandKind::Deterministic,
    b"deterministic command".to_vec(),
))?;
assert_eq!(entry.index, 1);
# Ok::<(), rhiza_quepaxa::Error>(())
```

See `examples/local_three_node.rs` for a complete runnable example.

## Runtime contract

- `RecorderRpc` implementations must enforce a finite deadline for every
  network or process-bound operation. The consensus engine performs quorum RPCs
  concurrently and may return after a quorum while slower calls finish.
- An ordinary command's proposal response does not wait for or require proof
  dissemination. Its decision is offered to one fixed proof worker per
  recorder. Each worker has room for one queued job in addition to its in-flight
  RPC, stores the verified command bytes before idempotently installing the
  proof, and drops new work when that bounded queue is full or disconnected.
  These best-effort failures never change proposal success; sustained
  saturation can therefore leave a recorder without a proof cache until later
  recovery or dissemination.
- Configuration-change decisions, and decisions reached after a transition was
  observed, still install their proof on a recorder quorum synchronously before
  the proposal succeeds.
- Dropping `ThreeNodeConsensus` never waits indefinitely for outstanding RPC
  workers, including proof workers. A transport that ignores its deadline can
  leak its own worker and resources, but cannot block consensus destruction.
- Call `finish_pending_rpcs` with an application-selected bound before removing
  local recorder storage or shutting down a transport used by accepted record
  or proof jobs. The drain does not recover proof jobs already dropped because
  a bounded worker queue was full.
- `PrioritySource` is injectable for deterministic simulation. The default uses
  the operating system random source through `getrandom` and supports all
  platforms supported by that crate.

## Recorder durability

Normal records are acknowledged from a bounded, checksummed append-only
`recorder.wal`. Each frame carries its generation and sequence, the previous
frame digest, the exact slot/configuration/head state, and an optional inline
command. Recovery replays only the continuous digest chain, truncates an
incomplete unacknowledged tail, and fails closed on a complete corrupt frame.
The WAL is checkpointed before its 16 MiB soft limit or 1,024-frame hard limit
is exceeded.

The steady path acknowledges only after the appended frame is durable. On
Linux it uses `File::sync_data` (`fdatasync`); other platforms conservatively
retain `File::sync_all`. Operations that change WAL metadata, including
checkpoint/rotation truncate and recovery tail repair, always use
`File::sync_all`. Checkpointing first durably replaces command, slot,
configuration, and recorded-head files and only then truncates and fully syncs
the stable WAL inode. Structural configuration changes drain the WAL and keep
their separate crash-recovery intent protocol.

These rules preserve the ordering contract: write frame, sync successfully,
publish the new in-memory Recorder state, then ACK. API-level recovery and
fault-injection tests cover this order. A physical power-loss matrix on ext4,
XFS, and the intended Kubernetes CSI remains a separate deployment gate.

### Recorder WAL sync benchmark

`recorder_sync_bench` measures the actual `RecorderFileStore::record_proposal`
steady WAL append and acknowledgement path without pulling in a rhiza backend
or network transport. A run is deliberately capped below the WAL checkpoint
boundary and emits one JSON object with throughput, successful-call latency
percentiles, error count, exact WAL byte/frame observations, and platform
metadata. Request construction is completed before timing.

```console
cargo run --release -p rhiza-quepaxa --example recorder_sync_bench -- \
  --warmup 100 --operations 500 --label native
```

On Linux, `File::sync_data` reaches the normal dynamically linked `fdatasync`
symbol. [`bench/support/fdatasync-as-fsync.c`](../../bench/support/fdatasync-as-fsync.c)
is the comparison shim: it forwards `fdatasync(fd)` to `fsync(fd)` and records
its intercept count at process exit.
[`bench/run-recorder-sync-linux.py`](../../bench/run-recorder-sync-linux.py)
builds the benchmark and shim once, rotates candidate order across balanced
Docker pairs, verifies that the shim observed exactly `warmup + operations`
calls, and preserves raw JSONL plus a summary with commands, hashes, Git state,
and container provenance.

```console
python3 bench/run-recorder-sync-linux.py --pairs 12
```

A 2026-07-17 Docker Desktop Linux/aarch64 diagnostic ran 12 balanced pairs,
each with 100 warmups and 800 measured records. All 19,200 measured records
succeeded. Median throughput was 2,983.9011487711614 ops/s for native
`fdatasync` and 1,911.5215089204817 ops/s with the `fsync` preload. Dividing
the aggregate medians gives 1.561008408666x. However, the median paired
`fsync-preload/native` ratio was 0.9278500671968066, and each candidate won
6/12 pairs. Native/preload median p50 was 240,437.5/398,624.5 ns, p95
793,479/1,239,624.5 ns, and p99
1,603,021/2,123,125 ns. Aggregate throughput and latency favor native, but the
paired result and win split remain mixed. All 12
preload runs observed the expected 900 intercepts, and every run observed 900
WAL frames in generation 1 without a checkpoint.

The tracked artifacts are
[`raw.jsonl`](../../docs/benchmarks/recorder-sync-linux-20260717/raw.jsonl)
(24 rows, 49,782 bytes) and
[`summary.json`](../../docs/benchmarks/recorder-sync-linux-20260717/summary.json)
(9,603 bytes). The summary records exact commands, hashes, dirty Git state, and
container provenance. The QuePaxa source SHA-256 is
`54ca511bd8be35e1b2deeb50a1f8f9ced66bb336194e4d7ba07c4473a9d60c1d`
and the benchmark binary SHA-256 is
`7bc075b29e7d49524ea51555b5cc95a0f6d1eea4b9eccff7d634caa27893459d`.
Its recorded runner SHA-256
`bbe7d010c56fae73cc2d65d252093e2e547b4c191a8e14c9ccd7aa7454ed0b7d`
matches the current runner. The fresh build provenance is retained under
`target/recorder-sync-linux-build-final-v3-20260717`, and the runner's full-reuse
gate verified it. The summary sets `production_valid=false`: measurements from
a dirty tree remain diagnostic, and Docker Desktop's virtual filesystem cannot
reproduce host power loss or the target CSI flush path. Linux `sync_data`
remains a correctness-preserving candidate implementation of the smaller
durability syscall. The aggregate Docker result is favorable, but paired
performance is inconclusive and is not a production speedup claim. Production
performance adoption requires clean physical crash/reopen and throughput/latency
testing on the target ext4/XFS/CSI stack.

## Compatibility policy

The minimum supported Rust version is 1.89. The public Rust API follows
semantic versioning. Recorder persistence and decision-proof encodings are
versioned and reject unsupported versions; their byte representation is not an
unversioned compatibility promise. HTTP or other wire protocols belong to the
embedding application and are not part of this crate.

`rhiza-core` and `rhiza-quepaxa` are released with matching minor
versions. Version 0.x may make breaking protocol or API changes in a minor
release, with migration notes in the repository.
