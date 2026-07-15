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

let base = std::env::temp_dir().join("rhiza-quepaxa-readme");
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
- Dropping `ThreeNodeConsensus` never waits indefinitely for outstanding RPC
  workers. A transport that ignores its deadline can leak its own worker and
  resources, but cannot block consensus destruction.
- Call `finish_pending_rpcs` with an application-selected bound before removing
  local recorder storage or shutting down a transport used by in-flight calls.
- `PrioritySource` is injectable for deterministic simulation. The default uses
  the operating system random source through `getrandom` and supports all
  platforms supported by that crate.

## Compatibility policy

The minimum supported Rust version is 1.89. The public Rust API follows
semantic versioning. Recorder persistence and decision-proof encodings are
versioned and reject unsupported versions; their byte representation is not an
unversioned compatibility promise. HTTP or other wire protocols belong to the
embedding application and are not part of this crate.

`rhiza-core` and `rhiza-quepaxa` are released with matching minor
versions. Version 0.x may make breaking protocol or API changes in a minor
release, with migration notes in the repository.
