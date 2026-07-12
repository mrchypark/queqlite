# queqlite

Queqlite is a SQLite materialized state machine ordered by QuePaxa. The agreed
qlog plus an official snapshot is authoritative; a local SQLite file is not.

## Architecture

The Rust workspace is Kubernetes-independent:

- `queqlite`: primary embedded Rust facade and lifecycle owner.
- `queqlite-core`: log, configuration, command, and snapshot domain types.
- `queqlite-quepaxa`: recorder RPC, durable recorder state, and consensus.
- `queqlite-log`: local binary qlog and compaction anchors.
- `queqlite-obj-store`: `object_store` adapters for S3, GCS, Azure, and tests.
- `queqlite-sqlite`: the SQLite materialized-state boundary.
- `queqlite-archive`: checkpoint V2, object metadata, and GC plans.
- `queqlite-node`: runtime, HTTP RPC, recovery, and authenticated live admin HTTP.
- `queqlite-cli`: client and object-store administration commands.

## Embedded Rust API

The primary product surface is the `queqlite` crate. `Queqlite` owns the node
runtime and background workers; cloneable `QueqliteHandle` values are weak
handles that stop working after owner shutdown. Applications inject recorder
and log transports without configuring HTTP or Kubernetes:

```rust,no_run
use queqlite::{EmbeddedConfig, EmbeddedIdentity, Queqlite, ReadConsistency};

# async fn example(
#     recorders: Vec<(String, Box<dyn queqlite::RecorderRpc>)>,
# ) -> Result<(), queqlite::Error> {
let config = EmbeddedConfig::new(
    EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
    "./data/node-1",
    vec!["node-1".into(), "node-2".into(), "node-3".into()],
    recorders,
    vec![],
    None,
);
let owner = Queqlite::open(config).await?;
let db = owner.handle();

db.put("request-1", "key", "value").await?;
let value = db.read("key", ReadConsistency::Local).await?;
assert_eq!(value.value.as_deref(), Some("value"));

owner.shutdown().await?;
# Ok(())
# }
```

`execute_sql` and `query` expose the same typed SQL, `RETURNING`, consistency,
and persistent idempotency contracts as the HTTP adapter. The HTTP routes and
CLI are secondary adapters over the same node service.

Kubernetes provides stable process identity, DNS, secrets, and orchestration;
Queqlite does not call Kubernetes APIs and receives no service-account token.
Each configuration has its own headless Service and StatefulSet named
`queqlite-c<config_id>`. Stable ordinals map to `node-1` through `node-N`.
Membership accepts 3 through 7 members through a version-1 JSON bundle:

```json
{
  "version": 1,
  "config_id": 1,
  "members": [
    {
      "node_id": "node-1",
      "url": "http://queqlite-c1-0.queqlite-c1:8081",
      "log_url": "http://queqlite-c1-0.queqlite-c1:8080",
      "token": "secret"
    }
  ]
}
```

The bundle is mounted from an immutable Secret and selected with
`QUEQLITE_CONFIG_BUNDLE_FILE`. Deployment does not use the deprecated
`QUEQLITE_PEER_1..3` variables. Peer tokens live inside the bundle Secret;
client/admin tokens and object-store credentials live in separate Secrets.
Never put any of those values in a ConfigMap.

## SQL API

Queqlite executes admitted deterministic SQLite DDL and DML as replicated,
idempotent command batches. Every `/v1/sql/execute` request has a stable request
ID; all statements in the request run in one SQLite transaction and either all
apply at the agreed qlog index or none do. QSQL v2 returns a typed result for
each statement, including statement-level `rows_affected` and bounded typed
`RETURNING` rows. The result is persisted with the request ID, so an exact retry
replays the original result rather than executing the SQL again.

`/v1/sql/query` accepts one read-only statement and supports `local`, applied-
index, and quorum read-barrier consistency. QSQL v2 effect replication uses
QEFX v1: SQLite session changesets are generated against the exact qlog base
index and hash, and carry the request digest, persisted result, and executor
fingerprint. If another command wins the proposed slot, the effect is
regenerated against the new exact base. Effects are capped at 256 KiB and are
applied with conflict-abort semantics; this is bounded effect replication, not
unrestricted arbitrary SQLite effect replication.

SQL parameters and result cells preserve SQLite `null`, `integer`, `real`,
`text`, and `blob` types. For example:

```bash
queqlite sql execute --url http://127.0.0.1:8080 \
  --request-id create-users \
  --sql 'CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)'

queqlite sql execute --url http://127.0.0.1:8080 \
  --request-id insert-ada \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":1},{"type":"text","value":"Ada"}]'

queqlite sql query --url http://127.0.0.1:8080 \
  --sql 'SELECT id, name FROM users WHERE id = ?1' \
  --params-json '[{"type":"integer","value":1}]' \
  --consistency barrier
```

Atomic multi-statement batches use the same authenticated JSON RPC directly.
Queqlite preflights a batch against the current agreed state before proposing
it, then deterministically replays the encoded SQL and parameters on every
node. SQL that can make replicas diverge or escape the state-machine boundary
is rejected: nondeterministic time/random/change-counter functions, direct
`__queqlite_*` access, `PRAGMA`, `ATTACH`/`DETACH`, TEMP objects, virtual tables,
and explicit transaction/savepoint control. Direct statement-replay `RETURNING`
is rejected. `RETURNING` is admitted only when a complete QEFX changeset can be
generated and validated. DDL, tables without a complete supported primary
key/schema, triggers, foreign keys, and indirect changes cannot use QEFX; they
fall back to deterministic statement replay only for non-RETURNING requests. A
`RETURNING` request in any of those cases fails closed. Query responses are
bounded by server row and byte limits.

Recovery metadata uses QANC v3 and binds the recovery generation,
configuration state, snapshot identity, and executor fingerprint. A mismatch
is rejected during recovery rather than replayed best-effort.

The message-level comparison with Hiqlite and the bounded effect-replication
contract are documented in [`docs/hiqlite-sql-message-review.md`](docs/hiqlite-sql-message-review.md).

## Storage Model

The Queqlite StatefulSet deliberately has no `volumeClaimTemplates`. Every pod
uses `emptyDir` for `/var/lib/queqlite`. StatefulSet identity is still useful:
it gives each ephemeral process a stable ordinal and DNS name while making a
replacement prove that recovery does not depend on an old local disk. A fresh
pod restores an official snapshot and then replays the exact qlog suffix.

This trades restart speed and object-store dependency for a smaller local
state-management surface. Production object storage must have an independent
failure domain and strong cross-process conditional writes. The local vind
RustFS Deployment also uses `emptyDir`; it simulates S3 compatibility but is
not production durability evidence.

Queqlite uses the generic `object_store` boundary. The deployment template is
S3-shaped for RustFS, AWS S3, or another compatible endpoint; runtime support
also includes GCS and Azure Blob through provider configuration. No provider or
Kubernetes API appears in consensus logic.

An application write does **not** perform an S3 CAS. Writes first commit through
QuePaxa and append locally. Checkpoint publication batches state into immutable
objects and conditionally advances a small manifest. `sync` durability may
publish for every acknowledged write to provide RPO0, while `bounded` and
`periodic` modes reduce object-store traffic with the documented lag tradeoff.

## Local Checks

```bash
cargo test
shellcheck scripts/*.sh
bash -n scripts/*.sh
yq eval '.' deploy/k8s/*.yaml >/dev/null
scripts/check-deploy.sh
```

Render a config-scoped StatefulSet without writing the bundle token to a YAML
artifact:

```bash
scripts/render-k8s-config.sh 1 3 config-c1.json target/config-c1.yaml
kubectl -n queqlite create secret generic queqlite-c1-bundle \
  --from-file=config.json=config-c1.json --dry-run=client -o yaml \
  | yq eval '.immutable = true' - \
  | kubectl -n queqlite create -f -
kubectl -n queqlite create -f target/config-c1.yaml
```

Set `QUEQLITE_IMAGE`, `QUEQLITE_CLUSTER_ID`, `QUEQLITE_EPOCH`,
`QUEQLITE_RECOVERY_GENERATION`, `QUEQLITE_S3_*`, and Secret-name overrides as
needed. The rendered resource uses `OnDelete`; do not mutate a live config's pod
template to reconfigure membership.

## Stop And Replace

Membership replacement is intentionally stop-the-world. There is no mixed
rolling transition between configurations. Client writes are unavailable from
Stop(S) until every successor reports Active(S+1). The bounded operator flow is:

1. Prepare a v1 successor draft with config ID S+1 and 3 through 7 members.
2. Confirm no successor StatefulSet already exists.
3. Call old live admin `membership/stop` with the admin bearer token.
4. Poll every old node until its exact state is `Stopped(S)`.
5. Bind the returned Stop entry, decision certificate, and old membership into
   the successor bundle.
6. Call old live admin checkpoint compaction and require format 2, then inspect
   the object-store checkpoint and independently require format 2.
7. Scale the stopped old StatefulSet to zero and verify zero replicas.
8. Create the immutable successor bundle Secret and config-scoped resources.
   Each fresh successor restores the official object-store checkpoint and
   installs the predecessor certificate before opening the runtime.
9. Require every successor to report `awaiting_activation`, activate S+1 once,
   then poll every node until it reports `Active(S+1)`.
10. Only after Active(S+1), permit GC planning and application.

Run the guarded workflow with:

```bash
QUEQLITE_KUBE_CONTEXT=my-vind-context \
QUEQLITE_K8S_NAMESPACE=queqlite-e2e \
scripts/replace-k8s-config.sh config-c1.json config-c2-draft.json
```

The live-admin routes share the client listener but have a distinct bearer
token and operation contract. Defaults are under `/v1/admin`; path variables on
the script allow a staged API rename without weakening response validation.
Every Job has an active deadline and `backoffLimit: 0`. Poll loops are bounded;
elapsed time and sleeps never establish correctness. Only observed node,
checkpoint, StatefulSet, and object-store state advances the workflow. Any
missing, malformed, mismatched, or timed-out observation aborts the operation.

## Checkpoint And GC

Node-local checkpoint compaction is a live-admin operation. Object-wide
checkpoint inspection and GC use a short-lived Queqlite CLI Job with generic
object-store credentials. Examples:

```bash
scripts/k8s-object-job.sh 2 config-c2.json checkpoint inspect

plan_json="$(scripts/gc-k8s.sh plan config-c2.json)"
plan_hash="$(jq -r .plan_hash <<<"$plan_json")"
scripts/gc-k8s.sh inspect config-c2.json "$plan_hash"
QUEQLITE_GC_CONFIRM_PLAN_HASH="$plan_hash" \
  scripts/gc-k8s.sh apply config-c2.json "$plan_hash"
```

`gc plan` is non-destructive and persists an identity-bound plan. `gc inspect`
must retrieve the same 64-character lowercase SHA-256 hash. `gc apply` refuses
to run unless the operator supplies that exact hash both as the argument and in
`QUEQLITE_GC_CONFIRM_PLAN_HASH`; the CLI also requires `--confirm`. Plans remain
subject to generation retention, grace, and minimum-age policy. Never delete
objects by prefix or bypass the plan evidence: manifests, snapshots, suffixes,
and a concurrently referenced old generation can otherwise be lost.

## Vind E2E

The local harness requires Docker, `kubectl`, `vcluster` (vind), `jq`, `yq`, and
OpenSSL:

```bash
scripts/e2e-vind-rustfs.sh
```

It creates a fresh namespace and RustFS bucket, asserts zero PVCs, boots config
1, writes snapshot and suffix data, compacts to checkpoint V2, performs a 3-to-3
stop-and-replace, proves fresh `emptyDir` restore by missing local markers and
successful reads, then plans, inspects, and applies old-object GC with exact
hash confirmation after stopping publishers and observing lease expiry. It
restarts the three nodes and verifies the retained generation afterward.
Cleanup is automatic by default. Set
`QUEQLITE_VIND_CLEANUP=0` to retain the cluster for diagnostics.

The benchmark client can keep serving writes while one node is replaced by
opening all three node endpoints and retrying only transport failures and
retryable HTTP responses. The request body and `request_id` are unchanged on
every attempt, so the persistent idempotency record remains the correctness
boundary. To measure deletion of the preferred proposer (`ordinal 0`):

```bash
QUEQLITE_BENCH_MULTI_ENDPOINT=1 \
QUEQLITE_BENCH_RESOURCE_SAMPLING=0 \
QUEQLITE_DURABILITY_MODE=periodic \
QUEQLITE_DURABILITY_INTERVAL=1s \
scripts/bench-vind.sh \
  --duration 60s --warmup 5s --concurrency 4 --workload write \
  --fault pod-delete --fault-offset 10s --fault-pod queqlite-c1-0
```

RustFS remains an object-storage simulator in this harness. The fault command
targets only a Queqlite pod; it does not inject RustFS failures.

The implemented fast-path, microbatch, failover, and OSS cost results are in
[docs/failover-throughput-optimization-2026-07-12.md](docs/failover-throughput-optimization-2026-07-12.md).
The primary-source protocol conformance and performance-comparability limits are
in [docs/quepaxa-paper-conformance-2026-07-12.md](docs/quepaxa-paper-conformance-2026-07-12.md).

## Deferred Performance Tuning

MAB-based preferred-proposer and hedge-delay auto-tuning is deliberately **not
implemented**. Its safety boundary, bounded action space, fallback behavior,
telemetry, and staged rollout requirements remain documentation-only in
[docs/mab-leader-hedge-tuning.md](docs/mab-leader-hedge-tuning.md).
