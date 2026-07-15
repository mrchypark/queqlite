# rhiza

`rhiza` is a distributed database family ordered by QuePaxa. The agreed qlog
plus an official snapshot is authoritative; a local materialized database file
is not.

## Product Status

- `rhiza sql` uses SQLite as its materialized state machine.
- `rhiza graph` uses LadybugDB and replicates bounded semantic document
  put/delete commands.
- `rhiza kv` uses redb and replicates bounded byte-key put/delete commands.

All three profiles use the same QuePaxa ordering, qlog recovery, authenticated
HTTP service, remote checkpoint, and deployment lifecycle. The database files
are rebuildable materializations of the authoritative snapshot and qlog.

Each cluster uses exactly one execution profile: SQL, graph, or KV. Nodes in
one cluster do not mix materialization engines. QuePaxa is the consensus
technology brand; SQLite, LadybugDB, and redb are the respective local
materialization engines.

## Architecture

The Rust workspace is Kubernetes-independent. Its primary crates are:

- `rhiza`: primary embedded Rust facade and lifecycle owner.
- `rhiza-core`: log, configuration, command, and snapshot domain types.
- `rhiza-quepaxa`: recorder RPC, durable recorder state, and consensus.
- `rhiza-log`: local binary qlog and compaction anchors.
- `rhiza-obj-store`: `object_store` adapters for S3, GCS, Azure, and tests.
- `rhiza-sql`: the `rhiza sql` SQLite materialized-state boundary.
- `rhiza-graph`: the `rhiza graph` LadybugDB state boundary.
- `rhiza-kv`: the `rhiza kv` redb state boundary.
- `rhiza-archive`: checkpoint V2, object metadata, and GC plans.
- `rhiza-node`: runtime, HTTP RPC, recovery, and authenticated live admin HTTP.
- `rhiza-cli`: client and object-store administration commands.
- `rhiza-testkit`: shared integration-test support.

## Execution Profiles

Serving, checkpoint, recovery, GC, and offline membership commands require one
explicit profile:

```bash
export RHIZA_EXECUTION_PROFILE=sql # sql, graph, or kv
export RHIZA_CLUSTER_ID=cluster-a
```

One cluster runs exactly one profile. `RHIZA_CLUSTER_ID` is the logical name;
the runtime binds consensus and checkpoint identity to the selected profile so
SQL, graph, and KV nodes cannot accidentally join one another.

The production Docker image is built with all workspace features. The same
image serves any profile selected by `RHIZA_EXECUTION_PROFILE`.

## Embedded Rust API

The implemented embedded surface for `rhiza sql` is the `rhiza` crate.
`Rhiza` owns the node runtime and background workers;
cloneable `RhizaHandle` values are weak handles that stop working after
owner shutdown. Applications inject recorder and log transports without
configuring HTTP or Kubernetes:

```rust,no_run
use rhiza::{EmbeddedConfig, EmbeddedIdentity, Rhiza, ReadConsistency};

# async fn example(
#     recorders: Vec<(String, Box<dyn rhiza::RecorderRpc>)>,
# ) -> Result<(), rhiza::Error> {
let config = EmbeddedConfig::new(
    EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
    "./data/node-1",
    vec!["node-1".into(), "node-2".into(), "node-3".into()],
    recorders,
    vec![],
    None,
);
let owner = Rhiza::open(config).await?;
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
the runtime does not call Kubernetes APIs and receives no service-account
token.
Each configuration has its own profile-scoped headless Service and StatefulSet
named `rhiza-<profile>-c<config_id>`. Stable ordinals map to `node-1` through
`node-N`.
Membership accepts 3 through 7 members through a version-1 JSON bundle:

```json
{
  "version": 1,
  "config_id": 1,
  "members": [
    {
      "node_id": "node-1",
      "url": "http://rhiza-sql-c1-0.rhiza-sql-c1:8081",
      "log_url": "http://rhiza-sql-c1-0.rhiza-sql-c1:8080",
      "token": "secret"
    }
  ]
}
```

The bundle is mounted from an immutable Secret and selected with
`RHIZA_CONFIG_BUNDLE_FILE`. Peer tokens live inside the bundle Secret;
client/admin tokens and object-store credentials live in separate Secrets.
Never put any of those values in a ConfigMap.

### Recorder transport candidate

HTTP/JSON remains the default recorder transport. The opt-in
`tcp-postcard` candidate replaces the HTTP recorder listener for that process
and uses persistent, lane-separated plaintext TCP connections for QuePaxa
recorder calls. Rollback means restarting with the `http` selector; the two
recorder transports are not exposed together:

```bash
export RHIZA_RECORDER_TRANSPORT=tcp-postcard
export RHIZA_RECORDER_TCP_LISTEN=0.0.0.0:8082
```

Every bundle member must then include its in-cluster TCP address:

```json
{
  "node_id": "node-1",
  "url": "http://node-1:8081",
  "log_url": "http://node-1:8080",
  "recorder_tcp_addr": "node-1:8082",
  "token": "secret"
}
```

`tcp-postcard` provides no encryption or cryptographic peer authentication.
The HELLO token and all recorder traffic cross the network in plaintext; HELLO
still fences accidental node/configuration mismatches, but it is not a security
boundary. Use this transport only when the Kubernetes cluster network, nodes,
CNI, and workloads are inside one trusted boundary. The supplied renderer
accepts only the generated headless-Service DNS addresses and exposes port 8082
only on that cluster-internal Service; it does not create an Ingress, NodePort,
LoadBalancer, `hostPort`, or `hostNetwork` listener. Apply a namespace-level
default-deny NetworkPolicy in environments that run untrusted workloads.

The removed `tcp-tls-postcard` selector, TLS environment variables, and
`recorder_tls_server_name` bundle field are rejected instead of acting as
compatibility aliases. This transport remains a benchmark candidate, not the
production default; promotion requires the documented multi-host durability,
reconnect, rollback, and soak gates. Reintroduce an authenticated encrypted
channel before any cross-cluster, externally reachable, or multi-tenant use.

## rhiza sql API

`rhiza sql` executes admitted deterministic SQLite DDL and DML as replicated,
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
rhiza sql execute --url http://127.0.0.1:8080 \
  --request-id create-users \
  --sql 'CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)'

rhiza sql execute --url http://127.0.0.1:8080 \
  --request-id insert-ada \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":1},{"type":"text","value":"Ada"}]'

rhiza sql query --url http://127.0.0.1:8080 \
  --sql 'SELECT id, name FROM users WHERE id = ?1' \
  --params-json '[{"type":"integer","value":1}]' \
  --consistency read_barrier
```

Atomic multi-statement batches use the same authenticated JSON RPC directly.
The SQL runtime preflights a batch against the current agreed state before
proposing it, then deterministically replays the encoded SQL and parameters on
every node. SQL that can make replicas diverge or escape the state-machine
boundary is rejected: nondeterministic time/random/change-counter functions,
direct `__rhiza_*` access, `PRAGMA`, `ATTACH`/`DETACH`, TEMP objects, virtual
tables, and explicit transaction/savepoint control. Direct statement-replay
`RETURNING` is rejected. `RETURNING` is admitted only when a complete QEFX
changeset can be generated and validated. DDL, tables without a complete
supported primary key/schema, triggers, foreign keys, and indirect changes
cannot use QEFX; they fall back to deterministic statement replay only for
non-RETURNING requests. A
`RETURNING` request in any of those cases fails closed. Query responses are
bounded by server row and byte limits.

Recovery metadata uses QANC v3 and binds the recovery generation,
configuration state, snapshot identity, and executor fingerprint. A mismatch
is rejected during recovery rather than replayed best-effort.

The message-level comparison with Hiqlite and the bounded effect-replication
contract are documented in [`docs/hiqlite-sql-message-review.md`](docs/hiqlite-sql-message-review.md).

## rhiza graph and rhiza kv HTTP APIs

The selected profile controls the client routes exposed by `rhiza serve`.
Graph clusters expose:

- `POST /v1/graph/documents/put`
- `POST /v1/graph/documents/delete`
- `POST /v1/graph/documents/get`
- `POST /v1/graph/query`

KV clusters expose:

- `POST /v1/kv/put`
- `POST /v1/kv/delete`
- `POST /v1/kv/get`

Every request uses `x-rhiza-version: 1` and the client bearer token. Mutations
require a stable `request_id`. Reads accept `local`, `read_barrier`, or
`{"applied_index": N}` consistency. A read response returns the value,
`applied_index`, and qlog `hash` from one materializer boundary.

`/v1/graph/query` accepts one statement from the public Graph Query V1 subset:
`MATCH (v:RhizaDocument) [WHERE v.id = $string_param] RETURN 1..=4
property-or-parameter projections [LIMIT nonnegative-literal]`. `RETURN`
accepts only whitelisted properties or scalar parameters, without aliases. The
fixed properties are `id`, `kind`, `bool_value`, `i64_value`, `u64_value`,
`f64_value`, `string_value`, and `bytes_value`. Every supplied parameter must be
referenced exactly, parameter names must be ASCII identifiers, parameters must
be scalar, and an ID predicate parameter must be a string.

Other labels, literal/non-ID/compound predicates, literal or aliased
projections, whole-node projections, relationships and paths, multiple
patterns, collections, functions, operators, `DISTINCT`, subqueries, `UNWIND`,
`ORDER BY`, `SKIP`, parameterized `LIMIT`, writes, DDL, transaction control,
standalone `CALL`, and reserved `__Rhiza*` objects are rejected.

Graph queries support the same consistency modes and return typed columns and
rows with the applied qlog tip from one materializer boundary. Each column has
`name` and `logical_type`, and each row cell is a tagged scalar value. Queries
default to `max_rows: 1000` and accept at most 10,000, while the stricter
4-cell ceiling bounds returned rows multiplied by projections. A query accepts
at most 4 return projections. Result data is limited to 1 MiB and the encoded
response to 4 MiB. Graph writes remain the
bounded semantic document commands. Query grammar, admission, row, cell, and
byte limit violations return the normal non-retryable `400 invalid_request`
JSON error without changing readiness; malformed request JSON continues to use
`invalid_json`. Internal Ladybug, storage, connection, or state-corruption
errors return `500` and latch the node out of readiness.

Graph values are typed as `null`, `bool`, `i64`, `u64`, `f64`, `string`, or
`bytes`; graph byte values use padded base64. For example:

```bash
curl -sS http://127.0.0.1:8080/v1/graph/documents/put \
  -H 'x-rhiza-version: 1' \
  -H "Authorization: Bearer $RHIZA_CLIENT_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"request_id":"graph-1","id":"doc-1","value":{"type":"string","value":"hello"}}'

curl -sS http://127.0.0.1:8080/v1/graph/documents/get \
  -H 'x-rhiza-version: 1' \
  -H "Authorization: Bearer $RHIZA_CLIENT_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"id":"doc-1","consistency":"read_barrier"}'

curl -sS http://127.0.0.1:8080/v1/graph/query \
  -H 'x-rhiza-version: 1' \
  -H "Authorization: Bearer $RHIZA_CLIENT_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"statement":{"cypher":"MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.id, v.string_value LIMIT 1","parameters":{"id":{"type":"string","value":"doc-1"}}},"consistency":"read_barrier","max_rows":100}'
```

Graph Query V1 parameters and results use tagged scalar `null`, `bool`, `i64`,
`u64`, `f64`, `string`, and `bytes` values. Bytes use canonical padded base64.

KV keys and values are bytes encoded as canonical padded base64 in both
requests and responses. `a2V5` and `dmFsdWU=` below decode to `key` and
`value`:

```bash
curl -sS http://127.0.0.1:8080/v1/kv/put \
  -H 'x-rhiza-version: 1' \
  -H "Authorization: Bearer $RHIZA_CLIENT_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"request_id":"kv-1","key":"a2V5","value":"dmFsdWU="}'

curl -sS http://127.0.0.1:8080/v1/kv/get \
  -H 'x-rhiza-version: 1' \
  -H "Authorization: Bearer $RHIZA_CLIENT_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"key":"a2V5","consistency":{"applied_index":1}}'
```

## Storage Model

The profile-scoped StatefulSet deliberately has no `volumeClaimTemplates`.
Every SQL, graph, or KV pod uses `emptyDir` for `/var/lib/rhiza`. StatefulSet
identity is still useful:
it gives each ephemeral process a stable ordinal and DNS name while making a
replacement prove that recovery does not depend on an old local disk. A fresh
pod restores an official snapshot and then replays the exact qlog suffix.

This trades restart speed and object-store dependency for a smaller local
state-management surface. Production object storage must have an independent
failure domain and strong cross-process conditional writes. The local vind
RustFS Deployment also uses `emptyDir`; it simulates S3 compatibility but is
not production durability evidence.

The runtime uses the generic `object_store` boundary. The deployment
template is S3-shaped for RustFS, AWS S3, or another compatible endpoint;
runtime support also includes GCS and Azure Blob through provider configuration.
No provider or Kubernetes API appears in consensus logic.

Remote checkpoint V2 stores the selected engine snapshot as opaque bytes plus
its identity, applied index/hash, configuration state, and materializer
fingerprint. Restore validates that envelope, rebinds the materializer to the
target node for cross-node recovery, installs it in a fresh data directory,
and replays the exact committed suffix after the snapshot. SQL, LadybugDB, and
redb files are never treated as an independent recovery authority.

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
RHIZA_EXECUTION_PROFILE=sql \
  scripts/render-k8s-config.sh 1 3 config-c1.json target/rhiza-sql-c1.yaml
kubectl -n rhiza create secret generic rhiza-sql-c1-bundle \
  --from-file=config.json=config-c1.json --dry-run=client -o yaml \
  | yq eval '.immutable = true' - \
  | kubectl -n rhiza create -f -
kubectl -n rhiza create -f target/rhiza-sql-c1.yaml
```

Set `RHIZA_IMAGE`, `RHIZA_CLUSTER_ID`, `RHIZA_EPOCH`,
`RHIZA_RECOVERY_GENERATION`, `RHIZA_S3_*`, and Secret-name overrides as
needed. `RHIZA_EXECUTION_PROFILE` is required and must be `sql`, `graph`, or
`kv`. The renderer scopes resource names, labels, data/config paths, and bundle
DNS to that profile. The rendered resource uses `OnDelete`; do not mutate a
live config's pod template to reconfigure membership.

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
RHIZA_KUBE_CONTEXT=my-vind-context \
RHIZA_K8S_NAMESPACE=rhiza-e2e \
RHIZA_EXECUTION_PROFILE=sql \
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
checkpoint inspection and GC use a short-lived `rhiza` CLI Job with generic
object-store credentials. Examples:

```bash
export RHIZA_EXECUTION_PROFILE=graph
scripts/k8s-object-job.sh 2 config-c2.json checkpoint inspect

plan_json="$(scripts/gc-k8s.sh plan config-c2.json)"
plan_hash="$(jq -r .plan_hash <<<"$plan_json")"
scripts/gc-k8s.sh inspect config-c2.json "$plan_hash"
RHIZA_GC_CONFIRM_PLAN_HASH="$plan_hash" \
  scripts/gc-k8s.sh apply config-c2.json "$plan_hash"
```

`gc plan` is non-destructive and persists an identity-bound plan. `gc inspect`
must retrieve the same 64-character lowercase SHA-256 hash. `gc apply` refuses
to run unless the operator supplies that exact hash both as the argument and in
`RHIZA_GC_CONFIRM_PLAN_HASH`; the CLI also requires `--confirm`. Plans remain
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
`RHIZA_VIND_CLEANUP=0` to retain the cluster for diagnostics.

The benchmark client can keep serving writes while one node is replaced by
opening all three node endpoints and retrying only transport failures and
retryable HTTP responses. The request body and `request_id` are unchanged on
every attempt, so the persistent idempotency record remains the correctness
boundary. To measure deletion of the preferred proposer (`ordinal 0`):

```bash
RHIZA_BENCH_MULTI_ENDPOINT=1 \
RHIZA_BENCH_RESOURCE_SAMPLING=0 \
RHIZA_DURABILITY_MODE=periodic \
RHIZA_DURABILITY_INTERVAL=1s \
scripts/bench-vind.sh \
  --duration 60s --warmup 5s --concurrency 4 --workload write \
  --fault pod-delete --fault-offset 10s --fault-pod rhiza-sql-c1-0
```

RustFS remains an object-storage simulator in this harness. The fault command
targets only a `rhiza sql` pod; it does not inject RustFS failures.

The implemented fast-path, microbatch, failover, and OSS cost results are in
[docs/failover-throughput-optimization-2026-07-12.md](docs/failover-throughput-optimization-2026-07-12.md).
The primary-source protocol conformance and performance-comparability limits are
in [docs/quepaxa-paper-conformance-2026-07-12.md](docs/quepaxa-paper-conformance-2026-07-12.md).

## Deferred Performance Tuning

MAB-based preferred-proposer and hedge-delay auto-tuning is deliberately **not
implemented**. Its safety boundary, bounded action space, fallback behavior,
telemetry, and staged rollout requirements remain documentation-only in
[docs/mab-leader-hedge-tuning.md](docs/mab-leader-hedge-tuning.md).
