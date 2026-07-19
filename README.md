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

The supported container image variants are built from the same `rhiza` binary
with different Cargo feature sets:

| Artifact | Docker build argument | Cargo feature selection |
| --- | --- | --- |
| `rhiza-sql` | `RHIZA_PROFILE=sql` | `--no-default-features --features sql` |
| `rhiza-graph` | `RHIZA_PROFILE=graph` | `--no-default-features --features graph` |
| `rhiza-kv` | `RHIZA_PROFILE=kv` | `--no-default-features --features kv` |
| `rhiza-all` | `RHIZA_PROFILE=all` | `--all-features` |

`rhiza-sql`, `rhiza-graph`, and `rhiza-kv` are the default release and
deployment matrix. `rhiza-all` is a convenience artifact for environments
that deliberately need one combined image; it does not change the one-profile
per-cluster contract or remove the explicit `RHIZA_EXECUTION_PROFILE`
requirement.

CI builds and validates all four variants without registry credentials. Image
publication and registry tags remain a separate release operation.

One parameterized Dockerfile builds all four artifacts:

```bash
docker build --build-arg RHIZA_PROFILE=sql -t rhiza-sql:dev .
docker build --build-arg RHIZA_PROFILE=graph -t rhiza-graph:dev .
docker build --build-arg RHIZA_PROFILE=kv -t rhiza-kv:dev .
docker build --build-arg RHIZA_PROFILE=all -t rhiza-all:dev .
```

Plain `docker build -t rhiza-all:dev .` defaults to the combined `all` build.
For normal deployment, select the scoped image matching the required profile;
for example, use `RHIZA_IMAGE=rhiza-sql:dev` together with
`RHIZA_EXECUTION_PROFILE=sql`.

Local Cargo builds that enable `graph` or `--all-features` also enable the
Ladybug custom-filesystem API and therefore require the Ladybug source pinned
by CI and the Dockerfile. Check out that revision once and set the source-build
environment for graph/all commands only:

```bash
git clone --branch feature/bitpacking-int64-min --single-branch \
  https://github.com/mrchypark/ladybug.git ladybug-source
git -C ladybug-source checkout f95c0700b841fad79a842b819a7b1721b53569b3
export LBUG_SOURCE_DIR="$PWD/ladybug-source"
export LBUG_RUST_BUILD_FROM_SOURCE=1
cargo build -p rhiza-cli --no-default-features --features graph
# Or: cargo build -p rhiza-cli --all-features
```

SQL-only and KV-only Cargo builds do not need this checkout or environment.

## Embedded Rust API

The `rhiza` crate exposes the SQL, graph, and KV profiles through one embedded
owner. Its default feature set is SQL-only; graph and KV are explicit opt-ins:

| Cargo features | Embedded profiles |
| --- | --- |
| default or `--no-default-features` | SQL |
| `--features graph` | SQL and graph |
| `--features kv` | SQL and KV |
| `--all-features` | SQL, graph, and KV |

`Rhiza` owns the node runtime and background workers;
cloneable `RhizaHandle` values are weak handles that stop working after
owner shutdown. Applications inject recorder and log transports without
configuring HTTP or Kubernetes:

```rust,no_run
use rhiza::{EmbeddedConfig, EmbeddedIdentity, ExecutionProfile, Rhiza, ReadConsistency};

# async fn example(
#     recorders: Vec<(String, Box<dyn rhiza::RecorderRpc>)>,
# ) -> Result<(), rhiza::Error> {
let config = EmbeddedConfig::new(
    EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
    "./data/node-1",
    ExecutionProfile::Sqlite,
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

For the SQL profile, `execute_sql` and `query` expose typed SQL, `RETURNING`,
consistency, and persistent idempotency. With the corresponding crate features,
graph profiles expose `mutate_graph` and `query_graph`; KV profiles expose
`put_kv`, `delete_kv`, `get_kv`, `scan_kv_range`, and `scan_kv_prefix`. Every
method checks the configured `ExecutionProfile`. HTTP routes and the CLI are
secondary adapters over the same node service contracts.

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

Set `RHIZA_RECORDER_TLS=on` to use the server-authenticated TLS 1.3 variant of
the same framed Postcard protocol. TLS is off by default and does not fall back
to plaintext when enabled. In addition to `RHIZA_RECORDER_TCP_LISTEN`, it
requires readable certificate, private-key, and CA-bundle files:

```bash
export RHIZA_RECORDER_TRANSPORT=tcp-postcard
export RHIZA_RECORDER_TLS=on
export RHIZA_RECORDER_TCP_LISTEN=0.0.0.0:8082
export RHIZA_RECORDER_TLS_CERT_FILE=/run/secrets/rhiza/recorder-tls/tls.crt
export RHIZA_RECORDER_TLS_KEY_FILE=/run/secrets/rhiza/recorder-tls/tls.key
export RHIZA_RECORDER_TLS_CA_FILE=/run/secrets/rhiza/recorder-tls/ca-bundle.pem
```

Every bundle member must also set `recorder_tls_server_name` to the DNS name in
that member's certificate SAN. The Kubernetes renderer takes
`RHIZA_RECORDER_TLS_SECRET`, mounts its `tls.crt`, `tls.key`, and
`ca-bundle.pem` keys, and uses the exact ordinal headless-Service DNS names.
Because all Pods in one StatefulSet mount the same Secret, its server
certificate must cover every ordinal member name in that configuration.
Set `RHIZA_RECORDER_TLS=off` (the default) for plaintext TCP/Postcard; TLS
files, TLS server names, or a TLS Secret are rejected in that mode. TLS cannot
be enabled with the HTTP transport, and the legacy `tcp-tls-postcard` transport
value is rejected so conflicting settings fail closed.

This is server-authenticated TLS, not mTLS. The encrypted HELLO exchange still
authenticates callers with configured peer tokens. It protects RecorderRpc
only; public APIs and log-fetch URLs keep their separately configured HTTP
security contract. HTTP/JSON remains the production default, and promotion of
either TCP variant still requires the documented multi-host durability,
reconnect, rollback, and soak gates.

## rhiza sql API

`rhiza sql` executes admitted deterministic SQLite DDL and DML as replicated,
idempotent command batches. Every `/v1/sql/execute` request has a stable request
ID; all statements in the request run in one SQLite transaction and either all
apply at the agreed qlog index or none do. QSQL v2 returns a typed result for
each statement, including statement-level `rows_affected` and bounded typed
`RETURNING` rows. The result is persisted with the request ID, so an exact retry
replays the original result rather than executing the SQL again.

`/v1/sql/query` accepts one read-only statement and supports `local`, applied-
index, and quorum read-barrier consistency. QSQL v2 is only the client request
encoding. Replication uses QWAL v1: the request runs once on an exact-base
staging copy, and the qlog carries canonical final page images bound to the
base index, hash, database digest, request digest, persisted result, and
executor fingerprint. If another command wins the proposed slot, the effect
is regenerated from the new base. Inline effects are capped at 256 KiB.

Read-only SQL runs only against the selected local materialization, so it may
use nondeterministic and runtime-introspection functions such as `random()`,
`datetime('now')`, and `sqlite_version()`. Replicated writes may also use
nondeterministic SQLite functions because only the winning staging result is
replicated; followers never execute the SQL again.
Read execution is interrupted after five seconds; a timeout returns retryable
`503 resource_exhausted`, releases the SQLite connection, and does not change
node readiness.

Queries otherwise support SQLite's broad read families, including standalone
`VALUES`, `SELECT`/`EXPLAIN QUERY PLAN`, recursive CTEs, window functions, and
JSON scalar and table-valued functions. Direct `PRAGMA` queries are limited to
observational names: `foreign_key_check`, `foreign_key_list`, `index_info`,
`index_list`, `index_xinfo`, `integrity_check`, `quick_check`, `table_info`,
`table_list`, and `table_xinfo` may take an argument; `application_id`,
`collation_list`, `compile_options`, `data_version`, `encoding`,
`freelist_count`, `function_list`, `module_list`, `page_count`, `pragma_list`,
`schema_version`, and `user_version` are no-argument only. Assignments,
`database_list`, and other unlisted pragmas are rejected.

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
proposing it, then replicates the resulting QWAL page effect. DDL, triggers,
foreign-key cascades, ROWID/AUTOINCREMENT behavior, nondeterministic functions,
indirect changes, and bounded `RETURNING` results are supported. Operations
that escape the state-machine boundary remain rejected: direct `__rhiza_*`
access, replicated-write `PRAGMA`, `ATTACH`/`DETACH`, TEMP objects, virtual
tables, extension loading, and explicit transaction/savepoint control. Query
and `RETURNING` responses are bounded by server row and byte limits.

SQLite storage is QWAL-only. A canonical user database must be paired with its
mandatory `.control` sidecar; legacy `__rhiza_meta` databases and old
QSQL/QEFX/qlog histories are not upgraded or dual-decoded. Upgrade by stopping
the old writers and bootstrapping every voter from one trusted QSNP snapshot.
The recording VFS currently runs in staging shadow/audit mode, while full
closed-file page diff and target digest verification remain the correctness
path.

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
- `POST /v1/kv/scan`

Every request uses `x-rhiza-version: 1` and the client bearer token. Mutations
require a stable `request_id`. Reads accept `local`, `read_barrier`, or
`{"applied_index": N}` consistency. A read response returns the value,
`applied_index`, and qlog `hash` from one materializer boundary.

`/v1/graph/query` accepts one labeled read-only Cypher statement supported by
the bundled LadybugDB engine. This includes labeled joins, aliases,
expressions, scalar functions, aggregates, bounded collections, whole nodes,
relationships, `DISTINCT`, `UNWIND`, `ORDER BY`, `SKIP`, and literal or
parameterized `LIMIT` where the referenced schema supports them. Supplied typed parameters must exactly match
the referenced parameters and may contain bounded scalar, list, or struct
values. Mutations, DDL, transaction control, standalone administrative calls,
external I/O, multiple statements, and the reserved `__Rhiza*` namespace are
rejected. Every node pattern must name a static, non-reserved label: LadybugDB
0.18.1 has no per-connection table ACL that could otherwise keep unlabeled
patterns from scanning rhiza's internal nodes. LadybugDB's prepared-statement
read-only classification is the final admission check.

Because LadybugDB 0.18.1 exposes no per-query memory or nested-value
cardinality cap, container-producing expressions are admitted only when rhiza
can prove their cumulative expansion size before execution. List/map literals,
bounded parameters, statically sized `repeat()`, and `range()` with integer
literal or integer parameter bounds are supported. Repeated parameter
references count separately, and projected expansions are multiplied by the
bounded result-row count against the same 1 MiB budget. Unbounded or
oversized `range()`, `collect()`, list comprehensions, and functions that
produce lists/maps from runtime data are rejected before LadybugDB execution.
Padding functions must have a statically bounded length; multiplicative
replacement functions are rejected. Direct node and relationship results
remain supported.

Every top-level `UNION` branch must contain exactly one explicit bounded
`LIMIT`, and the sum of branch limits must not exceed `max_rows`. A non-`UNION`
query is bounded by the server even when it omits `LIMIT`. Queries default to
`max_rows: 1000` and accept at most 10,000 rows; query text is limited to 64
KiB, serialized result data to 1 MiB, the encoded response to 4 MiB, and
execution to the 5-second server timeout. LadybugDB uses a shared 512 MiB
buffer pool with at most two query execution threads. The buffer pool bounds
engine-managed pages across the database; it is not a 1 MiB per-query or total
process RSS limit. There is no separate projection-count or result-cell limit.

Graph queries support the same consistency modes and return typed columns and
rows with the applied qlog tip from one materializer boundary. Result values
preserve Ladybug logical types, including nested collections and graph
node/relationship values. Graph writes remain bounded semantic document
commands. Admission, row, or byte limit violations return the normal
non-retryable `400 invalid_request` JSON error without changing readiness;
malformed request JSON continues to use `invalid_json`. Internal Ladybug,
storage, connection, or state-corruption errors return `500` and latch the node
out of readiness. Ladybug query timeout/interruption and buffer-pool exhaustion
return retryable `503 resource_exhausted` without changing readiness.

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
  -d '{"statement":{"cypher":"MATCH (v:RhizaDocument) WHERE v.id IN $ids RETURN v.id AS id, upper(v.string_value) AS value ORDER BY v.id LIMIT 10","parameters":{"ids":{"type":"list","value":[{"type":"string","value":"doc-1"}]}}},"consistency":"read_barrier","max_rows":100}'

rhiza graph query --url http://127.0.0.1:8080 \
  --cypher 'MATCH (v:RhizaDocument) RETURN v.id AS id ORDER BY v.id LIMIT 10' \
  --consistency read_barrier --max-rows 100
```

Graph parameters and results use tagged typed values. Bytes use canonical
padded base64.

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

rhiza kv scan --url http://127.0.0.1:8080 \
  --prefix-base64 a2V5 --limit 100 --consistency read_barrier
```

KV scan accepts either a prefix or a half-open range (`start` with optional
`end`) and returns ordered entries plus an opaque `next_cursor`. The default
page size is 100 and the maximum is 1,024 entries. A page is also capped at 1
MiB of combined raw key/value bytes and 2 MiB after JSON encoding. Entries and
the applied qlog tip are observed from one materializer boundary; continue a
scan by sending the returned cursor with the same prefix or range.

## Write batching and the Recorder fast path

The HTTP writer queue coalesces concurrent requests for up to eight members or
500 microseconds by default. Graph and KV members are encoded as one canonical,
ordered replicated batch, committed in one qlog entry, and applied atomically
while retaining an independent request ID and retry result for every member.
Oversized batches fall back to individual proposals.

On the ordinary QuePaxa fast path, the preferred proposal can be decided after
the phase-zero Recorder quorum; the command is piggybacked on the typed Record
request and persisted before a Recorder acknowledges it. Combining that path
with Graph/KV batching structurally reduces consensus proposals, qlog appends,
and materializer synchronization boundaries per request under concurrency. It
does not remove network, storage, checkpoint, or durability work, and the
batching window can add latency at light load; no fixed throughput or latency
claim follows from the structure alone.

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

The renderer derives the local image default from the required profile (for
example, `RHIZA_EXECUTION_PROFILE=sql` defaults to `rhiza-sql:dev`). Set
`RHIZA_IMAGE` to override it with a registry-qualified artifact and tag. Also
set `RHIZA_CLUSTER_ID`, `RHIZA_EPOCH`,
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
The current Recorder durability, typed-batch, and production-adapter transport
evidence is in
[docs/performance-optimization-2026-07-17.md](docs/performance-optimization-2026-07-17.md).
Its Linux WAL syscall comparison is reproducible with
[`bench/run-recorder-sync-linux.py`](bench/run-recorder-sync-linux.py) and
[`bench/support/fdatasync-as-fsync.c`](bench/support/fdatasync-as-fsync.c); the
auditable 12-pair artifacts are tracked as
[`raw.jsonl`](docs/benchmarks/recorder-sync-linux-20260717/raw.jsonl) and
[`summary.json`](docs/benchmarks/recorder-sync-linux-20260717/summary.json).
The 24-row raw artifact is about 48.6 KiB and the summary is about 9.4 KiB. That
run used a dirty worktree and Docker Desktop's virtual filesystem, so the
summary sets `production_valid=false`. Native `fdatasync` had 1.561x aggregate
median throughput and lower aggregate p50/p95/p99. However, the paired
`fsync/native` median was 0.928 and the win split was 6/12 each, so paired
performance remains inconclusive. Linux `sync_data` remains a
correctness-preserving candidate for the smaller durability syscall, not a
production speedup claim. Production adoption requires clean physical
crash/reopen and throughput/latency validation on the target
ext4/XFS/Kubernetes CSI stack.
The primary-source protocol conformance and performance-comparability limits are
in [docs/quepaxa-paper-conformance-2026-07-12.md](docs/quepaxa-paper-conformance-2026-07-12.md).

## Deferred Performance Tuning

MAB-based preferred-proposer and hedge-delay auto-tuning is deliberately **not
implemented**. Its safety boundary, bounded action space, fallback behavior,
telemetry, and staged rollout requirements remain documentation-only in
[docs/mab-leader-hedge-tuning.md](docs/mab-leader-hedge-tuning.md).
