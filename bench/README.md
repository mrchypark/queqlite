# Queqlite HTTP SQL benchmark

`queqlite-bench` is a standalone Rust package for reproducible load tests of the Queqlite HTTP SQL API. It deliberately lives outside the main workspace and only uses the already-present `reqwest`, `serde`, and `serde_json` dependencies.

## Run

Provide at least one endpoint and a client token. The benchmark creates its table unless `--skip-setup` is set.

```sh
cargo run --release --manifest-path bench/Cargo.toml -- \
  --endpoint http://127.0.0.1:7101 \
  --token "$QUEQLITE_CLIENT_TOKEN" \
  --duration 60s \
  --warmup 10s \
  --concurrency 16 \
  --workload mixed \
  --write-percent 50 > benchmark.json
```

`QUEQLITE_BENCH_ENDPOINT` (a comma-separated endpoint list) or `QUEQLITE_ENDPOINT`, plus `QUEQLITE_CLIENT_TOKEN` or `QUEQLITE_BENCH_TOKEN`, can replace the endpoint and token flags. `--endpoint` may be repeated to define deterministic preferred-first failover order; every request starts at the first endpoint and tries later endpoints only for retryable failures.

Use either concurrency-driven load (omit `--target-rate`) or an aggregate open-loop start rate:

```sh
cargo run --release --manifest-path bench/Cargo.toml -- \
  --endpoint http://127.0.0.1:7101 --token "$QUEQLITE_CLIENT_TOKEN" \
  --duration 30s --warmup 5s --concurrency 8 --target-rate 200 \
  --workload write
```

Durations accept `ms`, `s`, `m`, and `h` suffixes; a bare value means seconds. The default duration is `30s`, warmup is `5s`, concurrency is `1`, request timeout is `10s`, and fault-command timeout is `5m` (`--fault-timeout`).

## Workloads

- `read`: sends `SELECT request_id, value FROM <table> WHERE request_id = ?` to `/v1/sql/query`.
- `write`: sends a unique command request id and `INSERT INTO <table> ... RETURNING request_id, value` to `/v1/sql/execute`. A write counts as a committed transaction only when the execute response has an applied index and returns its own request id.
- `mixed`: deterministically selects writes according to `--write-percent` (default `50`).

The default table is `queqlite_bench`; use `--table` for an isolated name. It must be a simple ASCII SQL identifier. Setup creates `request_id TEXT PRIMARY KEY, value TEXT NOT NULL` through the same HTTP API.

## Fault Hooks

Run a shell command at an offset from the beginning of the measured window:

```sh
cargo run --release --manifest-path bench/Cargo.toml -- \
  --endpoint http://127.0.0.1:7101 --token "$QUEQLITE_CLIENT_TOKEN" \
  --duration 60s --concurrency 12 --workload mixed \
  --fault 20s leader-restart "kubectl rollout restart statefulset/queqlite"
```

The JSON report records the supplied fault tag, configured `offset_seconds`, actual `command_start_offset_seconds`, completion flag, exit status, and `succeeded`, `failed`, or `unfinished` status. Requests are also aggregated into `before`, `during`, and `after` windows: `during` starts when the command is invoked and ends when it returns. Traffic stops at the measurement deadline; the command may finish afterward, bounded by `--fault-timeout`. An unfinished command and its descendants are killed and reaped before the harness returns.

## JSON Output

The program prints one JSON report to stdout and diagnostic/setup errors to stderr. Tokens are never included. `measurement.totals` and each fault window provide:

- attempts, successes, error count and bounded error classes;
- successful committed transactions and committed transactions per configured second;
- an exponential microsecond latency histogram with p50, p95, and p99 estimates;
- configured and observed wall duration.

The command exits nonzero for invalid configuration, setup failures, failed fault commands, or fault commands that exceed `--fault-timeout`. The JSON report is printed before a fault failure is returned. Request failures, including expected failures during a fault scenario, are captured in the report so an interrupted-service run remains analyzable.

## Monthly Cost Calculator

`queqlite-cost` emits an itemized monthly USD estimate from retained GB-month,
object-call counts, and explicit egress input/rate. Its checked-in defaults are
[`rates-2026-07-12.json`](rates-2026-07-12.json), dated **2026-07-12**:

- AWS S3 Standard, `us-east-1`: `$0.023/GiB-month`, `$0.005/1k` PUT/LIST, `$0.0004/1k` GET.
- GCS Standard, `us-central1`: `$0.020/GiB-month`, with the GB-month input converted to GiB-month.
- Azure Blob Hot LRS, `eastus2`: `$0.0184/GiB-month`, with the same PUT/LIST/GET rates.
- DELETE is free in all defaults. RustFS local has zero call fees and requires its storage rate argument.

Egress defaults to zero and must be supplied as both a quantity and, when it is
not zero-cost, `--egress-usd-per-gb`.

```sh
cargo run --release --manifest-path bench/Cargo.toml --bin queqlite-cost -- \
  --provider aws-s3-standard-us-east-1 --retained-gb-month 100 \
  --put-count 2000 --list-count 1000 --get-count 1000 --delete-count 1000 \
  --egress-gb 10 --egress-usd-per-gb 0.09

cargo run --release --manifest-path bench/Cargo.toml --bin queqlite-cost -- \
  --provider rustfs-local --retained-gb-month 100 \
  --rustfs-storage-usd-per-gb-month 0.01
```

## vind Runner

`scripts/bench-vind.sh` creates a disposable vind cluster, deploys the existing
RustFS and three-node Queqlite manifests through their existing render helpers,
then writes an `artifacts.json` manifest. It cleans up by default.

```sh
scripts/bench-vind.sh --duration 60s --warmup 10s --concurrency 4 \
  --fault pod-delete --fault-offset 20s
```

The runner defaults to synchronous durability. Bounded and periodic runs must
provide their mode-specific positive duration; unrelated parameters are rejected:

```sh
QUEQLITE_DURABILITY_MODE=bounded QUEQLITE_DURABILITY_MAX_LAG=250ms \
  scripts/bench-vind.sh --duration 60s --workload write

QUEQLITE_DURABILITY_MODE=periodic QUEQLITE_DURABILITY_INTERVAL=2s \
  scripts/bench-vind.sh --duration 60s --workload write
```

Durability durations accept positive integer values with `ms`, `s`, `m`, or `h`
suffixes. `artifacts.json` records the selected mode and the applicable
`max_lag` or `interval` under `configuration.durability`; the rendered cluster
manifest records the same environment configuration.

`artifacts.json.provenance` binds the run to the Git commit and dirty state plus
the Docker image content ID and available repository digests. A dirty source
tree or missing immutable image ID leaves an ordinary local run usable but sets
`publishable: false` with a reason. Publication also requires a successful
benchmark and runner exit, no failed evidence collection, and verified cleanup;
`--keep` runs are therefore not publishable. Only artifacts marked
`publishable: true` support release or published performance evidence. A
skip-build run additionally requires the image's
`org.opencontainers.image.revision` label to match the exact Git commit.
The same provenance records the benchmark client SHA-256, Rust toolchain
versions, and normalized Kubernetes runtime image digests. Queqlite and RustFS
are always required; disabled object metering marks its nginx and AWS inventory
images `not_applicable` instead of requiring them. All three Queqlite pods must
run the same digest as the locally inspected image.

It applies fixed default resources to make comparisons controlled on the
8-core/24-GiB host: each Queqlite and RustFS container requests `250m` CPU and
`512Mi` memory, with `1000m` and `1Gi` limits. RustFS is only the local S3
simulator and its resources are reported separately from Queqlite. Override them through
`QUEQLITE_BENCH_{QUEQLITE,RUSTFS}_CPU_{REQUEST,LIMIT}` and
`QUEQLITE_BENCH_{QUEQLITE,RUSTFS}_MEMORY_{REQUEST,LIMIT}`. Resource JSONL
samples use containerd CRI stats and require all three Queqlite ordinals plus
RustFS and, when enabled, its object-meter sidecar. `resource-summary.json`
reports container-lifecycle CPU deltas plus average/peak memory using samples
inside, or immediately bracketing, the Rust-reported measurement window. A
pre-existing container uses its last pre-window counter as the baseline; a
container born in the window uses zero, and a same-identity counter regression
invalidates the evidence. Warmup and later cleanup samples are excluded.
Publishable evidence permits one missed collection plus one second of scheduling
jitter. The three-second collection timeout sends TERM and forces a kill one
second later, so the default continuity and final-coverage budget is twice the
two-second interval plus twice the four-second hard collection bound plus one
second (13 seconds). Final coverage may additionally finish the last hard-bounded
collection, for a 17-second wait budget. A pod-delete gap is accepted only for
the named Queqlite ordinal and only when it brackets the measured fault window
within the continuity budget; other missing component samples invalidate
publication. Disable resource sampling with
`QUEQLITE_BENCH_RESOURCE_SAMPLING=0`. A default-on nginx sidecar meters S3 method,
status, and byte counts, while an AWS CLI inventory records logical object count
and retained bytes in `object-usage.json`. Disable it with
`QUEQLITE_BENCH_OBJECT_USAGE_METERING=0`. The runner asserts that the deployment
uses zero PVCs. Its only fault hook deletes a Queqlite pod and waits for the
replacement to become Ready; it does not inject RustFS failures.

## Test

```sh
cargo test --manifest-path bench/Cargo.toml
```
