# Hiqlite 3-voter recovery drill

`scripts/e2e-hiqlite-recovery.sh` runs a focused zero-PVC recovery matrix against Hiqlite
v0.14.0. The default matrix is the cross product of one, two, and three failed voters with
60, 180, and 300 second failure holds: `f1-h60` through `f3-h300`.

Each cell starts and ends with three ready voters, `[1,2,3]` membership, a verified local and
consistent sentinel, no `HQL_BACKUP_RESTORE` environment, the original RustFS Pod UID, and zero
PVCs. The start boundary clears prior-cell sentinels before creating a cell-specific service
sentinel. F1 requires automatic learner-to-voter promotion and RPO 0. F2 expects fail-closed behavior
and uses operator backup recovery if automatic recovery does not occur. F3 always verifies a
completed external backup object before operator-triggered restoration.

Voter Pods use a zero-second termination grace period so a failure cell removes the selected
processes and their `emptyDir` data abruptly. Hold time starts only after every selected Pod has
disappeared and the old proxy connections have quiesced: write and consistent read must both fail,
and full loss also requires local read failure. Transition-window write ACKs are counted as evidence.
Rollout/deletion/connection-drain overhead is therefore excluded from the requested 60/180/300
seconds. The runner also stops scheduling a new four-probe sample when its aggregate timeout budget
could cross the requested recovery release, preventing no-quorum probe timeouts from inflating the
hold duration.

The host-side port-forward is treated as test infrastructure, not database availability. Before
every client operation the runner verifies that process, recreates it when necessary, and retries
once only when the port-forward itself died. This prevents host OOM kills from being counted as a
Hiqlite fail-closed result.

The default builds the exact clean commit `c8316c53799c509990475ea8e2aa2ef8679e070e` from
`HIQLITE_SOURCE_DIR`, or from a managed checkout when the directory is omitted. There is no assumed
official prebuilt image. To use a prebuilt image explicitly, set both `HIQLITE_BUILD_IMAGE=0` and
`HIQLITE_RECOVERY_IMAGE=<reference>`. Its resolved digest is recorded, while its source commit is
honestly recorded as unverified rather than inferred from its tag.

The pinned upstream commit has no checked-in `Cargo.lock`. The harness first verifies the source
checkout is clean and exact, archives that commit into an isolated build context, generates a
lockfile there, and records the generated lockfile's SHA-256 and origin. The Docker build then uses
`cargo build --locked`; the verified checkout itself remains unchanged.

For split long-running cells, `HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES=1` skips only the repeated
Docker build after re-verifying the exact clean source checkout. It requires explicit expected voter
and proxy image IDs plus the previously recorded generated-lockfile SHA-256, and aborts on any
content-ID mismatch.

When reusing an already isolated vcluster whose node has those exact images, set
`HIQLITE_RECOVERY_SKIP_IMAGE_LOAD=1` to avoid another host-side image archive/import cycle.

If the standalone vcluster driver is unavailable, `HIQLITE_RECOVERY_DIRECT_CLUSTER=1` runs against
the explicitly selected Kubernetes context. Use a dedicated local kubeconfig. The runner creates
only its two managed namespaces and deletes them only after both the managed label and run ID match;
it never loads images or mutates cluster-wide resources in this mode.

Hiqlite v0.14.0's built-in application proxy cannot start unchanged: its metrics route uses removed
Axum colon-parameter syntax, and its stream route omits the raft-type path sent by the same release's
client. A transparent L7 proxy is not equivalent because Hiqlite's proxy decodes and forwards its
Client API protocol. The harness therefore keeps every voter on the exact upstream image and builds
a separate proxy-only image with the auditable two-route patch
`hiqlite-proxy-axum8.patch`. Evidence records the patch SHA-256, proxy image, and upstream defect
separately from exact voter provenance.

The recovery client deliberately enables Hiqlite's `full` feature set. Hiqlite 0.14.0's Client API
uses a feature-gated bincode wire schema, while the standalone server's `server` feature expands to
`full`. A SQL-only remote client can compile from the same commit yet serialize a different enum
layout, so matching the server feature set is part of the protocol contract for this drill.

Run the static contract without creating a cluster:

```sh
scripts/check-e2e-hiqlite-recovery-static.sh
```

Run the live matrix only on a host with enough disk for the images and Rust client build:

```sh
scripts/e2e-hiqlite-recovery.sh
```

`HIQLITE_RECOVERY_HOLD_SECONDS` accepts one to three distinct non-negative integers and applies
them to every selected failure count. The default is the full 60/180/300 set. `recovery.jsonl`
records probes and cell summaries; `summary.json` is accepted only when it contains exactly the
selected unique cells in deterministic failure-count/hold order.
Set `HIQLITE_RECOVERY_FAIL_PEERS` to one or more unique values from `1,2,3` to split a resource-heavy
matrix into independently cleaned runs; the default remains the complete nine-cell cross product.
