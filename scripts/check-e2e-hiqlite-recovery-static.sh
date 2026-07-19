#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script="$repo_root/scripts/e2e-hiqlite-recovery.sh"
client_dir="$repo_root/bench/hiqlite-recovery-client"
cluster_manifest="$repo_root/deploy/k8s/hiqlite-recovery-cluster.yaml"
object_manifest="$repo_root/deploy/k8s/hiqlite-recovery-rustfs.yaml"
readme="$client_dir/README.md"

require_literal() {
  file="$1"
  literal="$2"
  grep -Fq -- "$literal" "$file" || {
    echo "missing Hiqlite recovery contract in ${file#"$repo_root"/}: $literal" >&2
    exit 1
  }
}

for file in \
  "$script" \
  "$client_dir/Cargo.toml" \
  "$client_dir/src/main.rs" \
  "$client_dir/Dockerfile.server" \
  "$readme" \
  "$cluster_manifest" \
  "$object_manifest"; do
  test -f "$file" || {
    echo "missing Hiqlite recovery artifact: ${file#"$repo_root"/}" >&2
    exit 1
  }
done

bash -n "$script"
yq eval '.' "$cluster_manifest" "$object_manifest" >/dev/null

require_literal "$script" 'c8316c53799c509990475ea8e2aa2ef8679e070e'
require_literal "$script" 'HIQLITE_SOURCE_DIR'
require_literal "$script" 'HIQLITE_BUILD_IMAGE:-1'
require_literal "$script" 'HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES:-0'
require_literal "$script" 'HIQLITE_RECOVERY_SKIP_IMAGE_LOAD:-0'
require_literal "$script" 'HIQLITE_RECOVERY_DIRECT_CLUSTER:-0'
require_literal "$script" 'direct_namespaces_created'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'owner="$(kubectl --context "$context" get namespace "$candidate"'
require_literal "$script" 'HIQLITE_RECOVERY_EXPECTED_LOCAL_IMAGE_ID'
require_literal "$script" 'HIQLITE_RECOVERY_EXPECTED_LOCAL_PROXY_IMAGE_ID'
require_literal "$script" 'verified-local-exact-source-reuse'
require_literal "$script" 'HIQLITE_BUILD_IMAGE=0 requires an explicit HIQLITE_RECOVERY_IMAGE'
require_literal "$script" 'image_source=user-supplied-prebuilt'
require_literal "$script" 'source_commit_basis=user-supplied-unverified'
require_literal "$script" 'image_source_commit=""'
# These assertions intentionally match unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'git -C "$source_dir" archive "$hiqlite_commit" | tar -x -C "$build_source_dir"'
# shellcheck disable=SC2016
require_literal "$script" 'cargo generate-lockfile --manifest-path "$build_source_dir/Cargo.toml"'
require_literal "$script" 'lockfile_origin=generated-from-exact-source'
require_literal "$script" 'cargo_lock_sha256'
require_literal "$script" 'ingress_kind=hiqlite-application-proxy'
require_literal "$script" 'hiqlite-proxy-axum8.patch'
require_literal "$script" 'proxy_patch_sha256'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'patch --directory "$proxy_build_source_dir" --strip=1 < "$proxy_patch_file"'
require_literal "$script" 'upstream_proxy_incompatibility='
require_literal "$script" 'omits the stream raft-type path'
require_literal "$script" 'resolved_image'
require_literal "$script" 'image_source'
require_literal "$script" 'source_commit_basis'
require_literal "$script" 'HIQLITE_RECOVERY_HOLD_SECONDS:-60,180,300'
require_literal "$script" 'HIQLITE_RECOVERY_FAIL_PEERS:-1,2,3'
require_literal "$script" 'HIQLITE_RECOVERY_QUORUM_LOSS_TIMEOUT_SECONDS:-60'
require_literal "$script" 'wait_failure_established'
require_literal "$script" 'transient_write_acks'
require_literal "$script" 'failure-established'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'probe_budget=$((probe_timeout * 4))'
require_literal "$script" 'ensure_port_forward'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'if ! kill -0 "$port_forward_pid"'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'for failure_count in "${failure_values[@]}"'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'for hold_seconds in "${hold_values[@]}"'
require_literal "$script" "cell_id=\"f1-h\$1\""
require_literal "$script" "cell_id=\"f2-h\$1\""
require_literal "$script" "cell_id=\"f3-h\$1\""
# These assertions intentionally match unexpanded jq source.
# shellcheck disable=SC2016
require_literal "$script" '($phases | length) == $expected_cell_count'
# shellcheck disable=SC2016
require_literal "$script" '($phases | map(.cell_id) | unique | length) == $expected_cell_count'
require_literal "$script" '(.hold_seconds | type) == "number"'
require_literal "$script" '(.failure_held_seconds >= .hold_seconds)'
require_literal "$script" '(.cell_id == "f\(.failure_count)-h\(.hold_seconds)")'
require_literal "$script" 'log_sync=Immediate'
require_literal "$script" 'recovery.jsonl'
require_literal "$script" 'summary.json'
require_literal "$script" 'service_rto_seconds'
require_literal "$script" 'full_rto_seconds'
require_literal "$script" 'failure_started_at'
require_literal "$script" 'failure_released_at'
require_literal "$script" 'expected_vs_observed'
# This assertion intentionally matches unexpanded shell source.
# shellcheck disable=SC2016
require_literal "$script" 'k scale statefulset/hiqlite-recovery --replicas="$survivors"'
require_literal "$script" 'k scale statefulset/hiqlite-recovery --replicas=3'
require_literal "$script" 'k scale statefulset/hiqlite-recovery --replicas=0'
require_literal "$script" 'HQL_BACKUP_RESTORE'
require_literal "$script" 'clear_restore_from_running_pods'
require_literal "$script" 'rustfs_uid'
require_literal "$script" 'markers_lost'
require_literal "$script" 'capture_learner_to_voter_evidence'
require_literal "$script" 'assert_probe_outcome'
require_literal "$script" 'voter_ids'
require_literal "$script" 'ack_sentinel_preserved'
require_literal "$script" 'rpo_to_backup'

require_literal "$client_dir/Cargo.toml" 'rev = "c8316c53799c509990475ea8e2aa2ef8679e070e"'
require_literal "$client_dir/Cargo.toml" 'default-features = false, features = ["full"]'
require_literal "$client_dir/Cargo.toml" '[workspace]'
require_literal "$client_dir/src/main.rs" 'Client::remote(args.nodes, false, false, args.secret, true, None, None)'
require_literal "$client_dir/Dockerfile.server" 'cargo build --locked --features server --release'
require_literal "$client_dir/Dockerfile.server" 'id=hiqlite-recovery-cargo-registry'
require_literal "$client_dir/Dockerfile.server" 'id=hiqlite-recovery-cargo-target'
require_literal "$client_dir/Dockerfile.server" 'install -D -m 0755 /work/target/release/hiqlite /out/hiqlite'
require_literal "$client_dir/Dockerfile.server" 'COPY --from=builder /out/hiqlite /app/hiqlite'
require_literal "$readme" 'f1-h60'
require_literal "$readme" 'f3-h300'
require_literal "$readme" 'feature-gated bincode wire schema'

if grep -Fq 'ghcr.io/sebadob/hiqlite' "$script" "$readme"; then
  echo "Hiqlite recovery harness references a nonexistent official image" >&2
  exit 1
fi
for command in execute reset query-local query-consistent backup metrics verify-sentinel; do
  require_literal "$client_dir/src/main.rs" "$command"
done

test "$(yq eval 'select(.kind == "StatefulSet") | .spec.replicas' "$cluster_manifest")" = 3
test "$(yq eval 'select(.kind == "StatefulSet") | .spec.updateStrategy.type' "$cluster_manifest")" = OnDelete
test "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec.terminationGracePeriodSeconds' "$cluster_manifest")" = 0
test "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec.volumes[] | select(.name == "data") | has("emptyDir")' "$cluster_manifest")" = true
test "$(yq eval 'select(.kind == "StatefulSet") | has("volumeClaimTemplates")' "$cluster_manifest")" = false
test "$(yq eval -r 'select(.kind == "StatefulSet") | .spec.template.spec.containers[] | select(.name == "hiqlite") | .env[] | select(.name == "HQL_LOG_SYNC") | .value' "$cluster_manifest")" = immediate
test "$(yq eval -r 'select(.kind == "StatefulSet") | .spec.template.spec.containers[] | select(.name == "hiqlite") | .imagePullPolicy' "$cluster_manifest")" = Never
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .image' "$cluster_manifest")" = __INGRESS_IMAGE__
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .imagePullPolicy' "$cluster_manifest")" = Never
test "$(yq eval -o=json 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .args' "$cluster_manifest" | jq -c .)" = '["proxy","--config-file","/dev/null","--log-level","debug"]'
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .readinessProbe.httpGet.path' "$cluster_manifest")" = /ping
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "hiqlite-recovery-proxy") | .spec.template.spec.containers[] | select(.name == "proxy") | .env[] | select(.name == "HQL_SECRET_API") | .value' "$cluster_manifest")" = __SECRET_API__
test "$(yq eval -r 'select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.volumes[] | select(.name == "data") | has("emptyDir")' "$object_manifest")" = true

if grep -Eq '(^|[[:space:]])(kind:[[:space:]]*PersistentVolumeClaim|volumeClaimTemplates:)' \
  "$cluster_manifest" "$object_manifest"; then
  echo "Hiqlite recovery drill must remain zero-PVC" >&2
  exit 1
fi

# jq expands these variables; the shell must preserve them literally.
# shellcheck disable=SC2016
summary_contract='
  .phases as $phases |
  ($phases | length) == 9 and
  ($phases | map(.cell_id)) == $expected_cells and
  ($phases | map(.cell_id) | unique | length) == 9 and
  ($phases | all(
    (.failure_count | type) == "number" and
    (.hold_seconds | type) == "number" and
    (.hold_seconds >= 0) and
    (.failure_held_seconds | type) == "number" and
    (.failure_held_seconds >= .hold_seconds) and
    (.phase == "f\(.failure_count)") and
    (.cell_id == "f\(.failure_count)-h\(.hold_seconds)")
  ))
'
expected_cells='["f1-h60","f1-h180","f1-h300","f2-h60","f2-h180","f2-h300","f3-h60","f3-h180","f3-h300"]'
valid_summary="$(jq -cn '
  {phases:[
    range(1;4) as $failure |
    [60,180,300][] as $hold |
    {phase:"f\($failure)",cell_id:"f\($failure)-h\($hold)",failure_count:$failure,
      hold_seconds:$hold,failure_held_seconds:$hold}
  ]}
')"
jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$valid_summary" >/dev/null
duplicate_summary="$(jq '.phases[8].cell_id = .phases[0].cell_id' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$duplicate_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a duplicate cell" >&2
  exit 1
fi
missing_hold_summary="$(jq 'del(.phases[8].hold_seconds)' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$missing_hold_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a missing hold duration" >&2
  exit 1
fi
mismatched_hold_summary="$(jq '.phases[0].hold_seconds = 61' <<< "$valid_summary")"
if jq -e --argjson expected_cells "$expected_cells" "$summary_contract" \
  <<< "$mismatched_hold_summary" >/dev/null; then
  echo "Hiqlite recovery summary contract accepted a mismatched hold duration" >&2
  exit 1
fi

echo "Hiqlite recovery static contract passed"
