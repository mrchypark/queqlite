#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
script="$repo_root/scripts/e2e-vind-rustfs.sh"

require_literal() {
  literal="$1"
  grep -Fq -- "$literal" "$script" || {
    echo "missing recovery-matrix contract: $literal" >&2
    exit 1
  }
}

bash -n "$script"

require_literal 'RHIZA_E2E_RECOVERY_MATRIX:-0'
require_literal 'RHIZA_E2E_RECOVERY_MATRIX_ONLY:-0'
require_literal 'RHIZA_RECOVERY_HOLD_SECONDS:-60,180,300'
require_literal 'RHIZA_RECOVERY_FAIL_PEERS:-1,2,3'
require_literal 'RHIZA_VIND_DIRECT_CLUSTER:-0'
require_literal 'RHIZA_VIND_SKIP_IMAGE_LOAD:-0'
require_literal 'RHIZA_VIND_DIRECT_CLUSTER=1 requires RHIZA_VIND_CONTEXT'
require_literal 'rhiza.dev/e2e-run-id'
require_literal 'recovery-matrix.jsonl'
require_literal 'service_rto_seconds'
require_literal 'full_rto_seconds'
require_literal 'failure_injected_at'
require_literal 'all_target_pods_deleted_at'
require_literal 'quorum_lost_at'
require_literal 'failure_released_at'
require_literal 'ack_ledger'
require_literal 'old_pod_uids'
require_literal 'new_pod_uids'
require_literal 'ack_sentinel_preserved'
require_literal 'markers_lost'
require_literal 'pvc_count'
require_literal 'failure_write_expected'
require_literal 'failure_read_barrier_expected'
require_literal 'survivor_local_read'
require_literal 'tip_hashes_equal'
require_literal 'recovery_deadline_exceeded'
require_literal 'matrix_expect_write_no_quorum'
require_literal '(.code == "write_timeout" or .code == "unavailable")'
require_literal 'matrix_expect_read_barrier_unavailable'
require_literal 'matrix_expect_f2_read_barrier_timeout'
require_literal 'failure_read_barrier_actual_detail'
require_literal 'read_no_quorum_latency_defect'
require_literal 'survivor_ready" = True'
require_literal 'endpoint_count" = 1'
# shellcheck disable=SC2016
require_literal 'case "$exit_code" in 28)'
require_literal 'Operation timed out after [0-9]+ milliseconds with 0 bytes received'
# shellcheck disable=SC2016
require_literal '[ "$matrix_last_http_status" = 503 ]'
require_literal '.code == "unavailable" and .retryable == true'
# shellcheck disable=SC2016
require_literal 'matrix_http_target="${name_c1}-0.${name_c1}"'
require_literal 'matrix_expect_zero_endpoint_transport_failure'
require_literal 'endpoint_count" = 0'
# shellcheck disable=SC2016
require_literal 'case "$exit_code" in 7|28)'
require_literal 'idempotency_boundary_verified'
require_literal '.node.active_config_id'
require_literal 'matrix_run_f1_availability_probe'
require_literal 'failure_probe_interval_seconds=10'
require_literal 'matrix_emit_summary'
require_literal 'same_pod_restart_covered:false'
require_literal 'arbitrary_leader_failure_covered:false'
# shellcheck disable=SC2016
require_literal 'k scale statefulset "$name_c1" --replicas="$cell_survivors"'
# shellcheck disable=SC2016
require_literal 'k scale statefulset "$name_c1" --replicas=3'
# shellcheck disable=SC2016
require_literal '"$BASH" scripts/wait-k8s-statefulset-ready.sh'

wait_script="$repo_root/scripts/wait-k8s-statefulset-ready.sh"
# shellcheck disable=SC2016
grep -Fq 'resource_json statefulset "$name" | jq' "$wait_script" || {
  echo "readiness check must stream StatefulSet JSON into jq" >&2
  exit 1
}
# shellcheck disable=SC2016
if grep -Fq '<<< "$statefulset_json"' "$wait_script"; then
  echo "readiness check must not use a potentially blocking StatefulSet here-string" >&2
  exit 1
fi

echo "e2e recovery matrix static contract passed"
