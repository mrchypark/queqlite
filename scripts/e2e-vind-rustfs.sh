#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
cluster="${QUEQLITE_VIND_CLUSTER:-queqlite-vind-${run_id}}"
namespace="${QUEQLITE_K8S_NAMESPACE:-queqlite-e2e}"
image="${QUEQLITE_IMAGE:-queqlite:dev}"
rustfs_image="${QUEQLITE_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.8}"
aws_image="${QUEQLITE_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
cleanup="${QUEQLITE_VIND_CLEANUP:-1}"
skip_build="${QUEQLITE_VIND_SKIP_BUILD:-0}"
target="${QUEQLITE_E2E_TARGET_DIR:-target/queqlite-e2e}/$run_id"
context=""
previous_context=""
created_cluster=false
marker=/var/lib/queqlite/emptydir-marker

die() { echo "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }; }
for tool in docker kubectl vcluster jq yq openssl; do require "$tool"; done
case "$cleanup" in 0|1) ;; *) die "QUEQLITE_VIND_CLEANUP must be 0 or 1";; esac
case "$skip_build" in 0|1) ;; *) die "QUEQLITE_VIND_SKIP_BUILD must be 0 or 1";; esac

k() { kubectl --context "$context" -n "$namespace" "$@"; }
capture_ready_context() {
  context="$(kubectl config current-context 2>/dev/null || true)"
  [ -n "$context" ] || die "vcluster did not select a Kubernetes context"
  for ((attempt=1; attempt<=120; attempt++)); do
    if kubectl --context "$context" get --raw=/readyz >/dev/null 2>&1; then
      return
    fi
    [ "$attempt" -lt 120 ] || die "Kubernetes API did not become ready for context $context"
    sleep 1
  done
}
cleanup_run() {
  status="$1"
  if [ "$status" -ne 0 ] && [ -n "$context" ]; then
    k get pods,deployments,statefulsets,jobs,services,persistentvolumeclaims -o wide >&2 || true
    k get events --sort-by=.metadata.creationTimestamp >&2 || true
  fi
  if [ "$cleanup" = 1 ] && "$created_cluster"; then
    vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || true
  fi
  [ -z "$previous_context" ] || kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
}
trap 'status=$?; cleanup_run "$status"; exit "$status"' EXIT

cd "$repo_root"
mkdir -p "$target"
chmod 700 "$target"
previous_context="$(kubectl config current-context 2>/dev/null || true)"

if [ "$skip_build" = 1 ]; then
  docker image inspect "$image" >/dev/null 2>&1 \
    || die "QUEQLITE_VIND_SKIP_BUILD=1 requires existing local image: $image"
else
  docker build -t "$image" .
fi
vcluster use driver docker >/dev/null
if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
  [ "${QUEQLITE_VIND_REUSE_EXISTING:-0}" = 1 ] || die "vind cluster already exists: $cluster"
  vcluster connect "$cluster" --driver docker >/dev/null
else
  vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster"
  created_cluster=true
fi
capture_ready_context
kubectl config use-context "$context" >/dev/null
if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  managed="$(kubectl --context "$context" get namespace "$namespace" \
    -o go-template='{{index .metadata.labels "queqlite.dev/e2e-managed"}}')"
  [ "$managed" = true ] || die "refusing to replace unmanaged namespace $namespace"
  kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null
fi
kubectl --context "$context" create namespace "$namespace" >/dev/null
kubectl --context "$context" label namespace "$namespace" \
  queqlite.dev/e2e-managed=true "queqlite.dev/e2e-run-id=$run_id" >/dev/null

node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node for image loading"
vcluster node load-image "$node" --image "$image"

client_token="$(openssl rand -hex 24)"
admin_token="$(openssl rand -hex 24)"
peer_tokens="$(jq -cn \
  --arg first "$(openssl rand -hex 24)" \
  --arg second "$(openssl rand -hex 24)" \
  --arg third "$(openssl rand -hex 24)" \
  '[$first, $second, $third]')"
[ "$(jq 'unique | length' <<< "$peer_tokens")" = 3 ] || die "peer tokens must be unique"
k create secret generic queqlite-auth \
  --from-literal=client-token="$client_token" \
  --from-literal=admin-token="$admin_token" >/dev/null

sed -e "s|__RUSTFS_IMAGE__|$rustfs_image|g" -e "s|__AWS_CLI_IMAGE__|$aws_image|g" \
  deploy/k8s/rustfs-e2e.yaml > "$target/rustfs.yaml"
yq eval '.' "$target/rustfs.yaml" >/dev/null
k apply -f "$target/rustfs.yaml" >/dev/null
k rollout status deployment/rustfs --timeout=240s >/dev/null
k wait --for=condition=complete job/rustfs-create-bucket --timeout=240s >/dev/null
rustfs_uid="$(k get pod -l app.kubernetes.io/name=rustfs -o jsonpath='{.items[0].metadata.uid}')"
[ -n "$rustfs_uid" ] || die "cannot capture RustFS pod UID"
[ -z "$(k get persistentvolumeclaims -o name)" ] || die "vind E2E must not create PVCs"

make_bundle() {
  id="$1" output="$2"
  jq -n --argjson id "$id" --argjson tokens "$peer_tokens" '
    {version:1, config_id:$id, members:[range(3) as $n | {
      node_id:("node-" + ($n + 1 | tostring)),
      url:("http://queqlite-c" + ($id|tostring) + "-" + ($n|tostring) + ".queqlite-c" + ($id|tostring) + ":8081"),
      log_url:("http://queqlite-c" + ($id|tostring) + "-" + ($n|tostring) + ".queqlite-c" + ($id|tostring) + ":8080"),
      token:$tokens[$n]
    }]}
  ' > "$output"
  chmod 600 "$output"
}
make_bundle 1 "$target/config-c1.json"
make_bundle 2 "$target/config-c2-draft.json"
jq -e '[.members[].token] | unique | length == 3' \
  "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
jq -se '(.[0].members | map(.token)) == (.[1].members | map(.token))' \
  "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
k create secret generic queqlite-c1-bundle --from-file=config.json="$target/config-c1.json" \
  --dry-run=client -o yaml | yq eval '.immutable = true' - | k create -f - >/dev/null

export QUEQLITE_IMAGE="$image" QUEQLITE_KUBE_CONTEXT="$context" QUEQLITE_K8S_NAMESPACE="$namespace"
export QUEQLITE_CLUSTER_ID=queqlite-vind QUEQLITE_RECOVERY_GENERATION=1
export QUEQLITE_CHECKPOINT_LEASE_MS=5000
export QUEQLITE_S3_ENDPOINT=http://rustfs:9000 QUEQLITE_OBJECT_SECRET=rustfs-credentials
export QUEQLITE_S3_ALLOW_HTTP=true

echo "== initialize object checkpoint and bootstrap config 1 =="
scripts/k8s-object-job.sh 1 "$target/config-c1.json" init-checkpoint >/dev/null
QUEQLITE_STARTUP_MODE=rejoin scripts/render-k8s-config.sh \
  1 3 "$target/config-c1.json" "$target/config-c1.yaml"
k create -f "$target/config-c1.yaml" >/dev/null
scripts/wait-k8s-statefulset-ready.sh queqlite-c1 3 1

client() {
  pod="$1"; shift
  k exec "$pod" -- queqlite "$@" --url http://127.0.0.1:8080
}
retry_client() {
  pod="$1"; shift
  retry_output="$target/retry-client.out"
  for ((attempt=1; attempt<=60; attempt++)); do
    if client "$pod" "$@" > "$retry_output" 2>/dev/null; then
      cat "$retry_output"
      return 0
    fi
    [ "$attempt" -lt 60 ] || return 1
    sleep 1
  done
}
client queqlite-c1-0 write --request-id "snapshot-${run_id}" --key snapshot --value restored
client queqlite-c1-0 sql execute --request-id "sql-schema-${run_id}" \
  --sql 'CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
client queqlite-c1-0 sql execute --request-id "sql-snapshot-${run_id}" \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":1},{"type":"text","value":"snapshot"}]'
compact_status="$target/compact-status-c1.json"
scripts/k8s-admin-job.sh queqlite-c1 queqlite-c1-0 GET \
  /v1/admin/membership/status > "$compact_status"
compact_request="$(jq -cn \
  --arg op "local-compact-${run_id}" \
  --argjson root "$(jq -c '.qlog_root' "$compact_status")" \
  '{operation_id:$op, expected_config_id:1, expected_recovery_generation:1, expected_root:$root}')"
compact="$target/compact-c1.json"
scripts/k8s-admin-job.sh queqlite-c1 queqlite-c1-0 POST \
  /v1/admin/checkpoint/compact "$compact_request" > "$compact"
jq -e '.anchor.format_version == 2' "$compact" >/dev/null
client queqlite-c1-0 write --request-id "suffix-${run_id}" --key suffix --value replayed
client queqlite-c1-0 sql execute --request-id "sql-suffix-${run_id}" \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":2},{"type":"text","value":"suffix"}]'
for ordinal in 0 1 2; do
  client "queqlite-c1-$ordinal" read --key suffix --consistency barrier --expect replayed
  # shellcheck disable=SC2016
  k exec "queqlite-c1-$ordinal" -- /bin/sh -ec 'printf marker > "$1"' sh "$marker"
done

echo "== compact locally, stop config 1, and replace 3 -> 3 =="
QUEQLITE_RECONFIG_WORK_DIR="$target/reconfigure" \
  scripts/replace-k8s-config.sh "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
QUEQLITE_RECONFIG_WORK_DIR="$target/reconfigure" \
  scripts/replace-k8s-config.sh "$target/config-c1.json" "$target/config-c2-draft.json" >/dev/null
successor="$target/reconfigure/config-c2.json"
final_checkpoint="$target/final-checkpoint-c1.json"
scripts/k8s-object-job.sh 1 "$target/config-c1.json" checkpoint inspect \
  > "$final_checkpoint"
jq -e '.format_version == 2 and .base.snapshot and (.segments | type == "array")' \
  "$final_checkpoint" >/dev/null

for ordinal in 0 1 2; do
  k exec "queqlite-c2-$ordinal" -- test ! -e "$marker"
  client "queqlite-c2-$ordinal" read --key snapshot --consistency barrier --expect restored
  client "queqlite-c2-$ordinal" read --key suffix --consistency barrier --expect replayed
  client "queqlite-c2-$ordinal" sql query \
    --sql 'SELECT id, name FROM users ORDER BY id' --consistency barrier \
    > "$target/sql-c2-${ordinal}.json"
  jq -e '.columns == ["id", "name"] and
    .rows == [[{"type":"integer","value":1},{"type":"text","value":"snapshot"}],
              [{"type":"integer","value":2},{"type":"text","value":"suffix"}]]' \
    "$target/sql-c2-${ordinal}.json" >/dev/null
done
client queqlite-c2-0 sql execute --request-id "sql-suffix-${run_id}" \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":2},{"type":"text","value":"suffix"}]'

echo "== plan, inspect, and apply old-generation GC with exact hash =="
client queqlite-c2-0 read --key suffix --consistency barrier --expect replayed
generation_compact="$target/generation-compact-c2.json"
generation_status="$target/generation-status-c2.json"
for ((attempt=1; attempt<=20; attempt++)); do
  scripts/k8s-admin-job.sh queqlite-c2 queqlite-c2-0 GET \
    /v1/admin/membership/status > "$generation_status"
  generation_compact_request="$(jq -cn \
    --arg op "generation-roll-compact-${run_id}-${attempt}" \
    --argjson root "$(jq -c '.qlog_root' "$generation_status")" \
    '{operation_id:$op, expected_config_id:2,
      expected_recovery_generation:1, expected_root:$root}')"
  if scripts/k8s-admin-job.sh queqlite-c2 queqlite-c2-0 POST \
    /v1/admin/checkpoint/compact "$generation_compact_request" \
    > "$generation_compact"; then
    break
  fi
  [ "$attempt" -lt 20 ] || die "active generation checkpoint compaction did not converge"
  sleep 1
done
jq -e '.anchor.format_version == 2 and .anchor.configuration_state.phase == "active"' \
  "$generation_compact" >/dev/null

echo "== restart successor container in place and rejoin preserved emptyDir state =="
restart_pod=queqlite-c2-1
restart_uid="$(k get pod "$restart_pod" -o jsonpath='{.metadata.uid}')"
restart_count="$(k get pod "$restart_pod" \
  -o jsonpath='{.status.containerStatuses[0].restartCount}')"
k exec "$restart_pod" -- /bin/sh -ec 'kill -TERM 1' >/dev/null 2>&1 || true
for ((attempt=1; attempt<=120; attempt++)); do
  current_uid="$(k get pod "$restart_pod" -o jsonpath='{.metadata.uid}')"
  [ "$current_uid" = "$restart_uid" ] || die "successor Pod was recreated during container restart"
  current_count="$(k get pod "$restart_pod" \
    -o jsonpath='{.status.containerStatuses[0].restartCount}')"
  ready="$(k get pod "$restart_pod" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}')"
  if [ "$current_count" -gt "$restart_count" ] && [ "$ready" = True ]; then
    break
  fi
  [ "$attempt" -lt 120 ] || die "successor container did not rejoin with preserved state"
  sleep 1
done
retry_client "$restart_pod" read --key suffix --consistency barrier --expect replayed

scripts/k8s-object-job.sh 2 "$successor" roll-checkpoint \
  --from-generation 1 --to-generation 2 >/dev/null
echo "== replace generation-1 pods with generation-2 S3 restores =="
k scale statefulset queqlite-c2 --replicas=0 >/dev/null
k wait --for=delete pod -l 'queqlite.dev/config-id=2' --timeout=180s >/dev/null
k set env statefulset/queqlite-c2 QUEQLITE_RECOVERY_GENERATION=2 >/dev/null
k scale statefulset queqlite-c2 --replicas=3 >/dev/null
scripts/wait-k8s-statefulset-ready.sh queqlite-c2 3 2
retry_client queqlite-c2-0 write --request-id "generation-2-${run_id}" \
  --key generation --value two
k delete pod queqlite-c2-1 --wait=true >/dev/null
scripts/wait-k8s-statefulset-ready.sh queqlite-c2 3 2
retry_client queqlite-c2-1 read --key generation --consistency barrier --expect two
client queqlite-c2-1 sql execute --request-id "sql-suffix-${run_id}" \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":2},{"type":"text","value":"suffix"}]'
client queqlite-c2-1 sql query --sql 'SELECT count(*) AS users FROM users' \
  --consistency barrier > "$target/sql-generation-2.json"
jq -e '.columns == ["users"] and .rows == [[{"type":"integer","value":2}]]' \
  "$target/sql-generation-2.json" >/dev/null

echo "== stop Queqlite publishers and let their GC leases expire =="
k scale statefulset queqlite-c2 --replicas=0 >/dev/null
k wait --for=delete pod -l 'queqlite.dev/config-id=2' --timeout=180s >/dev/null
sleep 6

plan="$target/gc-plan.json"
QUEQLITE_RECOVERY_GENERATION=2 QUEQLITE_GC_GRACE_MS=0 \
  QUEQLITE_GC_MIN_AGE_MS=0 QUEQLITE_GC_RETAIN_GENERATIONS=0 \
  scripts/gc-k8s.sh plan "$successor" > "$plan"
plan_hash="$(jq -er '.plan_hash' "$plan")"
QUEQLITE_RECOVERY_GENERATION=2 \
  scripts/gc-k8s.sh inspect "$successor" "$plan_hash" >/dev/null
report="$target/gc-report.json"
QUEQLITE_RECOVERY_GENERATION=2 QUEQLITE_GC_CONFIRM_PLAN_HASH="$plan_hash" \
  scripts/gc-k8s.sh apply "$successor" "$plan_hash" > "$report"
jq -e --arg hash "$plan_hash" '.plan_hash == $hash and (.results | length > 0)' \
  "$report" >/dev/null

k scale statefulset queqlite-c2 --replicas=3 >/dev/null
scripts/wait-k8s-statefulset-ready.sh queqlite-c2 3 2
retry_client queqlite-c2-0 read --key generation --consistency barrier --expect two

[ "$(k get pod -l app.kubernetes.io/name=rustfs -o jsonpath='{.items[0].metadata.uid}')" = "$rustfs_uid" ] \
  || die "RustFS changed during the restore lifecycle"
[ -z "$(k get persistentvolumeclaims -o name)" ] || die "vind E2E created a PVC"
echo "vind RustFS emptyDir restore, V2 compact, 3->3 replacement, and exact-hash GC passed"
