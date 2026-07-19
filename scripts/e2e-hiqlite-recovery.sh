#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
hiqlite_commit=c8316c53799c509990475ea8e2aa2ef8679e070e
hiqlite_release=0.14.0
openraft_version=0.9.24
log_sync=Immediate
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
cluster="${HIQLITE_RECOVERY_CLUSTER:-hiqlite-recovery-${run_id}}"
namespace="${HIQLITE_RECOVERY_NAMESPACE:-hiqlite-recovery}"
object_namespace="${HIQLITE_RECOVERY_OBJECT_NAMESPACE:-hiqlite-recovery-object}"
local_image="hiqlite-recovery:${hiqlite_commit:0:12}"
requested_image="${HIQLITE_RECOVERY_IMAGE:-$local_image}"
image="$requested_image"
resolved_image=""
image_source=exact-source-build
source_commit_basis=exact-commit
image_source_commit="$hiqlite_commit"
image_release="$hiqlite_release"
lockfile_origin=generated-from-exact-source
lockfile_sha256=""
ingress_kind=hiqlite-application-proxy
ingress_version="${hiqlite_release}+axum8-route-compat"
ingress_image="hiqlite-recovery-proxy:${hiqlite_commit:0:12}-axum8"
proxy_patch_file="$repo_root/bench/hiqlite-recovery-client/hiqlite-proxy-axum8.patch"
proxy_patch_sha256=""
upstream_proxy_incompatibility="v0.14.0 proxy uses Axum 0.7 route syntax and omits the stream raft-type path required by its v0.14.0 client"
rustfs_image="${HIQLITE_RECOVERY_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.8}"
aws_image="${HIQLITE_RECOVERY_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
hold_csv="${HIQLITE_RECOVERY_HOLD_SECONDS:-60,180,300}"
failure_csv="${HIQLITE_RECOVERY_FAIL_PEERS:-1,2,3}"
probe_interval="${HIQLITE_RECOVERY_PROBE_INTERVAL_SECONDS:-10}"
probe_timeout="${HIQLITE_RECOVERY_PROBE_TIMEOUT_SECONDS:-8}"
auto_recovery_timeout="${HIQLITE_RECOVERY_AUTO_TIMEOUT_SECONDS:-60}"
quorum_loss_timeout="${HIQLITE_RECOVERY_QUORUM_LOSS_TIMEOUT_SECONDS:-60}"
recovery_timeout="${HIQLITE_RECOVERY_TIMEOUT_SECONDS:-300}"
host_port="${HIQLITE_RECOVERY_PROXY_PORT:-18200}"
cleanup="${HIQLITE_RECOVERY_CLEANUP:-1}"
direct_cluster="${HIQLITE_RECOVERY_DIRECT_CLUSTER:-0}"
build_image="${HIQLITE_BUILD_IMAGE:-1}"
reuse_exact_local_images="${HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES:-0}"
skip_image_load="${HIQLITE_RECOVERY_SKIP_IMAGE_LOAD:-0}"
skip_client_build="${HIQLITE_RECOVERY_SKIP_CLIENT_BUILD:-0}"
target="${HIQLITE_RECOVERY_TARGET_DIR:-target/hiqlite-recovery}/${run_id}"
managed_source=false
if [ -n "${HIQLITE_SOURCE_DIR+x}" ]; then
  source_dir="$HIQLITE_SOURCE_DIR"
else
  source_dir="$target/hiqlite-source"
  managed_source=true
fi
client_manifest="$repo_root/bench/hiqlite-recovery-client/Cargo.toml"
client_bin="$repo_root/bench/hiqlite-recovery-client/target/release/hiqlite-recovery-client"
jsonl="$target/recovery.jsonl"
summary="$target/summary.json"
context=""
previous_context=""
created_cluster=false
direct_namespaces_created=false
port_forward_pid=""

die() { echo "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || die "missing required command: $1"; }
iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
epoch_now() { date +%s; }

case "$cleanup" in 0|1) ;; *) die "HIQLITE_RECOVERY_CLEANUP must be 0 or 1" ;; esac
case "$direct_cluster" in 0|1) ;; *) die "HIQLITE_RECOVERY_DIRECT_CLUSTER must be 0 or 1" ;; esac
case "$build_image" in 0|1) ;; *) die "HIQLITE_BUILD_IMAGE must be 0 or 1" ;; esac
case "$reuse_exact_local_images" in 0|1) ;; *) die "HIQLITE_RECOVERY_REUSE_EXACT_LOCAL_IMAGES must be 0 or 1" ;; esac
case "$skip_image_load" in 0|1) ;; *) die "HIQLITE_RECOVERY_SKIP_IMAGE_LOAD must be 0 or 1" ;; esac
case "$skip_client_build" in 0|1) ;; *) die "HIQLITE_RECOVERY_SKIP_CLIENT_BUILD must be 0 or 1" ;; esac
if [ "$build_image" = 0 ] && [ -z "${HIQLITE_RECOVERY_IMAGE:-}" ]; then
  die "HIQLITE_BUILD_IMAGE=0 requires an explicit HIQLITE_RECOVERY_IMAGE"
fi
if [ "$build_image" = 0 ] && [ -z "${HIQLITE_RECOVERY_PROXY_IMAGE:-}" ]; then
  die "HIQLITE_BUILD_IMAGE=0 requires an explicit HIQLITE_RECOVERY_PROXY_IMAGE"
fi
IFS=, read -r -a hold_values <<< "$hold_csv"
[ "${#hold_values[@]}" -ge 1 ] && [ "${#hold_values[@]}" -le 3 ] \
  || die "HIQLITE_RECOVERY_HOLD_SECONDS must contain one to three durations"
seen_holds=,
for value in "${hold_values[@]}"; do
  [[ "$value" =~ ^[0-9]+$ ]] \
    || die "HIQLITE_RECOVERY_HOLD_SECONDS values must be non-negative integers"
  case "$seen_holds" in
    *",$value,"*) die "HIQLITE_RECOVERY_HOLD_SECONDS values must be unique" ;;
  esac
  seen_holds="${seen_holds}${value},"
done
IFS=, read -r -a failure_values <<< "$failure_csv"
[ "${#failure_values[@]}" -ge 1 ] && [ "${#failure_values[@]}" -le 3 ] \
  || die "HIQLITE_RECOVERY_FAIL_PEERS must contain one to three failure counts"
seen_failures=,
for value in "${failure_values[@]}"; do
  case "$value" in 1|2|3) ;; *) die "HIQLITE_RECOVERY_FAIL_PEERS values must be 1, 2, or 3" ;; esac
  case "$seen_failures" in
    *",$value,"*) die "HIQLITE_RECOVERY_FAIL_PEERS values must be unique" ;;
  esac
  seen_failures="${seen_failures}${value},"
done
for value in "$probe_interval" "$probe_timeout" "$auto_recovery_timeout" \
  "$quorum_loss_timeout" "$recovery_timeout"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "timeout and interval values must be positive integers"
done

for tool in awk cargo curl docker git jq kubectl openssl patch sed tar vcluster yq; do require "$tool"; done
if command -v timeout >/dev/null 2>&1; then
  timeout_bin=timeout
elif command -v gtimeout >/dev/null 2>&1; then
  timeout_bin=gtimeout
else
  die "missing required command: timeout or gtimeout"
fi

k() { kubectl --context "$context" --namespace "$namespace" "$@"; }
kobj() { kubectl --context "$context" --namespace "$object_namespace" "$@"; }

cleanup_run() {
  local status="$1" candidate managed owner
  if [ -n "$port_forward_pid" ]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
  fi
  if [ "$status" -ne 0 ] && [ -n "$context" ]; then
    k get pods,deployments,statefulsets,services,persistentvolumeclaims -o wide >&2 || true
    k get events --sort-by=.metadata.creationTimestamp >&2 || true
    kobj get pods,deployments,jobs,services,persistentvolumeclaims -o wide >&2 || true
  fi
  if [ "$cleanup" = 1 ] && "$created_cluster"; then
    vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || true
  fi
  if [ "$cleanup" = 1 ] && "$direct_namespaces_created" && [ -n "$context" ]; then
    for candidate in "$namespace" "$object_namespace"; do
      managed="$(kubectl --context "$context" get namespace "$candidate" \
        -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}' 2>/dev/null || true)"
      owner="$(kubectl --context "$context" get namespace "$candidate" \
        -o go-template='{{index .metadata.labels "rhiza.dev/e2e-run-id"}}' 2>/dev/null || true)"
      if [ "$managed" = true ] && [ "$owner" = "$run_id" ]; then
        kubectl --context "$context" delete namespace "$candidate" --wait=false >/dev/null 2>&1 || true
      fi
    done
  fi
  if [ -n "$previous_context" ]; then
    kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  fi
}
trap 'status=$?; cleanup_run "$status"; exit "$status"' EXIT

record_event() {
  local phase="$1" event="$2" expected="$3" observed="$4" success="$5"
  local started_at="$6" finished_at="$7" duration="$8" detail="$9"
  jq -cn \
    --arg phase "$phase" \
    --arg event "$event" \
    --arg expected "$expected" \
    --arg observed "$observed" \
    --argjson success "$success" \
    --arg started_at "$started_at" \
    --arg finished_at "$finished_at" \
    --argjson duration_seconds "$duration" \
    --arg detail "$detail" \
    --arg hiqlite_commit "$hiqlite_commit" \
    --arg hiqlite_release "$hiqlite_release" \
    --arg image_release "$image_release" \
    --arg openraft_version "$openraft_version" \
    --arg log_sync "$log_sync" \
    --arg image_source "$image_source" \
    --arg source_commit_basis "$source_commit_basis" \
    --arg image_source_commit "$image_source_commit" \
    --arg lockfile_origin "$lockfile_origin" \
    --arg lockfile_sha256 "$lockfile_sha256" \
    --arg ingress_kind "$ingress_kind" \
    --arg ingress_version "$ingress_version" \
    --arg ingress_image "$ingress_image" \
    --arg proxy_patch_sha256 "$proxy_patch_sha256" \
    --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
    --arg resolved_image "$resolved_image" \
    '{schema_version:1,system:"hiqlite",phase:$phase,event:$event,
      expected:$expected,observed:$observed,success:$success,
      started_at:$started_at,finished_at:$finished_at,duration_seconds:$duration_seconds,
      detail:$detail,hiqlite_reference_commit:$hiqlite_commit,
      hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      hiqlite_reference_release:$hiqlite_release,
      hiqlite_release:(if $image_release == "" then null else $image_release end),
      openraft_version:$openraft_version,log_sync:$log_sync,
      image_source:$image_source,source_commit_basis:$source_commit_basis,
      image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      cargo_lock_origin:$lockfile_origin,
      cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
      ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
        patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
      upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
      resolved_image:$resolved_image,
      voters:3,storage:"emptyDir",zero_pvc:true}' >> "$jsonl"
}

prepare_source() {
  local actual_commit
  if "$managed_source" && [ ! -e "$source_dir" ]; then
    mkdir -p "$(dirname "$source_dir")"
    git clone --filter=blob:none --no-checkout https://github.com/sebadob/hiqlite.git "$source_dir"
    git -C "$source_dir" checkout --detach "$hiqlite_commit"
  fi
  [ -d "$source_dir/.git" ] || die "HIQLITE_SOURCE_DIR is not a Git checkout: $source_dir"
  actual_commit="$(git -C "$source_dir" rev-parse HEAD)"
  [ "$actual_commit" = "$hiqlite_commit" ] \
    || die "HIQLITE_SOURCE_DIR must be pinned to $hiqlite_commit, got $actual_commit"
  [ -z "$(git -C "$source_dir" status --porcelain --untracked-files=all)" ] \
    || die "HIQLITE_SOURCE_DIR must be a clean checkout"
}

build_artifacts() {
  if [ "$build_image" = 1 ]; then
    prepare_source
    if [ "$reuse_exact_local_images" = 1 ]; then
      local expected_image_id expected_proxy_id expected_lock_sha actual_proxy_id
      expected_image_id="${HIQLITE_RECOVERY_EXPECTED_LOCAL_IMAGE_ID:-}"
      expected_proxy_id="${HIQLITE_RECOVERY_EXPECTED_LOCAL_PROXY_IMAGE_ID:-}"
      expected_lock_sha="${HIQLITE_RECOVERY_EXPECTED_LOCKFILE_SHA256:-}"
      [ -n "$expected_image_id" ] && [ -n "$expected_proxy_id" ] \
        || die "exact local image reuse requires both expected image IDs"
      [ "${#expected_lock_sha}" -eq 64 ] \
        || die "exact local image reuse requires a 64-character expected lockfile SHA-256"
      resolved_image="$(docker image inspect --format '{{.Id}}' "$image")"
      actual_proxy_id="$(docker image inspect --format '{{.Id}}' "$ingress_image")"
      [ "$resolved_image" = "$expected_image_id" ] \
        || die "local voter image ID mismatch: expected $expected_image_id, got $resolved_image"
      [ "$actual_proxy_id" = "$expected_proxy_id" ] \
        || die "local proxy image ID mismatch: expected $expected_proxy_id, got $actual_proxy_id"
      proxy_patch_sha256="$(openssl dgst -sha256 -r "$proxy_patch_file" | awk '{print $1}')"
      lockfile_sha256="$expected_lock_sha"
      image_source=verified-local-exact-source-reuse
      lockfile_origin=reused-generated-from-exact-source
    else
    image_source=exact-source-build
    source_commit_basis=exact-commit
    image_source_commit="$hiqlite_commit"
    image_release="$hiqlite_release"
    build_source_dir="$target/hiqlite-build-context"
    [ ! -e "$build_source_dir" ] || die "build context already exists: $build_source_dir"
    mkdir -p "$build_source_dir"
    git -C "$source_dir" archive "$hiqlite_commit" | tar -x -C "$build_source_dir"
    cargo generate-lockfile --manifest-path "$build_source_dir/Cargo.toml"
    [ -f "$build_source_dir/Cargo.lock" ] \
      || die "cargo generate-lockfile did not create $build_source_dir/Cargo.lock"
    lockfile_sha256="$(openssl dgst -sha256 -r "$build_source_dir/Cargo.lock" | awk '{print $1}')"
    [ "${#lockfile_sha256}" -eq 64 ] || die "cannot calculate generated Cargo.lock SHA-256"
    docker build \
      --file "$repo_root/bench/hiqlite-recovery-client/Dockerfile.server" \
      --tag "$image" "$build_source_dir"
    resolved_image="$(docker image inspect --format '{{.Id}}' "$image")"
    proxy_build_source_dir="$target/hiqlite-proxy-build-context"
    mkdir -p "$proxy_build_source_dir"
    git -C "$source_dir" archive "$hiqlite_commit" | tar -x -C "$proxy_build_source_dir"
    cp "$build_source_dir/Cargo.lock" "$proxy_build_source_dir/Cargo.lock"
    patch --directory "$proxy_build_source_dir" --strip=1 < "$proxy_patch_file"
    proxy_patch_sha256="$(openssl dgst -sha256 -r "$proxy_patch_file" | awk '{print $1}')"
    [ "${#proxy_patch_sha256}" -eq 64 ] || die "cannot calculate proxy patch SHA-256"
    docker build \
      --file "$repo_root/bench/hiqlite-recovery-client/Dockerfile.server" \
      --tag "$ingress_image" "$proxy_build_source_dir"
    fi
  else
    image_source=user-supplied-prebuilt
    source_commit_basis=user-supplied-unverified
    image_source_commit=""
    image_release=""
    lockfile_origin=not-applicable-prebuilt
    lockfile_sha256=""
    docker pull "$image"
    resolved_image="$(docker image inspect --format '{{index .RepoDigests 0}}' "$image")"
    [ -n "$resolved_image" ] || die "cannot resolve pulled image digest for $image"
    ingress_image="$HIQLITE_RECOVERY_PROXY_IMAGE"
    docker pull "$ingress_image"
  fi
  if [ "$skip_client_build" = 1 ]; then
    [ -x "$client_bin" ] \
      || die "HIQLITE_RECOVERY_SKIP_CLIENT_BUILD=1 requires $client_bin"
  else
    cargo build --release --manifest-path "$client_manifest"
  fi
}

scale_failure() {
  survivors="$1"
  k scale statefulset/hiqlite-recovery --replicas="$survivors" >/dev/null
}

capture_ready_context() {
  local attempt
  if [ -z "$context" ]; then
    context="$(kubectl config current-context 2>/dev/null || true)"
  fi
  [ -n "$context" ] || die "vcluster did not select a Kubernetes context"
  for ((attempt=1; attempt<=120; attempt++)); do
    if kubectl --context "$context" get --raw=/readyz >/dev/null 2>&1; then return; fi
    [ "$attempt" -lt 120 ] || die "Kubernetes API did not become ready for $context"
    sleep 1
  done
}

create_managed_namespace() {
  local candidate="$1" managed
  if kubectl --context "$context" get namespace "$candidate" >/dev/null 2>&1; then
    managed="$(kubectl --context "$context" get namespace "$candidate" \
      -o go-template='{{index .metadata.labels "rhiza.dev/e2e-managed"}}')"
    [ "$managed" = true ] || die "refusing to replace unmanaged namespace $candidate"
    kubectl --context "$context" delete namespace "$candidate" --wait=true >/dev/null
  fi
  kubectl --context "$context" create namespace "$candidate" >/dev/null
  kubectl --context "$context" label namespace "$candidate" \
    rhiza.dev/e2e-managed=true "rhiza.dev/e2e-run-id=$run_id" >/dev/null
}

render_and_deploy() {
  local secret_raft secret_api
  secret_raft="$(openssl rand -hex 24)"
  secret_api="$(openssl rand -hex 24)"
  api_secret="$secret_api"
  sed \
    -e "s|__RUSTFS_IMAGE__|$rustfs_image|g" \
    -e "s|__AWS_CLI_IMAGE__|$aws_image|g" \
    "$repo_root/deploy/k8s/hiqlite-recovery-rustfs.yaml" > "$target/rustfs.yaml"
  sed \
    -e "s|__HIQLITE_IMAGE__|$image|g" \
    -e "s|__INGRESS_IMAGE__|$ingress_image|g" \
    -e "s|__OBJECT_NAMESPACE__|$object_namespace|g" \
    -e "s|__SECRET_RAFT__|$secret_raft|g" \
    -e "s|__SECRET_API__|$secret_api|g" \
    "$repo_root/deploy/k8s/hiqlite-recovery-cluster.yaml" > "$target/hiqlite.yaml"
  yq eval '.' "$target/rustfs.yaml" "$target/hiqlite.yaml" >/dev/null
  kobj apply -f "$target/rustfs.yaml" >/dev/null
  kobj rollout status deployment/rustfs --timeout=240s >/dev/null
  kobj rollout status deployment/rustfs-tools --timeout=240s >/dev/null
  kobj wait --for=condition=complete job/rustfs-create-hiqlite-bucket --timeout=240s >/dev/null
  rustfs_uid="$(kobj get pod -l app.kubernetes.io/component=object-store \
    -o jsonpath='{.items[0].metadata.uid}')"
  [ -n "$rustfs_uid" ] || die "cannot capture RustFS Pod UID"
  k apply -f "$target/hiqlite.yaml" >/dev/null
}

ready_replicas() {
  k get statefulset hiqlite-recovery -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true
}

wait_ready_replicas() {
  local expected="$1" timeout_seconds="$2" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    [ "$(ready_replicas)" = "$expected" ] && return 0
    sleep 1
  done
  return 1
}

wait_ready_pod() {
  local pod="$1" timeout_seconds="$2" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if [ "$(k get pod "$pod" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
      2>/dev/null || true)" = True ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_port_forward() {
  local attempt
  if [ -n "$port_forward_pid" ]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
    port_forward_pid=""
  fi
  k port-forward service/hiqlite-recovery-proxy "$host_port:8200" \
    > "$target/ingress-port-forward.log" 2>&1 &
  port_forward_pid=$!
  for ((attempt=1; attempt<=60; attempt++)); do
    if curl --fail --silent --max-time 2 "http://127.0.0.1:$host_port/ping" >/dev/null; then return; fi
    kill -0 "$port_forward_pid" >/dev/null 2>&1 || die "Hiqlite ingress port-forward exited"
    [ "$attempt" -lt 60 ] || die "Hiqlite ingress port-forward did not become ready"
    sleep 1
  done
}

ensure_port_forward() {
  if [ -z "$port_forward_pid" ] || ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    start_port_forward
  fi
}

run_client() {
  local seconds="$1" status
  shift
  ensure_port_forward
  if "$timeout_bin" "${seconds}s" "$client_bin" \
    --nodes "127.0.0.1:$host_port" --secret "$api_secret" "$@"; then
    return 0
  else
    status=$?
  fi
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    start_port_forward
    "$timeout_bin" "${seconds}s" "$client_bin" \
      --nodes "127.0.0.1:$host_port" --secret "$api_secret" "$@"
    return $?
  fi
  return "$status"
}

probe() {
  local phase="$1" operation="$2" expected="$3"
  local started_at started_epoch out success observed status finished_at duration detail
  shift 3
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  out="$target/probe-${phase}-${operation}-$(date +%s)-${RANDOM}.out"
  if run_client "$probe_timeout" "$@" > "$out" 2>&1; then
    if [ "$expected" = success ] && [ "$operation" = write ] \
      && ! jq -e '.acknowledged == true' "$out" >/dev/null 2>&1; then
      success=false
      observed=invalid-ack
      status=1
    elif [ "$expected" = success ] \
      && { [ "$operation" = local-query ] || [ "$operation" = query-consistent ]; } \
      && ! jq -e '.found == true' "$out" >/dev/null 2>&1; then
      success=false
      observed=missing-sentinel
      status=1
    else
      success=true
      observed=success
      status=0
    fi
  else
    success=false
    observed=failed
    status=1
  fi
  finished_at="$(iso_now)"
  duration=$(( $(epoch_now) - started_epoch ))
  detail="$(tail -c 4000 "$out")"
  record_event "$phase" "$operation" "$expected" "$observed" "$success" \
    "$started_at" "$finished_at" "$duration" "$detail"
  return "$status"
}

probe_window() {
  local phase="$1" hold_seconds="$2" write_expected="$3" local_expected="$4"
  local consistent_expected="$5" ack_file="$6" query_id="$7"
  local deadline iteration id value now remaining sleep_for probe_budget
  deadline=$(( $(epoch_now) + hold_seconds ))
  probe_budget=$((probe_timeout * 4))
  iteration=0
  while :; do
    iteration=$((iteration + 1))
    id="${phase}-hold-${iteration}-${run_id}"
    value="ack-${phase}-${iteration}"
    if probe "$phase" write "$write_expected" execute "$id" "$value"; then
      printf '%s\t%s\n' "$id" "$value" >> "$ack_file"
    fi
    probe "$phase" local-query "$local_expected" query-local "$query_id" || true
    probe "$phase" query-consistent "$consistent_expected" query-consistent "$query_id" || true
    probe "$phase" metrics observable metrics || true
    now="$(epoch_now)"
    [ "$now" -ge "$deadline" ] && break
    remaining=$((deadline - now))
    # Do not begin another four-probe sample if its timeout budget can cross the
    # requested recovery release. The first sample always runs, so every
    # operation still has evidence even for a short smoke hold.
    if [ "$remaining" -le "$probe_budget" ]; then
      sleep "$remaining"
      break
    fi
    sleep_for="$probe_interval"
    [ "$remaining" -lt "$sleep_for" ] && sleep_for="$remaining"
    sleep "$sleep_for"
  done
}

wait_failure_established() {
  local phase="$1" local_must_fail="$2" query_id="$3"
  local deadline attempt transition_id transition_value write_failed consistent_failed local_failed
  local transient_write_acks=0 out_prefix
  deadline=$(( $(epoch_now) + quorum_loss_timeout ))
  attempt=0
  while [ "$(epoch_now)" -le "$deadline" ]; do
    attempt=$((attempt + 1))
    transition_id="${phase}-transition-${attempt}-${run_id}"
    transition_value="transition-ack-${attempt}"
    out_prefix="$target/${phase}-failure-transition-${attempt}"
    write_failed=false
    consistent_failed=false
    local_failed=false
    if run_client "$probe_timeout" execute "$transition_id" "$transition_value" \
      > "${out_prefix}-write.out" 2>&1; then
      transient_write_acks=$((transient_write_acks + 1))
    else
      write_failed=true
    fi
    if ! run_client "$probe_timeout" query-consistent "$query_id" \
      > "${out_prefix}-consistent.out" 2>&1; then
      consistent_failed=true
    fi
    if [ "$local_must_fail" = false ] || ! run_client "$probe_timeout" query-local "$query_id" \
      > "${out_prefix}-local.out" 2>&1; then
      local_failed=true
    fi
    if "$write_failed" && "$consistent_failed" && "$local_failed"; then
      record_event "$phase" failure-established fail-closed fail-closed true \
        "$(iso_now)" "$(iso_now)" 0 \
        "attempts=$attempt transient_write_acks=$transient_write_acks local_must_fail=$local_must_fail"
      return 0
    fi
    sleep 1
  done
  record_event "$phase" failure-established fail-closed transition-never-quiesced false \
    "$(iso_now)" "$(iso_now)" "$quorum_loss_timeout" \
    "attempts=$attempt transient_write_acks=$transient_write_acks local_must_fail=$local_must_fail"
  return 1
}

assert_probe_outcome() {
  local phase="$1" operation="$2" expected_success="$3"
  jq -s -e \
    --arg phase "$phase" \
    --arg operation "$operation" \
    --argjson expected_success "$expected_success" \
    '([.[] | select(.phase == $phase and .event == $operation)]) as $samples |
      ($samples | length) > 0 and ($samples | all(.success == $expected_success))' \
    "$jsonl" >/dev/null
}

metrics_to() {
  local output="$1"
  run_client "$probe_timeout" metrics > "$output" 2>/dev/null
}

wait_service() {
  local timeout_seconds="$1" sentinel_id="$2" sentinel_value="$3" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if run_client "$probe_timeout" query-consistent "$sentinel_id" \
      | jq -e --arg value "$sentinel_value" '.found == true and .value == $value' \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_convergence() {
  local timeout_seconds="$1" output="$2" deadline
  deadline=$(( $(epoch_now) + timeout_seconds ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    if [ "$(ready_replicas)" = 3 ] && metrics_to "$output" \
      && jq -e '.running == true and .current_leader != null and
        .voter_ids == [1,2,3] and .node_ids == [1,2,3]' "$output" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

capture_uids() {
  local output="$1"
  k get pods -l app.kubernetes.io/component=voter -o json \
    | jq '[.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)' > "$output"
}

markers_lost() {
  local old_uids_file="$1" ordinal pod old_uid new_uid
  shift
  for ordinal in "$@"; do
    pod="hiqlite-recovery-$ordinal"
    old_uid="$(jq -er --arg pod "$pod" '.[] | select(.name == $pod) | .uid' "$old_uids_file")"
    new_uid="$(k get pod "$pod" -o jsonpath='{.metadata.uid}')"
    [ "$old_uid" != "$new_uid" ] || return 1
    k exec "$pod" -c marker-inspector -- test -f "/marker/emptydir-marker-$new_uid"
    k exec "$pod" -c marker-inspector -- test ! -e "/marker/emptydir-marker-$old_uid"
  done
}

verify_ack_file() {
  local ack_file="$1" id value
  while IFS=$'\t' read -r id value; do
    [ -n "$id" ] || continue
    run_client "$probe_timeout" verify-sentinel "$id" "$value" >/dev/null
  done < "$ack_file"
}

capture_learner_to_voter_evidence() {
  local phase="$1" node_id="$2" since_time="$3" pod
  local output="$target/${phase}-learner-to-voter.log"
  : > "$output"
  k get pods -l app.kubernetes.io/component=voter -o name \
    | while IFS= read -r pod; do
        k logs "$pod" -c hiqlite --since-time="$since_time" >> "$output" 2>&1 || true
      done
  grep -Eq "Added node ${node_id} as .* learner" "$output" \
    && grep -Eq "Added node ${node_id} as .* member" "$output"
}

verify_missing() {
  local id="$1"
  run_client "$probe_timeout" query-consistent "$id" | jq -e '.found == false' >/dev/null
}

list_backup_objects() {
  kobj exec deployment/rustfs-tools -- aws --endpoint-url http://rustfs:9000 \
    s3api list-objects-v2 --bucket hiqlite --output json
}

trigger_external_backup() {
  local label="$1" deadline objects key head_json candidate
  local before="$target/${label}-objects-before.txt"
  list_backup_objects | jq -r '.Contents[]?.Key' | sort > "$before"
  run_client 30 backup > "$target/${label}-backup-trigger.json"
  deadline=$(( $(epoch_now) + recovery_timeout ))
  while [ "$(epoch_now)" -le "$deadline" ]; do
    objects="$target/${label}-objects-current.json"
    if list_backup_objects > "$objects" 2>/dev/null; then
      key="$(jq -r '.Contents[]?.Key' "$objects" \
        | while IFS= read -r candidate; do
            if ! grep -Fqx -- "$candidate" "$before"; then printf '%s\n' "$candidate"; fi
          done | tail -n 1)"
      if [ -n "$key" ]; then
        head_json="$target/${label}-backup-head.json"
        if kobj exec deployment/rustfs-tools -- aws --endpoint-url http://rustfs:9000 \
          s3api head-object --bucket hiqlite --key "$key" --output json > "$head_json" \
          && jq -e '.ContentLength > 0' "$head_json" >/dev/null; then
          printf '%s\n' "$key"
          return 0
        fi
      fi
    fi
    sleep 1
  done
  return 1
}

set_restore_object() {
  local key="$1"
  k set env statefulset/hiqlite-recovery "HQL_BACKUP_RESTORE=s3:$key" >/dev/null
}

clear_restore_object() {
  k set env statefulset/hiqlite-recovery HQL_BACKUP_RESTORE- >/dev/null
  if k get statefulset hiqlite-recovery -o json \
    | jq -e '.spec.template.spec.containers[] | select(.name == "hiqlite") |
      .env // [] | any(.name == "HQL_BACKUP_RESTORE")' >/dev/null; then
    die "HQL_BACKUP_RESTORE remained in the StatefulSet template"
  fi
}

clear_restore_from_running_pods() {
  local phase="$1" ordinal pod
  clear_restore_object
  for ordinal in 2 1 0; do
    pod="hiqlite-recovery-$ordinal"
    k delete pod "$pod" --wait=true >/dev/null
    wait_ready_pod "$pod" "$recovery_timeout" \
      || die "$pod did not become ready without HQL_BACKUP_RESTORE"
    wait_convergence "$recovery_timeout" "$target/${phase}-env-cleared-${ordinal}-metrics.json" \
      || die "$phase did not converge after clearing restore env from $pod"
  done
  if k get pods -l app.kubernetes.io/component=voter -o json \
    | jq -e '[.items[].spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "HQL_BACKUP_RESTORE remained on a running voter after $phase recovery"
  fi
}

verify_cell_boundary() {
  local cell_id="$1" boundary="$2"
  local started_at started_epoch boundary_id boundary_value detail
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  [ "$(k get statefulset hiqlite-recovery -o jsonpath='{.spec.replicas}')" = 3 ] \
    || die "$cell_id $boundary boundary does not have three desired voters"
  wait_ready_replicas 3 "$recovery_timeout" \
    || die "$cell_id $boundary boundary does not have three ready voters"
  wait_convergence "$recovery_timeout" "$target/${cell_id}-${boundary}-metrics.json" \
    || die "$cell_id $boundary boundary did not converge"
  if k get statefulset hiqlite-recovery -o json \
    | jq -e '[.spec.template.spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "$cell_id $boundary boundary retained HQL_BACKUP_RESTORE in the template"
  fi
  if k get pods -l app.kubernetes.io/component=voter -o json \
    | jq -e '[.items[].spec.containers[] | select(.name == "hiqlite") |
      (.env // [])[]? | select(.name == "HQL_BACKUP_RESTORE")] | length > 0' >/dev/null; then
    die "$cell_id $boundary boundary retained HQL_BACKUP_RESTORE on a voter"
  fi
  [ "$(kobj get pod -l app.kubernetes.io/component=object-store \
    -o jsonpath='{.items[0].metadata.uid}')" = "$rustfs_uid" ] \
    || die "$cell_id $boundary boundary changed the RustFS Pod"
  [ -z "$(k get persistentvolumeclaims -o name)$(kobj get persistentvolumeclaims -o name)" ] \
    || die "$cell_id $boundary boundary no longer has zero PVCs"
  detail=three-voter-convergence
  if [ "$boundary" = start ]; then
    run_client 30 reset >/dev/null
    detail=three-voter-convergence-and-application-reset
  fi
  boundary_id="${cell_id}-${boundary}-boundary"
  boundary_value="healthy-${run_id}"
  run_client 30 execute "$boundary_id" "$boundary_value" >/dev/null
  run_client 30 verify-sentinel "$boundary_id" "$boundary_value" >/dev/null
  record_event "$cell_id" "boundary-$boundary" healthy healthy true "$started_at" \
    "$(iso_now)" "$(( $(epoch_now) - started_epoch ))" "$detail"
}

append_phase_summary() {
  local phase="$1" cell_id="$2" failure_count="$3" hold_seconds="$4"
  local expected_json="$5" observed_json="$6"
  local failure_started_at="$7" failure_released_at="$8"
  local service_rto="$9" full_rto="${10}" failure_held="${11}"
  jq -cn \
    --arg phase "$phase" \
    --arg cell_id "$cell_id" \
    --argjson failure_count "$failure_count" \
    --argjson hold_seconds "$hold_seconds" \
    --arg hiqlite_commit "$hiqlite_commit" \
    --arg hiqlite_release "$hiqlite_release" \
    --arg image_release "$image_release" \
    --arg openraft_version "$openraft_version" \
    --arg log_sync "$log_sync" \
    --arg image_source "$image_source" \
    --arg source_commit_basis "$source_commit_basis" \
    --arg image_source_commit "$image_source_commit" \
    --arg lockfile_origin "$lockfile_origin" \
    --arg lockfile_sha256 "$lockfile_sha256" \
    --arg ingress_kind "$ingress_kind" \
    --arg ingress_version "$ingress_version" \
    --arg ingress_image "$ingress_image" \
    --arg proxy_patch_sha256 "$proxy_patch_sha256" \
    --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
    --arg resolved_image "$resolved_image" \
    --arg failure_started_at "$failure_started_at" \
    --arg failure_released_at "$failure_released_at" \
    --argjson service_rto_seconds "$service_rto" \
    --argjson full_rto_seconds "$full_rto" \
    --argjson failure_held_seconds "$failure_held" \
    --argjson expected_vs_observed_expected "$expected_json" \
    --argjson expected_vs_observed_observed "$observed_json" \
    '{schema_version:1,system:"hiqlite",event:"phase_summary",phase:$phase,
      cell_id:$cell_id,failure_count:$failure_count,hold_seconds:$hold_seconds,
      hiqlite_reference_commit:$hiqlite_commit,
      hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      hiqlite_reference_release:$hiqlite_release,
      hiqlite_release:(if $image_release == "" then null else $image_release end),
      openraft_version:$openraft_version,log_sync:$log_sync,
      image_source:$image_source,source_commit_basis:$source_commit_basis,
      image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
      cargo_lock_origin:$lockfile_origin,
      cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
      ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
        patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
      upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
      resolved_image:$resolved_image,
      voters:3,storage:"emptyDir",zero_pvc:true,
      failure_started_at:$failure_started_at,failure_released_at:$failure_released_at,
      failure_held_seconds:$failure_held_seconds,
      service_rto_seconds:$service_rto_seconds,full_rto_seconds:$full_rto_seconds,
      expected_vs_observed:{expected:$expected_vs_observed_expected,
        observed:$expected_vs_observed_observed}}' >> "$jsonl"
}

cd "$repo_root"
mkdir -p "$target"
chmod 700 "$target"
: > "$jsonl"
build_artifacts

previous_context="$(kubectl config current-context 2>/dev/null || true)"
if [ "$direct_cluster" = 1 ]; then
  context="${HIQLITE_RECOVERY_DIRECT_CONTEXT:-$previous_context}"
  [ -n "$context" ] || die "direct cluster mode requires an active or explicit context"
else
  vcluster use driver docker >/dev/null
  if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
    [ "${HIQLITE_RECOVERY_REUSE_EXISTING:-0}" = 1 ] \
      || die "vind cluster already exists: $cluster"
    vcluster connect "$cluster" --driver docker >/dev/null
  else
    vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster"
    created_cluster=true
  fi
fi
capture_ready_context
kubectl config use-context "$context" >/dev/null
create_managed_namespace "$namespace"
[ "$direct_cluster" = 0 ] || direct_namespaces_created=true
create_managed_namespace "$object_namespace"
node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node"
if [ "$direct_cluster" = 0 ] && [ "$skip_image_load" = 0 ]; then
  vcluster node load-image "$node" --image "$image"
  vcluster node load-image "$node" --image "$ingress_image"
fi
render_and_deploy
wait_ready_replicas 3 "$recovery_timeout" || die "initial voters did not become ready"
k rollout status deployment/hiqlite-recovery-proxy --timeout="${recovery_timeout}s" >/dev/null
start_port_forward
run_client 30 execute baseline initial > "$target/baseline-execute.json"
run_client 30 verify-sentinel baseline initial > "$target/baseline-verify.json"

pvc_count=$(( $(k get persistentvolumeclaims -o name | wc -l) \
  + $(kobj get persistentvolumeclaims -o name | wc -l) ))
[ "$pvc_count" -eq 0 ] || die "Hiqlite recovery drill created $pvc_count PVCs"
capture_uids "$target/uids-initial.json"
jq -cn \
  --arg run_id "$run_id" --arg hiqlite_commit "$hiqlite_commit" \
  --arg hiqlite_release "$hiqlite_release" \
  --arg image_release "$image_release" \
  --arg openraft_version "$openraft_version" --arg source_dir "$source_dir" \
  --arg log_sync "$log_sync" --arg image_source "$image_source" \
  --arg source_commit_basis "$source_commit_basis" \
  --arg image_source_commit "$image_source_commit" \
  --arg lockfile_origin "$lockfile_origin" \
  --arg lockfile_sha256 "$lockfile_sha256" \
  --arg ingress_kind "$ingress_kind" \
  --arg ingress_version "$ingress_version" \
  --arg ingress_image "$ingress_image" \
  --arg proxy_patch_sha256 "$proxy_patch_sha256" \
  --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
  --arg requested_image "$requested_image" --arg resolved_image "$resolved_image" \
  --argjson exact_source_build "$build_image" \
  --argjson pvc_count "$pvc_count" --arg rustfs_uid "$rustfs_uid" \
  '{schema_version:1,system:"hiqlite",event:"run_started",run_id:$run_id,
    hiqlite_reference_commit:$hiqlite_commit,
    hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    hiqlite_reference_release:$hiqlite_release,
    hiqlite_release:(if $image_release == "" then null else $image_release end),
    openraft_version:$openraft_version,
    source_dir:(if $exact_source_build == 1 then $source_dir else null end),
    source_commit_basis:$source_commit_basis,
    image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    cargo_lock_origin:$lockfile_origin,
    cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
    ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
      patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
    upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
    log_sync:$log_sync,
    image_source:$image_source,requested_image:$requested_image,
    resolved_image:$resolved_image,voters:3,storage:"emptyDir",
    zero_pvc:true,pvc_count:$pvc_count,rustfs_uid:$rustfs_uid}' >> "$jsonl"

run_f1_cell() {
  local hold_seconds="$1" cell_id="f1-h$1"
  local service_id="${cell_id}-start-boundary" service_value="healthy-${run_id}"
  local ack_file="$target/${cell_id}-acks.tsv"
  local uids_file="$target/uids-before-${cell_id}.json"
  local started_at started_epoch released_at release_epoch failure_held
  local service_epoch full_epoch service_rto full_rto
  echo "== $cell_id: one peer lost for ${hold_seconds}s =="
  verify_cell_boundary "$cell_id" start
  : > "$ack_file"
  capture_uids "$uids_file"
  scale_failure 2
  k wait --for=delete pod/hiqlite-recovery-2 --timeout=180s >/dev/null
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  probe_window "$cell_id" "$hold_seconds" success success success "$ack_file" "$service_id"
  assert_probe_outcome "$cell_id" write true \
    || die "$cell_id write probe failed while quorum remained"
  assert_probe_outcome "$cell_id" local-query true \
    || die "$cell_id local query probe failed while quorum remained"
  assert_probe_outcome "$cell_id" query-consistent true \
    || die "$cell_id consistent query probe failed while quorum remained"
  released_at="$(iso_now)"
  release_epoch="$(epoch_now)"
  failure_held=$((release_epoch - started_epoch))
  k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
  wait_service "$recovery_timeout" "$service_id" "$service_value" \
    || die "$cell_id service did not recover"
  service_epoch="$(epoch_now)"
  wait_convergence "$recovery_timeout" "$target/${cell_id}-converged-metrics.json" \
    || die "$cell_id did not converge to three voters"
  capture_learner_to_voter_evidence "$cell_id" 3 "$released_at" \
    || die "$cell_id logs did not prove learner-to-voter promotion for node 3"
  markers_lost "$uids_file" 2 || die "$cell_id emptyDir marker was retained"
  verify_ack_file "$ack_file"
  verify_cell_boundary "$cell_id" end
  full_epoch="$(epoch_now)"
  service_rto=$((service_epoch - release_epoch))
  full_rto=$((full_epoch - release_epoch))
  append_phase_summary f1 "$cell_id" 1 "$hold_seconds" \
    '{"write":"success","local_query":"success","query_consistent":"success","auto_recovery":true,"rpo":"0"}' \
    '{"auto_recovery":true,"learner_to_voter":true,"markers_lost":true,"ack_sentinel_preserved":true,"voter_ids":[1,2,3]}' \
    "$started_at" "$released_at" "$service_rto" "$full_rto" "$failure_held"
}

run_f2_cell() {
  local hold_seconds="$1" cell_id="f2-h$1"
  local service_id="${cell_id}-start-boundary" service_value="healthy-${run_id}"
  local backup_id="${cell_id}-backup" after_id="${cell_id}-after-backup"
  local backup_started_at backup_started_epoch backup_key ack_file uids_file
  local started_at started_epoch released_at release_epoch failure_held
  local service_epoch full_epoch service_rto full_rto
  local dr_released_at dr_release_epoch observed
  local auto_recovered=false operator_dr=false rpo_to_backup=false
  echo "== $cell_id: two peers lost for ${hold_seconds}s =="
  verify_cell_boundary "$cell_id" start
  run_client 30 execute "$backup_id" "before-${cell_id}-backup" >/dev/null
  backup_started_at="$(iso_now)"
  backup_started_epoch="$(epoch_now)"
  backup_key="$(trigger_external_backup "$cell_id")" \
    || die "$cell_id prerequisite external backup did not complete"
  record_event "$cell_id" external-backup completed-object completed-object true \
    "$backup_started_at" "$(iso_now)" "$(( $(epoch_now) - backup_started_epoch ))" \
    "$backup_key"
  run_client 30 execute "$after_id" "must-disappear-on-${cell_id}-dr" >/dev/null
  uids_file="$target/uids-before-${cell_id}.json"
  ack_file="$target/${cell_id}-acks.tsv"
  capture_uids "$uids_file"
  : > "$ack_file"
  scale_failure 1
  k wait --for=delete pod/hiqlite-recovery-1 pod/hiqlite-recovery-2 --timeout=180s >/dev/null
  wait_failure_established "$cell_id" false "$service_id" \
    || die "$cell_id did not establish a stable no-quorum failure"
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  probe_window "$cell_id" "$hold_seconds" fail-closed stale-local-allowed fail-closed \
    "$ack_file" "$service_id"
  assert_probe_outcome "$cell_id" write false \
    || die "$cell_id accepted a write without old-cluster quorum"
  assert_probe_outcome "$cell_id" query-consistent false \
    || die "$cell_id served a consistent query without old-cluster quorum"
  released_at="$(iso_now)"
  release_epoch="$(epoch_now)"
  failure_held=$((release_epoch - started_epoch))
  k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
  if wait_convergence "$auto_recovery_timeout" "$target/${cell_id}-auto-metrics.json"; then
    auto_recovered=true
    wait_service "$recovery_timeout" "$service_id" "$service_value" \
      || die "$cell_id membership converged without service"
    service_epoch="$(epoch_now)"
    markers_lost "$uids_file" 1 2 \
      || die "$cell_id unexpected recovery did not replace both emptyDir voters"
    verify_ack_file "$ack_file"
    run_client 30 verify-sentinel "$backup_id" "before-${cell_id}-backup" >/dev/null
    run_client 30 verify-sentinel "$after_id" "must-disappear-on-${cell_id}-dr" >/dev/null
  else
    echo "$cell_id remained fail-closed; invoking operator backup DR"
    operator_dr=true
    k scale statefulset/hiqlite-recovery --replicas=0 >/dev/null
    k wait --for=delete pod -l app.kubernetes.io/component=voter --timeout=180s >/dev/null
    set_restore_object "$backup_key"
    dr_released_at="$(iso_now)"
    dr_release_epoch="$(epoch_now)"
    k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
    wait_service "$recovery_timeout" "$service_id" "$service_value" \
      || die "$cell_id operator DR did not restore service"
    service_epoch="$(epoch_now)"
    wait_convergence "$recovery_timeout" "$target/${cell_id}-dr-metrics.json" \
      || die "$cell_id operator DR did not converge"
    clear_restore_from_running_pods "$cell_id"
    run_client 30 verify-sentinel "$backup_id" "before-${cell_id}-backup" >/dev/null
    verify_missing "$after_id"
    rpo_to_backup=true
    markers_lost "$uids_file" 0 1 2 \
      || die "$cell_id operator DR retained an old emptyDir marker"
    record_event "$cell_id" operator-dr manual-trigger restored true "$dr_released_at" \
      "$(iso_now)" "$(( $(epoch_now) - dr_release_epoch ))" "$backup_key"
  fi
  verify_cell_boundary "$cell_id" end
  full_epoch="$(epoch_now)"
  service_rto=$((service_epoch - release_epoch))
  full_rto=$((full_epoch - release_epoch))
  observed="$(jq -cn \
    --argjson auto_recovery "$auto_recovered" \
    --argjson operator_dr "$operator_dr" \
    --argjson rpo_to_backup "$rpo_to_backup" \
    '{auto_recovery:$auto_recovery,operator_dr:$operator_dr,
      rpo_to_backup:$rpo_to_backup,markers_lost:true,voter_ids:[1,2,3]}')"
  append_phase_summary f2 "$cell_id" 2 "$hold_seconds" \
    '{"write":"fail-closed","query_consistent":"fail-closed","auto_recovery":false,"next":"operator_dr"}' \
    "$observed" "$started_at" "$released_at" "$service_rto" "$full_rto" "$failure_held"
}

run_f3_cell() {
  local hold_seconds="$1" cell_id="f3-h$1"
  local service_id="${cell_id}-start-boundary" service_value="healthy-${run_id}"
  local backup_id="${cell_id}-backup" after_id="${cell_id}-after-backup"
  local backup_started_at backup_started_epoch backup_key ack_file uids_file
  local started_at started_epoch released_at release_epoch failure_held
  local service_epoch full_epoch service_rto full_rto
  echo "== $cell_id: three peers lost for ${hold_seconds}s =="
  verify_cell_boundary "$cell_id" start
  run_client 30 execute "$backup_id" "present-in-${cell_id}-backup" >/dev/null
  backup_started_at="$(iso_now)"
  backup_started_epoch="$(epoch_now)"
  backup_key="$(trigger_external_backup "$cell_id")" \
    || die "$cell_id prerequisite external backup did not complete"
  record_event "$cell_id" external-backup completed-object completed-object true \
    "$backup_started_at" "$(iso_now)" "$(( $(epoch_now) - backup_started_epoch ))" \
    "$backup_key"
  run_client 30 execute "$after_id" "must-disappear-on-${cell_id}-dr" >/dev/null
  uids_file="$target/uids-before-${cell_id}.json"
  ack_file="$target/${cell_id}-acks.tsv"
  capture_uids "$uids_file"
  : > "$ack_file"
  scale_failure 0
  k wait --for=delete pod -l app.kubernetes.io/component=voter --timeout=180s >/dev/null
  wait_failure_established "$cell_id" true "$service_id" \
    || die "$cell_id did not establish a stable zero-voter failure"
  started_at="$(iso_now)"
  started_epoch="$(epoch_now)"
  probe_window "$cell_id" "$hold_seconds" fail-closed unavailable fail-closed \
    "$ack_file" "$service_id"
  assert_probe_outcome "$cell_id" write false \
    || die "$cell_id accepted a write with no voters"
  assert_probe_outcome "$cell_id" local-query false \
    || die "$cell_id served a local query with no voters"
  assert_probe_outcome "$cell_id" query-consistent false \
    || die "$cell_id served a consistent query with no voters"
  set_restore_object "$backup_key"
  released_at="$(iso_now)"
  release_epoch="$(epoch_now)"
  failure_held=$((release_epoch - started_epoch))
  k scale statefulset/hiqlite-recovery --replicas=3 >/dev/null
  wait_service "$recovery_timeout" "$service_id" "$service_value" \
    || die "$cell_id operator DR did not restore service"
  service_epoch="$(epoch_now)"
  wait_convergence "$recovery_timeout" "$target/${cell_id}-converged-metrics.json" \
    || die "$cell_id operator DR did not converge"
  clear_restore_from_running_pods "$cell_id"
  run_client 30 verify-sentinel "$backup_id" "present-in-${cell_id}-backup" >/dev/null
  verify_missing "$after_id"
  markers_lost "$uids_file" 0 1 2 \
    || die "$cell_id operator DR retained an old emptyDir marker"
  verify_cell_boundary "$cell_id" end
  full_epoch="$(epoch_now)"
  record_event "$cell_id" operator-dr manual-trigger restored true "$released_at" \
    "$(iso_now)" "$((full_epoch - release_epoch))" "$backup_key"
  service_rto=$((service_epoch - release_epoch))
  full_rto=$((full_epoch - release_epoch))
  append_phase_summary f3 "$cell_id" 3 "$hold_seconds" \
    '{"write":"fail-closed","query_consistent":"fail-closed","operator_dr":true,"rpo":"to_backup"}' \
    '{"operator_dr":true,"markers_lost":true,"ack_sentinel_preserved":true,"rpo_to_backup":true,"voter_ids":[1,2,3]}' \
    "$started_at" "$released_at" "$service_rto" "$full_rto" "$failure_held"
}

for failure_count in "${failure_values[@]}"; do
  for hold_seconds in "${hold_values[@]}"; do
    case "$failure_count" in
      1) run_f1_cell "$hold_seconds" ;;
      2) run_f2_cell "$hold_seconds" ;;
      3) run_f3_cell "$hold_seconds" ;;
    esac
  done
done

current_rustfs_uid="$(kobj get pod -l app.kubernetes.io/component=object-store \
  -o jsonpath='{.items[0].metadata.uid}')"
[ "$current_rustfs_uid" = "$rustfs_uid" ] \
  || die "RustFS Pod changed during voter failure lifecycle"
[ "$namespace" != "$object_namespace" ] || die "RustFS must be outside the voter namespace"
[ -z "$(k get persistentvolumeclaims -o name)$(kobj get persistentvolumeclaims -o name)" ] \
  || die "recovery drill no longer has zero PVCs"

hold_values_json="$(printf '%s\n' "${hold_values[@]}" | jq -Rsc 'split("\n")[:-1] | map(tonumber)')"
failure_values_json="$(printf '%s\n' "${failure_values[@]}" | jq -Rsc 'split("\n")[:-1] | map(tonumber)')"
expected_cells="$(jq -cn --argjson failures "$failure_values_json" --argjson holds "$hold_values_json" \
  '[$failures[] as $failure | $holds[] as $hold | "f\($failure)-h\($hold)"]')"
expected_cell_count=$(( ${#failure_values[@]} * ${#hold_values[@]} ))
jq -s \
  --arg run_id "$run_id" \
  --arg hiqlite_commit "$hiqlite_commit" \
  --arg hiqlite_release "$hiqlite_release" \
  --arg image_release "$image_release" \
  --arg openraft_version "$openraft_version" \
  --arg log_sync "$log_sync" \
  --arg image_source "$image_source" \
  --arg source_commit_basis "$source_commit_basis" \
  --arg image_source_commit "$image_source_commit" \
  --arg lockfile_origin "$lockfile_origin" \
  --arg lockfile_sha256 "$lockfile_sha256" \
  --arg ingress_kind "$ingress_kind" \
  --arg ingress_version "$ingress_version" \
  --arg ingress_image "$ingress_image" \
  --arg proxy_patch_sha256 "$proxy_patch_sha256" \
  --arg upstream_proxy_incompatibility "$upstream_proxy_incompatibility" \
  --arg resolved_image "$resolved_image" \
  --arg rustfs_uid "$rustfs_uid" \
  --argjson failure_counts "$failure_values_json" \
  --argjson hold_seconds "$hold_values_json" \
  '{schema_version:1,system:"hiqlite",run_id:$run_id,
    hiqlite_reference_commit:$hiqlite_commit,
    hiqlite_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    hiqlite_reference_release:$hiqlite_release,
    hiqlite_release:(if $image_release == "" then null else $image_release end),
    openraft_version:$openraft_version,log_sync:$log_sync,
    image_source:$image_source,source_commit_basis:$source_commit_basis,
    image_source_commit:(if $image_source_commit == "" then null else $image_source_commit end),
    cargo_lock_origin:$lockfile_origin,
    cargo_lock_sha256:(if $lockfile_sha256 == "" then null else $lockfile_sha256 end),
    ingress:{kind:$ingress_kind,version:$ingress_version,image:$ingress_image,
      patch_sha256:(if $proxy_patch_sha256 == "" then null else $proxy_patch_sha256 end)},
    upstream_proxy_incompatibility:$upstream_proxy_incompatibility,
    resolved_image:$resolved_image,
    voters:3,storage:"emptyDir",zero_pvc:true,
    rustfs_uid:$rustfs_uid,failure_counts:$failure_counts,hold_seconds:$hold_seconds,
    phases:[.[] | select(.event == "phase_summary")],events:length}' \
  "$jsonl" > "$summary"
jq -e --argjson expected_cells "$expected_cells" --argjson expected_cell_count "$expected_cell_count" '
  .phases as $phases |
  ($phases | length) == $expected_cell_count and
  ($phases | map(.cell_id)) == $expected_cells and
  ($phases | map(.cell_id) | unique | length) == $expected_cell_count and
  ($phases | all(
    (.failure_count | type) == "number" and
    (.hold_seconds | type) == "number" and
    (.hold_seconds >= 0) and
    (.failure_held_seconds | type) == "number" and
    (.failure_held_seconds >= .hold_seconds) and
    (.phase == "f\(.failure_count)") and
    (.cell_id == "f\(.failure_count)-h\(.hold_seconds)")
  ))
' "$summary" >/dev/null
echo "Hiqlite zero-PVC recovery drill passed: $summary"
