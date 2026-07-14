#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# shellcheck disable=SC2016 # These are literal source checks.
grep -Fq 'peer_tokens="$(for _ in 1 2 3; do openssl rand -hex 24; done' scripts/bench-vind.sh
# shellcheck disable=SC2016 # These are literal jq source checks.
grep -Fq 'token:$tokens[$n]' scripts/bench-vind.sh
grep -Fq 'export QUEQLITE_S3_ENDPOINT=http://rustfs:9000 QUEQLITE_OBJECT_SECRET=rustfs-credentials' scripts/bench-vind.sh
grep -Fq 'export QUEQLITE_S3_ALLOW_HTTP=true' scripts/bench-vind.sh

# shellcheck disable=SC1091 # Repository-local source; callers run from repo root.
source scripts/bench-vind.sh

render_verified_provenance_json() {
  render_provenance_json "$@" 0 0 ok ok
}

declare -F endpoint_ready >/dev/null
(
  stall_port_file="$tmp/stall-port"
  python3 -c 'import socket,sys,time; s=socket.socket(); s.bind(("127.0.0.1",0)); s.listen(); open(sys.argv[1],"w").write(str(s.getsockname()[1])); c,_=s.accept(); time.sleep(10)' \
    "$stall_port_file" & stall_server_pid=$!
  trap 'kill "$stall_server_pid" 2>/dev/null || true; wait "$stall_server_pid" 2>/dev/null || true' EXIT
  for _ in $(seq 1 500); do [ -s "$stall_port_file" ] && break; sleep 0.01; done
  [ -s "$stall_port_file" ]
  if endpoint_ready "http://127.0.0.1:$(cat "$stall_port_file")"; then
    echo "stalled readiness probe was accepted" >&2
    exit 1
  fi
  kill -0 "$stall_server_pid"
)

: > "$tmp/empty-resources.jsonl"
if validate_resource_sample_schema "$tmp/empty-resources.jsonl"; then
  echo "empty resource evidence was accepted" >&2
  exit 1
fi
resource_sample() {
  jq -cn --arg app "$1" --argjson epoch "$2" --argjson cpu "${3:-$2}" \
    --argjson memory "${4:-2}" \
    '{timestamp:"2026-07-13T00:00:00Z",timestamp_epoch_seconds:$epoch,
      source:"containerd_cri_stats",app:$app,pod:($app + "-0"),pod_uid:($app + "-uid"),
      container:$app,container_id:($app + "-container"),restart_count:0,
      cpu_usage_usec:$cpu,memory_bytes:$memory}'
}
resource_sample queqlite 100 > "$tmp/resources.jsonl"
if validate_resource_sample_schema "$tmp/resources.jsonl"; then
  echo "resource evidence without simulator samples was accepted" >&2
  exit 1
fi
resource_sample simulator 100 >> "$tmp/resources.jsonl"
validate_resource_sample_schema "$tmp/resources.jsonl"
if validate_resource_samples "$tmp/resources.jsonl" 120 200 2; then
  echo "single resource sample per app was accepted" >&2
  exit 1
fi
cp "$tmp/resources.jsonl" "$tmp/stale-resources.jsonl"
resource_sample queqlite 150 >> "$tmp/stale-resources.jsonl"
resource_sample simulator 150 >> "$tmp/stale-resources.jsonl"
if validate_resource_samples "$tmp/stale-resources.jsonl" 120 200 2; then
  echo "resource evidence that ends before measurement coverage was accepted" >&2
  exit 1
fi
{
  resource_sample queqlite 150
  resource_sample simulator 150
  resource_sample queqlite 199
  resource_sample simulator 199
} > "$tmp/late-resources.jsonl"
if validate_resource_samples "$tmp/late-resources.jsonl" 120 200 2; then
  echo "resource evidence that starts after measurement was accepted" >&2
  exit 1
fi
{
  resource_sample queqlite 118
  resource_sample simulator 118
  resource_sample queqlite 122
  resource_sample simulator 122
  resource_sample queqlite 198
  resource_sample simulator 198
  resource_sample queqlite 202
  resource_sample simulator 202
} > "$tmp/gapped-resources.jsonl"
if validate_resource_samples "$tmp/gapped-resources.jsonl" 120 200 2; then
  echo "resource evidence with a measurement outage was accepted" >&2
  exit 1
fi
resource_sample queqlite 201 >> "$tmp/resources.jsonl"
resource_sample simulator 201 >> "$tmp/resources.jsonl"
{
  for epoch in $(seq 118 6 202); do
    resource_sample queqlite "$epoch"
    resource_sample simulator "$epoch"
  done
} > "$tmp/jittered-resources.jsonl"
validate_resource_samples "$tmp/jittered-resources.jsonl" 120 200 2

{
  resource_sample queqlite 100
  resource_sample simulator 100
  for epoch in $(seq 122 6 200); do
    resource_sample queqlite "$epoch"
    resource_sample simulator "$epoch"
  done
  resource_sample queqlite 202
  resource_sample simulator 202
} > "$tmp/stale-boundary-resources.jsonl"
if validate_resource_samples "$tmp/stale-boundary-resources.jsonl" 120 200 2; then
  echo "resource evidence with a stale measurement boundary was accepted" >&2
  exit 1
fi

{
  resource_sample queqlite 0 999999 999999
  resource_sample queqlite 100
  resource_sample queqlite 150
  resource_sample queqlite 200
  resource_sample queqlite 999 999999 999999
  resource_sample simulator 0 999999 999999
  resource_sample simulator 99
  resource_sample simulator 149
  resource_sample simulator 201
  resource_sample simulator 999 999999 999999
} > "$tmp/window-resources.jsonl"
validate_resource_samples "$tmp/window-resources.jsonl" 120 190 50
summarize_resource_samples "$tmp/window-resources.jsonl" 120 190 > "$tmp/resource-summary.json"
jq -e '.samples == 6 and
  (all(.apps[]; .peak_memory_bytes == 2 and .average_memory_bytes == 2)) and
  ([.container_cpu_usage_usec_deltas[] | {key:.app,value:.delta_usec}] | from_entries) ==
    {queqlite:100,simulator:102}' \
  "$tmp/resource-summary.json" >/dev/null

: > "$tmp/empty-access.jsonl"
printf '%s\n' '{"metering":{"enabled":true,"status":"failed","requests":0},"retained":{"status":"ok","object_count":0,"retained_bytes":0}}' > "$tmp/invalid-meter.json"
if validate_object_evidence "$tmp/missing-access.jsonl" "$tmp/invalid-meter.json"; then
  echo "unreadable object meter output was accepted" >&2
  exit 1
fi
printf '%s\n' '{"metering":{"enabled":true,"status":"ok","requests":0},"retained":{"status":"ok","object_count":0,"retained_bytes":0}}' > "$tmp/empty-usage.json"
validate_object_evidence "$tmp/empty-access.jsonl" "$tmp/empty-usage.json"
printf '%s\n' '{"metering":{"enabled":true,"status":"ok","requests":0},"retained":{"status":"failed","object_count":null,"retained_bytes":null}}' > "$tmp/invalid-usage.json"
if validate_object_evidence "$tmp/empty-access.jsonl" "$tmp/invalid-usage.json"; then
  echo "null object inventory was accepted" >&2
  exit 1
fi
printf '%s\n' '{"method":"PUT","status":"200","request_bytes":"1","response_bytes":"2"}' > "$tmp/access.jsonl"
printf '%s\n' '{"metering":{"enabled":true,"status":"ok","requests":1},"retained":{"status":"ok","object_count":1,"retained_bytes":2}}' > "$tmp/usage.json"
validate_object_evidence "$tmp/access.jsonl" "$tmp/usage.json"
[ "$(evidence_exit_status 0 failed ok)" = 1 ]
[ "$(evidence_exit_status 7 ok ok)" = 7 ]
[ "$(evidence_exit_status 0 ok ok failed)" = 1 ]
failed_evidence="$(render_evidence_json failed ok true true)"
jq -e '.status == "failed" and .resource_sampling.status == "failed" and
  .object_metering.status == "ok"' <<< "$failed_evidence" >/dev/null
jq -e '.status == "disabled" and
  (.resource_sampling.enabled | not) and (.object_metering.enabled | not)' \
  <<< "$(render_evidence_json disabled disabled false false)" >/dev/null
[ "$(cleanup_outcome 0 1)" = ok ]
[ "$(cleanup_outcome 1 1)" = failed ]
[ "$(cleanup_outcome 0 0)" = failed ]
jq -e '.status == "failed" and (.cleaned_up | not)' \
  <<< "$(render_cleanup_json failed failed ok)" >/dev/null
jq -e '(.requested | not) and (.cleaned_up | not) and .namespace == "retained"' \
  <<< "$(render_cleanup_json skipped retained retained)" >/dev/null
jq -e '.started_at_epoch_seconds == 120 and .finished_at_epoch_seconds == 200' \
  <<< "$(render_measurement_window_json 120 200)" >/dev/null
printf '%s\n' '{"measurement":{"measurement_window":{"started_at_epoch_seconds":120.25,"finished_at_epoch_seconds":200.75}}}' > "$tmp/benchmark-report.json"
jq -e '.started_at_epoch_seconds == 120.25 and .finished_at_epoch_seconds == 200.75' \
  <<< "$(measurement_window_from_report "$tmp/benchmark-report.json")" >/dev/null

image_digest="sha256:$(printf 'a%.0s' {1..64})"
image_id="docker-pullable://queqlite@${image_digest}"
repo_digest="example/queqlite@sha256:$(printf 'b%.0s' {1..64})"
source_commit="$(printf 'c%.0s' {1..40})"
client_sha256="$(printf 'd%.0s' {1..64})"
rustc_vv=$'rustc 1.90.0\nbinary: rustc\ncommit-hash: fixture'
cargo_version='cargo 1.90.0 (fixture)'
inspect="$(jq -cn --arg id "$image_id" --arg digest "$repo_digest" '[{Id:$id,RepoDigests:[$digest]}]')"
matching_inspect="$(jq -cn --arg id "$image_id" --arg digest "$repo_digest" --arg revision "$source_commit" \
  '[{Id:$id,RepoDigests:[$digest],Config:{Labels:{"org.opencontainers.image.revision":$revision}}}]')"
runtime_images="$(jq -cn \
  --arg queqlite "containerd://${image_digest}" \
  --arg rustfs "docker-pullable://rustfs@sha256:$(printf '2%.0s' {1..64})" \
  --arg meter "containerd://sha256:$(printf '3%.0s' {1..64})" \
  --arg inventory "docker-pullable://aws@sha256:$(printf '4%.0s' {1..64})" \
  '{queqlite:[$queqlite,$queqlite,$queqlite],rustfs:[$rustfs],
    object_meter:[$meter],aws_cli_inventory:[$inventory]}')"
pod_status_fixture="$(jq -cn --argjson identities "$runtime_images" '{items:[
  {metadata:{labels:{"app.kubernetes.io/name":"queqlite"}},status:{containerStatuses:[
    {name:"queqlite",imageID:$identities.queqlite[0]}]}},
  {metadata:{labels:{"app.kubernetes.io/name":"queqlite"}},status:{containerStatuses:[
    {name:"queqlite",imageID:$identities.queqlite[1]}]}},
  {metadata:{labels:{"app.kubernetes.io/name":"queqlite"}},status:{containerStatuses:[
    {name:"queqlite",imageID:$identities.queqlite[2]}]}},
  {metadata:{labels:{"app.kubernetes.io/name":"rustfs"}},status:{containerStatuses:[
    {name:"rustfs",imageID:$identities.rustfs[0]},
    {name:"object-meter",imageID:$identities.object_meter[0]}]}}
]}')"
observed_runtime="$(printf '%s\n' "$pod_status_fixture" | runtime_image_ids_from_pods)"
printf '%s\n' "$observed_runtime" | jq -e --argjson expected "$runtime_images" '
  .queqlite == $expected.queqlite and .rustfs == $expected.rustfs and
  .object_meter == $expected.object_meter and .aws_cli_inventory == []
' >/dev/null
built_provenance="$(render_verified_provenance_json "$source_commit" false built queqlite:dev "$inspect" \
  "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
printf '%s\n' "$built_provenance" | jq -e --arg id "$image_digest" --arg digest "$repo_digest" '
  .publishable == true and .source.dirty == false and .image.build_mode == "built" and
  .image.content_id == $id and .image.repo_digests == [$digest] and .reasons == [] and
  (.execution.benchmark_client.sha256 | test("^[0-9a-f]{64}$")) and
  (.execution.toolchain.rustc_vv | startswith("rustc ")) and
  (.execution.toolchain.cargo_version | startswith("cargo ")) and
  all(.execution.runtime_images[]; .status == "verified" and (.image_digests | length) == 1)
' >/dev/null
failed_run_provenance="$(render_provenance_json "$source_commit" false built queqlite:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true \
  9 9 failed failed)"
printf '%s\n' "$failed_run_provenance" | jq -e '
  .publishable == false and
  (["benchmark_failed","run_failed","evidence_failed","cleanup_failed"] - .reasons == [])
' >/dev/null
kept_run_provenance="$(render_provenance_json "$source_commit" false built queqlite:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true \
  0 0 ok skipped)"
printf '%s\n' "$kept_run_provenance" | jq -e '
  .publishable == false and (.reasons | index("cleanup_not_verified") != null)
' >/dev/null
heterogeneous_runtime="$(printf '%s\n' "$runtime_images" | jq -c \
  --arg image "containerd://sha256:$(printf '5%.0s' {1..64})" '.queqlite[2] = $image')"
heterogeneous_provenance="$(render_verified_provenance_json "$source_commit" false built queqlite:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$heterogeneous_runtime" true)"
printf '%s\n' "$heterogeneous_provenance" | jq -e '
  .publishable == false and (.reasons | index("heterogeneous_queqlite_runtime_images") != null)
' >/dev/null
short_runtime="$(printf '%s\n' "$runtime_images" | jq -c '.queqlite = .queqlite[:2]')"
short_provenance="$(render_verified_provenance_json "$source_commit" false built queqlite:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$short_runtime" true)"
printf '%s\n' "$short_provenance" | jq -e '
  .publishable == false and (.reasons | index("unexpected_queqlite_runtime_image_count") != null)
' >/dev/null
mismatched_image="containerd://sha256:$(printf '6%.0s' {1..64})"
mismatched_runtime="$(printf '%s\n' "$runtime_images" | jq -c --arg image "$mismatched_image" \
  '.queqlite = [$image,$image,$image]')"
mismatched_provenance="$(render_verified_provenance_json "$source_commit" false built queqlite:dev \
  "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$mismatched_runtime" true)"
printf '%s\n' "$mismatched_provenance" | jq -e '
  .publishable == false and (.reasons | index("queqlite_runtime_image_mismatch") != null)
' >/dev/null
matching_provenance="$(render_verified_provenance_json "$source_commit" false skip-build queqlite:dev \
  "$matching_inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
printf '%s\n' "$matching_provenance" | jq -e --arg commit "$source_commit" '
  .publishable == true and .image.source_revision == $commit and .reasons == []
' >/dev/null
for revision in missing mismatch; do
  if [ "$revision" = missing ]; then candidate="$inspect"
  else candidate="$(jq -cn --arg id "$image_id" --arg revision deadbeef \
    '[{Id:$id,RepoDigests:[],Config:{Labels:{"org.opencontainers.image.revision":$revision}}}]')"
  fi
  unverified="$(render_verified_provenance_json "$source_commit" false skip-build queqlite:dev "$candidate" \
    "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
  printf '%s\n' "$unverified" | jq -e \
    '.publishable == false and .reasons == ["unverified_image_source"]' >/dev/null
done
for identity in benchmark_client rustc cargo; do
  identity_sha="$client_sha256" identity_rustc="$rustc_vv" identity_cargo="$cargo_version"
  case "$identity" in
    benchmark_client) identity_sha="" ;;
    rustc) identity_rustc="" ;;
    cargo) identity_cargo="" ;;
  esac
  missing_identity="$(render_verified_provenance_json "$source_commit" false built queqlite:dev "$inspect" \
    "$identity_sha" "$identity_rustc" "$identity_cargo" "$runtime_images" true)"
  reason="missing_or_invalid_${identity}_$( [ "$identity" = benchmark_client ] && printf sha256 || printf version )"
  printf '%s\n' "$missing_identity" | jq -e --arg reason "$reason" \
    '.publishable == false and (.reasons | index($reason) != null)' >/dev/null
done
for component in queqlite rustfs object_meter aws_cli_inventory; do
  for invalid in missing mutable; do
    if [ "$invalid" = missing ]; then value='[]'; else value='["latest"]'; fi
    invalid_runtime="$(printf '%s\n' "$runtime_images" | jq -c --arg component "$component" \
      --argjson value "$value" '.[$component] = $value')"
    invalid_provenance="$(render_verified_provenance_json "$source_commit" false built queqlite:dev \
      "$inspect" "$client_sha256" "$rustc_vv" "$cargo_version" "$invalid_runtime" true)"
    reason="missing_or_invalid_${component}_runtime_image"
    printf '%s\n' "$invalid_provenance" | jq -e --arg reason "$reason" \
      '.publishable == false and (.reasons | index($reason) != null)' >/dev/null
  done
done
disabled_runtime="$(printf '%s\n' "$runtime_images" | jq -c \
  '.object_meter = ["latest"] | .aws_cli_inventory = []')"
disabled_provenance="$(render_verified_provenance_json "$source_commit" false built queqlite:dev "$inspect" \
  "$client_sha256" "$rustc_vv" "$cargo_version" "$disabled_runtime" false)"
printf '%s\n' "$disabled_provenance" | jq -e '
  .publishable == true and .execution.runtime_images.object_meter.status == "not_applicable" and
  .execution.runtime_images.aws_cli_inventory.status == "not_applicable"
' >/dev/null
dirty_provenance="$(render_verified_provenance_json "$source_commit" true built queqlite:dev "$inspect" \
  "$client_sha256" "$rustc_vv" "$cargo_version" "$runtime_images" true)"
printf '%s\n' "$dirty_provenance" | jq -e \
  '.publishable == false and (.reasons | index("dirty_source") != null)' >/dev/null
missing_provenance="$(render_verified_provenance_json "$source_commit" false skip-build queqlite:dev \
  '[{"Id":"","RepoDigests":[]}]' "$client_sha256" "$rustc_vv" "$cargo_version" \
  "$runtime_images" true)"
printf '%s\n' "$missing_provenance" | jq -e \
  '.publishable == false and (.reasons | index("missing_immutable_image_identity") != null)' >/dev/null

build_line="$(grep -n 'cargo build --release --locked --manifest-path bench/Cargo.toml --bin queqlite-bench' scripts/bench-vind.sh | cut -d: -f1)"
# shellcheck disable=SC2016 # Literal source-order check.
sample_line="$(grep -n ': > "$resources_jsonl"' scripts/bench-vind.sh | cut -d: -f1)"
meter_line="$(grep -n "k exec .*': > /var/log/nginx/s3-access.log'" scripts/bench-vind.sh | cut -d: -f1)"
[ -n "$build_line" ] && [ "$build_line" -lt "$sample_line" ] && [ "$build_line" -lt "$meter_line" ]
# shellcheck disable=SC2016 # Literal direct-execution check.
grep -Fq 'QUEQLITE_CLIENT_TOKEN="$client_token" "$bench_binary" "${bench_args[@]}"' scripts/bench-vind.sh
grep -Fq 'scripts/check-bench-vind-static.sh' .github/workflows/ci.yml
grep -Fq 'YQ_VERSION: v4.47.2' .github/workflows/ci.yml

printf '%s\n' 'fixture port-forward failure' > "$tmp/port-forward-0.log"
if failure="$(
  # shellcheck disable=SC2034 # Read by assert_port_forward_alive from the sourced script.
  target="$tmp"
  # shellcheck disable=SC2034 # Read by assert_port_forward_alive from the sourced script.
  endpoint_urls=(http://127.0.0.1:18080)
  false &
  port_forward_pids=("$!")
  while kill -0 "${port_forward_pids[0]}" 2>/dev/null; do :; done
  assert_port_forward_alive 0 2>&1
)"; then
  echo "dead port-forward was accepted" >&2
  exit 1
fi
grep -Fq 'port-forward exited with status' <<< "$failure"
grep -Fq 'http://127.0.0.1:18080' <<< "$failure"
grep -Fq 'fixture port-forward failure' <<< "$failure"

printf '%s\n' 'fixture non-target failure' > "$tmp/port-forward-1.log"
if failure="$(
  target="$tmp"
  endpoint_urls=(http://127.0.0.1:18080 http://127.0.0.1:18081)
  sleep 30 & live_pid=$!
  trap 'kill "$live_pid" 2>/dev/null || true' EXIT
  false & dead_pid=$!
  port_forward_pids=("$live_pid" "$dead_pid")
  while kill -0 "$dead_pid" 2>/dev/null; do :; done
  assert_all_port_forwards_alive 2>&1
)"; then
  echo "dead non-target port-forward was accepted" >&2
  exit 1
fi
grep -Fq 'http://127.0.0.1:18081' <<< "$failure"
grep -Fq 'fixture non-target failure' <<< "$failure"

fake_bin="$tmp/fake-bin"
mkdir "$fake_bin"
# shellcheck disable=SC2016 # Written into the fake kubectl script verbatim.
printf '%s\n' '#!/usr/bin/env bash' \
  'printf "%s\n" "$*" >> "$KUBECTL_LOG"' \
  'case " $* " in' \
  '  *" get pod "*)' \
  '    if [ -n "${KUBECTL_REPLACEMENT_UID:-}" ] && [ -e "$KUBECTL_OLD_UID_SEEN" ]; then printf "%s" "$KUBECTL_REPLACEMENT_UID"; else : > "$KUBECTL_OLD_UID_SEEN"; printf old-pod-uid; fi ;;' \
  '  *" delete pod "*) [ "${KUBECTL_DELETE_FAIL:-1}" = 0 ] || exit 17 ;;' \
  '  *" wait "*) exit 0 ;;' \
  'esac' > "$fake_bin/kubectl"
chmod +x "$fake_bin/kubectl"
export KUBECTL_LOG="$tmp/kubectl.log"
export KUBECTL_OLD_UID_SEEN="$tmp/old-uid-seen"
failed_delete_command="$(build_pod_delete_fault_command fixture fixture queqlite-c1-1 '' '' '')"
if PATH="$fake_bin:$PATH" sh -c "$failed_delete_command"; then
  echo "failed pod deletion was accepted" >&2
  exit 1
fi
grep -Fq 'delete pod queqlite-c1-1' "$KUBECTL_LOG"
if grep -Fq ' wait ' "$KUBECTL_LOG"; then
  echo "replacement wait ran after failed pod deletion" >&2
  exit 1
fi
printf '%s\n' '#!/bin/sh' 'printf "1\n"' > "$fake_bin/seq"
printf '%s\n' '#!/bin/sh' 'exit 0' > "$fake_bin/sleep"
chmod +x "$fake_bin/seq" "$fake_bin/sleep"
: > "$KUBECTL_LOG"
if KUBECTL_DELETE_FAIL=0 PATH="$fake_bin:$PATH" sh -c "$failed_delete_command"; then
  echo "same-identity replacement pod was accepted" >&2
  exit 1
fi
if grep -Fq ' wait ' "$KUBECTL_LOG"; then
  echo "replacement wait ran for the deleted pod identity" >&2
  exit 1
fi
: > "$KUBECTL_LOG"
rm -f "$KUBECTL_OLD_UID_SEEN"
if ! KUBECTL_DELETE_FAIL=0 KUBECTL_REPLACEMENT_UID=new-pod-uid \
  PATH="$fake_bin:$PATH" sh -c "$failed_delete_command"; then
  echo "different ready replacement pod was rejected" >&2
  exit 1
fi
grep -Fq 'wait --for=condition=Ready pod/queqlite-c1-1' "$KUBECTL_LOG"

rebind_fixture="$tmp/rebind"
mkdir "$rebind_fixture"
(
  target="$rebind_fixture"
  endpoint_urls=(http://127.0.0.1:18081)
  printf '0\n' > "$rebind_fixture/starts"
  k() {
    local invocation
    invocation=$(( $(cat "$rebind_fixture/starts") + 1 ))
    printf '%s\n' "$invocation" > "$rebind_fixture/starts"
    : > "$rebind_fixture/started-$invocation"
    trap ': > "$rebind_fixture/stopped-$invocation"; exit 0' TERM INT
    while true; do sleep 1; done
  }
  curl() { [ -e "$rebind_fixture/allow-ready" ]; }
  wait_for_fixture() {
    for _ in $(seq 1 500); do [ -e "$1" ] && return; sleep 0.01; done
    return 1
  }
  supervise_rebinding_port_forward 0 queqlite-c1-1 18081 & supervisor_pid=$!
  trap 'kill "$supervisor_pid" 2>/dev/null || true; wait "$supervisor_pid" 2>/dev/null || true' EXIT
  wait_for_fixture "$rebind_fixture/started-1"
  : > "$rebind_fixture/port-forward-0.rebind-request"
  wait_for_fixture "$rebind_fixture/stopped-1"
  [ ! -e "$rebind_fixture/started-2" ]
  : > "$rebind_fixture/port-forward-0.replacement-ready"
  wait_for_fixture "$rebind_fixture/started-2"
  : > "$rebind_fixture/allow-ready"
  wait_for_fixture "$rebind_fixture/port-forward-0.rebound"
  kill -0 "$supervisor_pid"
)

benchmark_line="$(grep -n 'QUEQLITE_CLIENT_TOKEN=.*bench_binary' scripts/bench-vind.sh | tail -n 1 | cut -d: -f1)"
final_forward_check_line="$(grep -n 'assert_all_port_forwards_ready' scripts/bench-vind.sh | tail -n 1 | cut -d: -f1)"
[ -n "$final_forward_check_line" ] && [ "$benchmark_line" -lt "$final_forward_check_line" ]

jq -n '{version:1,config_id:1,members:[range(3) as $n | {
  node_id:"node-\($n + 1)",
  url:"http://queqlite-c1-\($n).queqlite-c1:8081",
  log_url:"http://queqlite-c1-\($n).queqlite-c1:8080",
  token:"fixture-peer-\($n + 1)"
}]}' > "$tmp/config.json"
jq -e '(.members | length) == 3 and ([.members[].token] | unique | length) == 3' \
  "$tmp/config.json" >/dev/null

export QUEQLITE_IMAGE=queqlite:fixture QUEQLITE_CLUSTER_ID=queqlite-vind
export QUEQLITE_RECOVERY_GENERATION=1 QUEQLITE_STARTUP_MODE=bootstrap
export QUEQLITE_S3_ENDPOINT=http://rustfs:9000 QUEQLITE_OBJECT_SECRET=rustfs-credentials
export QUEQLITE_S3_ALLOW_HTTP=true
QUEQLITE_OBJECT_JOB_RENDER_ONLY="$tmp/object-job.yaml" \
  scripts/k8s-object-job.sh 1 "$tmp/config.json" init-checkpoint
scripts/render-k8s-config.sh 1 3 "$tmp/config.json" "$tmp/cluster.yaml"

for manifest in "$tmp/object-job.yaml" "$tmp/cluster.yaml"; do
  yq eval -e '
    .spec.template.spec.containers[0].env[] |
    select(.name == "QUEQLITE_S3_ENDPOINT") |
    .value == "http://rustfs:9000"
  ' "$manifest" >/dev/null
  yq eval -e '
    .spec.template.spec.containers[0].env[] |
    select(.name == "QUEQLITE_S3_ALLOW_HTTP") |
    .value == "true"
  ' "$manifest" >/dev/null
  [ "$(yq eval -r '
    [.spec.template.spec.containers[0].env[] |
      select(.name == "QUEQLITE_S3_ACCESS_KEY" or .name == "QUEQLITE_S3_SECRET_KEY") |
      .valueFrom.secretKeyRef.name] | unique | .[]
  ' "$manifest")" = rustfs-credentials ]
done

echo "vind benchmark static checks passed"
