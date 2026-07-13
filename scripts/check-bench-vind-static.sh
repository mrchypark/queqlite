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
resource_sample queqlite 201 >> "$tmp/resources.jsonl"
resource_sample simulator 201 >> "$tmp/resources.jsonl"
validate_resource_samples "$tmp/resources.jsonl" 120 200 2

for epoch in 0 100 150 200 999; do
  if [ "$epoch" = 0 ] || [ "$epoch" = 999 ]; then cpu=999999; memory=999999
  else cpu="$epoch"; memory=2
  fi
  resource_sample queqlite "$epoch" "$cpu" "$memory"
  resource_sample simulator "$epoch" "$cpu" "$memory"
done > "$tmp/window-resources.jsonl"
summarize_resource_samples "$tmp/window-resources.jsonl" 120 190 > "$tmp/resource-summary.json"
jq -e '.samples == 6 and
  (all(.apps[]; .peak_memory_bytes == 2 and .average_memory_bytes == 2)) and
  (all(.container_cpu_usage_usec_deltas[]; .delta_usec == 100))' \
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

image_id="sha256:$(printf 'a%.0s' {1..64})"
repo_digest="example/queqlite@sha256:$(printf 'b%.0s' {1..64})"
source_commit="$(printf 'c%.0s' {1..40})"
inspect="$(jq -cn --arg id "$image_id" --arg digest "$repo_digest" '[{Id:$id,RepoDigests:[$digest]}]')"
for mode in built skip-build; do
  provenance="$(render_provenance_json "$source_commit" false "$mode" queqlite:dev "$inspect")"
  jq -e --arg mode "$mode" --arg id "$image_id" --arg digest "$repo_digest" '
    .publishable == true and .source.dirty == false and .image.build_mode == $mode and
    .image.content_id == $id and .image.repo_digests == [$digest] and .reasons == []
  ' <<< "$provenance" >/dev/null
done
dirty_provenance="$(render_provenance_json "$source_commit" true built queqlite:dev "$inspect")"
jq -e '.publishable == false and (.reasons | index("dirty_source") != null)' \
  <<< "$dirty_provenance" >/dev/null
missing_provenance="$(render_provenance_json "$source_commit" false skip-build queqlite:dev '[{"Id":"","RepoDigests":[]}]')"
jq -e '.publishable == false and (.reasons | index("missing_immutable_image_identity") != null)' \
  <<< "$missing_provenance" >/dev/null

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
