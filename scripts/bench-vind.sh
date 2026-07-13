#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
cluster="${QUEQLITE_VIND_CLUSTER:-queqlite-bench-${run_id}}"
namespace="${QUEQLITE_K8S_NAMESPACE:-queqlite-bench}"
image="${QUEQLITE_IMAGE:-queqlite:dev}"
rustfs_image="${QUEQLITE_RUSTFS_IMAGE:-rustfs/rustfs:1.0.0-beta.8}"
aws_image="${QUEQLITE_AWS_CLI_IMAGE:-amazon/aws-cli:2.17.36}"
nginx_image="${QUEQLITE_NGINX_IMAGE:-nginx:1.27-alpine}"
object_metering="${QUEQLITE_BENCH_OBJECT_USAGE_METERING:-1}"
resource_sampling="${QUEQLITE_BENCH_RESOURCE_SAMPLING:-1}"
multi_endpoint="${QUEQLITE_BENCH_MULTI_ENDPOINT:-0}"
durability_mode="${QUEQLITE_DURABILITY_MODE-sync}"
durability_max_lag="${QUEQLITE_DURABILITY_MAX_LAG-}"
durability_interval="${QUEQLITE_DURABILITY_INTERVAL-}"
durability_max_lag_set="${QUEQLITE_DURABILITY_MAX_LAG+x}"
durability_interval_set="${QUEQLITE_DURABILITY_INTERVAL+x}"
target_base="${QUEQLITE_BENCH_TARGET_DIR:-target/queqlite-bench}"
duration=30s
warmup=5s
concurrency=4
target_rate=""
workload=mixed
write_percent=50
fault=none
fault_offset=10s
fault_pod=queqlite-c1-1
sample_interval=2
queqlite_cpu_request="${QUEQLITE_BENCH_QUEQLITE_CPU_REQUEST:-250m}"
queqlite_cpu_limit="${QUEQLITE_BENCH_QUEQLITE_CPU_LIMIT:-1000m}"
queqlite_memory_request="${QUEQLITE_BENCH_QUEQLITE_MEMORY_REQUEST:-512Mi}"
queqlite_memory_limit="${QUEQLITE_BENCH_QUEQLITE_MEMORY_LIMIT:-1Gi}"
rustfs_cpu_request="${QUEQLITE_BENCH_RUSTFS_CPU_REQUEST:-250m}"
rustfs_cpu_limit="${QUEQLITE_BENCH_RUSTFS_CPU_LIMIT:-1000m}"
rustfs_memory_request="${QUEQLITE_BENCH_RUSTFS_MEMORY_REQUEST:-512Mi}"
rustfs_memory_limit="${QUEQLITE_BENCH_RUSTFS_MEMORY_LIMIT:-1Gi}"
keep=false
context=""
previous_context=""
created_cluster=false
namespace_created=false
port_forward_pids=()
sampler_pid=""
benchmark_status=255
cleanup_status=0
cleaned_up=false
cleanup_verification_status=skipped
namespace_cleanup_status=not_created
vcluster_cleanup_status=not_created
resource_evidence_status=disabled
object_evidence_status=disabled
object_meter_reset_status=disabled
measurement_started_at_epoch_seconds=""
measurement_finished_at_epoch_seconds=""
source_git_commit=""
source_dirty=true
image_build_mode="unknown"
image_inspect_json='[]'
[ "$resource_sampling" = 0 ] || resource_evidence_status=pending
[ "$object_metering" = 0 ] || {
  object_evidence_status=pending
  object_meter_reset_status=pending
}

usage() {
  printf '%s\n' \
    'usage: scripts/bench-vind.sh [options]' \
    '  --duration D --warmup D --concurrency N --target-rate R' \
    '  --workload read|write|mixed --write-percent N' \
    '  --fault none|pod-delete' \
    '  --fault-offset D --fault-pod POD' \
    '  --sample-interval SECONDS --keep' \
    '' \
    'Resource defaults are 250m/512Mi requests and 1000m/1Gi limits for each' \
    'Queqlite or RustFS container. Override with QUEQLITE_BENCH_{QUEQLITE,RUSTFS}_*' \
    'CPU_{REQUEST,LIMIT} and MEMORY_{REQUEST,LIMIT} environment variables.' \
    'Set QUEQLITE_BENCH_RESOURCE_SAMPLING=0 to omit containerd CRI sampling.' \
    'Set QUEQLITE_BENCH_OBJECT_USAGE_METERING=0 to omit the nginx S3 counting proxy.' \
    'Set QUEQLITE_BENCH_MULTI_ENDPOINT=1 to route retries across all three nodes.' \
    'Durability defaults to sync. Set QUEQLITE_DURABILITY_MODE=bounded with' \
    'QUEQLITE_DURABILITY_MAX_LAG, or periodic with QUEQLITE_DURABILITY_INTERVAL.' \
    '' \
    'It creates a vind cluster, deploys RustFS plus a three-node Queqlite cluster,' \
    'runs bench/queqlite-bench through a local port-forward, and emits artifacts.json.' >&2
}

die() { echo "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || die "missing required command: $1"; }

assert_port_forward_alive() {
  local index="$1" pid="${port_forward_pids[$1]}" status=0
  kill -0 "$pid" 2>/dev/null && return
  if wait "$pid"; then status=0; else status=$?; fi
  echo "port-forward exited with status $status: ${endpoint_urls[$index]}" >&2
  sed 's/^/  /' "$target/port-forward-$index.log" >&2
  exit 1
}

validate_resource_sample_schema() {
  jq -s -e '
    any(.[]; .app == "queqlite") and any(.[]; .app == "simulator") and all(.[];
      (.timestamp | type == "string" and length > 0) and
      (.timestamp_epoch_seconds | type == "number" and . >= 0) and
      .source == "containerd_cri_stats" and
      (.app == "queqlite" or .app == "simulator") and
      ([.pod,.pod_uid,.container,.container_id] | all(type == "string")) and
      ([.restart_count,.cpu_usage_usec,.memory_bytes] | all(type == "number" and . >= 0)))
  ' "$1" >/dev/null 2>&1
}

validate_resource_samples() {
  validate_resource_sample_schema "$1" && jq -s -e \
    --argjson start "$2" --argjson end "$3" --argjson interval "$4" '
    . as $samples |
    $start >= 0 and $end >= $start and $interval > 0 and
    all(["queqlite","simulator"][];
      . as $app |
      [$samples[] | select(.app == $app)] as $app_samples |
      ($app_samples | length) >= 2 and
      ([$app_samples[].timestamp_epoch_seconds] | min) <= $start and
      ([$app_samples[].timestamp_epoch_seconds] | max) >= $end and
      ([$app_samples[].timestamp_epoch_seconds] | max) >
        ([$app_samples[].timestamp_epoch_seconds] | min))
  ' "$1" >/dev/null 2>&1
}

resource_samples_cover_end() {
  jq -s -e --argjson end "$2" '
    . as $samples |
    all(["queqlite","simulator"][];
      . as $app | any($samples[]; .app == $app and .timestamp_epoch_seconds >= $end))
  ' "$1" >/dev/null 2>&1
}

summarize_resource_samples() {
  jq -s --argjson start "$2" --argjson end "$3" '
    . as $samples |
    ([$samples[].timestamp_epoch_seconds | select(. <= $start)] | max) as $before |
    ([$samples[].timestamp_epoch_seconds | select(. >= $end)] | min) as $after |
    [$samples[] | select(
      .timestamp_epoch_seconds == $before or
      (.timestamp_epoch_seconds >= $start and .timestamp_epoch_seconds <= $end) or
      .timestamp_epoch_seconds == $after)] as $window |
    def cpu_deltas: group_by([.app,.pod_uid,.container,.container_id]) |
      map(sort_by(.timestamp_epoch_seconds) as $g |
        {app:$g[0].app,pod:$g[0].pod,pod_uid:$g[0].pod_uid,container:$g[0].container,
         container_id:$g[0].container_id,first:$g[0].cpu_usage_usec,last:$g[-1].cpu_usage_usec,
         delta_usec:([0,($g[-1].cpu_usage_usec - $g[0].cpu_usage_usec)] | max)});
    def memory_by_app: group_by([.timestamp_epoch_seconds,.app]) |
      map({timestamp:.[0].timestamp,timestamp_epoch_seconds:.[0].timestamp_epoch_seconds,
        app:.[0].app,memory_bytes:(map(.memory_bytes) | add)});
    ($window | cpu_deltas) as $cpu | ($window | memory_by_app) as $memory |
    {status:"ok",measurement_window:{started_at_epoch_seconds:$start,
      finished_at_epoch_seconds:$end},samples:($window | length),
     container_cpu_usage_usec_deltas:$cpu,
     apps:(["queqlite","simulator"] | map(. as $app |
       ($memory | map(select(.app == $app))) as $app_memory |
       {app:$app,cpu_usage_usec:($cpu | map(select(.app == $app) | .delta_usec) | add // 0),
        memory_samples:($app_memory | length),
        average_memory_bytes:(if ($app_memory | length) == 0 then null else
          (($app_memory | map(.memory_bytes) | add) / ($app_memory | length) | floor) end),
        peak_memory_bytes:($app_memory | map(.memory_bytes) | max // null)}))}
  ' "$1"
}

validate_object_evidence() {
  local request_count
  [ -r "$1" ] || return 1
  request_count="$(jq -s -er '
    def numeric: type == "number" or (type == "string" and test("^[0-9]+$"));
    if all(.[];
      (.method | type == "string" and length > 0) and
      (.status | numeric) and (.request_bytes | numeric) and (.response_bytes | numeric))
    then length else error("invalid meter output") end
  ' "$1" 2>/dev/null)" || return 1
  jq -e --argjson request_count "$request_count" '
      .metering.enabled == true and .metering.status == "ok" and
      .metering.requests == $request_count and
      .retained.status == "ok" and
      (.retained.object_count | type == "number" and . >= 0) and
      (.retained.retained_bytes | type == "number" and . >= 0)
    ' "$2" >/dev/null 2>&1
}

evidence_overall_status() {
  if [ "$1" = failed ] || [ "$2" = failed ]; then
    echo failed
  elif [ "$1" = disabled ] && [ "$2" = disabled ]; then
    echo disabled
  else
    echo ok
  fi
}

evidence_exit_status() {
  if [ "$1" -ne 0 ]; then echo "$1"
  elif [ "$(evidence_overall_status "$2" "$3")" = failed ] || [ "${4:-ok}" = failed ]; then echo 1
  else echo 0
  fi
}

render_evidence_json() {
  jq -n --arg resource "$1" --arg object "$2" \
    --argjson resource_enabled "$3" --argjson object_enabled "$4" \
    --arg overall "$(evidence_overall_status "$1" "$2")" \
    '{status:$overall,
      resource_sampling:{enabled:$resource_enabled,status:$resource},
      object_metering:{enabled:$object_enabled,status:$object}}'
}

cleanup_outcome() {
  if [ "$1" -eq 0 ] && [ "$2" -ne 0 ]; then echo ok; else echo failed; fi
}

render_cleanup_json() {
  jq -n --arg status "$1" --arg namespace_status "$2" --arg vcluster_status "$3" \
    '{requested:($status != "skipped"),status:$status,cleaned_up:($status == "ok"),
      namespace:$namespace_status,vcluster:$vcluster_status}'
}

render_measurement_window_json() {
  jq -n --arg started "$1" --arg finished "$2" \
    '{started_at_epoch_seconds:(if $started == "" then null else ($started | tonumber) end),
      finished_at_epoch_seconds:(if $finished == "" then null else ($finished | tonumber) end)}'
}

measurement_window_from_report() {
  jq -ce '
    .measurement.measurement_window as $window |
    $window |
    select(($window.started_at_epoch_seconds | type == "number" and . >= 0) and
      ($window.finished_at_epoch_seconds | type == "number" and . >= 0) and
      ($window.finished_at_epoch_seconds >= $window.started_at_epoch_seconds))
  ' "$1"
}

render_provenance_json() {
  jq -n --arg commit "$1" --argjson dirty "$2" --arg build_mode "$3" \
    --arg image_reference "$4" --argjson inspect "$5" '
    ($inspect[0].Id // "") as $content_id |
    (($inspect[0].RepoDigests // []) | map(select(type == "string"))) as $repo_digests |
    ([if ($commit | test("^[0-9a-f]{40,64}$") | not) then "missing_git_commit" else empty end,
      if $dirty then "dirty_source" else empty end,
      if ($content_id | test("^sha256:[0-9a-f]{64}$") | not)
        then "missing_immutable_image_identity" else empty end]) as $reasons |
    {publishable:($reasons | length == 0),reasons:$reasons,
     source:{git_commit:(if $commit == "" then null else $commit end),dirty:$dirty,clean:($dirty | not)},
     image:{reference:$image_reference,build_mode:$build_mode,
       content_id:(if $content_id == "" then null else $content_id end),repo_digests:$repo_digests}}
  '
}

# Allow the static check to exercise process-failure handling without starting a cluster.
[ "${BASH_SOURCE[0]}" = "$0" ] || return 0

validate_duration() {
  local name="$1" value="$2" amount
  case "$value" in
    *ms) amount="${value%ms}" ;;
    *s|*m|*h) amount="${value%?}" ;;
    *) die "$name must be a positive duration with ms/s/m/h suffix" ;;
  esac
  case "$amount" in ''|*[!0-9]*) die "$name must be a positive duration with ms/s/m/h suffix" ;; esac
  [ -n "${amount//0/}" ] || die "$name must be a positive duration with ms/s/m/h suffix"
}

case "$durability_mode" in
  sync)
    [ -z "$durability_max_lag_set" ] || die "QUEQLITE_DURABILITY_MAX_LAG is irrelevant for sync durability"
    [ -z "$durability_interval_set" ] || die "QUEQLITE_DURABILITY_INTERVAL is irrelevant for sync durability"
    ;;
  bounded)
    [ -n "$durability_max_lag_set" ] && [ -n "$durability_max_lag" ] ||
      die "QUEQLITE_DURABILITY_MAX_LAG is required for bounded durability"
    [ -z "$durability_interval_set" ] || die "QUEQLITE_DURABILITY_INTERVAL is irrelevant for bounded durability"
    validate_duration QUEQLITE_DURABILITY_MAX_LAG "$durability_max_lag"
    ;;
  periodic)
    [ -n "$durability_interval_set" ] && [ -n "$durability_interval" ] ||
      die "QUEQLITE_DURABILITY_INTERVAL is required for periodic durability"
    [ -z "$durability_max_lag_set" ] || die "QUEQLITE_DURABILITY_MAX_LAG is irrelevant for periodic durability"
    validate_duration QUEQLITE_DURABILITY_INTERVAL "$durability_interval"
    ;;
  *) die "QUEQLITE_DURABILITY_MODE must be sync|bounded|periodic" ;;
esac

while [ "$#" -gt 0 ]; do
  case "$1" in
    --duration|--warmup|--concurrency|--target-rate|--workload|--write-percent|--fault|--fault-offset|--fault-pod|--sample-interval)
      [ "$#" -ge 2 ] || die "$1 requires a value"
      case "$1" in
        --duration) duration="$2" ;;
        --warmup) warmup="$2" ;;
        --concurrency) concurrency="$2" ;;
        --target-rate) target_rate="$2" ;;
        --workload) workload="$2" ;;
        --write-percent) write_percent="$2" ;;
        --fault) fault="$2" ;;
        --fault-offset) fault_offset="$2" ;;
        --fault-pod) fault_pod="$2" ;;
        --sample-interval) sample_interval="$2" ;;
      esac
      shift 2 ;;
    --keep) keep=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

case "$fault" in none|pod-delete) ;; *) die "--fault must be none or pod-delete";; esac
case "$object_metering" in 0|1) ;; *) die "QUEQLITE_BENCH_OBJECT_USAGE_METERING must be 0 or 1";; esac
case "$resource_sampling" in 0|1) ;; *) die "QUEQLITE_BENCH_RESOURCE_SAMPLING must be 0 or 1";; esac
case "$multi_endpoint" in 0|1) ;; *) die "QUEQLITE_BENCH_MULTI_ENDPOINT must be 0 or 1";; esac
case "$sample_interval" in ''|*[!0-9]*) die "--sample-interval must be a positive integer";; esac
[ "$sample_interval" -gt 0 ] || die "--sample-interval must be a positive integer"
for tool in cargo curl docker jq kubectl openssl sed timeout vcluster yq; do require "$tool"; done

target="$repo_root/$target_base/$run_id"
benchmark_json="$target/benchmark.json"
resources_jsonl="$target/resources.jsonl"
resource_summary="$target/resource-summary.json"
resource_sampler_log="$target/resource-sampler.log"
checkpoint_drain_json="$target/checkpoint-drain.json"
object_access_log="$target/s3-access.jsonl"
object_usage_json="$target/object-usage.json"
artifacts_json="$target/artifacts.json"
rendered_rustfs="$target/rustfs.yaml"
rendered_cluster="$target/queqlite-c1.yaml"
stop_sampler="$target/.stop-sampler"

k() { kubectl --context "$context" -n "$namespace" "$@"; }
shell_quote() { printf '%q' "$1"; }

sample_resources() {
  printf 'resource sampler started: context=%s namespace=%s\n' "$context" "$namespace"
  while [ ! -e "$stop_sampler" ]; do
    timestamp_epoch_seconds="$(date -u +%s)"
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    summary="$(timeout 3 docker exec "vcluster.cp.$cluster" crictl stats -o json 2>/dev/null || true)"
    if ! jq -e --arg namespace "$namespace" '
      any(.stats[]?; .attributes.labels["io.kubernetes.pod.namespace"] == $namespace)
    ' <<< "$summary" >/dev/null 2>&1; then
      printf 'containerd stats unavailable\n'
      sleep "$sample_interval"
      continue
    fi
    jq -c --arg timestamp "$timestamp" --argjson timestamp_epoch_seconds "$timestamp_epoch_seconds" \
      --arg namespace "$namespace" '
      .stats[] |
      select(.attributes.labels["io.kubernetes.pod.namespace"] == $namespace) |
      .attributes.metadata.name as $container |
      .attributes.labels["io.kubernetes.pod.name"] as $pod |
      select($container == "queqlite" or $container == "rustfs" or $container == "object-meter") |
      select($container != "queqlite" or ($pod | startswith("queqlite-c1-"))) |
      {timestamp:$timestamp,timestamp_epoch_seconds:$timestamp_epoch_seconds,source:"containerd_cri_stats",
       app:(if $container == "queqlite" then "queqlite" else "simulator" end),
       pod:$pod,
       pod_uid:(.attributes.labels["io.kubernetes.pod.uid"] // ""),container:$container,
       container_id:.attributes.id,
       restart_count:(.attributes.annotations["io.kubernetes.container.restartCount"] // "0" | tonumber),
       cpu_usage_usec:((.cpu.usageCoreNanoSeconds.value // "0" | tonumber) / 1000 | floor),
       memory_bytes:(.memory.workingSetBytes.value // .memory.usageBytes.value // "0" | tonumber)}
    ' <<< "$summary" >> "$resources_jsonl"
    sleep "$sample_interval"
  done
}

wait_for_resource_coverage() {
  local deadline=$((SECONDS + sample_interval + 2))
  while ! resource_samples_cover_end "$resources_jsonl" "$measurement_finished_at_epoch_seconds"; do
    kill -0 "$sampler_pid" 2>/dev/null || return 1
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 1
  done
}

collect_object_usage() {
  local pod phase usage_pod meter_enabled meter_status inventory_status retained retained_output usage_tmp
  meter_enabled=false
  [ "$object_metering" = 1 ] && meter_enabled=true
  if [ -z "$context" ] || ! k get service rustfs >/dev/null 2>&1; then
    : > "$object_access_log"
    jq -n --argjson enabled "$meter_enabled" \
      '{metering:{enabled:$enabled,status:(if $enabled then "failed" else "disabled" end),
        error:(if $enabled then "rustfs service unavailable" else null end),
        source:(if $enabled then "nginx_access_log" else null end),requests:0,
        request_bytes:0,response_bytes:0,by_method_status:[]},
        retained:{status:"failed",object_count:null,retained_bytes:null}}' > "$object_usage_json"
    if [ "$object_metering" = 0 ]; then return 0; else return 1; fi
  fi
  pod="$(k get pod -l app.kubernetes.io/name=rustfs -o json 2>/dev/null | jq -r \
    '.items[] | select(any(.spec.containers[]; .name == "object-meter")) | .metadata.name' | head -n 1 || true)"
  meter_status=disabled
  if [ "$object_metering" = 1 ] && [ -n "$pod" ]; then
    if k exec "$pod" -c object-meter -- cat /var/log/nginx/s3-access.log > "$object_access_log" 2>/dev/null; then
      meter_status=ok
    else
      meter_status=failed
    fi
  elif [ "$object_metering" = 1 ]; then
    meter_status=failed
    : > "$object_access_log"
  else
    : > "$object_access_log"
  fi

  usage_pod="bench-object-usage"
  k delete pod "$usage_pod" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  inventory_status=failed
  if jq -n --arg image "$aws_image" '{apiVersion:"v1",kind:"Pod",metadata:{name:"bench-object-usage"},spec:{
    automountServiceAccountToken:false,enableServiceLinks:false,restartPolicy:"Never",containers:[{
      name:"aws-cli",image:$image,imagePullPolicy:"IfNotPresent",
      command:["/bin/sh","-c"],args:["aws --endpoint-url http://rustfs:9000 s3api list-objects-v2 --bucket queqlite --output json"],
      env:[
        {name:"AWS_ACCESS_KEY_ID",valueFrom:{secretKeyRef:{name:"rustfs-credentials",key:"access-key"}}},
        {name:"AWS_SECRET_ACCESS_KEY",valueFrom:{secretKeyRef:{name:"rustfs-credentials",key:"secret-key"}}},
        {name:"AWS_DEFAULT_REGION",value:"us-east-1"},{name:"AWS_EC2_METADATA_DISABLED",value:"true"}
      ]}]}}' | k apply -f - >/dev/null 2>&1; then
    inventory_status=pending
  fi
  phase=""
  if [ "$inventory_status" = pending ]; then
    for _ in $(seq 1 90); do
      phase="$(k get pod "$usage_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      case "$phase" in Succeeded|Failed) break ;; esac
      sleep 1
    done
  fi
  retained='{"object_count":null,"retained_bytes":null}'
  if [ "$phase" = Succeeded ]; then
    if retained_output="$(k logs "$usage_pod" 2>/dev/null)" &&
      retained="$(jq -ce \
        '{object_count:((.Contents // []) | length),retained_bytes:((.Contents // []) | map(.Size) | add // 0)} |
         select((.object_count | type == "number" and . >= 0) and
           (.retained_bytes | type == "number" and . >= 0))' <<< "$retained_output" 2>/dev/null)"; then
      inventory_status=ok
    fi
  fi
  [ "$inventory_status" = ok ] || inventory_status=failed
  usage_tmp="$object_usage_json.tmp"
  if ! jq -s --argjson enabled "$meter_enabled" --arg meter_status "$meter_status" \
    --arg inventory_status "$inventory_status" --argjson retained "$retained" '
    {metering:{enabled:$enabled,status:$meter_status,
      source:(if $enabled then "nginx_access_log" else null end),
      requests:length,request_bytes:(map(.request_bytes | tonumber) | add // 0),
      response_bytes:(map(.response_bytes | tonumber) | add // 0),
      by_method_status:(group_by([.method,.status]) | map({method:.[0].method,status:(.[0].status | tonumber),
        requests:length,request_bytes:(map(.request_bytes | tonumber) | add),
        response_bytes:(map(.response_bytes | tonumber) | add)}))},
     retained:($retained + {status:$inventory_status})}' \
    "$object_access_log" > "$usage_tmp"; then
    jq -n --argjson enabled "$meter_enabled" \
      '{metering:{enabled:$enabled,status:(if $enabled then "failed" else "disabled" end),
        error:"invalid meter output",source:(if $enabled then "nginx_access_log" else null end),
        requests:0,request_bytes:0,response_bytes:0,by_method_status:[]},
       retained:{status:"failed",object_count:null,retained_bytes:null}}' > "$usage_tmp"
  fi
  mv "$usage_tmp" "$object_usage_json"
  k delete pod "$usage_pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  [ "$object_metering" = 0 ] || validate_object_evidence "$object_access_log" "$object_usage_json"
}

wait_for_checkpoint_drain() {
  local status elapsed endpoint_url
  local start_epoch=$SECONDS
  local status_file="$target/.checkpoint-status.json"
  for _ in $(seq 1 120); do
    status=""
    for endpoint_url in "${endpoint_urls[@]}"; do
      status="$(curl --max-time 3 -fsS -H 'x-queqlite-version: 1' -H "Authorization: Bearer $admin_token" \
        "$endpoint_url/v1/admin/membership/status" 2>/dev/null || true)"
      [ -z "$status" ] || break
    done
    elapsed=$((SECONDS - start_epoch))
    printf '%s' "$status" > "$status_file"
    if jq -e '
      .checkpoint_root != null and
      .checkpoint_root.index == .qlog_root.index and
      .checkpoint_root.hash == .qlog_root.hash
    ' "$status_file" >/dev/null 2>&1; then
      jq --argjson wait_seconds "$elapsed" \
        '{wait_seconds:$wait_seconds,qlog_root:.qlog_root,checkpoint_root:.checkpoint_root}' \
        "$status_file" > "$checkpoint_drain_json"
      rm -f "$status_file"
      return 0
    fi
    sleep 1
  done
  jq --argjson wait_seconds "$((SECONDS - start_epoch))" \
    '{wait_seconds:$wait_seconds,qlog_root:(.qlog_root // null),checkpoint_root:(.checkpoint_root // null)}' \
    "$status_file" > "$checkpoint_drain_json" 2>/dev/null || true
  rm -f "$status_file"
  return 1
}

emit_artifacts() {
  local resource_enabled=false object_enabled=false evidence cleanup measurement_window provenance
  [ "$resource_sampling" = 0 ] || resource_enabled=true
  [ "$object_metering" = 0 ] || object_enabled=true
  evidence="$(render_evidence_json "$resource_evidence_status" "$object_evidence_status" \
    "$resource_enabled" "$object_enabled")"
  cleanup="$(render_cleanup_json "$cleanup_verification_status" \
    "$namespace_cleanup_status" "$vcluster_cleanup_status")"
  measurement_window="$(render_measurement_window_json \
    "$measurement_started_at_epoch_seconds" "$measurement_finished_at_epoch_seconds")"
  provenance="$(render_provenance_json "$source_git_commit" "$source_dirty" \
    "$image_build_mode" "$image" "$image_inspect_json")"
  jq -n \
    --arg run_id "$run_id" \
    --arg cluster "$cluster" \
    --arg namespace "$namespace" \
    --arg benchmark "$benchmark_json" \
    --arg resources "$resources_jsonl" \
    --arg resource_summary "$resource_summary" \
    --arg checkpoint_drain "$checkpoint_drain_json" \
    --arg object_access_log "$object_access_log" \
    --arg object_usage "$object_usage_json" \
    --arg rustfs_manifest "$rendered_rustfs" \
    --arg cluster_manifest "$rendered_cluster" \
    --arg durability_mode "$durability_mode" \
    --arg durability_max_lag "$durability_max_lag" \
    --arg durability_interval "$durability_interval" \
    --argjson benchmark_exit "$benchmark_status" \
    --argjson run_exit "$cleanup_status" \
    --argjson evidence "$evidence" \
    --argjson cleanup "$cleanup" \
    --argjson measurement_window "$measurement_window" \
    --argjson provenance "$provenance" \
    --argjson cleaned_up "$cleaned_up" \
    '{run_id:$run_id,cluster:$cluster,namespace:$namespace,benchmark_exit_status:$benchmark_exit,
      exit_status:$run_exit,evidence:$evidence,cleanup:$cleanup,provenance:$provenance,
      measurement_window:$measurement_window,
      configuration:{durability:{mode:$durability_mode,
        max_lag:(if $durability_max_lag == "" then null else $durability_max_lag end),
        interval:(if $durability_interval == "" then null else $durability_interval end)}},
      cleaned_up:$cleaned_up,artifacts:{benchmark_json:$benchmark,resource_samples_jsonl:$resources,
      resource_summary_json:$resource_summary,checkpoint_drain_json:$checkpoint_drain,
      object_access_log_jsonl:$object_access_log,
      object_usage_json:$object_usage,rustfs_manifest:$rustfs_manifest,cluster_manifest:$cluster_manifest}}' > "$artifacts_json"
}

cleanup_run() {
  cleanup_status="$1"
  mkdir -p "$target"
  if [ "$resource_sampling" = 1 ] &&
    { [ -z "$sampler_pid" ] || ! kill -0 "$sampler_pid" 2>/dev/null; }; then
    resource_evidence_status=failed
  fi
  touch "$stop_sampler" 2>/dev/null || true
  [ -z "$sampler_pid" ] || kill "$sampler_pid" 2>/dev/null || true
  [ -z "$sampler_pid" ] || wait "$sampler_pid" 2>/dev/null || true
  for pid in "${port_forward_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${port_forward_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [ "$object_metering" = 0 ]; then
    : > "$object_access_log"
    jq -n '{metering:{enabled:false,status:"disabled",source:null,requests:0,
      request_bytes:0,response_bytes:0,by_method_status:[]},
      retained:{status:"disabled",object_count:null,retained_bytes:null}}' > "$object_usage_json"
  elif collect_object_usage && [ "$object_meter_reset_status" = ok ]; then
    object_evidence_status=ok
  else
    object_evidence_status=failed
  fi
  if [ "$resource_sampling" = 0 ]; then
    jq -n '{status:"disabled",samples:0,container_cpu_usage_usec_deltas:[],apps:[]}' \
      > "$resource_summary"
  elif [ "$resource_evidence_status" != failed ] &&
    validate_resource_samples "$resources_jsonl" "$measurement_started_at_epoch_seconds" \
      "$measurement_finished_at_epoch_seconds" "$sample_interval" &&
    summarize_resource_samples "$resources_jsonl" "$measurement_started_at_epoch_seconds" \
      "$measurement_finished_at_epoch_seconds" > "$resource_summary"; then
    resource_evidence_status=ok
  else
    resource_evidence_status=failed
    jq -n '{status:"failed",error:"resource samples unavailable or invalid",samples:0,
      container_cpu_usage_usec_deltas:[],apps:[]}' > "$resource_summary"
  fi
  if [ "$keep" = false ]; then
    if [ "$namespace_created" = true ]; then
      namespace_delete_status=0
      kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null 2>&1 ||
        namespace_delete_status=$?
      namespace_present_status=0
      if namespace_output="$(kubectl --context "$context" get namespace "$namespace" \
        --ignore-not-found -o name 2>/dev/null)" && [ -z "$namespace_output" ]; then
        namespace_present_status=1
      fi
      namespace_cleanup_status="$(cleanup_outcome "$namespace_delete_status" \
        "$namespace_present_status")"
    fi
    if [ "$created_cluster" = true ]; then
      vcluster_delete_status=0
      vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || vcluster_delete_status=$?
      vcluster_present_status=0
      if vcluster_output="$(vcluster list --driver docker --output json 2>/dev/null)" &&
        ! grep -Fq "\"${cluster}\"" <<< "$vcluster_output"; then
        vcluster_present_status=1
      fi
      vcluster_cleanup_status="$(cleanup_outcome "$vcluster_delete_status" \
        "$vcluster_present_status")"
    fi
    if { [ "$namespace_cleanup_status" = ok ] || [ "$namespace_cleanup_status" = not_created ]; } &&
      { [ "$vcluster_cleanup_status" = ok ] || [ "$vcluster_cleanup_status" = not_created ] ||
        [ "$vcluster_cleanup_status" = not_owned ]; }; then
      cleanup_verification_status=ok
      cleaned_up=true
    else
      cleanup_verification_status=failed
    fi
  fi
  [ -z "$previous_context" ] || kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  cleanup_status="$(evidence_exit_status "$cleanup_status" \
    "$resource_evidence_status" "$object_evidence_status" "$cleanup_verification_status")"
  if ! emit_artifacts; then
    echo "failed to write benchmark artifacts" >&2
    [ "$cleanup_status" -ne 0 ] || cleanup_status=1
  fi
  if [ "$cleanup_status" -eq 0 ]; then
    cat "$artifacts_json"
  else
    echo "benchmark artifacts: $artifacts_json" >&2
  fi
}

on_exit() {
  status=$?
  trap - EXIT
  cleanup_run "$status"
  exit "$cleanup_status"
}
trap on_exit EXIT

cd "$repo_root"
source_git_commit="$(git rev-parse --verify HEAD 2>/dev/null || true)"
source_status=""
if source_status="$(git status --porcelain --untracked-files=normal 2>/dev/null)" &&
  [ -z "$source_status" ]; then
  source_dirty=false
fi
mkdir -p "$target"
chmod 700 "$target"
previous_context="$(kubectl config current-context 2>/dev/null || true)"

if [ "${QUEQLITE_VIND_SKIP_BUILD:-0}" = 1 ]; then
  docker image inspect "$image" >/dev/null 2>&1 || die "missing local image: $image"
  image_build_mode=skip-build
else
  docker build -t "$image" .
  image_build_mode=built
fi
image_inspect_json="$(docker image inspect "$image" 2>/dev/null || printf '[]')"
vcluster use driver docker >/dev/null
if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
  [ "${QUEQLITE_VIND_REUSE_EXISTING:-0}" = 1 ] || die "vind cluster already exists: $cluster"
  vcluster connect "$cluster" --driver docker >/dev/null
  vcluster_cleanup_status=not_owned
else
  vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster" >/dev/null
  created_cluster=true
  vcluster_cleanup_status=retained
fi
context="$(kubectl config current-context 2>/dev/null || true)"
[ -n "$context" ] || die "vcluster did not select a Kubernetes context"

if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  managed="$(kubectl --context "$context" get namespace "$namespace" -o go-template='{{index .metadata.labels "queqlite.dev/bench-managed"}}')"
  [ "$managed" = true ] || die "refusing to replace unmanaged namespace $namespace"
  kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null
fi
kubectl --context "$context" create namespace "$namespace" >/dev/null
namespace_created=true
namespace_cleanup_status=retained
kubectl --context "$context" label namespace "$namespace" queqlite.dev/bench-managed=true \
  "queqlite.dev/bench-run-id=$run_id" >/dev/null

node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node"
vcluster node load-image "$node" --image "$image" >/dev/null

client_token="$(openssl rand -hex 24)"
admin_token="$(openssl rand -hex 24)"
peer_tokens="$(for _ in 1 2 3; do openssl rand -hex 24; done | jq -Rsc 'split("\n")[:-1]')"
k create secret generic queqlite-auth --from-literal=client-token="$client_token" \
  --from-literal=admin-token="$admin_token" >/dev/null
sed -e "s|__RUSTFS_IMAGE__|$rustfs_image|g" -e "s|__AWS_CLI_IMAGE__|$aws_image|g" \
  deploy/k8s/rustfs-e2e.yaml > "$rendered_rustfs"
yq eval '.' "$rendered_rustfs" >/dev/null
export RUSTFS_CPU_REQUEST="$rustfs_cpu_request" RUSTFS_CPU_LIMIT="$rustfs_cpu_limit"
export RUSTFS_MEMORY_REQUEST="$rustfs_memory_request" RUSTFS_MEMORY_LIMIT="$rustfs_memory_limit"
yq eval -i '(select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.containers[] | select(.name == "rustfs") | .resources) = {"requests": {"cpu": strenv(RUSTFS_CPU_REQUEST), "memory": strenv(RUSTFS_MEMORY_REQUEST)}, "limits": {"cpu": strenv(RUSTFS_CPU_LIMIT), "memory": strenv(RUSTFS_MEMORY_LIMIT)}}' "$rendered_rustfs"
if [ "$object_metering" = 1 ]; then
  # shellcheck disable=SC2016 # nginx expands these access-log variables.
  nginx_config='events {}
http {
  log_format s3 escape=json '\''{"method":"$request_method","status":$status,"request_bytes":$request_length,"response_bytes":$bytes_sent}'\'';
  access_log /var/log/nginx/s3-access.log s3;
  server {
    listen 9002;
    client_max_body_size 0;
    location / {
      proxy_request_buffering off;
      proxy_buffering off;
      proxy_http_version 1.1;
      proxy_set_header Host $http_host;
      proxy_set_header Connection "";
      proxy_pass http://127.0.0.1:9000;
    }
  }
}'
  k create configmap rustfs-object-meter --from-literal=nginx.conf="$nginx_config" >/dev/null
  export NGINX_IMAGE="$nginx_image"
  yq eval -i '
    (select(.kind == "Service" and .metadata.name == "rustfs") | .spec.ports[] | select(.name == "s3") | .targetPort) = "s3-meter" |
    (select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.volumes) += [{"name":"object-meter-config","configMap":{"name":"rustfs-object-meter"}},{"name":"object-meter-log","emptyDir":{}}] |
    (select(.kind == "Deployment" and .metadata.name == "rustfs") | .spec.template.spec.containers) += [{
      "name":"object-meter","image":strenv(NGINX_IMAGE),"imagePullPolicy":"IfNotPresent",
      "ports":[{"name":"s3-meter","containerPort":9002}],
      "volumeMounts":[{"name":"object-meter-config","mountPath":"/etc/nginx/nginx.conf","subPath":"nginx.conf","readOnly":true},{"name":"object-meter-log","mountPath":"/var/log/nginx"}],
      "readinessProbe":{"tcpSocket":{"port":"s3-meter"},"initialDelaySeconds":1,"periodSeconds":2}
    }]' "$rendered_rustfs"
fi
k apply -f "$rendered_rustfs" >/dev/null
k rollout status deployment/rustfs --timeout=240s >/dev/null
k wait --for=condition=complete job/rustfs-create-bucket --timeout=240s >/dev/null

bundle="$target/config-c1.json"
jq -n --argjson tokens "$peer_tokens" '
  {version:1,config_id:1,members:[range(3) as $n | {
    node_id:("node-" + ($n + 1 | tostring)),
    url:("http://queqlite-c1-" + ($n|tostring) + ".queqlite-c1:8081"),
    log_url:("http://queqlite-c1-" + ($n|tostring) + ".queqlite-c1:8080"), token:$tokens[$n]
  }]}
' > "$bundle"
chmod 600 "$bundle"
k create secret generic queqlite-c1-bundle --from-file=config.json="$bundle" --dry-run=client -o yaml |
  yq eval '.immutable = true' - | k create -f - >/dev/null

export QUEQLITE_IMAGE="$image" QUEQLITE_KUBE_CONTEXT="$context" QUEQLITE_K8S_NAMESPACE="$namespace"
export QUEQLITE_CLUSTER_ID=queqlite-vind QUEQLITE_RECOVERY_GENERATION=1
export QUEQLITE_S3_ENDPOINT=http://rustfs:9000 QUEQLITE_OBJECT_SECRET=rustfs-credentials
export QUEQLITE_S3_ALLOW_HTTP=true
scripts/k8s-object-job.sh 1 "$bundle" init-checkpoint >/dev/null
QUEQLITE_STARTUP_MODE=bootstrap scripts/render-k8s-config.sh 1 3 "$bundle" "$rendered_cluster"
export QUEQLITE_CPU_REQUEST="$queqlite_cpu_request" QUEQLITE_CPU_LIMIT="$queqlite_cpu_limit"
export QUEQLITE_MEMORY_REQUEST="$queqlite_memory_request" QUEQLITE_MEMORY_LIMIT="$queqlite_memory_limit"
yq eval -i '(select(.kind == "StatefulSet" and .metadata.name == "queqlite-c1") | .spec.template.spec.containers[] | select(.name == "queqlite") | .resources) = {"requests": {"cpu": strenv(QUEQLITE_CPU_REQUEST), "memory": strenv(QUEQLITE_MEMORY_REQUEST)}, "limits": {"cpu": strenv(QUEQLITE_CPU_LIMIT), "memory": strenv(QUEQLITE_MEMORY_LIMIT)}}' "$rendered_cluster"
k create -f "$rendered_cluster" >/dev/null
scripts/wait-k8s-statefulset-ready.sh queqlite-c1 3 1
[ -z "$(k get persistentvolumeclaims -o name)" ] || die "benchmark deployment created a PVC"
# Bootstrap is a one-time genesis operation. OnDelete keeps the current pods
# running while making every future emptyDir replacement restore and rejoin.
k set env statefulset/queqlite-c1 QUEQLITE_STARTUP_MODE=rejoin >/dev/null

local_port="${QUEQLITE_BENCH_PORT:-18080}"
endpoint_urls=()
endpoint_count=1
[ "$multi_endpoint" = 0 ] || endpoint_count=3
for ordinal in $(seq 0 $((endpoint_count - 1))); do
  port=$((local_port + ordinal))
  k port-forward "pod/queqlite-c1-$ordinal" "${port}:8080" \
    > "$target/port-forward-$ordinal.log" 2>&1 &
  port_forward_pids+=("$!")
  endpoint_urls+=("http://127.0.0.1:${port}")
done

for index in "${!endpoint_urls[@]}"; do
  endpoint_url="${endpoint_urls[$index]}"
  for _ in $(seq 1 60); do
    curl -fsS "$endpoint_url/readyz" >/dev/null 2>&1 && break
    assert_port_forward_alive "$index"
    sleep 1
  done
  if ! curl -fsS "$endpoint_url/readyz" >/dev/null; then
    assert_port_forward_alive "$index"
    die "port-forward did not become ready: $endpoint_url"
  fi
done

setup_body="$(jq -n --arg request_id "$run_id-setup" '
  {request_id:$request_id,statements:[
    {sql:"CREATE TABLE IF NOT EXISTS queqlite_bench (request_id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",parameters:[]},
    {sql:"INSERT INTO queqlite_bench (request_id, value) VALUES (?, ?)",parameters:[
      {type:"text",value:"queqlite-bench-seed"},{type:"text",value:"value-queqlite-bench-seed"}
    ]}
  ]}
')"
curl -fsS -H 'x-queqlite-version: 1' -H "Authorization: Bearer $client_token" \
  -H 'Content-Type: application/json' \
  --data "$setup_body" "http://127.0.0.1:${local_port}/v1/sql/execute" >/dev/null

bench_target_dir="$(cargo metadata --locked --manifest-path bench/Cargo.toml --format-version 1 --no-deps | jq -r .target_directory)"
cargo build --release --locked --manifest-path bench/Cargo.toml --bin queqlite-bench
bench_binary="$bench_target_dir/release/queqlite-bench"
[ -x "$bench_binary" ] || die "benchmark binary was not built: $bench_binary"

: > "$resources_jsonl"
if [ "$resource_sampling" = 1 ]; then
  sample_resources >"$resource_sampler_log" 2>&1 &
  sampler_pid=$!
  for _ in $(seq 1 10); do
    validate_resource_sample_schema "$resources_jsonl" && break
    kill -0 "$sampler_pid" 2>/dev/null || break
    sleep 1
  done
  validate_resource_sample_schema "$resources_jsonl" ||
    die "resource sampler did not produce a valid initial sample"
else
  printf 'resource sampling disabled\n' > "$resource_sampler_log"
fi
if [ "$object_metering" = 1 ]; then
  meter_pod="$(k get pod -l app.kubernetes.io/name=rustfs -o json | jq -r \
    '.items[] | select(any(.spec.containers[]; .name == "object-meter")) | .metadata.name' | head -n 1)"
  if k exec "$meter_pod" -c object-meter -- sh -c ': > /var/log/nginx/s3-access.log'; then
    object_meter_reset_status=ok
  else
    object_meter_reset_status=failed
    die "failed to reset object meter"
  fi
fi
bench_args=(--duration "$duration" --warmup "$warmup" --concurrency "$concurrency"
  --workload "$workload" --write-percent "$write_percent" --skip-setup)
for endpoint_url in "${endpoint_urls[@]}"; do bench_args+=(--endpoint "$endpoint_url"); done
[ -z "$target_rate" ] || bench_args+=(--target-rate "$target_rate")
case "$fault" in
  pod-delete)
    fault_command="kubectl --context $(shell_quote "$context") -n $(shell_quote "$namespace") delete pod $(shell_quote "$fault_pod") --wait=true >/dev/null; for attempt in \$(seq 1 240); do kubectl --context $(shell_quote "$context") -n $(shell_quote "$namespace") get pod $(shell_quote "$fault_pod") >/dev/null 2>&1 && break; sleep 1; done; kubectl --context $(shell_quote "$context") -n $(shell_quote "$namespace") wait --for=condition=Ready pod/$(shell_quote "$fault_pod") --timeout=240s >/dev/null"
    bench_args+=(--fault "$fault_offset" pod-delete "$fault_command") ;;
esac

if QUEQLITE_CLIENT_TOKEN="$client_token" "$bench_binary" "${bench_args[@]}" > "$benchmark_json"; then
  benchmark_status=0
else
  benchmark_status=$?
fi
[ "$benchmark_status" -eq 0 ] || exit "$benchmark_status"
measurement_window="$(measurement_window_from_report "$benchmark_json")" ||
  die "benchmark report has no valid measurement window"
measurement_started_at_epoch_seconds="$(jq -r .started_at_epoch_seconds <<< "$measurement_window")"
measurement_finished_at_epoch_seconds="$(jq -r .finished_at_epoch_seconds <<< "$measurement_window")"
if [ "$resource_sampling" = 1 ]; then
  wait_for_resource_coverage || die "resource sampler did not cover the measurement end"
fi
wait_for_checkpoint_drain || die "checkpoint did not drain to the committed qlog tip"
