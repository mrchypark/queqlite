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
port_forward_pids=""
sampler_pid=""
benchmark_status=255

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
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    summary="$(timeout 3 docker exec "vcluster.cp.$cluster" crictl stats -o json 2>/dev/null || true)"
    if ! jq -e --arg namespace "$namespace" '
      any(.stats[]?; .attributes.labels["io.kubernetes.pod.namespace"] == $namespace)
    ' <<< "$summary" >/dev/null 2>&1; then
      printf 'containerd stats unavailable\n'
      sleep "$sample_interval"
      continue
    fi
    jq -c --arg timestamp "$timestamp" --arg namespace "$namespace" '
      .stats[] |
      select(.attributes.labels["io.kubernetes.pod.namespace"] == $namespace) |
      .attributes.metadata.name as $container |
      .attributes.labels["io.kubernetes.pod.name"] as $pod |
      select($container == "queqlite" or $container == "rustfs" or $container == "object-meter") |
      select($container != "queqlite" or ($pod | startswith("queqlite-c1-"))) |
      {timestamp:$timestamp,source:"containerd_cri_stats",
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

collect_object_usage() {
  local pod phase usage_pod meter_enabled
  meter_enabled=false
  [ "$object_metering" = 1 ] && meter_enabled=true
  if [ -z "$context" ] || ! k get service rustfs >/dev/null 2>&1; then
    : > "$object_access_log"
    jq -n --argjson enabled "$meter_enabled" \
      '{metering:{enabled:$enabled,source:(if $enabled then "nginx_access_log" else null end),requests:0,
        request_bytes:0,response_bytes:0,by_method_status:[]},
        retained:{object_count:null,retained_bytes:null}}' > "$object_usage_json"
    return
  fi
  pod="$(k get pod -l app.kubernetes.io/name=rustfs -o json 2>/dev/null | jq -r \
    '.items[] | select(any(.spec.containers[]; .name == "object-meter")) | .metadata.name' | head -n 1 || true)"
  if [ "$object_metering" = 1 ] && [ -n "$pod" ]; then
    k exec "$pod" -c object-meter -- cat /var/log/nginx/s3-access.log > "$object_access_log" 2>/dev/null || true
  else
    : > "$object_access_log"
  fi

  usage_pod="bench-object-usage"
  k delete pod "$usage_pod" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  jq -n --arg image "$aws_image" '{apiVersion:"v1",kind:"Pod",metadata:{name:"bench-object-usage"},spec:{
    automountServiceAccountToken:false,enableServiceLinks:false,restartPolicy:"Never",containers:[{
      name:"aws-cli",image:$image,imagePullPolicy:"IfNotPresent",
      command:["/bin/sh","-c"],args:["aws --endpoint-url http://rustfs:9000 s3api list-objects-v2 --bucket queqlite --output json"],
      env:[
        {name:"AWS_ACCESS_KEY_ID",valueFrom:{secretKeyRef:{name:"rustfs-credentials",key:"access-key"}}},
        {name:"AWS_SECRET_ACCESS_KEY",valueFrom:{secretKeyRef:{name:"rustfs-credentials",key:"secret-key"}}},
        {name:"AWS_DEFAULT_REGION",value:"us-east-1"},{name:"AWS_EC2_METADATA_DISABLED",value:"true"}
      ]}]}}' | k apply -f - >/dev/null 2>&1 || true
  phase=""
  for _ in $(seq 1 90); do
    phase="$(k get pod "$usage_pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded|Failed) break ;; esac
    sleep 1
  done
  retained='{"object_count":null,"retained_bytes":null}'
  if [ "$phase" = Succeeded ]; then
    retained="$(k logs "$usage_pod" 2>/dev/null | jq -c \
      '{object_count:((.Contents // []) | length),retained_bytes:((.Contents // []) | map(.Size) | add // 0)}' \
      2>/dev/null || printf '%s' "$retained")"
  fi
  jq -s --argjson enabled "$meter_enabled" --argjson retained "$retained" '
    {metering:{enabled:$enabled,source:(if $enabled then "nginx_access_log" else null end),
      requests:length,request_bytes:(map(.request_bytes | tonumber) | add // 0),
      response_bytes:(map(.response_bytes | tonumber) | add // 0),
      by_method_status:(group_by([.method,.status]) | map({method:.[0].method,status:(.[0].status | tonumber),
        requests:length,request_bytes:(map(.request_bytes | tonumber) | add),
        response_bytes:(map(.response_bytes | tonumber) | add)}))},retained:$retained}' \
    "$object_access_log" > "$object_usage_json"
  k delete pod "$usage_pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
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
  local cleaned_up=true
  [ "$keep" = true ] && cleaned_up=false
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
    --argjson cleaned_up "$cleaned_up" \
    '{run_id:$run_id,cluster:$cluster,namespace:$namespace,benchmark_exit_status:$benchmark_exit,
      configuration:{durability:{mode:$durability_mode,
        max_lag:(if $durability_max_lag == "" then null else $durability_max_lag end),
        interval:(if $durability_interval == "" then null else $durability_interval end)}},
      cleaned_up:$cleaned_up,artifacts:{benchmark_json:$benchmark,resource_samples_jsonl:$resources,
      resource_summary_json:$resource_summary,checkpoint_drain_json:$checkpoint_drain,
      object_access_log_jsonl:$object_access_log,
      object_usage_json:$object_usage,rustfs_manifest:$rustfs_manifest,cluster_manifest:$cluster_manifest}}' > "$artifacts_json"
}

cleanup_run() {
  status="$1"
  mkdir -p "$target"
  touch "$stop_sampler" 2>/dev/null || true
  [ -z "$sampler_pid" ] || kill "$sampler_pid" 2>/dev/null || true
  [ -z "$sampler_pid" ] || wait "$sampler_pid" 2>/dev/null || true
  for pid in $port_forward_pids; do kill "$pid" 2>/dev/null || true; done
  for pid in $port_forward_pids; do wait "$pid" 2>/dev/null || true; done
  collect_object_usage || true
  if [ -s "$resources_jsonl" ]; then
    jq -s '
      def cpu_deltas: group_by([.app,.pod_uid,.container,.container_id]) | map(sort_by(.timestamp) as $g |
        {app:$g[0].app,pod:$g[0].pod,pod_uid:$g[0].pod_uid,container:$g[0].container,
         container_id:$g[0].container_id,first:$g[0].cpu_usage_usec,last:$g[-1].cpu_usage_usec,
         delta_usec:([0,($g[-1].cpu_usage_usec - $g[0].cpu_usage_usec)] | max)});
      def memory_by_app: group_by([.timestamp,.app]) | map({timestamp:.[0].timestamp,app:.[0].app,
        memory_bytes:(map(.memory_bytes) | add)});
      . as $samples | ($samples | cpu_deltas) as $cpu | ($samples | memory_by_app) as $memory |
      {samples:($samples | length),container_cpu_usage_usec_deltas:$cpu,
       apps:(["queqlite","simulator"] | map(. as $app |
         ($memory | map(select(.app == $app))) as $app_memory |
         {app:$app,cpu_usage_usec:($cpu | map(select(.app == $app) | .delta_usec) | add // 0),
          memory_samples:($app_memory | length),
          average_memory_bytes:(if ($app_memory | length) == 0 then null else (($app_memory | map(.memory_bytes) | add) / ($app_memory | length) | floor) end),
          peak_memory_bytes:($app_memory | map(.memory_bytes) | max // null)}))}' \
      "$resources_jsonl" > "$resource_summary"
  else
    jq -n '{samples:0,container_cpu_usage_usec_deltas:[],apps:[]}' > "$resource_summary"
  fi
  emit_artifacts
  if [ "$keep" = false ] && [ -n "$context" ]; then
    k delete namespace "$namespace" --wait=true >/dev/null 2>&1 || true
  fi
  if [ "$keep" = false ] && [ "$created_cluster" = true ]; then
    vcluster delete "$cluster" --driver docker >/dev/null 2>&1 || true
  fi
  [ -z "$previous_context" ] || kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
  if [ "$status" -eq 0 ]; then
    cat "$artifacts_json"
  else
    echo "benchmark artifacts: $artifacts_json" >&2
  fi
}

on_exit() {
  status=$?
  trap - EXIT
  cleanup_run "$status"
  exit "$status"
}
trap on_exit EXIT

cd "$repo_root"
mkdir -p "$target"
chmod 700 "$target"
previous_context="$(kubectl config current-context 2>/dev/null || true)"

if [ "${QUEQLITE_VIND_SKIP_BUILD:-0}" = 1 ]; then
  docker image inspect "$image" >/dev/null 2>&1 || die "missing local image: $image"
else
  docker build -t "$image" .
fi
vcluster use driver docker >/dev/null
if vcluster list --driver docker --output json | grep -Fq "\"${cluster}\""; then
  [ "${QUEQLITE_VIND_REUSE_EXISTING:-0}" = 1 ] || die "vind cluster already exists: $cluster"
  vcluster connect "$cluster" --driver docker >/dev/null
else
  vcluster create "$cluster" --driver docker --kube-config-context-name "$cluster" >/dev/null
  created_cluster=true
fi
context="$(kubectl config current-context 2>/dev/null || true)"
[ -n "$context" ] || die "vcluster did not select a Kubernetes context"

if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
  managed="$(kubectl --context "$context" get namespace "$namespace" -o go-template='{{index .metadata.labels "queqlite.dev/bench-managed"}}')"
  [ "$managed" = true ] || die "refusing to replace unmanaged namespace $namespace"
  kubectl --context "$context" delete namespace "$namespace" --wait=true >/dev/null
fi
kubectl --context "$context" create namespace "$namespace" >/dev/null
kubectl --context "$context" label namespace "$namespace" queqlite.dev/bench-managed=true \
  "queqlite.dev/bench-run-id=$run_id" >/dev/null

node="$(kubectl --context "$context" get nodes -o jsonpath='{.items[0].metadata.name}')"
[ -n "$node" ] || die "cannot discover vind node"
vcluster node load-image "$node" --image "$image" >/dev/null

client_token="$(openssl rand -hex 24)"
admin_token="$(openssl rand -hex 24)"
peer_token="$(openssl rand -hex 24)"
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
jq -n --arg token "$peer_token" '
  {version:1,config_id:1,members:[range(3) as $n | {
    node_id:("node-" + ($n + 1 | tostring)),
    url:("http://queqlite-c1-" + ($n|tostring) + ".queqlite-c1:8081"),
    log_url:("http://queqlite-c1-" + ($n|tostring) + ".queqlite-c1:8080"), token:$token
  }]}
' > "$bundle"
chmod 600 "$bundle"
k create secret generic queqlite-c1-bundle --from-file=config.json="$bundle" --dry-run=client -o yaml |
  yq eval '.immutable = true' - | k create -f - >/dev/null

export QUEQLITE_IMAGE="$image" QUEQLITE_KUBE_CONTEXT="$context" QUEQLITE_K8S_NAMESPACE="$namespace"
export QUEQLITE_CLUSTER_ID=queqlite-vind QUEQLITE_RECOVERY_GENERATION=1
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
  port_forward_pids="$port_forward_pids $!"
  endpoint_urls+=("http://127.0.0.1:${port}")
done
for endpoint_url in "${endpoint_urls[@]}"; do
  for _ in $(seq 1 60); do
    curl -fsS "$endpoint_url/readyz" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "$endpoint_url/readyz" >/dev/null || die "port-forward did not become ready: $endpoint_url"
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

: > "$resources_jsonl"
if [ "$resource_sampling" = 1 ]; then
  sample_resources >"$resource_sampler_log" 2>&1 &
  sampler_pid=$!
  for _ in $(seq 1 10); do
    [ -s "$resources_jsonl" ] && break
    kill -0 "$sampler_pid" 2>/dev/null || break
    sleep 1
  done
  if [ ! -s "$resources_jsonl" ]; then
    printf 'resource sampler did not produce an initial sample\n' >> "$resource_sampler_log"
  fi
else
  printf 'resource sampling disabled\n' > "$resource_sampler_log"
fi
if [ "$object_metering" = 1 ]; then
  meter_pod="$(k get pod -l app.kubernetes.io/name=rustfs -o json | jq -r \
    '.items[] | select(any(.spec.containers[]; .name == "object-meter")) | .metadata.name' | head -n 1)"
  k exec "$meter_pod" -c object-meter -- sh -c ': > /var/log/nginx/s3-access.log'
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

if QUEQLITE_CLIENT_TOKEN="$client_token" cargo run --release --manifest-path bench/Cargo.toml -- "${bench_args[@]}" > "$benchmark_json"; then
  benchmark_status=0
else
  benchmark_status=$?
  exit "$benchmark_status"
fi
wait_for_checkpoint_drain || die "checkpoint did not drain to the committed qlog tip"
