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
