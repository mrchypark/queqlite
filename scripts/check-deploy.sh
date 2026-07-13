#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
for tool in jq yq shellcheck; do
  command -v "$tool" >/dev/null || { echo "missing required command: $tool" >&2; exit 127; }
done

shellcheck scripts/*.sh
bash -n scripts/*.sh
yq eval '.' deploy/k8s/*.yaml >/dev/null

if grep -R -nE '^[[:space:]]*kind:[[:space:]]*PersistentVolumeClaim|^[[:space:]]*volumeClaimTemplates:' deploy; then
  echo "PVCs are forbidden" >&2
  exit 1
fi
if grep -R -nE 'QUEQLITE_PEER_[1-7]' deploy scripts; then
  echo "legacy peer environment variables are forbidden" >&2
  exit 1
fi
if grep -R -nE 'kind:[[:space:]]*ConfigMap' deploy; then
  echo "deployment config and credentials must use Secrets" >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
for replicas in 3 7; do
  id="$replicas"
  jq -n --argjson id "$id" --argjson replicas "$replicas" '
    {version:1, config_id:$id, members:[range($replicas) as $n | {
      node_id:("node-" + ($n + 1 | tostring)),
      url:("http://queqlite-c" + ($id|tostring) + "-" + ($n|tostring) + ".queqlite-c" + ($id|tostring) + ":8081"),
      log_url:("http://queqlite-c" + ($id|tostring) + "-" + ($n|tostring) + ".queqlite-c" + ($id|tostring) + ":8080"),
      token:"not-a-real-secret"
    }]}
  ' > "$tmp/config-${id}.json"
  scripts/render-k8s-config.sh "$id" "$replicas" \
    "$tmp/config-${id}.json" "$tmp/config-${id}.yaml" successor
  yq eval '.' "$tmp/config-${id}.yaml" >/dev/null
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.replicas' "$tmp/config-${id}.yaml")" = "$replicas" ]
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.updateStrategy.type' "$tmp/config-${id}.yaml")" = OnDelete ]
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec.volumes[] | select(.name == "data") | has("emptyDir")' "$tmp/config-${id}.yaml")" = true ]
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec | has("initContainers")' "$tmp/config-${id}.yaml")" = false ]
done

grep -Fq "successor:{config_id:\$successor_id,members:\$members,digest:\$digest}" \
  scripts/replace-k8s-config.sh
grep -Fq "stop_proof: \$stopped[0].stop.proof" scripts/replace-k8s-config.sh
compact_line="$(grep -n 'publishing final checkpoint V2' scripts/replace-k8s-config.sh | cut -d: -f1)"
fork_line="$(grep -n 'forking stopped checkpoint' scripts/replace-k8s-config.sh | cut -d: -f1)"
start_line="$(grep -n 'QUEQLITE_STARTUP_MODE=disaster' scripts/replace-k8s-config.sh | cut -d: -f1)"
[ "$compact_line" -lt "$fork_line" ]
[ "$fork_line" -lt "$start_line" ]
grep -Fq "context=\"\$(kubectl config current-context" scripts/e2e-vind-rustfs.sh
grep -Fq 'get --raw=/readyz' scripts/e2e-vind-rustfs.sh
# Assert literal runtime variables in the helper call.
# shellcheck disable=SC2016
grep -Fq 'scripts/wait-k8s-statefulset-ready.sh "$new_name" "$new_replicas" "$new_id"' \
  scripts/replace-k8s-config.sh
if grep -Fq "wait --for=jsonpath='{.status.phase}'=Running" scripts/replace-k8s-config.sh; then
  echo "configuration replacement must wait for Ready pods, not merely Running pods" >&2
  exit 1
fi
if grep -Eq 'vcluster-docker_|for candidate in' scripts/e2e-vind-rustfs.sh; then
  echo "vind E2E must use the actual selected context" >&2
  exit 1
fi

QUEQLITE_OBJECT_JOB_RENDER_ONLY="$tmp/object-job.yaml" \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" init-checkpoint $'multiline\nargument'
yq eval '.' "$tmp/object-job.yaml" >/dev/null
[ "$(yq eval -r '.spec.template.spec.containers[0].args[0]' "$tmp/object-job.yaml")" = init-checkpoint ]
[ "$(yq eval -r '.spec.template.spec.containers[0].args[1]' "$tmp/object-job.yaml")" = $'multiline\nargument' ]
if grep -Eq '__[A-Z0-9_]+__' "$tmp/object-job.yaml"; then
  echo "object Job contains an unrendered placeholder" >&2
  exit 1
fi

mkdir "$tmp/ready-fixture"
jq -n '{metadata:{generation:4}, spec:{replicas:3}, status:{observedGeneration:4,readyReplicas:3}}' \
  > "$tmp/ready-fixture/statefulset.json"
for ordinal in 0 1 2; do
  jq -n --arg id 3 '{
    metadata:{labels:{"queqlite.dev/config-id":$id}},
    status:{conditions:[{type:"Ready",status:"True"}]}
  }' > "$tmp/ready-fixture/queqlite-c3-${ordinal}.json"
done
QUEQLITE_STATEFULSET_FIXTURE_DIR="$tmp/ready-fixture" \
  scripts/wait-k8s-statefulset-ready.sh queqlite-c3 3 3
jq '.status.readyReplicas = 2' "$tmp/ready-fixture/statefulset.json" \
  > "$tmp/ready-fixture/not-ready.json"
mv "$tmp/ready-fixture/not-ready.json" "$tmp/ready-fixture/statefulset.json"
if QUEQLITE_STATEFULSET_FIXTURE_DIR="$tmp/ready-fixture" \
  scripts/wait-k8s-statefulset-ready.sh queqlite-c3 3 3; then
  echo "StatefulSet readiness check accepted insufficient ready replicas" >&2
  exit 1
fi

QUEQLITE_AUTH_SECRET=rendered-auth \
  scripts/render-k8s-config.sh 3 3 "$tmp/config-3.json" "$tmp/auth-cluster.yaml"
QUEQLITE_AUTH_SECRET=rendered-auth QUEQLITE_ADMIN_JOB_RENDER_ONLY="$tmp/admin-job.yaml" \
  scripts/k8s-admin-job.sh queqlite-c3 queqlite-c3-0 GET /v1/admin/membership/status
yq eval '.' "$tmp/admin-job.yaml" >/dev/null
post_body='{"operation_id":"op-1","expected_config_id":3,"successor":{"config_id":4}}'
QUEQLITE_AUTH_SECRET=rendered-auth QUEQLITE_ADMIN_JOB_RENDER_ONLY="$tmp/admin-post-job.yaml" \
  scripts/k8s-admin-job.sh queqlite-c3 queqlite-c3-0 POST /v1/admin/membership/stop "$post_body"
yq eval '.' "$tmp/admin-post-job.yaml" >/dev/null
post_command="$(yq eval -r '.spec.template.spec.containers[0].args[0]' "$tmp/admin-post-job.yaml")"
# Match variables expanded inside the Job container.
# shellcheck disable=SC2016
case "$post_command" in
  *'--data "$QUEQLITE_ADMIN_BODY"'*'${QUEQLITE_ADMIN_PATH}'*) ;;
  *) echo "admin Job must pass request data through quoted environment variables" >&2; exit 1;;
esac
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] | select(.name == "QUEQLITE_ADMIN_BODY") | .value' "$tmp/admin-post-job.yaml")" = "$post_body" ]
tricky_path="/v1/admin/o'connor"
printf -v tricky_body '%s\n' \
  '{' \
  '  "operation_id": "op'\''s-safe",' \
  '  "note": "line one\nline two"' \
  '}'
tricky_body="${tricky_body%$'\n'}"
QUEQLITE_AUTH_SECRET=rendered-auth QUEQLITE_ADMIN_JOB_RENDER_ONLY="$tmp/admin-tricky-job.yaml" \
  scripts/k8s-admin-job.sh queqlite-c3 queqlite-c3-0 POST "$tricky_path" "$tricky_body"
yq eval '.' "$tmp/admin-tricky-job.yaml" >/dev/null
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] | select(.name == "QUEQLITE_ADMIN_PATH") | .value' "$tmp/admin-tricky-job.yaml")" = "$tricky_path" ]
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] | select(.name == "QUEQLITE_ADMIN_BODY") | .value' "$tmp/admin-tricky-job.yaml")" = "$tricky_body" ]
tricky_command="$(yq eval -r '.spec.template.spec.containers[0].args[0]' "$tmp/admin-tricky-job.yaml")"
case "$tricky_command" in
  *"$tricky_path"*|*"op's-safe"*)
    echo "admin request data was interpolated into the shell command" >&2
    exit 1
    ;;
esac
server_secret="$(yq eval -r '
  select(.kind == "StatefulSet") |
  .spec.template.spec.containers[] | select(.name == "queqlite") |
  .env[] | select(.name == "QUEQLITE_ADMIN_TOKEN") |
  .valueFrom.secretKeyRef.name + ":" + .valueFrom.secretKeyRef.key
' "$tmp/auth-cluster.yaml")"
job_secret="$(yq eval -r '
  .spec.template.spec.containers[] | select(.name == "curl") |
  .env[] | select(.name == "QUEQLITE_ADMIN_TOKEN") |
  .valueFrom.secretKeyRef.name + ":" + .valueFrom.secretKeyRef.key
' "$tmp/admin-job.yaml")"
[ "$server_secret" = "$job_secret" ]
[ "$server_secret" = 'rendered-auth:admin-token' ]
# shellcheck disable=SC2016
yq eval -e '
  .spec.template.spec.containers[] | select(.name == "curl") |
  .args[0] | (contains("Authorization: Bearer ${QUEQLITE_ADMIN_TOKEN}") and
    contains("x-queqlite-version: 1"))
' "$tmp/admin-job.yaml" >/dev/null

representative='{"node":{"configuration_status":"active"},"qlog_root":{"index":1,"hash":"00"}}'
printf '%s' "$representative" > "$tmp/admin-response.json"
admin_stdout="$(QUEQLITE_ADMIN_JOB_RESPONSE_FILE="$tmp/admin-response.json" \
  scripts/k8s-admin-job.sh queqlite-c3 queqlite-c3-0 GET /v1/admin/membership/status)"
[ "$admin_stdout" = "$representative" ]
printf '%s' "$representative" > "$tmp/object-response.json"
inspect_stdout="$(QUEQLITE_OBJECT_JOB_RESPONSE_FILE="$tmp/object-response.json" \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" checkpoint inspect)"
[ "$inspect_stdout" = "$representative" ]
init_message='checkpoint initialized: durable_tip=0'
printf '%s' "$init_message" > "$tmp/object-response.txt"
init_stdout="$(QUEQLITE_OBJECT_JOB_RESPONSE_FILE="$tmp/object-response.txt" \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" init-checkpoint)"
[ "$init_stdout" = "$init_message" ]

mkdir "$tmp/transient-bin"
cp scripts/test-fixtures/kubectl-transient.sh "$tmp/transient-bin/kubectl"
chmod +x "$tmp/transient-bin/kubectl"
transient_admin='{"status":"retried"}'
admin_retry_stdout="$(
  PATH="$tmp/transient-bin:$PATH" \
  QUEQLITE_KUBECTL_FIXTURE_STATE="$tmp/admin-kubectl-state" \
  QUEQLITE_KUBECTL_FIXTURE_RESPONSE="$transient_admin" \
  scripts/k8s-admin-job.sh queqlite-c3 queqlite-c3-0 GET /v1/admin/membership/status
)"
[ "$admin_retry_stdout" = "$transient_admin" ]
[ "$(cat "$tmp/admin-kubectl-state")" = 3 ]
object_retry_stdout="$(
  PATH="$tmp/transient-bin:$PATH" \
  QUEQLITE_KUBECTL_FIXTURE_STATE="$tmp/object-kubectl-state" \
  QUEQLITE_KUBECTL_FIXTURE_RESPONSE=checkpoint-retried \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" checkpoint inspect
)"
[ "$object_retry_stdout" = checkpoint-retried ]
[ "$(cat "$tmp/object-kubectl-state")" = 3 ]

echo "deployment static checks passed"
