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
      token:("not-a-real-secret-" + ($n + 1 | tostring))
    }]}
  ' > "$tmp/config-${id}.json"
  [ "$(jq '[.members[].token] | unique | length' "$tmp/config-${id}.json")" = "$replicas" ]
  scripts/render-k8s-config.sh "$id" "$replicas" \
    "$tmp/config-${id}.json" "$tmp/config-${id}.yaml" successor
  yq eval '.' "$tmp/config-${id}.yaml" >/dev/null
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.replicas' "$tmp/config-${id}.yaml")" = "$replicas" ]
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.updateStrategy.type' "$tmp/config-${id}.yaml")" = OnDelete ]
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec.volumes[] | select(.name == "data") | has("emptyDir")' "$tmp/config-${id}.yaml")" = true ]
  [ "$(yq eval 'select(.kind == "StatefulSet") | .spec.template.spec | has("initContainers")' "$tmp/config-${id}.yaml")" = false ]
  [ "$(yq eval -r 'select(.kind == "StatefulSet") |
    .spec.template.spec.containers[0].env[] |
    select(.name == "QUEQLITE_S3_ALLOW_HTTP") | .value' \
    "$tmp/config-${id}.yaml")" = false ]
  if yq eval -r 'select(.kind == "StatefulSet") |
    .spec.template.spec.containers[0].env[].name' "$tmp/config-${id}.yaml" |
    grep -Eq '^QUEQLITE_S3_(ENDPOINT|ACCESS_KEY|SECRET_KEY)$'; then
    echo "provider-chain render retained optional S3 endpoint or credentials" >&2
    exit 1
  fi
done

jq '(.members[].token) = "duplicate"' "$tmp/config-3.json" > "$tmp/config-3-duplicate.json"
if scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3-duplicate.json" "$tmp/invalid-duplicate-token.yaml"; then
  echo "render accepted duplicate peer tokens" >&2
  exit 1
fi
jq '.members[0].token = "peer secret"' "$tmp/config-3.json" \
  > "$tmp/config-3-spaced-token.json"
if scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3-spaced-token.json" "$tmp/invalid-spaced-token.yaml"; then
  echo "render accepted a peer token containing whitespace" >&2
  exit 1
fi
jq '.members[0].token = "peer-sécret"' "$tmp/config-3.json" \
  > "$tmp/config-3-nonascii-token.json"
if scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3-nonascii-token.json" "$tmp/invalid-nonascii-token.yaml"; then
  echo "render accepted a non-ASCII peer token" >&2
  exit 1
fi
jq '.unknown = true' "$tmp/config-3.json" > "$tmp/config-3-unknown-field.json"
if scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3-unknown-field.json" "$tmp/invalid-unknown-field.yaml"; then
  echo "render accepted an unknown bundle field" >&2
  exit 1
fi
jq '.members[0].unknown = true' "$tmp/config-3.json" \
  > "$tmp/config-3-unknown-member-field.json"
if scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3-unknown-member-field.json" "$tmp/invalid-unknown-member-field.yaml"; then
  echo "render accepted an unknown member field" >&2
  exit 1
fi

jq '.config_id = 4 |
  .members |= to_entries | .members |= map(
    .value.url = "http://queqlite-c4-\(.key).queqlite-c4:8081" |
    .value.log_url = "http://queqlite-c4-\(.key).queqlite-c4:8080" | .value
  )' "$tmp/config-3.json" > "$tmp/config-4.json"
jq '.members[0].node_id = "other-1"' "$tmp/config-4.json" \
  > "$tmp/config-4-invalid-node.json"
jq '.members[0].token = " "' "$tmp/config-4.json" \
  > "$tmp/config-4-invalid-token.json"
jq '.members[0].url = "not-a-url"' "$tmp/config-4.json" \
  > "$tmp/config-4-invalid-url.json"
jq '.members[0].token = "peer secret"' "$tmp/config-4.json" \
  > "$tmp/config-4-spaced-token.json"
jq '.members[0].token = "peer-sécret"' "$tmp/config-4.json" \
  > "$tmp/config-4-nonascii-token.json"
jq '.members[0].unknown = true' "$tmp/config-4.json" \
  > "$tmp/config-4-unknown-member-field.json"
jq '.version = 2' "$tmp/config-3.json" > "$tmp/config-3-version-2.json"
stub_bin="$tmp/stub-bin"
mkdir "$stub_bin"
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' ': > "$KUBECTL_MARKER"' 'exit 99' \
  > "$stub_bin/kubectl"
chmod +x "$stub_bin/kubectl"
assert_replace_rejects_before_kubectl() {
  local draft="$1" label="$2"
  local marker="$tmp/${label}.kubectl-called"
  local transition_dir="$tmp/${label}-transition" rc
  set +e
  PATH="$stub_bin:$PATH" KUBECTL_MARKER="$marker" \
    QUEQLITE_RECONFIG_WORK_DIR="$transition_dir" \
    scripts/replace-k8s-config.sh "$tmp/config-3.json" "$draft" \
    >/dev/null 2>&1
  rc=$?
  set -e
  [ "$rc" = 65 ]
  [ ! -e "$marker" ]
  [ ! -e "$transition_dir/stop-c3.state.json" ]
}
assert_replace_rejects_before_kubectl "$tmp/config-4-invalid-node.json" invalid-node
assert_replace_rejects_before_kubectl "$tmp/config-4-invalid-token.json" invalid-token
assert_replace_rejects_before_kubectl "$tmp/config-4-invalid-url.json" invalid-url
assert_replace_rejects_before_kubectl "$tmp/config-4-spaced-token.json" spaced-token
assert_replace_rejects_before_kubectl "$tmp/config-4-nonascii-token.json" nonascii-token
assert_replace_rejects_before_kubectl \
  "$tmp/config-4-unknown-member-field.json" unknown-member-field

invalid_old_marker="$tmp/invalid-old-version.kubectl-called"
invalid_old_dir="$tmp/invalid-old-version-transition"
set +e
PATH="$stub_bin:$PATH" KUBECTL_MARKER="$invalid_old_marker" \
  QUEQLITE_RECONFIG_WORK_DIR="$invalid_old_dir" \
  scripts/replace-k8s-config.sh \
    "$tmp/config-3-version-2.json" "$tmp/config-4.json" >/dev/null 2>&1
invalid_old_rc=$?
set -e
[ "$invalid_old_rc" = 65 ]
[ ! -e "$invalid_old_marker" ]
[ ! -e "$invalid_old_dir/stop-c3.state.json" ]

for invalid_env in \
  'QUEQLITE_EPOCH=abc' \
  'QUEQLITE_EPOCH=0' \
  'QUEQLITE_RECOVERY_GENERATION=abc' \
  'QUEQLITE_RECOVERY_GENERATION=0' \
  'QUEQLITE_S3_ALLOW_HTTP=maybe'; do
  invalid_env_marker="$tmp/${invalid_env//=/_}.kubectl-called"
  invalid_env_dir="$tmp/${invalid_env//=/_}-transition"
  set +e
  env "$invalid_env" PATH="$stub_bin:$PATH" KUBECTL_MARKER="$invalid_env_marker" \
    QUEQLITE_RECONFIG_WORK_DIR="$invalid_env_dir" \
    scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
    >/dev/null 2>&1
  invalid_env_rc=$?
  set -e
  [ "$invalid_env_rc" = 65 ]
  [ ! -e "$invalid_env_marker" ]
  [ ! -e "$invalid_env_dir/stop-c3.state.json" ]
done

wrong_live_status="$tmp/wrong-live-members.json"
jq -n '{
  cluster_id:"queqlite-vind",
  epoch:1,
  node:{active_config_id:3,configuration_state:{phase:"active",config_id:3}},
  members:["node-1","node-2","other-node"],
  recovery_generation:1,
  qlog_root:{index:0,hash:[range(32) | 0]},
  checkpoint_root:null,
  stopped_transition:null
}' > "$wrong_live_status"
wrong_members_dir="$tmp/wrong-members-transition"
wrong_members_log="$tmp/wrong-members.kubectl-log"
preflight_bin="$tmp/preflight-bin"
mkdir "$preflight_bin"
cp scripts/test-fixtures/kubectl-preflight-failure.sh "$preflight_bin/kubectl"
chmod +x "$preflight_bin/kubectl"
if [ -n "${QUEQLITE_TEST_QUEQLITE_BIN:-}" ]; then
  queqlite_fixture_bin="$QUEQLITE_TEST_QUEQLITE_BIN"
else
  cargo build --locked -p queqlite-cli
  queqlite_fixture_bin=target/debug/queqlite
fi
[ -x "$queqlite_fixture_bin" ]
export QUEQLITE_KUBECTL_FIXTURE_QUEQLITE="$queqlite_fixture_bin"
export QUEQLITE_KUBECTL_FIXTURE_BUNDLE_FILE="$tmp/config-3.json"
export QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE="$tmp/object-job.state"
export QUEQLITE_KUBECTL_FIXTURE_OBJECT_RESPONSE="$tmp/object-job.response"
valid_auth_secret="$tmp/valid-auth-secret.json"
jq -n \
  --arg client "$(printf '%s' successor-client | openssl base64 -A)" \
  --arg admin "$(printf '%s' successor-admin | openssl base64 -A)" \
  '{data:{"client-token":$client,"admin-token":$admin}}' > "$valid_auth_secret"

jq 'del(.predecessor) | .config_id = 5 |
  .members |= to_entries | .members |= map(
    .value.url = "http://queqlite-c5-\(.key).queqlite-c5:8081" |
    .value.log_url = "http://queqlite-c5-\(.key).queqlite-c5:8080" | .value
  )' scripts/test-fixtures/config-4-predecessor.json > "$tmp/config-5.json"
jq '.predecessor.stop_proof.Phase2.config_digest[0] = 1' \
  scripts/test-fixtures/config-4-predecessor.json > "$tmp/config-4-bad-digest.json"
jq '.predecessor.stop_entry.hash[0] = 1' \
  scripts/test-fixtures/config-4-predecessor.json > "$tmp/config-4-bad-entry-hash.json"
jq '.predecessor.stop_proof.Phase2.proposal.value.command_hash[0] = 1' \
  scripts/test-fixtures/config-4-predecessor.json > "$tmp/config-4-bad-command-binding.json"
jq '([range(31) | 0] + [1]) as $low |
  ([range(31) | 0] + [2]) as $high |
  .predecessor.stop_proof.Phase2.proposal.priority = $low |
  (.predecessor.stop_proof.Phase2.summaries[].aggregate_prior.priority) = $low |
  .predecessor.stop_proof.Phase2.summaries[0].aggregate_prior.priority = $high' \
  scripts/test-fixtures/config-4-predecessor.json > "$tmp/config-4-bad-phase2-maximum.json"

assert_semantic_bundle_rejected() {
  local bundle="$1" label="$2"
  local transition_dir="$tmp/${label}-semantic-transition"
  local command_log="$tmp/${label}-semantic.kubectl-log" rc
  set +e
  QUEQLITE_KUBECTL_FIXTURE_BUNDLE_FILE="$bundle" \
    PATH="$preflight_bin:$PATH" \
    QUEQLITE_KUBECTL_FIXTURE_PROFILE=semantic \
    QUEQLITE_KUBECTL_FIXTURE_LOG="$command_log" \
    QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$wrong_live_status" \
    QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE="$valid_auth_secret" \
    QUEQLITE_RECONFIG_WORK_DIR="$transition_dir" \
    scripts/replace-k8s-config.sh "$bundle" "$tmp/config-5.json" \
    >/dev/null 2>&1
  rc=$?
  set -e
  [ "$rc" = 65 ]
  [ ! -e "$transition_dir/stop-c4.state.json" ]
  grep -Fq 'validate-config-bundle --stdin' "$command_log"
  if grep -Eq 'admin |checkpoint inspect|scale statefulset|apply |create secret|membership/stop' \
    "$command_log"; then
    echo "semantic bundle rejection allowed a transition action: $label" >&2
    exit 1
  fi
}

assert_semantic_bundle_rejected "$tmp/config-4-bad-digest.json" bad-digest
assert_semantic_bundle_rejected "$tmp/config-4-bad-entry-hash.json" bad-entry-hash
assert_semantic_bundle_rejected "$tmp/config-4-bad-command-binding.json" bad-command-binding
assert_semantic_bundle_rejected \
  scripts/test-fixtures/config-4-wrong-successor.json wrong-successor-binding
assert_semantic_bundle_rejected "$tmp/config-4-bad-phase2-maximum.json" bad-phase2-maximum

set +e
PATH="$preflight_bin:$PATH" \
  QUEQLITE_KUBECTL_FIXTURE_PROFILE=wrong-members \
  QUEQLITE_KUBECTL_FIXTURE_LOG="$wrong_members_log" \
  QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$wrong_live_status" \
  QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE="$valid_auth_secret" \
  QUEQLITE_RECONFIG_WORK_DIR="$wrong_members_dir" \
  scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
  >/dev/null 2>&1
wrong_members_rc=$?
set -e
[ "$wrong_members_rc" = 65 ]
[ ! -e "$wrong_members_dir/stop-c3.state.json" ]
grep -Fq 'admin GET /v1/admin/membership/status' "$wrong_members_log"
if grep -Fq 'checkpoint inspect' "$wrong_members_log"; then
  echo "wrong live membership reached the object-store preflight" >&2
  exit 1
fi

valid_live_status="$tmp/valid-live-members.json"
jq -n '{
  cluster_id:"queqlite-vind",
  epoch:1,
  node:{active_config_id:3,configuration_state:{phase:"active",config_id:3}},
  members:["node-1","node-2","node-3"],
  recovery_generation:1,
  qlog_root:{index:0,hash:[range(32) | 0]},
  checkpoint_root:null,
  stopped_transition:null
}' > "$valid_live_status"
assert_object_preflight_blocks_stop() {
  local profile="$1"
  local transition_dir="$tmp/${profile}-preflight-transition"
  local command_log="$tmp/${profile}-preflight.kubectl-log" rc
  shift
  set +e
  env "$@" PATH="$preflight_bin:$PATH" \
    QUEQLITE_KUBECTL_FIXTURE_PROFILE="$profile" \
    QUEQLITE_KUBECTL_FIXTURE_LOG="$command_log" \
    QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$valid_live_status" \
    QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE="$valid_auth_secret" \
    QUEQLITE_RECONFIG_WORK_DIR="$transition_dir" \
    scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
    >/dev/null 2>&1
  rc=$?
  set -e
  [ "$rc" = 1 ]
  [ ! -e "$transition_dir/stop-c3.state.json" ]
  grep -Fq 'checkpoint inspect' "$command_log"
  if grep -Eq 'scale statefulset|apply |create secret|membership/stop' "$command_log"; then
    echo "object-store preflight allowed an irreversible transition action" >&2
    exit 1
  fi
}

assert_object_preflight_blocks_stop provider
assert_object_preflight_blocks_stop endpoint \
  QUEQLITE_S3_ENDPOINT=http://127.0.0.1:1 QUEQLITE_S3_ALLOW_HTTP=true

assert_live_identity_rejected() {
  local filter="$1" label="$2"
  local status="$tmp/${label}-status.json"
  local transition_dir="$tmp/${label}-transition"
  local command_log="$tmp/${label}.kubectl-log" rc
  jq "$filter" "$valid_live_status" > "$status"
  set +e
  PATH="$preflight_bin:$PATH" \
    QUEQLITE_KUBECTL_FIXTURE_PROFILE=identity \
    QUEQLITE_KUBECTL_FIXTURE_LOG="$command_log" \
    QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$status" \
    QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE="$valid_auth_secret" \
    QUEQLITE_RECONFIG_WORK_DIR="$transition_dir" \
    scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
    >/dev/null 2>&1
  rc=$?
  set -e
  [ "$rc" = 65 ]
  [ ! -e "$transition_dir/stop-c3.state.json" ]
  grep -Fq 'admin GET /v1/admin/membership/status' "$command_log"
  if grep -Eq 'checkpoint inspect|admin POST|scale statefulset|apply |create secret' \
    "$command_log"; then
    echo "live identity mismatch allowed a transition action: $label" >&2
    exit 1
  fi
}

assert_live_identity_rejected '.cluster_id = "other-cluster"' wrong-cluster
assert_live_identity_rejected '.epoch = 2' wrong-epoch
assert_live_identity_rejected '.recovery_generation = 2' wrong-generation

assert_auth_secret_rejected() {
  local secret="$1" label="$2"
  local transition_dir="$tmp/${label}-auth-transition"
  local command_log="$tmp/${label}-auth.kubectl-log" rc
  set +e
  PATH="$preflight_bin:$PATH" \
    QUEQLITE_KUBECTL_FIXTURE_PROFILE=auth \
    QUEQLITE_KUBECTL_FIXTURE_LOG="$command_log" \
    QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$valid_live_status" \
    QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE="$secret" \
    QUEQLITE_RECONFIG_WORK_DIR="$transition_dir" \
    scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
    >/dev/null 2>&1
  rc=$?
  set -e
  [ "$rc" = 65 ]
  [ ! -e "$transition_dir/stop-c3.state.json" ]
  grep -Fq 'get secret queqlite-auth -o json' "$command_log"
  if grep -Eq 'admin |checkpoint inspect|scale statefulset|apply |create secret' \
    "$command_log"; then
    echo "invalid auth Secret allowed a transition action: $label" >&2
    exit 1
  fi
}

jq 'del(.data["admin-token"])' "$valid_auth_secret" > "$tmp/missing-admin-auth.json"
assert_auth_secret_rejected "$tmp/missing-admin-auth.json" missing-admin
jq --arg blank "$(printf ' ' | openssl base64 -A)" \
  '.data["client-token"] = $blank' "$valid_auth_secret" > "$tmp/blank-client-auth.json"
assert_auth_secret_rejected "$tmp/blank-client-auth.json" blank-client
jq '.data["admin-token"] = .data["client-token"]' "$valid_auth_secret" \
  > "$tmp/shared-auth.json"
assert_auth_secret_rejected "$tmp/shared-auth.json" shared-client-admin
jq --arg peer "$(printf '%s' not-a-real-secret-1 | openssl base64 -A)" \
  '.data["admin-token"] = $peer' "$valid_auth_secret" > "$tmp/peer-auth.json"
assert_auth_secret_rejected "$tmp/peer-auth.json" peer-collision

missing_secret_dir="$tmp/missing-secret-transition"
missing_secret_log="$tmp/missing-secret.kubectl-log"
set +e
PATH="$preflight_bin:$PATH" \
  QUEQLITE_KUBECTL_FIXTURE_PROFILE=missing-secret \
  QUEQLITE_KUBECTL_FIXTURE_LOG="$missing_secret_log" \
  QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$valid_live_status" \
  QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE="$valid_auth_secret" \
  QUEQLITE_RECONFIG_WORK_DIR="$missing_secret_dir" \
  QUEQLITE_OBJECT_SECRET=missing-object-credentials \
  scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
  >/dev/null 2>&1
missing_secret_rc=$?
set -e
[ "$missing_secret_rc" = 65 ]
[ ! -e "$missing_secret_dir/stop-c3.state.json" ]
grep -Fq 'get secret missing-object-credentials' "$missing_secret_log"
if grep -Fq 'checkpoint inspect' "$missing_secret_log"; then
  echo "missing explicit credentials reached the object-store Job" >&2
  exit 1
fi

fake_checkpoint="$tmp/fake-checkpoint.json"
jq -n '{identity:{config_id:3}}' > "$fake_checkpoint"
for bypass_env in \
  "QUEQLITE_OBJECT_JOB_RESPONSE_FILE=$fake_checkpoint" \
  "QUEQLITE_OBJECT_JOB_RENDER_ONLY=$tmp/render-only.yaml" \
  "QUEQLITE_ADMIN_JOB_RESPONSE_FILE=$valid_live_status" \
  "QUEQLITE_ADMIN_JOB_RENDER_ONLY=$tmp/admin-render-only.yaml" \
  "QUEQLITE_STATEFULSET_FIXTURE_DIR=$tmp/statefulset-fixture"; do
  bypass_dir="$tmp/${bypass_env%%=*}-transition"
  bypass_log="$tmp/${bypass_env%%=*}.kubectl-log"
  set +e
  env "$bypass_env" PATH="$preflight_bin:$PATH" \
    QUEQLITE_KUBECTL_FIXTURE_PROFILE=provider \
    QUEQLITE_KUBECTL_FIXTURE_LOG="$bypass_log" \
    QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE="$valid_live_status" \
    QUEQLITE_RECONFIG_WORK_DIR="$bypass_dir" \
    scripts/replace-k8s-config.sh "$tmp/config-3.json" "$tmp/config-4.json" \
    >/dev/null 2>&1
  bypass_rc=$?
  set -e
  [ "$bypass_rc" = 65 ]
  [ ! -e "$bypass_dir/stop-c3.state.json" ]
  [ ! -e "$bypass_log" ]
done

stop_successor="$(jq -cn '{config_id:4,members:["node-1","node-2","node-3"],
  digest:[range(32) | 0]}')"
set +e
scripts/k8s-stop-state.sh prepare "$tmp/invalid-successor.state.json" 3 4 \
  "$(jq -c '.unknown = true' <<< "$stop_successor")" stop-invalid-successor
unknown_successor_rc=$?
set -e
[ "$unknown_successor_rc" = 65 ]
[ ! -e "$tmp/invalid-successor.state.json" ]
stop_state="$tmp/stop-c3.state.json"
first_stop_operation="$(scripts/k8s-stop-state.sh prepare \
  "$stop_state" 3 4 "$stop_successor" stop-first)"
second_stop_operation="$(scripts/k8s-stop-state.sh prepare \
  "$stop_state" 3 4 "$stop_successor" stop-should-not-replace)"
[ "$first_stop_operation" = stop-first ]
[ "$second_stop_operation" = "$first_stop_operation" ]
jq -n --argjson successor "$stop_successor" '{
  node:{configuration_status:"stopped",active_config_id:3,
    configuration_state:{phase:"stopped"}},
  stopped_transition:{
    stop:{version:2,entry:{config_id:3,index:9,hash:[range(32) | 1]},proof:{}},
    successor:$successor}
}' > "$tmp/stopped-status.json"
scripts/k8s-stop-state.sh recover \
  "$stop_state" "$tmp/stopped-status.json" "$tmp/recovered-stop.json"
jq -e --arg operation "$first_stop_operation" --argjson successor "$stop_successor" '
  .operation_id == $operation and .stop.version == 2 and .successor == $successor
' "$tmp/recovered-stop.json" >/dev/null
scripts/k8s-stop-state.sh validate "$stop_state" "$tmp/recovered-stop.json"
legacy_stop_state="$tmp/legacy-stop-c3.state.json"
legacy_stop_operation="$(jq -er '.operation_id' "$tmp/recovered-stop.json")"
[ "$(scripts/k8s-stop-state.sh prepare "$legacy_stop_state" 3 4 \
  "$stop_successor" "$legacy_stop_operation")" = "$legacy_stop_operation" ]
scripts/k8s-stop-state.sh validate "$legacy_stop_state" "$tmp/recovered-stop.json"
successor_draft="$tmp/successor-draft.json"
jq 'del(.predecessor)' "$tmp/config-4.json" > "$successor_draft"
partial_successor_bundle="$tmp/partial-successor-bundle.json"
printf '{"version":' > "$partial_successor_bundle"
scripts/k8s-stop-state.sh write-bundle \
  "$tmp/recovered-stop.json" "$tmp/config-3.json" "$successor_draft" \
  "$partial_successor_bundle"
jq -e '
  .version == 1 and .config_id == 4 and .predecessor.version == 2 and
  .predecessor.stop_entry.config_id == 3 and .predecessor.stop_proof != null
' "$partial_successor_bundle" >/dev/null
valid_predecessor_bundle=scripts/test-fixtures/config-4-predecessor.json
scripts/render-k8s-config.sh 4 3 \
  "$valid_predecessor_bundle" "$tmp/valid-predecessor.yaml" successor

assert_predecessor_rejected() {
  local filter="$1" label="$2"
  local invalid_bundle="$tmp/invalid-predecessor-${label}.json"
  jq "$filter" "$valid_predecessor_bundle" > "$invalid_bundle"
  if scripts/render-k8s-config.sh 4 3 \
    "$invalid_bundle" "$tmp/invalid-predecessor-${label}.yaml" successor; then
    echo "render accepted malformed predecessor $label" >&2
    exit 1
  fi
}
assert_predecessor_rejected '.predecessor.version = 1' version
assert_predecessor_rejected '.predecessor.members = "not-an-array"' members
assert_predecessor_rejected '.predecessor.stop_entry = null' stop-entry
assert_predecessor_rejected '.predecessor.stop_proof = null' stop-proof
assert_predecessor_rejected '.predecessor.unknown = true' unknown-field
for bundle_attempt in "$partial_successor_bundle".attempt.*; do
  [ ! -e "$bundle_attempt" ]
done
durable_transition_secret="$tmp/post-scale-transition-secret.json"
jq -n \
  --arg stop "$(openssl base64 -A -in "$tmp/recovered-stop.json")" \
  --arg bundle "$(openssl base64 -A -in "$partial_successor_bundle")" \
  '{data:{"stop.json":$stop,"config.json":$bundle}}' \
  > "$durable_transition_secret"
post_scale_stop="$tmp/post-scale-workdir/stop-c3.json"
post_scale_bundle="$tmp/post-scale-workdir/config-c4.json"
mkdir "$tmp/post-scale-workdir"
scripts/k8s-stop-state.sh hydrate "$durable_transition_secret" \
  "$tmp/config-3.json" "$successor_draft" "$post_scale_stop" "$post_scale_bundle"
jq -e '.stop.entry.config_id == 3 and .successor.config_id == 4' \
  "$post_scale_stop" >/dev/null
jq -e '.config_id == 4 and .predecessor.stop_entry.config_id == 3' \
  "$post_scale_bundle" >/dev/null
jq -e 'del(.data["stop.json"])' "$durable_transition_secret" \
  > "$tmp/incomplete-transition-secret.json"
set +e
scripts/k8s-stop-state.sh hydrate "$tmp/incomplete-transition-secret.json" \
  "$tmp/config-3.json" "$successor_draft" \
  "$tmp/incomplete-stop.json" "$tmp/incomplete-bundle.json"
incomplete_transition_rc=$?
set -e
[ "$incomplete_transition_rc" = 65 ]
[ ! -e "$tmp/incomplete-stop.json" ]
[ ! -e "$tmp/incomplete-bundle.json" ]
jq '.operation_id = "stop-other"' "$tmp/recovered-stop.json" \
  > "$tmp/mismatched-stop-operation.json"
set +e
scripts/k8s-stop-state.sh validate \
  "$stop_state" "$tmp/mismatched-stop-operation.json"
mismatched_operation_rc=$?
set -e
[ "$mismatched_operation_rc" = 65 ]
jq 'del(.stop.proof)' "$tmp/recovered-stop.json" > "$tmp/missing-stop-proof.json"
set +e
scripts/k8s-stop-state.sh validate "$stop_state" "$tmp/missing-stop-proof.json"
missing_proof_rc=$?
set -e
[ "$missing_proof_rc" = 65 ]
jq '.stopped_transition.successor.members = ["other-1","other-2","other-3"]' \
  "$tmp/stopped-status.json" > "$tmp/mismatched-stopped-status.json"
set +e
scripts/k8s-stop-state.sh recover \
  "$stop_state" "$tmp/mismatched-stopped-status.json" "$tmp/invalid-stop.json"
mismatched_stop_rc=$?
set -e
[ "$mismatched_stop_rc" = 65 ]
for attempt in "$stop_state".attempt.*; do
  [ ! -e "$attempt" ] || { echo "atomic Stop state attempt file leaked" >&2; exit 1; }
done

QUEQLITE_S3_ENDPOINT=http://rustfs:9000 \
QUEQLITE_OBJECT_SECRET=rustfs-credentials \
QUEQLITE_S3_ALLOW_HTTP=true \
  scripts/render-k8s-config.sh 3 3 \
    "$tmp/config-3.json" "$tmp/config-3-rustfs.yaml" successor
[ "$(yq eval -r 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ENDPOINT") | .value' \
  "$tmp/config-3-rustfs.yaml")" = http://rustfs:9000 ]
[ "$(yq eval -r 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ALLOW_HTTP") | .value' \
  "$tmp/config-3-rustfs.yaml")" = true ]
[ "$(yq eval -r 'select(.kind == "StatefulSet") |
  .spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ACCESS_KEY" or
    .name == "QUEQLITE_S3_SECRET_KEY") |
  .valueFrom.secretKeyRef |
  .name + ":" + (has("optional") | tostring)' "$tmp/config-3-rustfs.yaml" |
  grep -c '^rustfs-credentials:false$')" = 2 ]
if QUEQLITE_S3_ENDPOINT='' scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3.json" "$tmp/invalid-empty-endpoint.yaml"; then
  echo "render accepted an explicitly empty S3 endpoint" >&2
  exit 1
fi
if QUEQLITE_OBJECT_SECRET='' scripts/render-k8s-config.sh 3 3 \
  "$tmp/config-3.json" "$tmp/invalid-empty-object-secret.yaml"; then
  echo "render accepted an explicitly empty object credential secret" >&2
  exit 1
fi

# shellcheck disable=SC2016
grep -Fq '{config_id:$id,members:$members,digest:$digest}' \
  scripts/replace-k8s-config.sh
# shellcheck disable=SC2016
grep -Fq 'scripts/k8s-stop-state.sh prepare "$stop_state"' scripts/replace-k8s-config.sh
stop_state_line="$(grep -n 'k8s-stop-state.sh prepare' scripts/replace-k8s-config.sh | cut -d: -f1)"
object_preflight_line="$(grep -n 'k8s-object-job.sh.*checkpoint inspect' \
  scripts/replace-k8s-config.sh | head -n 1 | cut -d: -f1)"
# shellcheck disable=SC2016
successor_preflight_line="$(grep -n '"$successor_draft" "$successor_preflight_yaml" successor' \
  scripts/replace-k8s-config.sh | cut -d: -f1)"
# shellcheck disable=SC2016
first_kubectl_line="$(grep -n '"${k\[@\]}" get statefulset "$old_name"' \
  scripts/replace-k8s-config.sh | head -n 1 | cut -d: -f1)"
# shellcheck disable=SC2016
grep -Fq 'k8s-stop-state.sh validate "$stop_state" "$stop_json"' \
  scripts/replace-k8s-config.sh
# shellcheck disable=SC2016
stop_validate_line="$(grep -n 'k8s-stop-state.sh validate "$stop_state" "$stop_json"' \
  scripts/replace-k8s-config.sh | head -n 1 | cut -d: -f1)"
# shellcheck disable=SC2016
stop_post_line="$(grep -n 'POST "$stop_path"' scripts/replace-k8s-config.sh | cut -d: -f1)"
[ "$stop_state_line" -lt "$stop_post_line" ]
[ "$stop_state_line" -lt "$stop_validate_line" ]
[ "$stop_validate_line" -lt "$stop_post_line" ]
[ "$successor_preflight_line" -lt "$first_kubectl_line" ]
[ "$successor_preflight_line" -lt "$stop_state_line" ]
[ "$object_preflight_line" -lt "$stop_state_line" ]
grep -Fq 'k8s-stop-state.sh recover' scripts/replace-k8s-config.sh
grep -Fq 'incomplete successor bundle artifact will be rebuilt' \
  scripts/replace-k8s-config.sh
grep -Fq 'k8s-stop-state.sh write-bundle' scripts/replace-k8s-config.sh
grep -Fq 'k8s-stop-state.sh hydrate' scripts/replace-k8s-config.sh
grep -Fq "stop_proof: \$stopped[0].stop.proof" scripts/k8s-stop-state.sh
compact_line="$(grep -n 'publishing final checkpoint V2' scripts/replace-k8s-config.sh | cut -d: -f1)"
fork_line="$(grep -n 'forking stopped checkpoint' scripts/replace-k8s-config.sh | cut -d: -f1)"
durable_secret_line="$(grep -n -- '--from-file=stop.json=' scripts/replace-k8s-config.sh | cut -d: -f1)"
# shellcheck disable=SC2016
scale_down_line="$(grep -n 'scale statefulset "$old_name" --replicas=0' \
  scripts/replace-k8s-config.sh | cut -d: -f1)"
start_line="$(grep -n 'QUEQLITE_STARTUP_MODE=disaster' scripts/replace-k8s-config.sh | cut -d: -f1)"
[ "$compact_line" -lt "$fork_line" ]
[ "$fork_line" -lt "$start_line" ]
[ "$durable_secret_line" -lt "$scale_down_line" ]
grep -Fq "context=\"\$(kubectl config current-context" scripts/e2e-vind-rustfs.sh
grep -Fq 'get --raw=/readyz' scripts/e2e-vind-rustfs.sh
grep -Fq 'export QUEQLITE_S3_ENDPOINT=http://rustfs:9000 QUEQLITE_OBJECT_SECRET=rustfs-credentials' \
  scripts/e2e-vind-rustfs.sh
grep -Fq 'export QUEQLITE_S3_ALLOW_HTTP=true' scripts/e2e-vind-rustfs.sh
# shellcheck disable=SC2016
grep -Fq 'token:$tokens[$n]' \
  scripts/e2e-vind-rustfs.sh
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
[ "$(yq eval '[.spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ENDPOINT" or
    .name == "QUEQLITE_S3_ACCESS_KEY" or
    .name == "QUEQLITE_S3_SECRET_KEY")] | length' "$tmp/object-job.yaml")" = 0 ]
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ALLOW_HTTP") | .value' \
  "$tmp/object-job.yaml")" = false ]
if grep -Eq '__[A-Z0-9_]+__' "$tmp/object-job.yaml"; then
  echo "object Job contains an unrendered placeholder" >&2
  exit 1
fi
QUEQLITE_S3_ENDPOINT=http://rustfs:9000 \
QUEQLITE_OBJECT_SECRET=rustfs-credentials \
QUEQLITE_S3_ALLOW_HTTP=true \
QUEQLITE_OBJECT_JOB_RENDER_ONLY="$tmp/object-job-rustfs.yaml" \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" checkpoint inspect
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ENDPOINT") | .value' \
  "$tmp/object-job-rustfs.yaml")" = http://rustfs:9000 ]
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ALLOW_HTTP") | .value' \
  "$tmp/object-job-rustfs.yaml")" = true ]
[ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
  select(.name == "QUEQLITE_S3_ACCESS_KEY" or
    .name == "QUEQLITE_S3_SECRET_KEY") |
  .valueFrom.secretKeyRef |
  .name + ":" + (has("optional") | tostring)' "$tmp/object-job-rustfs.yaml" |
  grep -c '^rustfs-credentials:false$')" = 2 ]
if QUEQLITE_S3_ENDPOINT='' QUEQLITE_OBJECT_JOB_RENDER_ONLY="$tmp/invalid-object-job.yaml" \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" checkpoint inspect; then
  echo "object Job accepted an explicitly empty S3 endpoint" >&2
  exit 1
fi
if QUEQLITE_OBJECT_SECRET='' QUEQLITE_OBJECT_JOB_RENDER_ONLY="$tmp/invalid-object-job.yaml" \
  scripts/k8s-object-job.sh 3 "$tmp/config-3.json" checkpoint inspect; then
  echo "object Job accepted an explicitly empty object credential secret" >&2
  exit 1
fi
for invalid_env in \
  'QUEQLITE_EPOCH=abc' \
  'QUEQLITE_EPOCH=0' \
  'QUEQLITE_RECOVERY_GENERATION=abc' \
  'QUEQLITE_RECOVERY_GENERATION=0' \
  'QUEQLITE_S3_ALLOW_HTTP=maybe'; do
  if env "$invalid_env" QUEQLITE_OBJECT_JOB_RENDER_ONLY="$tmp/invalid-object-job.yaml" \
    scripts/k8s-object-job.sh 3 "$tmp/config-3.json" checkpoint inspect; then
    echo "object Job accepted invalid environment: $invalid_env" >&2
    exit 1
  fi
done

mkdir "$tmp/ready-fixture"
jq -n '{metadata:{generation:4}, spec:{replicas:3},
  status:{observedGeneration:4,readyReplicas:3,updateRevision:"revision-4"}}' \
  > "$tmp/ready-fixture/statefulset.json"
for ordinal in 0 1 2; do
  jq -n --arg id 3 '{
    metadata:{labels:{"queqlite.dev/config-id":$id,
      "controller-revision-hash":"revision-4"}},
    status:{conditions:[{type:"Ready",status:"True"}]}
  }' > "$tmp/ready-fixture/queqlite-c3-${ordinal}.json"
done
QUEQLITE_STATEFULSET_FIXTURE_DIR="$tmp/ready-fixture" \
  scripts/wait-k8s-statefulset-ready.sh queqlite-c3 3 3
jq '.metadata.labels["controller-revision-hash"] = "revision-3"' \
  "$tmp/ready-fixture/queqlite-c3-1.json" > "$tmp/ready-fixture/stale-pod.json"
mv "$tmp/ready-fixture/stale-pod.json" "$tmp/ready-fixture/queqlite-c3-1.json"
if QUEQLITE_STATEFULSET_FIXTURE_DIR="$tmp/ready-fixture" \
  scripts/wait-k8s-statefulset-ready.sh queqlite-c3 3 3; then
  echo "StatefulSet readiness check accepted a stale controller revision" >&2
  exit 1
fi
jq '.metadata.labels["controller-revision-hash"] = "revision-4"' \
  "$tmp/ready-fixture/queqlite-c3-1.json" > "$tmp/ready-fixture/current-pod.json"
mv "$tmp/ready-fixture/current-pod.json" "$tmp/ready-fixture/queqlite-c3-1.json"
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
