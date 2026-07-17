#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 2 ] || {
  echo "usage: $0 OLD_BUNDLE_JSON SUCCESSOR_DRAFT_JSON" >&2
  exit 64
}
if [ -n "${RHIZA_OBJECT_JOB_RESPONSE_FILE+x}" ] ||
  [ -n "${RHIZA_OBJECT_JOB_RENDER_ONLY+x}" ] ||
  [ -n "${RHIZA_ADMIN_JOB_RESPONSE_FILE+x}" ] ||
  [ -n "${RHIZA_ADMIN_JOB_RENDER_ONLY+x}" ] ||
  [ -n "${RHIZA_STATEFULSET_FIXTURE_DIR+x}" ]; then
  echo "test-only Job response/render hooks are forbidden during configuration replacement" >&2
  exit 65
fi
old_bundle="$1"
successor_draft="$2"
profile="${RHIZA_EXECUTION_PROFILE-}"
namespace="${RHIZA_K8S_NAMESPACE:-rhiza-e2e}"
context="${RHIZA_KUBE_CONTEXT:-}"
work_dir="${RHIZA_RECONFIG_WORK_DIR:-target/rhiza-reconfigure-${profile}}"
status_path="${RHIZA_ADMIN_STATUS_PATH:-/v1/admin/membership/status}"
stop_path="${RHIZA_ADMIN_STOP_PATH:-/v1/admin/membership/stop}"
compact_path="${RHIZA_ADMIN_COMPACT_PATH:-/v1/admin/checkpoint/compact}"
activate_path="${RHIZA_ADMIN_ACTIVATE_PATH:-/v1/admin/membership/activate}"
cluster_id="${RHIZA_CLUSTER_ID:-rhiza-vind}"
effective_cluster_id="rhiza:${profile}:${cluster_id}"
epoch="${RHIZA_EPOCH:-1}"
generation="${RHIZA_RECOVERY_GENERATION:-1}"
auth_secret="${RHIZA_AUTH_SECRET:-rhiza-auth}"
object_secret="${RHIZA_OBJECT_SECRET-}"
object_secret_set="${RHIZA_OBJECT_SECRET+x}"

case "$profile" in
  sql|graph|kv) ;;
  *) echo "RHIZA_EXECUTION_PROFILE must be sql|graph|kv" >&2; exit 65 ;;
esac

for tool in kubectl jq yq openssl; do command -v "$tool" >/dev/null || { echo "missing required command: $tool" >&2; exit 127; }; done
old_id="$(jq -er '.config_id' "$old_bundle")"
new_id="$(jq -er '.config_id' "$successor_draft")"
old_replicas="$(jq -er '.members | length' "$old_bundle")"
new_replicas="$(jq -er '.members | length' "$successor_draft")"
[ "$new_id" -eq $((old_id + 1)) ] || { echo "successor config_id must be S+1" >&2; exit 65; }
case "$old_replicas:$new_replicas" in [3-7]:[3-7]) ;; *) exit 65;; esac
jq -e '.version == 1 and (.predecessor | not)' "$successor_draft" >/dev/null

umask 077
old_preflight_yaml="$(mktemp)"
successor_preflight_yaml="$(mktemp)"
trap 'rm -f "$old_preflight_yaml" "$successor_preflight_yaml"' EXIT
scripts/render-k8s-config.sh \
  "$old_id" "$old_replicas" "$old_bundle" "$old_preflight_yaml"
scripts/render-k8s-config.sh \
  "$new_id" "$new_replicas" "$successor_draft" "$successor_preflight_yaml" successor

old_name="rhiza-${profile}-c${old_id}"
new_name="rhiza-${profile}-c${new_id}"
mkdir -p "$work_dir"
chmod 700 "$work_dir"
stop_json="$work_dir/stop-c${old_id}.json"
stop_state="$work_dir/stop-c${old_id}.state.json"
successor_bundle="$work_dir/config-c${new_id}.json"
successor_yaml="$work_dir/config-c${new_id}.yaml"
source_inspect_json="$work_dir/checkpoint-c${old_id}.json"
compact_json="$work_dir/compact-c${old_id}.json"
forked_json="$work_dir/fork-c${old_id}-to-c${new_id}.json"
target_inspect_json="$work_dir/checkpoint-c${new_id}.json"
status_json="$work_dir/status.json"
object_preflight_json="$work_dir/checkpoint-c${old_id}.preflight.json"
bundle_preflight_json="$work_dir/config-c${old_id}.preflight.json"

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
"${k[@]}" get statefulset "$old_name" >/dev/null

validate_runtime_bundle() {
  local bundle="$1" expected_id="$2" label="$3"
  if ! "${k[@]}" exec -i "${old_name}-0" -- rhiza validate-config-bundle --stdin \
    < "$bundle" > "$bundle_preflight_json"; then
    echo "runtime rejected the $label configuration bundle" >&2
    exit 65
  fi
  jq -e --argjson id "$expected_id" '.config_id == $id' \
    "$bundle_preflight_json" >/dev/null || {
    echo "runtime rejected the $label configuration bundle" >&2
    exit 65
  }
}

durable_resume=false
if durable_secret_json="$("${k[@]}" get secret "${new_name}-bundle" -o json 2>/dev/null)"; then
  durable_secret_file="$work_dir/${new_name}-bundle.secret.json"
  printf '%s' "$durable_secret_json" > "$durable_secret_file"
  scripts/k8s-stop-state.sh hydrate "$durable_secret_file" \
    "$old_bundle" "$successor_draft" "$stop_json" "$successor_bundle"
  rm -f "$durable_secret_file"
  durable_resume=true
  echo "recovered transition state from durable Secret ${new_name}-bundle" >&2
fi
if ! auth_secret_json="$("${k[@]}" get secret "$auth_secret" -o json 2>/dev/null)"; then
  echo "runtime authentication Secret is unavailable: $auth_secret" >&2
  exit 65
fi
jq -e --slurpfile successor "$successor_draft" '
  def auth_token:
    type == "string" and (explode | length > 0 and all(. >= 33 and . <= 126));
  (.data["client-token"] |
    if type == "string" and length > 0 then (try @base64d catch null) else null end) as $client |
  (.data["admin-token"] |
    if type == "string" and length > 0 then (try @base64d catch null) else null end) as $admin |
  ($client | auth_token) and ($admin | auth_token) and
  $client != $admin and
  all($successor[0].members[].token; . != $client and . != $admin)
' <<< "$auth_secret_json" >/dev/null || {
  echo "runtime authentication Secret has invalid or conflicting tokens" >&2
  exit 65
}
if [ -n "$object_secret_set" ] &&
  ! "${k[@]}" get secret "$object_secret" >/dev/null; then
  echo "object credential Secret is unavailable: $object_secret" >&2
  exit 65
fi
resume=false
if [ -s "$stop_json" ] && [ -s "$successor_bundle" ]; then
  jq -e --argjson old "$old_id" --argjson new "$new_id" '
    .stop.version == 2 and .stop.entry.config_id == $old and
    .successor.config_id == $new
  ' "$stop_json" >/dev/null
  if ! jq empty "$successor_bundle" >/dev/null 2>&1; then
    echo "incomplete successor bundle artifact will be rebuilt: $successor_bundle" >&2
    rm -f "$successor_bundle"
  elif jq -e --argjson new "$new_id" '
    .version == 1 and .config_id == $new and .predecessor.version == 2
  ' "$successor_bundle" >/dev/null; then
    resume="$durable_resume"
  else
    echo "existing successor bundle is valid JSON but does not match configuration $new_id: $successor_bundle" >&2
    exit 65
  fi
fi

admin() {
  RHIZA_KUBE_CONTEXT="$context" RHIZA_K8S_NAMESPACE="$namespace" \
    scripts/k8s-admin-job.sh "$@"
}

if ! "$durable_resume"; then
  if ! mounted_bundle_json="$("${k[@]}" get secret "${old_name}-bundle" -o json 2>/dev/null |
    jq -er '.data["config.json"] | @base64d')"; then
    echo "runtime configuration bundle Secret is unavailable or invalid: ${old_name}-bundle" >&2
    exit 65
  fi
  jq -e --argjson mounted "$mounted_bundle_json" '. == $mounted' "$old_bundle" >/dev/null || {
    echo "runtime configuration bundle differs from the old bundle input" >&2
    exit 65
  }
  validate_runtime_bundle "$old_bundle" "$old_id" old
  validate_runtime_bundle "$successor_draft" "$new_id" successor-draft
  expected_old_members="$(jq -ec '[.members[].node_id] | sort' "$old_bundle")"
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    if ! admin "$old_name" "${old_name}-${ordinal}" GET "$status_path" \
      > "$status_json"; then
      echo "cannot verify live membership for ${old_name}-${ordinal}" >&2
      exit 65
    fi
    jq -e --arg cluster "$effective_cluster_id" --arg profile "$profile" --argjson epoch "$epoch" \
      --argjson generation "$generation" --argjson id "$old_id" \
      --argjson members "$expected_old_members" '
      .cluster_id == $cluster and .execution_profile == $profile and .epoch == $epoch and
      .recovery_generation == $generation and
      .node.active_config_id == $id and
      .node.configuration_state.config_id == $id and
      .members == $members and (.members | length) == ($members | length)
    ' "$status_json" >/dev/null || {
      echo "live membership does not match the old configuration bundle: ${old_name}-${ordinal}" >&2
      exit 65
    }
  done
  echo "preflighting checkpoint and object-store access"
  RHIZA_RECOVERY_GENERATION="$generation" \
    scripts/k8s-object-job.sh "$old_id" "$old_bundle" checkpoint inspect \
    > "$object_preflight_json"
  jq -e --argjson id "$old_id" '.identity.config_id == $id' \
    "$object_preflight_json" >/dev/null || {
    echo "object-store preflight returned a checkpoint for another configuration" >&2
    exit 65
  }
  echo "preflighting Kubernetes transition mutations"
  "${k[@]}" create secret generic "${new_name}-bundle" \
    --from-file=config.json="$successor_draft" --from-file=stop.json="$old_bundle" \
    --dry-run=client -o yaml \
    | yq eval '.immutable = true' - \
    | "${k[@]}" create --dry-run=server -f - >/dev/null
  "${k[@]}" scale statefulset "$old_name" --replicas=0 --dry-run=server >/dev/null
  "${k[@]}" apply --server-side --dry-run=server --validate=false \
    -f "$successor_preflight_yaml" >/dev/null
fi

be64() {
  printf '%b' "$(printf '%016x' "$1" | sed 's/../\\x&/g')"
}

successor_digest() {
  digest_input="$(mktemp)"
  trap 'rm -f "$digest_input"' RETURN
  printf 'QMEM\0\1' > "$digest_input"
  be64 "$new_replicas" >> "$digest_input"
  while IFS= read -r member; do
    be64 "${#member}" >> "$digest_input"
    printf '%s' "$member" >> "$digest_input"
  done < <(jq -r '[.members[].node_id] | sort[]' "$successor_draft")
  openssl dgst -sha256 -binary "$digest_input" \
    | od -An -v -tu1 \
    | awk '{for (i=1; i<=NF; i++) values[++n]=$i} END {printf "["; for (i=1; i<=n; i++) printf "%s%s", (i>1 ? "," : ""), values[i]; print "]"}'
}

echo "stopping configuration $old_id"
successor_members="$(jq -c '[.members[].node_id] | sort' "$successor_draft")"
successor_digest_json="$(successor_digest)"
stop_successor="$(jq -cn --argjson id "$new_id" --argjson members "$successor_members" \
  --argjson digest "$successor_digest_json" \
  '{config_id:$id,members:$members,digest:$digest}')"
if [ -s "$stop_json" ]; then
  jq -e --argjson successor "$stop_successor" '.successor == $successor' \
    "$stop_json" >/dev/null || { echo "existing Stop response differs from successor draft" >&2; exit 65; }
  stop_candidate="$(jq -er '.operation_id' "$stop_json")"
else
  stop_candidate="stop-c${old_id}-to-c${new_id}-$(date -u +%Y%m%dT%H%M%SZ)"
fi
stop_operation="$(scripts/k8s-stop-state.sh prepare "$stop_state" \
  "$old_id" "$new_id" "$stop_successor" "$stop_candidate")"
if [ -s "$stop_json" ]; then
  scripts/k8s-stop-state.sh validate "$stop_state" "$stop_json"
fi
stop_request="$(jq -cn --arg op "$stop_operation" --argjson id "$old_id" \
  --argjson successor "$stop_successor" \
  '{operation_id:$op, expected_config_id:$id, successor:$successor}')"

recover_stop_from_status() {
  local rc
  admin "$old_name" "${old_name}-0" GET "$status_path" > "$status_json" || return 1
  if scripts/k8s-stop-state.sh recover "$stop_state" "$status_json" "$stop_json"; then
    return 0
  else
    rc=$?
  fi
  [ "$rc" -eq 1 ] && return 1
  return "$rc"
}
if ! "$resume"; then
stop_ready=false
if recover_stop_from_status; then
  stop_ready=true
else
  rc=$?
  [ "$rc" -eq 1 ] || exit "$rc"
fi
if ! "$stop_ready"; then
  stop_attempt_json="$stop_json.attempt"
  for ((attempt=1; attempt<=60; attempt++)); do
    if admin "$old_name" "${old_name}-0" POST "$stop_path" "$stop_request" \
      > "$stop_attempt_json"; then
      if ! scripts/k8s-stop-state.sh validate "$stop_state" "$stop_attempt_json"; then
        rm -f "$stop_attempt_json"
        exit 65
      fi
      mv "$stop_attempt_json" "$stop_json"
      break
    fi
    rm -f "$stop_attempt_json"
    if recover_stop_from_status; then
      break
    else
      rc=$?
      [ "$rc" -eq 1 ] || exit "$rc"
    fi
    [ "$attempt" -lt 60 ] || { echo "configuration stop did not converge" >&2; exit 1; }
    sleep 1
  done
fi
scripts/k8s-stop-state.sh validate "$stop_state" "$stop_json"

for ((attempt=1; attempt<=60; attempt++)); do
  all_stopped=true
  for ((ordinal=0; ordinal<old_replicas; ordinal++)); do
    admin "$old_name" "${old_name}-${ordinal}" GET "$status_path" \
      > "$status_json" || { all_stopped=false; break; }
    jq -e --argjson id "$old_id" \
      '.node.configuration_status == "stopped" and .node.active_config_id == $id and .node.configuration_state.phase == "stopped"' \
      "$status_json" >/dev/null || { all_stopped=false; break; }
  done
  "$all_stopped" && break
  [ "$attempt" -lt 60 ] || { echo "not every old node reached Stopped(S)" >&2; exit 1; }
done

scripts/k8s-stop-state.sh write-bundle \
  "$stop_json" "$old_bundle" "$successor_draft" "$successor_bundle"
chmod 600 "$stop_json"
validate_runtime_bundle "$successor_bundle" "$new_id" successor

echo "publishing final checkpoint V2"
admin "$old_name" "${old_name}-0" GET "$status_path" > "$status_json"
compact_request="$(jq -cn \
  --arg op "compact-c${old_id}-${stop_operation}" \
  --argjson id "$old_id" \
  --argjson generation "$generation" \
  --argjson root "$(jq -c '.qlog_root' "$status_json")" \
  '{operation_id:$op, expected_config_id:$id,
    expected_recovery_generation:$generation, expected_root:$root}')"
admin "$old_name" "${old_name}-0" POST "$compact_path" "$compact_request" \
  > "$compact_json"
jq -e '.anchor.format_version == 2' "$compact_json" >/dev/null
RHIZA_RECOVERY_GENERATION="$generation" \
  scripts/k8s-object-job.sh "$old_id" "$old_bundle" checkpoint inspect \
  > "$source_inspect_json"
jq -e --argjson id "$old_id" \
  '.format_version == 2 and .identity.config_id == $id and .base.snapshot and
   .base.snapshot.anchor.configuration_state.phase == "stopped"' \
  "$source_inspect_json" >/dev/null
fi

expected_bundle_b64="$(openssl base64 -A -in "$successor_bundle")"
expected_stop_b64="$(openssl base64 -A -in "$stop_json")"
if "${k[@]}" get secret "${new_name}-bundle" >/dev/null 2>&1; then
  actual_bundle_b64="$("${k[@]}" get secret "${new_name}-bundle" -o jsonpath='{.data.config\.json}')"
  actual_stop_b64="$("${k[@]}" get secret "${new_name}-bundle" -o jsonpath='{.data.stop\.json}')"
  if [ "$actual_bundle_b64" != "$expected_bundle_b64" ] ||
    [ "$actual_stop_b64" != "$expected_stop_b64" ]; then
    echo "existing successor transition Secret differs from the resume artifacts" >&2
    exit 65
  fi
else
  "${k[@]}" create secret generic "${new_name}-bundle" \
    --from-file=config.json="$successor_bundle" --from-file=stop.json="$stop_json" \
    --dry-run=client -o yaml \
    | yq eval '.immutable = true' - \
    | "${k[@]}" create -f - >/dev/null
fi

if ! scripts/k8s-object-job.sh "$new_id" "$successor_bundle" validate-config-bundle \
  > "$bundle_preflight_json"; then
  echo "runtime rejected the successor configuration bundle" >&2
  exit 65
fi
jq -e --argjson id "$new_id" '.config_id == $id' "$bundle_preflight_json" >/dev/null || {
  echo "runtime rejected the successor configuration bundle" >&2
  exit 65
}

echo "forking stopped checkpoint into configuration $new_id"
RHIZA_RECOVERY_GENERATION="$generation" \
  scripts/k8s-object-job.sh "$new_id" "$successor_bundle" checkpoint fork-successor \
    --from-config-id "$old_id" \
    --from-generation "$generation" > "$forked_json"
jq -e --argjson old "$old_id" --argjson new "$new_id" '
  .format_version == 2 and .identity.config_id == $new and
  .successor_transition.predecessor.config_id == $old and
  .successor_transition.successor.config_id == $new and
  .base.snapshot.anchor.configuration_state.phase == "stopped"
' "$forked_json" >/dev/null
scripts/k8s-object-job.sh "$new_id" "$successor_bundle" checkpoint inspect \
  > "$target_inspect_json"
jq -e --argjson id "$new_id" \
  '.identity.config_id == $id and .successor_transition.successor.config_id == $id' \
  "$target_inspect_json" >/dev/null

echo "scaling stopped configuration $old_id to zero"
"${k[@]}" scale statefulset "$old_name" --replicas=0 >/dev/null
"${k[@]}" wait --for=delete pod \
  -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${old_id}" \
  --timeout=180s >/dev/null
[ "$("${k[@]}" get statefulset "$old_name" -o jsonpath='{.spec.replicas}')" = 0 ]
[ -z "$("${k[@]}" get pod \
  -l "rhiza.dev/execution-profile=${profile},rhiza.dev/config-id=${old_id}" -o name)" ]

RHIZA_STARTUP_MODE=rejoin scripts/render-k8s-config.sh \
  "$new_id" "$new_replicas" "$successor_bundle" "$successor_yaml" successor
"${k[@]}" apply --dry-run=client --validate=false -f "$successor_yaml" >/dev/null
"${k[@]}" apply -f "$successor_yaml" >/dev/null
scripts/wait-k8s-statefulset-ready.sh "$new_name" "$new_replicas" "$new_id"

successor_already_active=false
for ((attempt=1; attempt<=60; attempt++)); do
  all_ready=true
  all_active=true
  for ((ordinal=0; ordinal<new_replicas; ordinal++)); do
    admin "$new_name" "${new_name}-${ordinal}" GET "$status_path" \
      > "$status_json" || { all_ready=false; all_active=false; break; }
    phase="$(jq -er --argjson id "$new_id" '
      select(.node.active_config_id == $id) | .node.configuration_status
    ' "$status_json")" || { all_ready=false; all_active=false; break; }
    case "$phase" in
      active) ;;
      awaiting_activation) all_active=false ;;
      *) all_ready=false; all_active=false; break ;;
    esac
  done
  if "$all_ready"; then
    "$all_active" && successor_already_active=true
    break
  fi
  [ "$attempt" -lt 60 ] || { echo "not every successor node reached a resumable state" >&2; exit 1; }
done

if ! "$successor_already_active"; then
  echo "activating configuration $new_id"
  activate_request="$(jq -cn --arg op "activate-c${new_id}-${stop_operation}" --argjson id "$new_id" \
    '{operation_id:$op, expected_config_id:$id}')"
  admin "$new_name" "${new_name}-0" POST "$activate_path" "$activate_request" >/dev/null
fi
for ((attempt=1; attempt<=60; attempt++)); do
  all_active=true
  for ((ordinal=0; ordinal<new_replicas; ordinal++)); do
    admin "$new_name" "${new_name}-${ordinal}" GET "$status_path" \
      > "$status_json" || { all_active=false; break; }
    jq -e --argjson id "$new_id" \
      '.node.configuration_status == "active" and .node.active_config_id == $id and .node.configuration_state.phase == "active"' \
      "$status_json" >/dev/null || { all_active=false; break; }
  done
  "$all_active" && break
  [ "$attempt" -lt 60 ] || { echo "not every successor node reached Active(S+1)" >&2; exit 1; }
done
RHIZA_KUBE_CONTEXT="$context" RHIZA_K8S_NAMESPACE="$namespace" \
  scripts/wait-k8s-statefulset-ready.sh "$new_name" "$new_replicas" "$new_id"

echo "configuration $new_id is Active; GC is now permitted"
echo "$successor_bundle"
