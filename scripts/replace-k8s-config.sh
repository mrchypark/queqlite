#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 2 ] || {
  echo "usage: $0 OLD_BUNDLE_JSON SUCCESSOR_DRAFT_JSON" >&2
  exit 64
}
old_bundle="$1"
successor_draft="$2"
namespace="${QUEQLITE_K8S_NAMESPACE:-queqlite-e2e}"
context="${QUEQLITE_KUBE_CONTEXT:-}"
work_dir="${QUEQLITE_RECONFIG_WORK_DIR:-target/queqlite-reconfigure}"
status_path="${QUEQLITE_ADMIN_STATUS_PATH:-/v1/admin/membership/status}"
stop_path="${QUEQLITE_ADMIN_STOP_PATH:-/v1/admin/membership/stop}"
compact_path="${QUEQLITE_ADMIN_COMPACT_PATH:-/v1/admin/checkpoint/compact}"
activate_path="${QUEQLITE_ADMIN_ACTIVATE_PATH:-/v1/admin/membership/activate}"
generation="${QUEQLITE_RECOVERY_GENERATION:-1}"

for tool in kubectl jq yq openssl; do command -v "$tool" >/dev/null || { echo "missing required command: $tool" >&2; exit 127; }; done
old_id="$(jq -er '.config_id' "$old_bundle")"
new_id="$(jq -er '.config_id' "$successor_draft")"
old_replicas="$(jq -er '.members | length' "$old_bundle")"
new_replicas="$(jq -er '.members | length' "$successor_draft")"
[ "$new_id" -eq $((old_id + 1)) ] || { echo "successor config_id must be S+1" >&2; exit 65; }
case "$old_replicas:$new_replicas" in [3-7]:[3-7]) ;; *) exit 65;; esac
jq -e '.version == 1 and (.predecessor | not)' "$successor_draft" >/dev/null

old_name="queqlite-c${old_id}"
new_name="queqlite-c${new_id}"
mkdir -p "$work_dir"
chmod 700 "$work_dir"
stop_json="$work_dir/stop-c${old_id}.json"
successor_bundle="$work_dir/config-c${new_id}.json"
successor_yaml="$work_dir/config-c${new_id}.yaml"
source_inspect_json="$work_dir/checkpoint-c${old_id}.json"
compact_json="$work_dir/compact-c${old_id}.json"
forked_json="$work_dir/fork-c${old_id}-to-c${new_id}.json"
target_inspect_json="$work_dir/checkpoint-c${new_id}.json"
status_json="$work_dir/status.json"

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
"${k[@]}" get statefulset "$old_name" >/dev/null
resume=false
if [ -s "$stop_json" ] && [ -s "$successor_bundle" ]; then
  jq -e --argjson old "$old_id" --argjson new "$new_id" '
    .stop.version == 2 and .stop.entry.config_id == $old and
    .successor.config_id == $new
  ' "$stop_json" >/dev/null
  jq -e --argjson new "$new_id" '
    .version == 1 and .config_id == $new and .predecessor.version == 2
  ' "$successor_bundle" >/dev/null
  resume=true
fi

admin() {
  QUEQLITE_KUBE_CONTEXT="$context" QUEQLITE_K8S_NAMESPACE="$namespace" \
    scripts/k8s-admin-job.sh "$@"
}

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
if "$resume"; then
  stop_operation="$(jq -er '.operation_id' "$stop_json")"
else
  stop_operation="stop-c${old_id}-to-c${new_id}-$(date -u +%Y%m%dT%H%M%SZ)"
fi
successor_members="$(jq -c '[.members[].node_id] | sort' "$successor_draft")"
successor_digest_json="$(successor_digest)"
stop_request="$(jq -cn --arg op "$stop_operation" --argjson id "$old_id" \
  --argjson successor_id "$new_id" --argjson members "$successor_members" \
  --argjson digest "$successor_digest_json" \
  '{operation_id:$op, expected_config_id:$id,
    successor:{config_id:$successor_id,members:$members,digest:$digest}}')"
if ! "$resume"; then
stop_attempt_json="$stop_json.attempt"
for ((attempt=1; attempt<=60; attempt++)); do
  if admin "$old_name" "${old_name}-0" POST "$stop_path" "$stop_request" \
    > "$stop_attempt_json"; then
    mv "$stop_attempt_json" "$stop_json"
    break
  fi
  rm -f "$stop_attempt_json"
  [ "$attempt" -lt 60 ] || { echo "configuration stop did not converge" >&2; exit 1; }
  sleep 1
done
jq -e --argjson id "$old_id" '
  (.operation_id | length > 0) and .stop.version == 2 and
  .stop.entry.config_id == $id and .stop.proof and .successor
' "$stop_json" >/dev/null

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

jq --slurpfile stopped "$stop_json" --slurpfile old "$old_bundle" '
  . + {predecessor: {
    version: 2,
    members: [$old[0].members[].node_id],
    stop_entry: $stopped[0].stop.entry,
    stop_proof: $stopped[0].stop.proof
  }}
' "$successor_draft" > "$successor_bundle"
chmod 600 "$stop_json" "$successor_bundle"

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
QUEQLITE_RECOVERY_GENERATION="$generation" \
  scripts/k8s-object-job.sh "$old_id" "$old_bundle" checkpoint inspect \
  > "$source_inspect_json"
jq -e --argjson id "$old_id" \
  '.format_version == 2 and .identity.config_id == $id and .base.snapshot and
   .base.snapshot.anchor.configuration_state.phase == "stopped"' \
  "$source_inspect_json" >/dev/null
fi

expected_bundle_b64="$(openssl base64 -A -in "$successor_bundle")"
if "${k[@]}" get secret "${new_name}-bundle" >/dev/null 2>&1; then
  actual_bundle_b64="$("${k[@]}" get secret "${new_name}-bundle" -o jsonpath='{.data.config\.json}')"
  [ "$actual_bundle_b64" = "$expected_bundle_b64" ] || {
    echo "existing successor bundle Secret differs from the resume bundle" >&2
    exit 65
  }
else
  "${k[@]}" create secret generic "${new_name}-bundle" \
    --from-file=config.json="$successor_bundle" --dry-run=client -o yaml \
    | yq eval '.immutable = true' - \
    | "${k[@]}" create -f - >/dev/null
fi

echo "forking stopped checkpoint into configuration $new_id"
QUEQLITE_RECOVERY_GENERATION="$generation" \
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
"${k[@]}" wait --for=delete pod -l "queqlite.dev/config-id=${old_id}" --timeout=180s >/dev/null
[ "$("${k[@]}" get statefulset "$old_name" -o jsonpath='{.spec.replicas}')" = 0 ]
[ -z "$("${k[@]}" get pod -l "queqlite.dev/config-id=${old_id}" -o name)" ]

QUEQLITE_STARTUP_MODE=disaster scripts/render-k8s-config.sh \
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
QUEQLITE_KUBE_CONTEXT="$context" QUEQLITE_K8S_NAMESPACE="$namespace" \
  scripts/wait-k8s-statefulset-ready.sh "$new_name" "$new_replicas" "$new_id"

echo "configuration $new_id is Active; GC is now permitted"
echo "$successor_bundle"
