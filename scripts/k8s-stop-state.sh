#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "$*" >&2
  exit 65
}

case "${1-}" in
  prepare)
    [ "$#" -eq 6 ] || die "usage: $0 prepare STATE OLD_ID NEW_ID SUCCESSOR_JSON CANDIDATE_OPERATION_ID"
    state_file="$2"
    old_id="$3"
    new_id="$4"
    successor="$5"
    candidate="$6"
    case "$old_id:$new_id" in
      *[!0-9:]*|:*|*:) die "configuration ids must be positive integers" ;;
    esac
    [ "$old_id" -gt 0 ] && [ "$new_id" -gt 0 ] || die "configuration ids must be positive integers"
    [ -n "$candidate" ] || die "candidate Stop operation id must not be empty"
    jq -e --argjson new "$new_id" '
      .config_id == $new and
      (.members | type == "array" and length >= 3) and
      (.digest | type == "array" and length == 32)
    ' <<< "$successor" >/dev/null || die "invalid successor descriptor"

    if [ -e "$state_file" ]; then
      [ -s "$state_file" ] || die "Stop state file is empty"
      jq -e --argjson old "$old_id" --argjson new "$new_id" \
        --argjson successor "$successor" '
        .version == 1 and .old_config_id == $old and .new_config_id == $new and
        (.operation_id | type == "string" and length > 0) and
        .successor == $successor
      ' "$state_file" >/dev/null || die "existing Stop state does not match requested transition"
    else
      state_attempt="${state_file}.attempt.$$"
      trap 'rm -f "$state_attempt"' EXIT
      umask 077
      jq -n --argjson old "$old_id" --argjson new "$new_id" \
        --arg operation "$candidate" --argjson successor "$successor" '
        {version:1, old_config_id:$old, new_config_id:$new,
         operation_id:$operation, successor:$successor}
      ' > "$state_attempt"
      chmod 600 "$state_attempt"
      mv "$state_attempt" "$state_file"
      trap - EXIT
    fi
    jq -er '.operation_id' "$state_file"
    ;;
  recover)
    [ "$#" -eq 4 ] || die "usage: $0 recover STATE STATUS_JSON STOP_RESPONSE_JSON"
    state_file="$2"
    status_file="$3"
    response_file="$4"
    [ -s "$state_file" ] || die "Stop state is unavailable"
    [ -s "$status_file" ] || die "admin status is unavailable"
    old_id="$(jq -er '.old_config_id' "$state_file")"
    operation="$(jq -er '.operation_id' "$state_file")"
    successor="$(jq -ec '.successor' "$state_file")"
    if ! jq -e '
      .node.configuration_status == "stopped" and .stopped_transition != null
    ' "$status_file" >/dev/null; then
      exit 1
    fi
    jq -e --argjson old "$old_id" --argjson successor "$successor" '
      .node.configuration_status == "stopped" and
      .node.active_config_id == $old and
      .node.configuration_state.phase == "stopped" and
      .stopped_transition.stop.version == 2 and
      .stopped_transition.stop.entry.config_id == $old and
      .stopped_transition.stop.proof != null and
      .stopped_transition.successor == $successor
    ' "$status_file" >/dev/null || die "stopped transition does not match persisted Stop state"
    response_attempt="${response_file}.attempt.$$"
    trap 'rm -f "$response_attempt"' EXIT
    umask 077
    jq --arg operation "$operation" '
      {operation_id:$operation, stop:.stopped_transition.stop,
       successor:.stopped_transition.successor}
    ' "$status_file" > "$response_attempt"
    chmod 600 "$response_attempt"
    mv "$response_attempt" "$response_file"
    trap - EXIT
    ;;
  *)
    die "usage: $0 prepare|recover ..."
    ;;
esac
