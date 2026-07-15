#!/usr/bin/env bash
set -euo pipefail

state_file="${RHIZA_KUBECTL_FIXTURE_STATE:?}"

case " $* " in
  *" get service "*" -o json "*)
    profile="${RHIZA_EXECUTION_PROFILE:?}"
    service="${*: -3:1}"
    config_id="${service##*c}"
    [ "${RHIZA_KUBECTL_FIXTURE_TARGET_MISMATCH-}" != service ] || profile=graph
    jq -cn --arg service "$service" --arg profile "$profile" --arg config_id "$config_id" '
      {metadata:{name:$service,labels:{"rhiza.dev/execution-profile":$profile,
        "rhiza.dev/config-id":$config_id}},spec:{selector:{
        "rhiza.dev/execution-profile":$profile,"rhiza.dev/config-id":$config_id}}}'
    ;;
  *" get pod "*" -o json "*)
    profile="${RHIZA_EXECUTION_PROFILE:?}"
    pod="${*: -3:1}"
    service="${pod%-*}"
    config_id="${service##*c}"
    [ "${RHIZA_KUBECTL_FIXTURE_TARGET_MISMATCH-}" != pod ] || profile=graph
    jq -cn --arg service "$service" --arg pod "$pod" --arg profile "$profile" \
      --arg config_id "$config_id" '
      {metadata:{name:$pod,labels:{"rhiza.dev/execution-profile":$profile,
        "rhiza.dev/config-id":$config_id},ownerReferences:[{kind:"StatefulSet",
        name:$service,controller:true}]}}'
    ;;
  *" create "*) exit 0 ;;
  *" get job/"*)
    count=0
    [ ! -f "$state_file" ] || read -r count < "$state_file"
    count=$((count + 1))
    printf '%s\n' "$count" > "$state_file"
    case "$count" in
      1|2) exit 1 ;;
      *)
        case "$*" in
          *'@.type=="Complete"'*) printf '%s' True ;;
        esac
        exit 0
        ;;
    esac
    ;;
  *" logs job/"*)
    response="${RHIZA_KUBECTL_FIXTURE_RESPONSE-}"
    [ -n "$response" ] || response='{}'
    printf '%s' "$response"
    ;;
esac
