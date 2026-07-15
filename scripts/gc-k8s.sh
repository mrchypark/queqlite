#!/usr/bin/env bash
set -euo pipefail

[ "$#" -ge 2 ] || { echo "usage: $0 plan|inspect|apply CONFIG_BUNDLE [PLAN_HASH]" >&2; exit 64; }
action="$1" bundle="$2" hash="${3:-}"
config_id="$(jq -er '.config_id' "$bundle")"
response="$(mktemp)"
trap 'rm -f "$response"' EXIT

case "$action" in
  plan)
    [ -z "$hash" ] || exit 64
    operation_id="${RHIZA_GC_OPERATION_ID:-gc-$(date -u +%Y%m%dT%H%M%SZ)}"
    scripts/k8s-object-job.sh "$config_id" "$bundle" gc plan \
      --operation-id "$operation_id" \
      --retain-generations "${RHIZA_GC_RETAIN_GENERATIONS:-2}" \
      --grace-ms "${RHIZA_GC_GRACE_MS:-60000}" \
      --min-age-ms "${RHIZA_GC_MIN_AGE_MS:-60000}" > "$response"
    jq -e '.plan_hash | strings | test("^[0-9a-f]{64}$")' "$response" >/dev/null
    cat "$response"
    ;;
  inspect)
    [[ "$hash" =~ ^[0-9a-f]{64}$ ]] || exit 64
    scripts/k8s-object-job.sh "$config_id" "$bundle" gc inspect --plan-hash "$hash"
    ;;
  apply)
    [[ "$hash" =~ ^[0-9a-f]{64}$ ]] || exit 64
    scripts/k8s-object-job.sh "$config_id" "$bundle" gc inspect --plan-hash "$hash" \
      > "$response"
    [ "$(jq -er '.plan.plan_hash' "$response")" = "$hash" ] || {
      echo "inspected plan hash does not match exact confirmation" >&2
      exit 65
    }
    [ "${RHIZA_GC_CONFIRM_PLAN_HASH:-}" = "$hash" ] || {
      echo "set RHIZA_GC_CONFIRM_PLAN_HASH to the exact inspected hash" >&2
      exit 65
    }
    scripts/k8s-object-job.sh "$config_id" "$bundle" gc apply --plan-hash "$hash" --confirm
    ;;
  *) exit 64 ;;
esac
