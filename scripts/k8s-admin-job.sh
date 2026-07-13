#!/usr/bin/env bash
set -euo pipefail

[ "$#" -ge 4 ] && [ "$#" -le 5 ] || {
  echo "usage: $0 SERVICE POD METHOD PATH [JSON_BODY]" >&2
  exit 64
}
service="$1" pod="$2" method="$3" path="$4" body="${5-}"
[ -n "$body" ] || body='{}'
namespace="${QUEQLITE_K8S_NAMESPACE:-queqlite-e2e}"
context="${QUEQLITE_KUBE_CONTEXT:-}"
auth_secret="${QUEQLITE_AUTH_SECRET:-queqlite-auth}"
curl_image="${QUEQLITE_CURL_IMAGE:-curlimages/curl:8.10.1}"
job="ql-admin-$(date +%s)-$$-${RANDOM}"
manifest="$(mktemp)"
response="$(mktemp)"
trap 'rm -f "$manifest" "$response"' EXIT

emit_single_json() {
  file="$1"
  if ! jq -e -s 'length == 1' "$file" >/dev/null; then
    echo "admin Job stdout must contain exactly one JSON document" >&2
    cat "$file" >&2
    return 1
  fi
  cat "$file"
}

case "$service$pod" in *[!a-z0-9-]*) exit 64;; esac
case "$method" in GET|POST|PUT) ;; *) exit 64;; esac
case "$path" in /*) ;; *) exit 64;; esac
printf '%s' "$body" | jq -e . >/dev/null

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
sed \
  -e "s|__JOB_NAME__|$job|g" \
  -e 's|__CURL_IMAGE__|curlimages/curl:8.10.1|g' \
  -e 's|__METHOD__|GET|g' \
  -e 's|__BODY__|{}|g' \
  -e 's|__POD__|pod|g' \
  -e 's|__SERVICE__|service|g' \
  -e 's|__PATH__|/|g' \
  -e 's|__AUTH_SECRET__|queqlite-auth|g' \
  deploy/k8s/queqlite-admin-job.yaml > "$manifest"
# These variables expand inside the Job container.
# shellcheck disable=SC2016
export QUEQLITE_ADMIN_JOB_COMMAND='exec curl --fail-with-body --silent --show-error \
  --connect-timeout 5 --max-time 90 \
  -X "$QUEQLITE_ADMIN_METHOD" \
  -H "Authorization: Bearer ${QUEQLITE_ADMIN_TOKEN}" \
  -H "x-queqlite-version: 1" \
  -H "Content-Type: application/json" \
  --data "$QUEQLITE_ADMIN_BODY" \
  "http://${QUEQLITE_ADMIN_POD}.${QUEQLITE_ADMIN_SERVICE}:8080${QUEQLITE_ADMIN_PATH}"'
export QUEQLITE_ADMIN_JOB_IMAGE="$curl_image"
export QUEQLITE_ADMIN_JOB_AUTH_SECRET="$auth_secret"
export QUEQLITE_ADMIN_METHOD="$method"
export QUEQLITE_ADMIN_BODY="$body"
export QUEQLITE_ADMIN_POD="$pod"
export QUEQLITE_ADMIN_SERVICE="$service"
export QUEQLITE_ADMIN_PATH="$path"
yq eval --inplace '
  .spec.template.spec.containers[0].image = strenv(QUEQLITE_ADMIN_JOB_IMAGE) |
  .spec.template.spec.containers[0].args[0] = strenv(QUEQLITE_ADMIN_JOB_COMMAND) |
  (.spec.template.spec.containers[0].env[] |
    select(.name == "QUEQLITE_ADMIN_TOKEN").valueFrom.secretKeyRef.name) =
      strenv(QUEQLITE_ADMIN_JOB_AUTH_SECRET) |
  .spec.template.spec.containers[0].env += [
    {"name":"QUEQLITE_ADMIN_METHOD", "value":strenv(QUEQLITE_ADMIN_METHOD)},
    {"name":"QUEQLITE_ADMIN_BODY", "value":strenv(QUEQLITE_ADMIN_BODY)},
    {"name":"QUEQLITE_ADMIN_POD", "value":strenv(QUEQLITE_ADMIN_POD)},
    {"name":"QUEQLITE_ADMIN_SERVICE", "value":strenv(QUEQLITE_ADMIN_SERVICE)},
    {"name":"QUEQLITE_ADMIN_PATH", "value":strenv(QUEQLITE_ADMIN_PATH)}
  ]
' "$manifest"

if [ -n "${QUEQLITE_ADMIN_JOB_RENDER_ONLY:-}" ]; then
  cp "$manifest" "$QUEQLITE_ADMIN_JOB_RENDER_ONLY"
  exit 0
fi
if [ -n "${QUEQLITE_ADMIN_JOB_RESPONSE_FILE:-}" ]; then
  emit_single_json "$QUEQLITE_ADMIN_JOB_RESPONSE_FILE"
  exit
fi

"${k[@]}" create -f "$manifest" >/dev/null
deadline=$((SECONDS + 130))
while :; do
  complete="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
  if [ "$complete" = True ]; then
    if ! "${k[@]}" logs "job/$job" > "$response"; then
      cat "$response" >&2
      exit 1
    fi
    emit_single_json "$response"
    exit 0
  fi
  failed="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
  if [ "$failed" = True ]; then
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  fi
  [ "$SECONDS" -lt "$deadline" ] || {
    echo "timed out waiting for admin Job $job" >&2
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  }
  sleep 1
done
