#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 CONFIG_ID REPLICAS BUNDLE_JSON OUTPUT_YAML [successor]" >&2
  exit 64
}

[ "$#" -ge 4 ] && [ "$#" -le 5 ] || usage
config_id="$1"
replicas="$2"
bundle="$3"
output="$4"
successor="${5:-}"

case "$config_id" in ''|*[!0-9]*|0) usage;; esac
case "$replicas" in 3|4|5|6|7) ;; *) usage;; esac
[ -r "$bundle" ] || { echo "cannot read bundle: $bundle" >&2; exit 66; }

require() { command -v "$1" >/dev/null || { echo "missing required command: $1" >&2; exit 127; }; }
require jq
require sed
require yq

jq -e --argjson id "$config_id" --argjson replicas "$replicas" '
  ((keys | sort) == ["config_id", "members", "version"] or
   (keys | sort) == ["config_id", "members", "predecessor", "version"]) and
  .version == 1 and .config_id == $id and
  (.members | length) == $replicas and
  all(.members[];
    (keys | sort) == ["log_url", "node_id", "token", "url"]) and
  ((has("predecessor") | not) or .predecessor == null or
    (.predecessor |
      type == "object" and
      (keys | sort) == ["members", "stop_entry", "stop_proof", "version"] and
      .version == 2 and
      (.members | type == "array") and
      (.stop_entry | type == "object") and
      (.stop_proof | type == "object"))) and
  ([.members[].node_id] | sort) ==
    [range(1; $replicas + 1) | "node-\(.)"] and
  ([.members[].token] | all(type == "string" and test("^[!-~]+$"))) and
  ([.members[].token] | unique | length) == $replicas and
  ([.members | sort_by(.node_id)[] | {node_id, url, log_url}] ==
    [range(0; $replicas) as $n | {
      node_id: "node-\($n + 1)",
      url: "http://queqlite-c\($id)-\($n).queqlite-c\($id):8081",
      log_url: "http://queqlite-c\($id)-\($n).queqlite-c\($id):8080"
    }])
' "$bundle" >/dev/null || { echo "invalid v1 bundle/config/replica identity" >&2; exit 65; }

name="queqlite-c${config_id}"
image="${QUEQLITE_IMAGE:-queqlite:dev}"
cluster_id="${QUEQLITE_CLUSTER_ID:-queqlite-vind}"
epoch="${QUEQLITE_EPOCH:-1}"
generation="${QUEQLITE_RECOVERY_GENERATION:-1}"
startup="${QUEQLITE_STARTUP_MODE:-rejoin}"
durability="${QUEQLITE_DURABILITY_MODE-sync}"
durability_max_lag="${QUEQLITE_DURABILITY_MAX_LAG-}"
durability_interval="${QUEQLITE_DURABILITY_INTERVAL-}"
durability_max_lag_set="${QUEQLITE_DURABILITY_MAX_LAG+x}"
durability_interval_set="${QUEQLITE_DURABILITY_INTERVAL+x}"
s3_endpoint="${QUEQLITE_S3_ENDPOINT-}"
s3_endpoint_set="${QUEQLITE_S3_ENDPOINT+x}"
s3_bucket="${QUEQLITE_S3_BUCKET:-queqlite}"
s3_region="${QUEQLITE_S3_REGION:-us-east-1}"
s3_http="${QUEQLITE_S3_ALLOW_HTTP:-false}"
auth_secret="${QUEQLITE_AUTH_SECRET:-queqlite-auth}"
object_secret="${QUEQLITE_OBJECT_SECRET-}"
object_secret_set="${QUEQLITE_OBJECT_SECRET+x}"
checkpoint_lease_ms="${QUEQLITE_CHECKPOINT_LEASE_MS:-300000}"
bundle_secret="${name}-bundle"

die() { echo "$*" >&2; exit 65; }
u64_max=18446744073709551615
decimal_at_most() {
  local value="$1" maximum="$2" LC_ALL=C
  while [ "${value#0}" != "$value" ]; do value="${value#0}"; done
  [ -n "$value" ] || return 1
  if [ "${#value}" -ne "${#maximum}" ]; then
    [ "${#value}" -lt "${#maximum}" ]
    return
  fi
  [[ "$value" < "$maximum" || "$value" = "$maximum" ]]
}
validate_positive_u64() {
  local name="$1" value="$2"
  case "$value" in
    ''|*[!0-9]*) die "$name must be a positive integer" ;;
  esac
  decimal_at_most "$value" "$u64_max" || die "$name must be a positive integer"
}
validate_positive_u64 QUEQLITE_EPOCH "$epoch"
validate_positive_u64 QUEQLITE_RECOVERY_GENERATION "$generation"
case "$s3_http" in
  true|false|1|0) ;;
  *) die "QUEQLITE_S3_ALLOW_HTTP must be true|false|1|0" ;;
esac
validate_positive_u64 QUEQLITE_CHECKPOINT_LEASE_MS "$checkpoint_lease_ms"
[ -z "$s3_endpoint_set" ] || [ -n "$s3_endpoint" ] ||
  die "QUEQLITE_S3_ENDPOINT must not be empty when set"
[ -z "$object_secret_set" ] || [ -n "$object_secret" ] ||
  die "QUEQLITE_OBJECT_SECRET must not be empty when set"
validate_duration() {
  local name="$1" value="$2" amount maximum
  case "$value" in
    *ms) amount="${value%ms}"; maximum=18446744073709551615 ;;
    *s) amount="${value%s}"; maximum=18446744073709551 ;;
    *m) amount="${value%m}"; maximum=307445734561825 ;;
    *h) amount="${value%h}"; maximum=5124095576030 ;;
    *) die "$name must be a positive duration with ms/s/m/h suffix" ;;
  esac
  case "$amount" in ''|*[!0-9]*) die "$name must be a positive duration with ms/s/m/h suffix" ;; esac
  decimal_at_most "$amount" "$u64_max" ||
    die "$name must be a positive duration with ms/s/m/h suffix"
  decimal_at_most "$amount" "$maximum" || die "$name duration is too large"
}

durability_max_lag_env=""
durability_interval_env=""
case "$durability" in
  sync)
    [ -z "$durability_max_lag_set" ] || die "QUEQLITE_DURABILITY_MAX_LAG is irrelevant for sync durability"
    [ -z "$durability_interval_set" ] || die "QUEQLITE_DURABILITY_INTERVAL is irrelevant for sync durability"
    ;;
  bounded)
    [ -n "$durability_max_lag_set" ] && [ -n "$durability_max_lag" ] ||
      die "QUEQLITE_DURABILITY_MAX_LAG is required for bounded durability"
    [ -z "$durability_interval_set" ] || die "QUEQLITE_DURABILITY_INTERVAL is irrelevant for bounded durability"
    validate_duration QUEQLITE_DURABILITY_MAX_LAG "$durability_max_lag"
    durability_max_lag_env="            - {name: QUEQLITE_DURABILITY_MAX_LAG, value: $durability_max_lag}"
    ;;
  periodic)
    [ -n "$durability_interval_set" ] && [ -n "$durability_interval" ] ||
      die "QUEQLITE_DURABILITY_INTERVAL is required for periodic durability"
    [ -z "$durability_max_lag_set" ] || die "QUEQLITE_DURABILITY_MAX_LAG is irrelevant for periodic durability"
    validate_duration QUEQLITE_DURABILITY_INTERVAL "$durability_interval"
    durability_interval_env="            - {name: QUEQLITE_DURABILITY_INTERVAL, value: $durability_interval}"
    ;;
  *) die "QUEQLITE_DURABILITY_MODE must be sync|bounded|periodic" ;;
esac

case "$successor" in
  '') successor_flag=false ;;
  successor) successor_flag=true ;;
  *) usage ;;
esac

escape() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
object_secret_placeholder="${object_secret:-unused-object-credentials}"
sed \
  -e "s|__CONFIG_NAME__|$(escape "$name")|g" \
  -e "s|__CONFIG_ID__|${config_id}|g" \
  -e "s|__REPLICAS__|${replicas}|g" \
  -e "s|__QUEQLITE_IMAGE__|$(escape "$image")|g" \
  -e "s|__CLUSTER_ID__|$(escape "$cluster_id")|g" \
  -e "s|__EPOCH__|${epoch}|g" \
  -e "s|__RECOVERY_GENERATION__|${generation}|g" \
  -e "s|__STARTUP_MODE__|$(escape "$startup")|g" \
  -e "s|__DURABILITY_MODE__|$(escape "$durability")|g" \
  -e "s|__CHECKPOINT_LEASE_MS__|${checkpoint_lease_ms}|g" \
  -e "s|            # __DURABILITY_MAX_LAG_ENV__|$(escape "$durability_max_lag_env")|g" \
  -e "s|            # __DURABILITY_INTERVAL_ENV__|$(escape "$durability_interval_env")|g" \
  -e "s|__S3_BUCKET__|$(escape "$s3_bucket")|g" \
  -e "s|__S3_REGION__|$(escape "$s3_region")|g" \
  -e "s|__S3_ALLOW_HTTP__|$(escape "$s3_http")|g" \
  -e "s|__AUTH_SECRET__|$(escape "$auth_secret")|g" \
  -e "s|__OBJECT_SECRET__|$(escape "$object_secret_placeholder")|g" \
  -e "s|__BUNDLE_SECRET__|$(escape "$bundle_secret")|g" \
  -e "s|__SUCCESSOR__|${successor_flag}|g" \
  deploy/k8s/queqlite-cluster.yaml > "$output"
export S3_ENDPOINT="$s3_endpoint" S3_ENDPOINT_SET="$s3_endpoint_set"
export OBJECT_SECRET="$object_secret" OBJECT_SECRET_SET="$object_secret_set"
yq eval --inplace '
  (select(.kind == "StatefulSet") |
    .spec.template.spec.containers[] | select(.name == "queqlite") | .env) |= (
      map(select(.name != "QUEQLITE_S3_ENDPOINT" and
        .name != "QUEQLITE_S3_ACCESS_KEY" and
        .name != "QUEQLITE_S3_SECRET_KEY")) +
      ([{"name":"QUEQLITE_S3_ENDPOINT", "value":strenv(S3_ENDPOINT)}] |
        map(select(strenv(S3_ENDPOINT_SET) == "x"))) +
      ([
        {"name":"QUEQLITE_S3_ACCESS_KEY", "valueFrom":{"secretKeyRef":{
          "name":strenv(OBJECT_SECRET), "key":"access-key"}}},
        {"name":"QUEQLITE_S3_SECRET_KEY", "valueFrom":{"secretKeyRef":{
          "name":strenv(OBJECT_SECRET), "key":"secret-key"}}}
      ] | map(select(strenv(OBJECT_SECRET_SET) == "x")))
    )
' "$output"
if grep -Eq '__[A-Z0-9_]+__' "$output"; then
  echo "unrendered placeholder" >&2
  exit 65
fi
