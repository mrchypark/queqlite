#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$QUEQLITE_KUBECTL_FIXTURE_LOG"

case " $* " in
  *" get statefulset queqlite-c"*) exit 0 ;;
  *" get secret queqlite-c"*)
    case "$*" in *"-bundle -o json") ;; *) exit 99 ;; esac
    arguments=("$@")
    requested=""
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = get ] && [ "${arguments[index + 1]}" = secret ]; then
        requested="${arguments[index + 2]}"
        break
      fi
    done
    source_id="$(jq -r '.config_id' "$QUEQLITE_KUBECTL_FIXTURE_BUNDLE_FILE")"
    if [ "$requested" = "queqlite-c${source_id}-bundle" ]; then
      jq -n --arg bundle "$(openssl base64 -A -in "$QUEQLITE_KUBECTL_FIXTURE_BUNDLE_FILE")" \
        '{data:{"config.json":$bundle}}'
    else
      exit 1
    fi
    ;;
  *" get secret queqlite-auth -o json "*) cat "$QUEQLITE_KUBECTL_FIXTURE_AUTH_RESPONSE" ;;
  *" get secret missing-object-credentials "*) exit 1 ;;
  *" exec -i queqlite-c"*" -- queqlite validate-config-bundle --stdin "*)
    "$QUEQLITE_KUBECTL_FIXTURE_QUEQLITE" validate-config-bundle --stdin
    ;;
  *" create secret generic "*" --dry-run=client -o yaml "*)
    arguments=("$@")
    for ((index=0; index + 2 < ${#arguments[@]}; index++)); do
      if [ "${arguments[index]}" = secret ] && [ "${arguments[index + 1]}" = generic ]; then
        secret_name="${arguments[index + 2]}"
        break
      fi
    done
    printf 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s\ndata:\n  config.json: e30=\n  stop.json: e30=\n' \
      "$secret_name"
    ;;
  *" create --dry-run=server -f - "*)
    yq eval -e '.kind == "Secret" and .immutable == true' - >/dev/null
    [ "$QUEQLITE_KUBECTL_FIXTURE_PROFILE" != dry-run-secret-denied ]
    ;;
  *" scale statefulset "*" --replicas=0 --dry-run=server "*)
    [ "$QUEQLITE_KUBECTL_FIXTURE_PROFILE" != dry-run-scale-denied ]
    ;;
  *" apply --server-side --dry-run=server --validate=false -f "*)
    [ "$QUEQLITE_KUBECTL_FIXTURE_PROFILE" != dry-run-apply-denied ]
    ;;
  *" create -f "*)
    manifest="${*: -1}"
    if [ "$(yq eval -r '.spec.template.spec.containers[0].name' "$manifest")" = curl ]; then
      method="$(yq eval -r '.spec.template.spec.containers[0].env[] |
        select(.name == "QUEQLITE_ADMIN_METHOD") | .value' "$manifest")"
      path="$(yq eval -r '.spec.template.spec.containers[0].env[] |
        select(.name == "QUEQLITE_ADMIN_PATH") | .value' "$manifest")"
      [ "$method $path" = "GET /v1/admin/membership/status" ]
      printf 'admin %s %s\n' "$method" "$path" >> "$QUEQLITE_KUBECTL_FIXTURE_LOG"
    else
      args="$(yq eval -r '.spec.template.spec.containers[0].args | join(" ")' "$manifest")"
      printf '%s\n' "$args" >> "$QUEQLITE_KUBECTL_FIXTURE_LOG"
      case "$args" in
        validate-config-bundle)
          if QUEQLITE_CONFIG_BUNDLE_FILE="$QUEQLITE_KUBECTL_FIXTURE_BUNDLE_FILE" \
            "$QUEQLITE_KUBECTL_FIXTURE_QUEQLITE" validate-config-bundle \
            > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_RESPONSE" 2>/dev/null; then
            printf success > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE"
          else
            printf failed > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE"
          fi
          ;;
        "checkpoint inspect")
          case "$QUEQLITE_KUBECTL_FIXTURE_PROFILE" in
            endpoint)
              [ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
                select(.name == "QUEQLITE_S3_ENDPOINT") | .value' "$manifest")" = \
                http://127.0.0.1:1 ]
              ;;
            *)
              [ "$(yq eval '[.spec.template.spec.containers[0].env[] |
                select(.name == "QUEQLITE_S3_ENDPOINT" or
                  .name == "QUEQLITE_S3_ACCESS_KEY" or
                  .name == "QUEQLITE_S3_SECRET_KEY")] | length' "$manifest")" = 0 ]
              ;;
          esac
          case "$QUEQLITE_KUBECTL_FIXTURE_PROFILE" in
            dry-run-*)
              source_id="$(jq -r '.config_id' "$QUEQLITE_KUBECTL_FIXTURE_BUNDLE_FILE")"
              jq -n --argjson id "$source_id" '{identity:{config_id:$id}}' \
                > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_RESPONSE"
              printf success > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE"
              ;;
            *)
              printf failed > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE"
              : > "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_RESPONSE"
              ;;
          esac
          ;;
        *) exit 99 ;;
      esac
    fi
    ;;
  *" get job/ql-admin-"*"Complete"*) printf 'True' ;;
  *" get job/ql-admin-"*"Failed"*) exit 0 ;;
  *" logs job/ql-admin-"*) cat "$QUEQLITE_KUBECTL_FIXTURE_ADMIN_RESPONSE" ;;
  *" get job/ql-object-"*"Complete"*)
    [ "$(cat "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE")" = success ] && printf 'True'
    ;;
  *" get job/ql-object-"*"Failed"*)
    [ "$(cat "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_STATE")" = failed ] && printf 'True'
    ;;
  *" logs job/ql-object-"*)
    if [ -s "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_RESPONSE" ]; then
      cat "$QUEQLITE_KUBECTL_FIXTURE_OBJECT_RESPONSE"
    else
      echo "fixture object-store preflight failed" >&2
    fi
    ;;
  *) exit 99 ;;
esac
