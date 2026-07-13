#!/usr/bin/env bash
set -euo pipefail

state_file="${QUEQLITE_KUBECTL_FIXTURE_STATE:?}"

case " $* " in
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
    response="${QUEQLITE_KUBECTL_FIXTURE_RESPONSE-}"
    [ -n "$response" ] || response='{}'
    printf '%s' "$response"
    ;;
esac
