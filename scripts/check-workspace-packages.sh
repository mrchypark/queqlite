#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repo_root"

if (( $# != 0 )); then
  echo "this release guard has a fixed package set and accepts no arguments" >&2
  exit 64
fi

# Package the eight crates in the initial SQL-only registry release, with
# dependencies before consumers. Graph, KV, client, CLI, testkit, and the basic
# app server are intentionally absent; testkit and the example set `publish = false`.
cargo package --locked --allow-dirty --no-verify \
  -p rhiza-core \
  -p rhiza-log \
  -p rhiza-obj-store \
  -p rhiza-quepaxa \
  -p rhiza-archive \
  -p rhiza-sql \
  -p rhiza-node \
  -p rhizadb
