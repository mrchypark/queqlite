#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repo_root"

# Keep workspace dependencies before their consumers. The basic app server is
# intentionally absent because its manifest sets `publish = false`.
cargo package --locked --allow-dirty --no-verify \
  -p rhiza-core \
  -p rhiza-log \
  -p rhiza-obj-store \
  -p rhiza-quepaxa \
  -p rhiza-graph \
  -p rhiza-archive \
  -p rhiza-kv \
  -p rhiza-sql \
  -p rhiza-testkit \
  -p rhiza-node \
  -p rhiza-client \
  -p rhizadb \
  -p rhiza-cli \
  "$@"
