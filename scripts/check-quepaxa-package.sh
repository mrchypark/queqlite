#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

cd "$repo_root"
rm -rf target/package
cargo package -p queqlite-core -p queqlite-quepaxa --no-verify "$@"

tar -xzf target/package/queqlite-core-0.1.0.crate -C "$work_dir"
tar -xzf target/package/queqlite-quepaxa-0.1.0.crate -C "$work_dir"

mkdir -p "$work_dir/consumer/src"
cat >"$work_dir/consumer/Cargo.toml" <<EOF
[package]
name = "quepaxa-package-smoke"
version = "0.0.0"
edition = "2021"

[dependencies]
queqlite-quepaxa = { path = "$work_dir/queqlite-quepaxa-0.1.0" }

[patch.crates-io]
queqlite-core = { path = "$work_dir/queqlite-core-0.1.0" }
EOF

cat >"$work_dir/consumer/src/main.rs" <<'EOF'
use queqlite_quepaxa::{Command, CommandKind, Membership};

fn main() {
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let command = Command::new(CommandKind::Deterministic, b"smoke".to_vec());
    assert_eq!(membership.quorum_size(), 2);
    assert_eq!(command.payload(), b"smoke");
}
EOF

cargo run --quiet --manifest-path "$work_dir/consumer/Cargo.toml"
