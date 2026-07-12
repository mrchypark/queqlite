# queqlite-core

Shared, deterministic value types for Queqlite's replicated log and consensus
packages. This crate contains no networking, storage, SQLite, Tokio, or
Kubernetes integration.

`queqlite-core` and `queqlite-quepaxa` use matching minor versions. Public
serialized types are not a stable wire protocol unless a format version is
explicitly documented by the owning package.

Minimum supported Rust version: 1.89.
