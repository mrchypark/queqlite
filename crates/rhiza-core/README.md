# rhiza-core

Shared, deterministic value types for rhiza's replicated log and consensus
packages. This crate contains no networking, storage, SQLite, Tokio, or
Kubernetes integration.

`rhiza-core` and `rhiza-quepaxa` use matching minor versions. Public
serialized types are not a stable wire protocol unless a format version is
explicitly documented by the owning package.

Minimum supported Rust version: 1.89.
