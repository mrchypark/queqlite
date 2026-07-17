# Rust SQLite conformance spike

This is an isolated, non-production experiment comparing `graphitesql = 0.1.3`
with the bundled SQLite exposed by `rusqlite = 0.40.1`. It does not connect to
Rhiza, `rhiza-sql`, or QuePaxa. The empty `[workspace]` keeps it out of the root
workspace and default CI.

Run from this directory:

```sh
cargo test
cargo run --release -- --bench-iterations 200
```

The default command prints a readable table, writes the authoritative
`results/conformance-summary.json`, then exits with code `2` when the hard stop
is active. For diagnostic artifact generation only, acknowledge that expected
non-zero gate explicitly:

```sh
cargo run --release -- --bench-iterations 200 --allow-hard-stop
```

`--output <path>` may be used by tests or one-off diagnostics; it has no second
implicit default. The command writes a machine-readable JSON report before
enforcing its exit code.
The report includes exact crate/engine versions, target triple, corpus SHA-256,
and Git head/dirty state. `--bench-iterations` is the iteration count **per
sample** for both engines. The diagnostic microbenchmark runs six samples with
the engine order alternating each sample, records the common warmup count,
materializes and checksums the same observable query results, and reports the
median plus min/max spread. It runs only after all correctness gates pass and is
never evidence of production readiness.

Hard stop: any correctness `FAIL` or `BLOCKED_CAPABILITY`, or any policy/
cancellation capability not `PASS`, means the candidate must not replace the
reference engine. Known missing APIs are reported, never skipped. In particular,
the candidate connection remains inside its creating process; the adversarial
query is killed by the parent at a hard deadline and its temporary database is
discarded.
