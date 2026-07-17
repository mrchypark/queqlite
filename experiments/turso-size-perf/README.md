# Turso 0.7.0 vs rusqlite 0.40.1 size/performance spike

Conclusion on this current target: embedding Turso did **not** make the small
binary smaller. The stripped Turso binary is exactly 10,598,960 bytes versus
2,274,992 bytes for bundled rusqlite: Turso is 4.6588998994x the size
(8,323,968 bytes / 365.890% larger). It also has 172 normal dependency packages
in the resolved tree versus 11 for rusqlite.

Turso was not uniformly faster. It was faster for point insert/update/read in
this final run, but 2.87x slower for the ordered scan and 2.15x slower for the
transaction batch. Whole-process ratios ranged from 0.814x to 1.251x and were
mixed rather than a consistent win. FULL-durability timings had wide and in one
case bimodal spreads, so these are small isolated CLI observations, not a full
Rhiza image or workload result.

## Scope and method

This directory is a standalone crate with an empty `[workspace]`; it does not
join or modify Rhiza's root workspace. Versions and features are pinned:

- `turso = =0.7.0, default-features = false`
- `rusqlite = =0.40.1, features = ["bundled"]`
- `tokio = =1.48.0, features = ["rt-multi-thread", "sync"]` in **both**
  binaries

Both CLIs create the same schema and values, issue the same SQL shape, use a
fresh temporary database per process, perform two warmups, alternate backend
order, then retain eight samples. Both warmup and retained sets have each
backend first equally often. Tables below show median `[min, max]`. Every
non-contention case produced the same 64-bit observable-result checksum across
engines and all samples. Timings use the unstripped release binaries; stripped
copies are measured for size only.

After each process exits, the Python runner independently reopens its database
with stdlib SQLite, reads `id,value ORDER BY id`, and recomputes row count and
FNV-1a checksum. Every retained record must match the binary's reported values;
multiwriter records additionally require persisted row count to equal successful
writes. A mismatch prevents the summary from being written as authoritative.

Both engines were explicitly requested and observed as `journal_mode=wal`,
`synchronous=2` (`FULL`), and `busy_timeout=0ms`. There is only one FULL
durability stratum; no NORMAL data is mixed in. Each binary creates the same
eight-thread Tokio runtime. Runtime construction is separately timed and is
excluded from open/setup/operation timing. External process timing includes
loader/startup, runtime creation, open, setup/work, output, and shutdown.

Environment: `aarch64-apple-darwin`, macOS 26.3, rustc 1.95.0, cargo 1.95.0,
Git `c9cf59024689fb3a30812dbf17ff018dc053fd28`, dirty worktree,
`origin/main=42f17cf6d271cef9124a70e2795a92a1357cfb66`.

## Binary size and payload boundary

| Backend | Unstripped bytes | Stripped bytes | Normal deps | Dynamic libraries |
| --- | ---: | ---: | ---: | --- |
| Turso | 11,451,376 | 10,598,960 | 172 | CoreFoundation, libiconv, libSystem |
| rusqlite | 2,437,760 | 2,274,992 | 11 | libSystem |

The unstripped Turso/rusqlite ratio is exactly 4.6974993437x; the stripped
ratio is 4.6588998994x. The local copies are under `artifacts/{turso,rusqlite}`
after running the harness. They and the `otool -L` lists are the smallest
honest current-target payload inputs measured here.

These are Mach-O binaries and macOS system libraries. They cannot be copied
into a Linux scratch/distroless image, so this experiment does **not** claim a
container payload size or a full Rhiza Docker image size. A Linux container
comparison requires rebuilding the same pinned harness for Rhiza's actual
Linux target and measuring that binary plus its runtime `.so` closure.

## Single-connection results

Primary timings are milliseconds. The warm-reopen primary metric is explicitly
`open_ns`; its `operation_ns` is exactly zero in every sample. All other rows
use `operation_ns`. `process` is the external fresh-process measurement for the
same case. The ratio is Turso median / rusqlite median, so values over 1 mean
Turso took longer.

| Case | Primary metric | Turso primary ms | rusqlite primary ms | Primary ratio | Turso process ms | rusqlite process ms | Process ratio |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cold create | `operation_ns` | 2.511 `[0.333, 3.715]` | 2.638 `[0.425, 4.812]` | 0.952x | 10.304 `[9.741, 12.806]` | 9.282 `[8.525, 10.508]` | 1.110x |
| warm reopen | `open_ns` | 0.256 `[0.246, 0.280]` | 2.902 `[0.294, 4.365]` | 0.088x | 9.597 `[8.202, 12.490]` | 11.445 `[9.594, 13.221]` | 0.838x |
| 32 point inserts | `operation_ns` | 12.333 `[9.160, 14.993]` | 15.675 `[11.290, 17.856]` | 0.787x | 22.319 `[19.325, 28.401]` | 27.426 `[23.964, 30.875]` | 0.814x |
| 32 point updates | `operation_ns` | 13.359 `[10.902, 23.448]` | 16.127 `[9.998, 18.618]` | 0.828x | 26.032 `[22.525, 41.875]` | 28.368 `[23.888, 31.963]` | 0.918x |
| 500 point reads | `operation_ns` | 0.464 `[0.443, 0.951]` | 0.559 `[0.519, 1.223]` | 0.831x | 12.666 `[9.250, 15.229]` | 13.782 `[10.435, 17.526]` | 0.919x |
| 1,000-row ordered scan | `operation_ns` | 0.286 `[0.256, 0.530]` | 0.100 `[0.093, 0.110]` | 2.871x | 12.423 `[8.743, 19.147]` | 12.143 `[8.828, 19.275]` | 1.023x |
| 500-row transaction batch | `operation_ns` | 0.625 `[0.590, 3.346]` | 0.291 `[0.265, 1.827]` | 2.146x | 7.795 `[6.902, 13.024]` | 6.230 `[5.011, 11.033]` | 1.251x |

An internal operation can favor Turso while the whole process favors rusqlite
because `operation_ns` brackets only the named SQL work. External process time
also includes loading the much larger binary, dynamic initialization, argument
parsing, runtime construction, database open/pragmas, untimed fixture setup,
checksum verification, JSON output, and shutdown. “Cold process” here means a
fresh process and fresh database, not an operating-system page-cache flush.

For the cold-create case, the separately measured open medians were 1,196,187
ns for Turso and 1,661,895 ns for rusqlite (0.720x). The warm-open
`operation_ns`
median/min/max is `0/0/0` for both backends.

## Multi-writer results

Each writer uses its own connection and attempts 16 disjoint autocommit inserts
with a zero busy timeout. Success totals and busy rates aggregate eight samples.
Time is median `[min, max]` milliseconds for completing all attempts. Successful
ops/s is the median per-sample rate, not attempted ops/s.

| Writers | Turso success / attempts | rusqlite success / attempts | Turso busy | rusqlite busy | Turso time ms | rusqlite time ms | Turso successful ops/s | rusqlite successful ops/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 128 / 128 | 128 / 128 | 0.00% | 0.00% | 0.938 `[0.833, 1.109]` | 1.757 `[1.582, 3.429]` | 17,051 | 9,104 |
| 2 | 256 / 256 | 128 / 256 | 0.00% | 50.00% | 1.606 `[1.517, 1.804]` | 1.699 `[1.497, 2.902]` | 19,931 | 9,419 |
| 4 | 234 / 512 | 118 / 512 | 54.30% | 76.95% | 1.535 `[0.944, 2.438]` | 1.895 `[1.182, 2.449]` | 18,210 | 7,677 |
| 8 | 118 / 1,024 | 77 / 1,024 | 88.48% | 92.48% | 1.021 `[0.418, 2.023]` | 1.975 `[1.647, 2.708]` | 13,712 | 4,397 |

The shorter 4/8-writer times mostly reflect fast `BUSY` failures, not useful
scaling. With zero wait, neither backend provides acceptable 4/8-writer success
rates. Results are scheduler-sensitive and should not be extrapolated to a
production retry/backoff policy.

## Provenance and authority

The experiment directory is untracked in the dirty parent worktree, so the Git
commit alone cannot identify these inputs. The report hashes the trackable
experiment inputs (`Cargo.toml`, `Cargo.lock`, `run.py`, `src/**/*.rs`, and
`tests/**/*.py`) with a path/content-framed SHA-256 tree digest:

```text
source tree  81da07fc20740643f67c8fa5e1651a81e056f59a6eb4a6d4df4b4373a304d96c
Cargo.lock   79eb1c92a0a66aadfa8c5fbac45a21f6d7f98d98cfda96c37705170d8f628bfc
Turso release unstripped d08a3e851cc9e6de04b75832473920de5cbf1949b12c4a9946a60aed1c1f6b5f
Turso release stripped   9b60db763e4ce531906b8c4c9c0b93c4575aa84a7a5fbfe3e527ad4e837cf5a1
rusqlite release unstripped 2f5bc259b0a0e35ad74769bca4614661d4a4390f07867249dbf644e76286b238
rusqlite release stripped   8c46b4f51a09c266b740b6bf779f4b5bb36f5654f7dc8d5b880e5d175e432a08
```

The JSON contains all 220 exact generated invocations (warmups included), 176
retained records, artifact paths/hashes, and source file list. It is marked
`validated_at_generation` only after source/artifact provenance, balanced order,
summary structure, persisted observations, and invocation coverage agree.
Re-run `python3 run.py --verify results/summary.json` to reject stale source or
binary artifacts before treating the file as authoritative.

## Rhiza API gaps are a separate hard stop

This benchmark proves only the exercised open/SQL/transaction/concurrency
surface. A source scan of the pinned Turso 0.7.0 public Rust API found no public
equivalent for Rhiza's current use of:

- SQLite authorizer callbacks for deterministic-write and read-only policy;
- SQLite session/changeset capture, validation, and conflict-aware apply;
- progress-handler cancellation used to enforce SQL query deadlines;
- online backup used to create consistent snapshots;
- `Statement::readonly()` checks; and
- read-write/no-create open flags used when validating existing database and
  snapshot files.

These gaps are not benchmark failures and were not approximated in timed code.
They require a correctness/capability spike and production design before Turso
could replace rusqlite. Better microbenchmark numbers cannot override a missing
policy, cancellation, replication-effect, or snapshot API.

## Reproduce and verify

From this directory:

```sh
cargo fmt --all -- --check
cargo check --no-default-features --features rusqlite-backend --bin rusqlite-size-perf
cargo check --no-default-features --features turso-backend --bin turso-size-perf
cargo clippy --no-default-features --features rusqlite-backend --bin rusqlite-size-perf -- -D warnings
cargo clippy --no-default-features --features turso-backend --bin turso-size-perf -- -D warnings
cargo test --no-default-features
python3 -m unittest discover -s tests -v
python3 run.py --samples 8 --warmups 2
python3 run.py --verify results/summary.json
cargo tree --locked --prefix none --no-default-features --features turso-backend -e features
cargo tree --locked --prefix none --no-default-features --features rusqlite-backend -e features
```

Canonical single invocation after the release build:

```sh
artifacts/turso/turso-size-perf.unstripped --db /tmp/turso.db --scenario ordered_scan --count 1000 --writers 1
artifacts/rusqlite/rusqlite-size-perf.unstripped --db /tmp/rusqlite.db --scenario ordered_scan --count 1000 --writers 1
```

The authoritative nanosecond-level raw samples, environment, exact generated
commands, provenance hashes, sizes, ratios, error/busy rates, persisted row
counts, independently verified checksum values, and summary are in
`results/summary.json`. Resolved normal and feature dependency trees are in
`results/cargo-tree-{turso,rusqlite}.txt` and
`results/cargo-tree-{turso,rusqlite}-features.txt`.
