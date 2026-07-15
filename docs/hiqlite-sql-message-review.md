# Hiqlite SQL Message Review

> Status: architecture reference. Reviewed against Hiqlite commit
> `c8316c53799c509990475ea8e2aa2ef8679e070e` (Hiqlite 0.14-era source).

## What Hiqlite Sends

Hiqlite does not replicate a SQLite file mutation produced by one node. It
serializes a typed request and applies that request through OpenRaft:

```text
remote Client
  -> bincode ApiStreamRequest { request_id, payload }
  -> WebSocket connected to the current Raft leader
  -> QueryWrite::{Execute, ExecuteReturning, Transaction, Batch, Migration}
  -> OpenRaft EntryPayload::Normal(QueryWrite)
  -> state-machine apply
  -> local writer actor
  -> prepared SQLite statement / transaction
  -> typed Response
```

A client embedded in the leader skips the client WebSocket and calls
`client_write` directly. A remote client tracks the leader and retries after a
leader update. The API stream and Raft transport both use serde types encoded by
bincode. The SQL log payload is a `Query { sql, params }`; `Param` preserves
NULL, integer, real, text, and blob values. Transactions can also reference the
first returned row of an earlier statement by column index or name.

Reads do not normally enter the replicated log. Hiqlite offers local reads and
a more expensive consistent read routed through the leader. Writes, migrations,
backups, and an RTT marker are separate `QueryWrite` variants.

## Useful Patterns for rhiza sql

Adopt or retain these patterns:

- A typed, versioned write envelope, separate from transport and consensus.
- Explicit operation variants instead of inferring execute, transaction,
  migration, or maintenance semantics from SQL text.
- Typed parameters and result cells.
- A transaction message containing an ordered statement list.
- A single SQLite writer boundary with prepared-statement caching.
- Separate local and consistency-barrier read paths.
- A response envelope correlated to the client request ID.

rhiza sql now has this shape in QSQL v2: it carries a stable request ID, typed
statements, an executor fingerprint, and a statement-level result with
`rows_affected` plus bounded typed `RETURNING` rows. QSQL v2 persists that
typed result with the request record, making exact retries return the original
result. QuePaxa still orders opaque bytes and SQLite still provides the atomic
transaction boundary; this is an application-envelope change, not a SQL-aware
consensus protocol.

Do not adopt these Hiqlite-specific choices:

- Leader forwarding. A QuePaxa preferred proposer is only a latency hint and
  must not become an exclusive write authority.
- Panicking a replica when a forbidden function reaches the writer. Admission
  must reject before proposal, and apply must fail closed with diagnosable
  recovery behavior.
- Treating a nondeterministic-function denylist as proof that arbitrary SQL text
  is deterministic.
- Coupling SQL messages to Kubernetes or object storage. Those stay outside the
  consensus and state-machine contracts.

## Correctness Limit of Statement Replication

Both systems execute the same SQL text independently on each SQLite replica.
Blocking `random()`, time functions, connection-local counters, `PRAGMA`, TEMP,
`ATTACH`, and virtual tables removes common hazards but does not make every
SQLite write deterministic.

One counterexample is implicit ROWID allocation. If a table has already used
ROWID `9223372036854775807`, SQLite may choose positive candidate ROWIDs
pseudo-randomly. No SQL function appears for an authorizer or function denylist
to reject. SQLite versions, compile options, extensions, collations, and
connection settings are additional executor-compatibility boundaries.

Therefore rhiza sql's statement-replay fallback calls its write surface
**admitted deterministic SQL**. It supports DDL and DML within an explicit
policy, but it must not promise unrestricted SQLite write semantics. Direct
statement-replay `RETURNING` is rejected because its typed rows must be
persisted and replayed exactly. Read-only SQL can be broader because its result
is not replayed as replicated state.

## Bounded Effect Replication

The implemented bounded path for eligible QSQL v2 writes is QEFX v1 effect
replication:

```text
request coordinator
  -> execute once in an isolated SQLite transaction at the exact qlog base
  -> capture a deterministic, bounded SQLite session changeset
  -> QuePaxa decides effect bytes + request identity + base position
  -> every node applies the same effect
  -> qlog and snapshot recovery replay the effect and persisted result
```

QEFX is intentionally bounded and is not unrestricted arbitrary effect
replication. Its envelope binds the exact base qlog index and hash, the SQLite
executor fingerprint, the canonical request digest, the persisted typed
result, and a session changeset capped at 256 KiB. QEFX v1 is the effect format
and the surrounding qlog entry supplies the decided position; cluster,
configuration, and recovery-generation identity are validated by the qlog and
QANC recovery metadata.

The effect path explicitly validates complete table schema and primary keys.
DDL, tables without a complete supported primary key/schema, triggers, foreign
keys, and indirect trigger/foreign-key changes are outside QEFX. They fall back
to deterministic statement replay only when the admitted request has no
`RETURNING`; a `RETURNING` request fails closed. QEFX application uses
`SQLITE_CHANGESET_ABORT` for conflicts. If a competing command occupies the
proposed slot, the coordinator regenerates the effect against that command's
new exact qlog base instead of reusing stale effect bytes.

Snapshots and QANC v3 recovery anchors carry the executor fingerprint and
recovery metadata. A mismatch is rejected during recovery rather than
attempting best-effort execution.

## Staged Decision

1. Keep QSQL v2 as the typed command envelope and retain deterministic
   statement replay only as the bounded non-RETURNING fallback.
2. Keep QuePaxa unaware of SQL. It orders a versioned opaque state-machine
   command in every stage.
3. Keep QEFX v1 limited to validated, bounded session changesets and test
   duplicate delivery, conflicts, crash points, exact-base regeneration,
   snapshot restore, and fingerprint mismatch rejection.
4. Keep MAB preferred-proposer and hedge-delay tuning documentation-only.

## Sources

- [Hiqlite repository](https://github.com/sebadob/hiqlite)
- [Hiqlite crate documentation](https://docs.rs/crate/hiqlite/latest)
- [SQLite ROWID allocation](https://www.sqlite.org/autoinc.html)
- [SQLite deterministic functions](https://www.sqlite.org/deterministic.html)
- [SQLite session extension](https://www.sqlite.org/sessionintro.html)
- [SQLite WAL format](https://www.sqlite.org/walformat.html)
