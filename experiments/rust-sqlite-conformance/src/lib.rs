use fallible_streaming_iterator::FallibleStreamingIterator;
use graphitesql::{Connection as GraphiteConnection, QueryResult as GraphiteResult, Value};
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetIter, ConflictAction, Session};
use rusqlite::types::ValueRef;
use rusqlite::{Connection as ReferenceConnection, MAIN_DB};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const CORPUS_VERSION: &str = "v1";
pub const CORPUS: &str = include_str!("../corpus/v1.sql");
pub const CORPUS_SHA256_V1: &str =
    "e3252ab25165741e49b6e811fb6cbd2bacd33208133022a099087a1353ef33c8";
pub const DEFAULT_SUMMARY_PATH: &str = "results/conformance-summary.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Pass,
    Fail,
    BlockedCapability,
}

#[derive(Clone, Debug, Serialize)]
pub struct Check {
    pub name: String,
    pub status: Status,
    pub detail: String,
}

impl Check {
    fn pass(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Pass,
            detail: detail.into(),
        }
    }

    fn fail(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Fail,
            detail: detail.into(),
        }
    }

    fn blocked(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::BlockedCapability,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Cell {
    storage_type: &'static str,
    value: JsonValue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Observable {
    columns: Vec<String>,
    rows: Vec<Vec<Cell>>,
}

#[derive(Debug, Serialize)]
pub struct Provenance {
    graphitesql_crate: &'static str,
    graphitesql_sqlite_target: &'static str,
    graphitesql_reported_sqlite: String,
    rusqlite_crate: &'static str,
    reference_sqlite: String,
    target: &'static str,
    corpus_version: &'static str,
    corpus_sha256: String,
    git_head: Option<String>,
    git_dirty: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BenchMetric {
    pub engine: &'static str,
    pub operation: &'static str,
    pub iterations_per_sample: u64,
    pub warmup_iterations: u64,
    pub samples: usize,
    pub median_ns_per_operation: u128,
    pub min_ns_per_operation: u128,
    pub max_ns_per_operation: u128,
    pub sample_ns_per_operation: Vec<u128>,
    pub observable_checksum: u64,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkMethodology {
    pub samples: usize,
    pub iterations_per_sample: u64,
    pub warmup_iterations: u64,
    pub engine_order: &'static str,
    pub observable_work: &'static str,
    pub statistic: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub label: &'static str,
    pub production_ready: bool,
    pub provenance: Provenance,
    pub correctness: Vec<Check>,
    pub policy_and_cancellation: Vec<Check>,
    pub benchmark_methodology: BenchmarkMethodology,
    pub microbenchmark: Vec<BenchMetric>,
    pub benchmark_skipped_reason: Option<String>,
    pub hard_stop: bool,
    pub hard_stop_criteria: &'static str,
}

static TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Result<Self, String> {
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rust-sqlite-conformance-{tag}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|e| format!("create {}: {e}", path.display()))?;
        Ok(Self(path))
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub fn corpus_digest() -> String {
    format!("{:x}", Sha256::digest(CORPUS.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn graphite_cell(value: &Value) -> Cell {
    match value {
        Value::Null => Cell {
            storage_type: "null",
            value: JsonValue::Null,
        },
        Value::Integer(v) => Cell {
            storage_type: "integer",
            value: json!(v),
        },
        Value::Real(v) => Cell {
            storage_type: "real",
            value: json!(v),
        },
        Value::Text(v) => Cell {
            storage_type: "text",
            value: json!(v.to_string()),
        },
        Value::Blob(v) => Cell {
            storage_type: "blob",
            value: json!(hex(v)),
        },
    }
}

fn reference_cell(value: ValueRef<'_>) -> Cell {
    match value {
        ValueRef::Null => Cell {
            storage_type: "null",
            value: JsonValue::Null,
        },
        ValueRef::Integer(v) => Cell {
            storage_type: "integer",
            value: json!(v),
        },
        ValueRef::Real(v) => Cell {
            storage_type: "real",
            value: json!(v),
        },
        ValueRef::Text(v) => Cell {
            storage_type: "text",
            value: json!(String::from_utf8_lossy(v)),
        },
        ValueRef::Blob(v) => Cell {
            storage_type: "blob",
            value: json!(hex(v)),
        },
    }
}

fn graphite_observable(result: &GraphiteResult) -> Observable {
    Observable {
        columns: result.columns.clone(),
        rows: result
            .rows
            .iter()
            .map(|row| row.iter().map(graphite_cell).collect())
            .collect(),
    }
}

fn reference_query(conn: &ReferenceConnection, sql: &str) -> Result<Observable, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let columns = stmt
        .column_names()
        .iter()
        .map(ToString::to_string)
        .collect();
    let column_count = stmt.column_count();
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let mut cells = Vec::with_capacity(column_count);
        for index in 0..column_count {
            cells.push(reference_cell(
                row.get_ref(index).map_err(|e| e.to_string())?,
            ));
        }
        out.push(cells);
    }
    Ok(Observable { columns, rows: out })
}

fn mismatch(name: &str, candidate: &Observable, reference: &Observable) -> Check {
    Check::fail(
        name,
        format!(
            "observable mismatch; candidate={} reference={}",
            serde_json::to_string(candidate).unwrap_or_default(),
            serde_json::to_string(reference).unwrap_or_default()
        ),
    )
}

pub fn run_core_differential() -> Check {
    let run = || -> Result<String, String> {
        if corpus_digest() != CORPUS_SHA256_V1 {
            return Err(format!(
                "corpus v1 changed without a version bump: expected {CORPUS_SHA256_V1}, got {}",
                corpus_digest()
            ));
        }
        let mut candidate = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        let reference = ReferenceConnection::open_in_memory().map_err(|e| e.to_string())?;
        candidate
            .execute_batch(CORPUS)
            .map_err(|e| format!("candidate corpus: {e}"))?;
        reference
            .execute_batch(CORPUS)
            .map_err(|e| format!("reference corpus: {e}"))?;

        let queries = [
            "SELECT id, k, n, payload, note FROM items ORDER BY id",
            "SELECT seq, item_id, op FROM audit ORDER BY seq",
            "SELECT type, name, tbl_name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        ];
        for sql in queries {
            let got = graphite_observable(
                &candidate
                    .query(sql)
                    .map_err(|e| format!("candidate query `{sql}`: {e}"))?,
            );
            let want = reference_query(&reference, sql)?;
            if got != want {
                return Err(mismatch("fixed_sql_corpus", &got, &want).detail);
            }
        }

        let returning_sql = "INSERT INTO items(id,k,n,payload,note) VALUES (4,'delta',4.25,x'0405','returned') RETURNING id,k,n,payload,note";
        let got = graphite_observable(
            &candidate
                .execute_returning(returning_sql, &Default::default())
                .map_err(|e| format!("candidate RETURNING: {e}"))?,
        );
        let want = reference_query(&reference, returning_sql)?;
        if got != want {
            return Err(mismatch("returning", &got, &want).detail);
        }
        Ok(format!(
            "corpus {} ({}) matched columns, storage types, values, row order, schema, triggers, transactions, UPSERT/REPLACE/multi-row, and RETURNING",
            CORPUS_VERSION,
            corpus_digest()
        ))
    };
    match run() {
        Ok(detail) => Check::pass("fixed_sql_corpus_and_returning", detail),
        Err(detail) => Check::fail("fixed_sql_corpus_and_returning", detail),
    }
}

fn integrity(conn: &ReferenceConnection) -> Result<(), String> {
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| e.to_string())?;
    if result == "ok" {
        Ok(())
    } else {
        Err(format!("integrity_check returned {result:?}"))
    }
}

fn reference_text_rows(conn: &ReferenceConnection) -> Result<Vec<(i64, String)>, String> {
    let mut stmt = conn
        .prepare("SELECT id,v FROM t ORDER BY id")
        .map_err(|e| e.to_string())?;
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

pub fn run_interoperability() -> Check {
    let run = || -> Result<String, String> {
        let temp = TempDir::new("interop")?;
        let candidate_file = temp.file("candidate.db");
        {
            let mut conn = GraphiteConnection::create(&candidate_file.to_string_lossy())
                .map_err(|e| e.to_string())?;
            conn.execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'candidate'),(2,'shared');",
            )
            .map_err(|e| e.to_string())?;
        }
        let reference = ReferenceConnection::open(&candidate_file).map_err(|e| e.to_string())?;
        integrity(&reference)?;
        let candidate_seed = reference_text_rows(&reference)?;
        if candidate_seed != vec![(1, "candidate".into()), (2, "shared".into())] {
            return Err(format!(
                "reference did not preserve candidate-created rows: {candidate_seed:?}"
            ));
        }
        reference
            .execute("INSERT INTO t VALUES(3,'reference-write')", [])
            .map_err(|e| e.to_string())?;
        drop(reference);
        let candidate = GraphiteConnection::open(&candidate_file.to_string_lossy())
            .map_err(|e| e.to_string())?;
        let rows = candidate
            .query("SELECT id,v FROM t ORDER BY id")
            .map_err(|e| e.to_string())?
            .rows;
        let expected = vec![
            vec![Value::Integer(1), Value::Text("candidate".into())],
            vec![Value::Integer(2), Value::Text("shared".into())],
            vec![Value::Integer(3), Value::Text("reference-write".into())],
        ];
        if rows != expected {
            return Err(format!(
                "candidate did not preserve reference mutation of candidate file: {rows:?}"
            ));
        }
        drop(candidate);

        let reference_file = temp.file("reference.db");
        {
            let reference =
                ReferenceConnection::open(&reference_file).map_err(|e| e.to_string())?;
            reference
                .execute_batch(
                    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'reference');",
                )
                .map_err(|e| e.to_string())?;
        }
        {
            let mut candidate = GraphiteConnection::open(&reference_file.to_string_lossy())
                .map_err(|e| e.to_string())?;
            let before = candidate
                .query("SELECT v FROM t WHERE id=1")
                .map_err(|e| e.to_string())?;
            if before.rows != vec![vec![Value::Text("reference".into())]] {
                return Err(format!("candidate read reference rows: {:?}", before.rows));
            }
            candidate
                .execute("INSERT INTO t VALUES(2,'candidate-write')")
                .map_err(|e| e.to_string())?;
        }
        let reference = ReferenceConnection::open(&reference_file).map_err(|e| e.to_string())?;
        integrity(&reference)?;
        let final_rows = reference_text_rows(&reference)?;
        if final_rows != vec![(1, "reference".into()), (2, "candidate-write".into())] {
            return Err(format!(
                "reference did not preserve original plus candidate write: {final_rows:?}"
            ));
        }
        Ok("candidate-created and reference-created files were each opened and mutated by the other engine with exact ordered rows/values preserved; both passed integrity_check".into())
    };
    match run() {
        Ok(detail) => Check::pass("file_interoperability_both_directions", detail),
        Err(detail) => Check::fail("file_interoperability_both_directions", detail),
    }
}

pub fn run_wal_reopen() -> Check {
    let run = || -> Result<String, String> {
        let temp = TempDir::new("wal")?;
        let reference_file = temp.file("reference-wal.db");
        let reference = ReferenceConnection::open(&reference_file).map_err(|e| e.to_string())?;
        reference
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); PRAGMA wal_checkpoint(TRUNCATE); INSERT INTO t VALUES(1,'wal-only');",
            )
            .map_err(|e| e.to_string())?;
        if !reference_file.with_extension("db-wal").exists()
            && !PathBuf::from(format!("{}-wal", reference_file.display())).exists()
        {
            return Err("reference did not leave a WAL companion".into());
        }
        let candidate = GraphiteConnection::open_readonly(&reference_file.to_string_lossy())
            .map_err(|e| format!("candidate WAL overlay open: {e}"))?;
        let rows = candidate
            .query("SELECT id,v FROM t ORDER BY id")
            .map_err(|e| e.to_string())?
            .rows;
        if rows != vec![vec![Value::Integer(1), Value::Text("wal-only".into())]] {
            return Err(format!("WAL overlay rows: {rows:?}"));
        }
        drop(candidate);
        drop(reference);
        let reopened = GraphiteConnection::open(&reference_file.to_string_lossy())
            .map_err(|e| format!("candidate reopen after reference close: {e}"))?;
        if reopened
            .query("SELECT count(*) FROM t")
            .map_err(|e| e.to_string())?
            .rows[0][0]
            != Value::Integer(1)
        {
            return Err("reopen lost reference WAL content".into());
        }

        let candidate_file = temp.file("candidate-reopen.db");
        {
            let mut conn = GraphiteConnection::create(&candidate_file.to_string_lossy())
                .map_err(|e| e.to_string())?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two');",
            )
            .map_err(|e| e.to_string())?;
        }
        let conn = GraphiteConnection::open(&candidate_file.to_string_lossy())
            .map_err(|e| e.to_string())?;
        if conn
            .query("SELECT count(*) FROM t")
            .map_err(|e| e.to_string())?
            .rows[0][0]
            != Value::Integer(2)
        {
            return Err("candidate WAL/reopen count mismatch".into());
        }
        Ok("read uncheckpointed reference WAL, reopened after recovery/checkpoint, and reopened candidate WAL database".into())
    };
    match run() {
        Ok(detail) => Check::pass("wal_reopen_recovery", detail),
        Err(detail) => Check::fail("wal_reopen_recovery", detail),
    }
}

pub fn run_snapshot_backup() -> Check {
    let run = || -> Result<String, String> {
        let temp = TempDir::new("snapshot")?;
        let snapshot_file = temp.file("candidate-snapshot.db");
        let mut candidate = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        candidate
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'snapshot'),(2,'consistent');",
            )
            .map_err(|e| e.to_string())?;
        let snapshot = candidate.serialize().map_err(|e| e.to_string())?;
        fs::write(&snapshot_file, &snapshot).map_err(|e| e.to_string())?;
        let reference = ReferenceConnection::open(&snapshot_file).map_err(|e| e.to_string())?;
        integrity(&reference)?;
        let snapshot_observed = reference_query(&reference, "SELECT id,v FROM t ORDER BY id")?;

        let backup_file = temp.file("reference-backup.db");
        let source = ReferenceConnection::open_in_memory().map_err(|e| e.to_string())?;
        source
            .execute_batch(
                "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT); INSERT INTO t VALUES(1,'snapshot'),(2,'consistent');",
            )
            .map_err(|e| e.to_string())?;
        source
            .backup(MAIN_DB, &backup_file, None)
            .map_err(|e| e.to_string())?;
        let candidate_backup =
            GraphiteConnection::open(&backup_file.to_string_lossy()).map_err(|e| e.to_string())?;
        let backup_observed = graphite_observable(
            &candidate_backup
                .query("SELECT id,v FROM t ORDER BY id")
                .map_err(|e| e.to_string())?,
        );
        if snapshot_observed != backup_observed {
            return Err(mismatch("snapshot_backup", &backup_observed, &snapshot_observed).detail);
        }
        Ok(format!(
            "candidate serialize artifact ({} bytes) passed reference integrity_check; reference online backup matched when opened by candidate",
            snapshot.len()
        ))
    };
    match run() {
        Ok(detail) => Check::pass("snapshot_and_backup_artifacts", detail),
        Err(detail) => Check::fail("snapshot_and_backup_artifacts", detail),
    }
}

fn normalize_changeset(bytes: &[u8]) -> Result<Vec<JsonValue>, String> {
    let mut slice = bytes;
    let input: &mut dyn Read = &mut slice;
    let mut iter = ChangesetIter::start_strm(&input).map_err(|e| e.to_string())?;
    let mut changes = Vec::new();
    while let Some(item) = iter.next().map_err(|e| e.to_string())? {
        let op = item.op().map_err(|e| e.to_string())?;
        let count = usize::try_from(op.number_of_columns()).map_err(|e| e.to_string())?;
        let code = op.code();
        let omitted = || vec![None; count];
        let old_values = || {
            (0..count)
                .map(|i| normalize_optional_changeset_value(item.old_value(i)))
                .collect::<Result<Vec<_>, _>>()
        };
        let new_values = || {
            (0..count)
                .map(|i| normalize_optional_changeset_value(item.new_value(i)))
                .collect::<Result<Vec<_>, _>>()
        };
        let (old, new) = match code {
            Action::SQLITE_INSERT => (omitted(), new_values()?),
            Action::SQLITE_DELETE => (old_values()?, omitted()),
            Action::SQLITE_UPDATE => (old_values()?, new_values()?),
            other => return Err(format!("unsupported changeset operation: {other:?}")),
        };
        changes.push(json!({
            "table": op.table_name(),
            "operation": format!("{code:?}"),
            "indirect": op.indirect(),
            "primary_key": item.pk().map_err(|e| e.to_string())?,
            "old": old,
            "new": new,
        }));
    }
    Ok(changes)
}

fn normalize_optional_changeset_value(
    value: rusqlite::Result<ValueRef<'_>>,
) -> Result<Option<Cell>, String> {
    match value {
        Ok(value) => Ok(Some(reference_cell(value))),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

pub fn run_session_cross_apply() -> Check {
    let run = || -> Result<String, String> {
        const SETUP: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n REAL); INSERT INTO t VALUES(1,'before',1.0),(2,'delete',2.0);";
        const DML: &str = "UPDATE t SET v='after', n=1.5 WHERE id=1; DELETE FROM t WHERE id=2; INSERT INTO t VALUES(3,'insert',3.0);";

        let mut candidate = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        candidate.execute_batch(SETUP).map_err(|e| e.to_string())?;
        let candidate_session = candidate.create_session();
        candidate_session.attach();
        candidate.execute_batch(DML).map_err(|e| e.to_string())?;
        let candidate_bytes = candidate
            .session_changeset(&candidate_session)
            .map_err(|e| e.to_string())?;

        let reference_generator =
            ReferenceConnection::open_in_memory().map_err(|e| e.to_string())?;
        reference_generator
            .execute_batch(SETUP)
            .map_err(|e| e.to_string())?;
        let mut reference_session =
            Session::new(&reference_generator).map_err(|e| e.to_string())?;
        reference_session
            .attach(None::<&str>)
            .map_err(|e| e.to_string())?;
        reference_generator
            .execute_batch(DML)
            .map_err(|e| e.to_string())?;
        let mut reference_bytes = Vec::new();
        reference_session
            .changeset_strm(&mut reference_bytes)
            .map_err(|e| e.to_string())?;

        let candidate_normalized = normalize_changeset(&candidate_bytes)?;
        let reference_normalized = normalize_changeset(&reference_bytes)?;
        if candidate_normalized != reference_normalized {
            return Err(format!(
                "normalized changesets differ; candidate={} reference={}",
                serde_json::to_string(&candidate_normalized).unwrap_or_default(),
                serde_json::to_string(&reference_normalized).unwrap_or_default()
            ));
        }

        let mut candidate_apply = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        candidate_apply
            .execute_batch(SETUP)
            .map_err(|e| e.to_string())?;
        candidate_apply
            .changeset_apply(&reference_bytes)
            .map_err(|e| format!("cross-apply reference to candidate: {e}"))?;
        let reverse_got = graphite_observable(
            &candidate_apply
                .query("SELECT id,v,n FROM t ORDER BY id")
                .map_err(|e| e.to_string())?,
        );
        let reference_final =
            reference_query(&reference_generator, "SELECT id,v,n FROM t ORDER BY id")?;
        if reverse_got != reference_final {
            return Err(mismatch(
                "reference_changeset_cross_apply",
                &reverse_got,
                &reference_final,
            )
            .detail);
        }

        let apply_target = ReferenceConnection::open_in_memory().map_err(|e| e.to_string())?;
        apply_target
            .execute_batch(SETUP)
            .map_err(|e| e.to_string())?;
        let mut input = candidate_bytes.as_slice();
        apply_target
            .apply_strm(&mut input, None::<fn(&str) -> bool>, |_kind, _item| {
                ConflictAction::SQLITE_CHANGESET_ABORT
            })
            .map_err(|e| format!("cross-apply candidate to reference: {e}"))?;
        let got = reference_query(&apply_target, "SELECT id,v,n FROM t ORDER BY id")?;
        if got != reference_final {
            return Err(mismatch("changeset_cross_apply", &got, &reference_final).detail);
        }
        Ok(format!(
            "candidate generated {} bytes; normalized {} changes equal reference; candidate changeset applied to rusqlite and rusqlite changeset applied to candidate",
            candidate_bytes.len(),
            candidate_normalized.len()
        ))
    };
    match run() {
        Ok(detail) => Check::pass("session_changeset_normalize_cross_apply", detail),
        Err(detail) => Check::fail("session_changeset_normalize_cross_apply", detail),
    }
}

pub fn run_timeout_probe(exe: &Path) -> Check {
    let run = || -> Result<String, String> {
        let temp = TempDir::new("timeout")?;
        let db = temp.file("discarded.db");
        let mut child = Command::new(exe)
            .arg("--adversarial-child")
            .arg(&db)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("spawn timeout child: {e}"))?;
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                return Err(format!("long query exited before hard deadline: {status}"));
            }
            if Instant::now() >= deadline {
                child
                    .kill()
                    .map_err(|e| format!("kill timeout child: {e}"))?;
                child
                    .wait()
                    .map_err(|e| format!("reap timeout child: {e}"))?;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let artifacts = [
            db.clone(),
            PathBuf::from(format!("{}-journal", db.display())),
            PathBuf::from(format!("{}-wal", db.display())),
            PathBuf::from(format!("{}-shm", db.display())),
        ];
        for artifact in &artifacts {
            if artifact.exists() {
                fs::remove_file(artifact).map_err(|e| {
                    format!("delete killed-child artifact {}: {e}", artifact.display())
                })?;
            }
        }
        if let Some(leftover) = artifacts.iter().find(|path| path.exists()) {
            return Err(format!(
                "killed-child artifact still exists: {}",
                leftover.display()
            ));
        }
        let next = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        let rows = next.query("SELECT 6 * 7").map_err(|e| e.to_string())?.rows;
        if rows != vec![vec![Value::Integer(42)]] {
            return Err(format!("post-timeout case returned {rows:?}"));
        }
        Ok("candidate stayed process-confined; parent killed long-query child at 200 ms, explicitly deleted and verified absence of its DB/sidecars, and the next candidate case returned 42".into())
    };
    match run() {
        Ok(detail) => Check::pass("subprocess_hard_deadline_recovery", detail),
        Err(detail) => Check::fail("subprocess_hard_deadline_recovery", detail),
    }
}

pub fn run_adversarial_child(path: &Path) -> Result<(), String> {
    let mut conn =
        GraphiteConnection::create(&path.to_string_lossy()).map_err(|e| e.to_string())?;
    conn.execute("CREATE TABLE marker(v INTEGER)")
        .map_err(|e| e.to_string())?;
    conn.query(
        "WITH RECURSIVE cnt(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM cnt WHERE x<100000000) SELECT sum(x) FROM cnt",
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub fn run_policy_capabilities() -> Vec<Check> {
    let mut checks = Vec::new();
    let authorizer = (|| -> Result<Check, String> {
        let mut conn = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        conn.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            .map_err(|e| e.to_string())?;
        let seen = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&seen);
        conn.set_authorizer(move |action, first, second, _, _| {
            captured.borrow_mut().push((
                action,
                first.unwrap_or_default().to_string(),
                second.unwrap_or_default().to_string(),
            ));
            0
        });
        conn.query("SELECT a,b FROM t").map_err(|e| e.to_string())?;
        let seen = seen.borrow();
        let column_reads = seen.iter().filter(|(code, _, column)| {
            *code == graphitesql::exec::auth_action::READ && !column.is_empty()
        });
        if column_reads.count() >= 2 {
            Ok(Check::pass(
                "fine_grained_authorizer",
                format!("observed per-column READ actions: {seen:?}"),
            ))
        } else {
            Ok(Check::fail(
                "fine_grained_authorizer",
                format!("only statement/table-level actions observed: {seen:?}"),
            ))
        }
    })();
    checks.push(authorizer.unwrap_or_else(|e| Check::blocked("fine_grained_authorizer", e)));

    checks.push(Check::blocked(
        "deterministic_progress_handler",
        "graphitesql 0.1.3 exports no progress-handler API; instruction-budget cancellation cannot be installed",
    ));
    checks.push(Check::blocked(
        "interrupt_handle",
        "graphitesql 0.1.3 exports no interrupt handle API; hard deadlines require process termination",
    ));

    let explain = match GraphiteConnection::open_memory().and_then(|mut conn| {
        conn.execute_batch("CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER);")?;
        conn.query("EXPLAIN SELECT a.id FROM a JOIN b ON a.id=b.id")
    }) {
        Ok(_) => Check::pass(
            "complete_explain_opcode_policy",
            "join EXPLAIN produced an opcode listing",
        ),
        Err(e) => Check::fail(
            "complete_explain_opcode_policy",
            format!("representative join EXPLAIN is incomplete: {e}"),
        ),
    };
    checks.push(explain);

    checks.push(pragma_capability(
        "trusted_schema",
        "PRAGMA trusted_schema",
        Some("PRAGMA trusted_schema=OFF"),
    ));
    checks.push(pragma_capability(
        "compile_options",
        "PRAGMA compile_options",
        None,
    ));
    checks
}

fn pragma_capability(name: &str, query: &str, mutation: Option<&str>) -> Check {
    let run = || -> Result<usize, String> {
        let mut conn = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
        let before = conn.query(query).map_err(|e| e.to_string())?;
        if let Some(sql) = mutation {
            conn.execute(sql).map_err(|e| e.to_string())?;
            let after = conn.query(query).map_err(|e| e.to_string())?;
            if before.rows == after.rows {
                return Err(format!("{query} did not observably change after `{sql}`"));
            }
            Ok(after.rows.len())
        } else if before.rows.is_empty() {
            Err(format!("{query} returned no rows"))
        } else {
            Ok(before.rows.len())
        }
    };
    match run() {
        Ok(rows) => Check::pass(name, format!("{query} returned {rows} observable row(s)")),
        Err(detail) if detail.contains("Unsupported") || detail.contains("unsupported") => {
            Check::blocked(name, detail)
        }
        Err(detail) => Check::fail(name, detail),
    }
}

fn provenance() -> Provenance {
    let graphitesql_reported_sqlite = GraphiteConnection::open_memory()
        .and_then(|conn| conn.query("SELECT sqlite_version()"))
        .ok()
        .and_then(|result| result.rows.first().and_then(|row| row.first()).cloned())
        .map(|value| match value {
            Value::Text(text) => text.to_string(),
            other => format!("{other:?}"),
        })
        .unwrap_or_else(|| "unavailable".into());
    let git_head = git(&["rev-parse", "HEAD"]);
    let git_dirty = git(&["status", "--porcelain"]).map(|s| !s.is_empty());
    Provenance {
        graphitesql_crate: "0.1.3",
        graphitesql_sqlite_target: graphitesql::TARGET_SQLITE_VERSION,
        graphitesql_reported_sqlite,
        rusqlite_crate: "0.40.1",
        reference_sqlite: rusqlite::version().into(),
        target: env!("CONFORMANCE_TARGET"),
        corpus_version: CORPUS_VERSION,
        corpus_sha256: corpus_digest(),
        git_head,
        git_dirty,
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

const BENCH_SAMPLES: usize = 6;

struct BenchSample {
    operation: &'static str,
    elapsed_ns: u128,
    checksum: u64,
}

pub fn run_microbenchmark(iterations: u64) -> Result<Vec<BenchMetric>, String> {
    let iterations = iterations.max(1);
    let warmup = (iterations / 10).clamp(1, 20);
    let mut collected: BTreeMap<(&'static str, &'static str), Vec<(u128, u64)>> = BTreeMap::new();

    for sample in 0..BENCH_SAMPLES {
        let (first_engine, first, second_engine, second) = if sample % 2 == 0 {
            (
                "graphitesql",
                candidate_bench_sample(iterations, warmup)?,
                "rusqlite",
                reference_bench_sample(iterations, warmup)?,
            )
        } else {
            (
                "rusqlite",
                reference_bench_sample(iterations, warmup)?,
                "graphitesql",
                candidate_bench_sample(iterations, warmup)?,
            )
        };
        verify_sample_checksums(&first, &second)?;
        collect_samples(&mut collected, first_engine, first);
        collect_samples(&mut collected, second_engine, second);
    }

    collected
        .into_iter()
        .map(|((engine, operation), samples)| {
            let checksum = samples
                .first()
                .map(|sample| sample.1)
                .ok_or_else(|| format!("no samples for {engine}/{operation}"))?;
            if samples.iter().any(|sample| sample.1 != checksum) {
                return Err(format!(
                    "observable checksum varied across samples for {engine}/{operation}"
                ));
            }
            let mut ns_per_operation: Vec<u128> = samples
                .iter()
                .map(|sample| sample.0 / u128::from(iterations))
                .collect();
            ns_per_operation.sort_unstable();
            Ok(BenchMetric {
                engine,
                operation,
                iterations_per_sample: iterations,
                warmup_iterations: warmup,
                samples: ns_per_operation.len(),
                median_ns_per_operation: (ns_per_operation[ns_per_operation.len() / 2 - 1]
                    + ns_per_operation[ns_per_operation.len() / 2])
                    / 2,
                min_ns_per_operation: ns_per_operation[0],
                max_ns_per_operation: ns_per_operation[ns_per_operation.len() - 1],
                sample_ns_per_operation: ns_per_operation,
                observable_checksum: checksum,
            })
        })
        .collect()
}

fn collect_samples(
    collected: &mut BTreeMap<(&'static str, &'static str), Vec<(u128, u64)>>,
    engine: &'static str,
    samples: Vec<BenchSample>,
) {
    for sample in samples {
        collected
            .entry((engine, sample.operation))
            .or_default()
            .push((sample.elapsed_ns, sample.checksum));
    }
}

fn verify_sample_checksums(first: &[BenchSample], second: &[BenchSample]) -> Result<(), String> {
    for left in first {
        let right = second
            .iter()
            .find(|right| right.operation == left.operation)
            .ok_or_else(|| format!("missing paired benchmark for {}", left.operation))?;
        if left.checksum != right.checksum {
            return Err(format!(
                "observable checksum mismatch for {}: {} != {}",
                left.operation, left.checksum, right.checksum
            ));
        }
    }
    Ok(())
}

fn candidate_bench_sample(iterations: u64, warmup: u64) -> Result<Vec<BenchSample>, String> {
    let mut samples = Vec::new();

    let mut conn = candidate_bench_connection()?;
    for i in 0..warmup {
        conn.execute(&format!("INSERT INTO bench VALUES(-{},-{})", i + 1, i + 1))
            .map_err(|e| e.to_string())?;
    }
    samples.push(timed_sample("point_insert", iterations, |i| {
        conn.execute(&format!("INSERT INTO bench VALUES({i},{i})"))
            .map(|changed| changed as u64)
            .map_err(|e| e.to_string())
    })?);

    let mut conn = candidate_seeded_connection(iterations + warmup)?;
    for i in iterations..iterations + warmup {
        conn.execute(&format!("UPDATE bench SET v=v+1 WHERE id={i}"))
            .map_err(|e| e.to_string())?;
    }
    samples.push(timed_sample("point_update", iterations, |i| {
        conn.execute(&format!("UPDATE bench SET v=v+1 WHERE id={i}"))
            .map(|changed| changed as u64)
            .map_err(|e| e.to_string())
    })?);

    let conn = candidate_seeded_connection(iterations.max(warmup))?;
    for i in 0..warmup {
        let result = conn
            .query(&format!("SELECT v FROM bench WHERE id={}", i % iterations))
            .map_err(|e| e.to_string())?;
        std::hint::black_box(observable_checksum(&graphite_observable(&result)));
    }
    samples.push(timed_sample("point_read", iterations, |i| {
        conn.query(&format!("SELECT v FROM bench WHERE id={i}"))
            .map(|result| observable_checksum(&graphite_observable(&result)))
            .map_err(|e| e.to_string())
    })?);

    let conn = candidate_seeded_connection(iterations)?;
    for _ in 0..warmup {
        let result = conn
            .query("SELECT id,v FROM bench ORDER BY id")
            .map_err(|e| e.to_string())?;
        std::hint::black_box(observable_checksum(&graphite_observable(&result)));
    }
    samples.push(timed_sample("ordered_scan", iterations, |_| {
        conn.query("SELECT id,v FROM bench ORDER BY id")
            .map(|result| observable_checksum(&graphite_observable(&result)))
            .map_err(|e| e.to_string())
    })?);
    Ok(samples)
}

fn reference_bench_sample(iterations: u64, warmup: u64) -> Result<Vec<BenchSample>, String> {
    let mut samples = Vec::new();

    let conn = reference_bench_connection()?;
    for i in 0..warmup {
        conn.execute(
            &format!("INSERT INTO bench VALUES(-{},-{})", i + 1, i + 1),
            [],
        )
        .map_err(|e| e.to_string())?;
    }
    samples.push(timed_sample("point_insert", iterations, |i| {
        conn.execute(&format!("INSERT INTO bench VALUES({i},{i})"), [])
            .map(|changed| changed as u64)
            .map_err(|e| e.to_string())
    })?);

    let conn = reference_seeded_connection(iterations + warmup)?;
    for i in iterations..iterations + warmup {
        conn.execute(&format!("UPDATE bench SET v=v+1 WHERE id={i}"), [])
            .map_err(|e| e.to_string())?;
    }
    samples.push(timed_sample("point_update", iterations, |i| {
        conn.execute(&format!("UPDATE bench SET v=v+1 WHERE id={i}"), [])
            .map(|changed| changed as u64)
            .map_err(|e| e.to_string())
    })?);

    let conn = reference_seeded_connection(iterations.max(warmup))?;
    for i in 0..warmup {
        let result = reference_query(
            &conn,
            &format!("SELECT v FROM bench WHERE id={}", i % iterations),
        )?;
        std::hint::black_box(observable_checksum(&result));
    }
    samples.push(timed_sample("point_read", iterations, |i| {
        reference_query(&conn, &format!("SELECT v FROM bench WHERE id={i}"))
            .map(|result| observable_checksum(&result))
    })?);

    let conn = reference_seeded_connection(iterations)?;
    for _ in 0..warmup {
        let result = reference_query(&conn, "SELECT id,v FROM bench ORDER BY id")?;
        std::hint::black_box(observable_checksum(&result));
    }
    samples.push(timed_sample("ordered_scan", iterations, |_| {
        reference_query(&conn, "SELECT id,v FROM bench ORDER BY id")
            .map(|result| observable_checksum(&result))
    })?);
    Ok(samples)
}

fn candidate_bench_connection() -> Result<GraphiteConnection, String> {
    let mut conn = GraphiteConnection::open_memory().map_err(|e| e.to_string())?;
    conn.execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, v INTEGER)")
        .map_err(|e| e.to_string())?;
    Ok(conn)
}

fn candidate_seeded_connection(rows: u64) -> Result<GraphiteConnection, String> {
    let mut conn = candidate_bench_connection()?;
    for i in 0..rows {
        conn.execute(&format!("INSERT INTO bench VALUES({i},{i})"))
            .map_err(|e| e.to_string())?;
    }
    Ok(conn)
}

fn reference_bench_connection() -> Result<ReferenceConnection, String> {
    let conn = ReferenceConnection::open_in_memory().map_err(|e| e.to_string())?;
    conn.execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, v INTEGER)", [])
        .map_err(|e| e.to_string())?;
    Ok(conn)
}

fn reference_seeded_connection(rows: u64) -> Result<ReferenceConnection, String> {
    let conn = reference_bench_connection()?;
    for i in 0..rows {
        conn.execute(&format!("INSERT INTO bench VALUES({i},{i})"), [])
            .map_err(|e| e.to_string())?;
    }
    Ok(conn)
}

fn timed_sample<F>(
    operation: &'static str,
    iterations: u64,
    mut work: F,
) -> Result<BenchSample, String>
where
    F: FnMut(u64) -> Result<u64, String>,
{
    let mut checksum = 0_u64;
    let start = Instant::now();
    for i in 0..iterations {
        checksum = checksum.wrapping_add(work(i)?);
    }
    let elapsed_ns = start.elapsed().as_nanos();
    std::hint::black_box(checksum);
    Ok(BenchSample {
        operation,
        elapsed_ns,
        checksum,
    })
}

fn observable_checksum(observable: &Observable) -> u64 {
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    for column in &observable.columns {
        checksum = checksum_bytes(checksum, column.as_bytes());
    }
    for row in &observable.rows {
        for cell in row {
            checksum = checksum_bytes(checksum, cell.storage_type.as_bytes());
            checksum = checksum_bytes(checksum, cell.value.to_string().as_bytes());
        }
    }
    checksum
}

fn checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x100_0000_01b3);
    }
    checksum
}

pub fn run_all(exe: &Path, bench_iterations: u64) -> Summary {
    let bench_iterations = bench_iterations.max(1);
    let benchmark_methodology = BenchmarkMethodology {
        samples: BENCH_SAMPLES,
        iterations_per_sample: bench_iterations,
        warmup_iterations: (bench_iterations / 10).clamp(1, 20),
        engine_order: "alternating each sample; each engine runs first three times",
        observable_work: "equal SQL shape; query results fully materialized and checksummed; write affected-row counts checksummed",
        statistic: "median of six samples (mean of middle pair) with min/max spread",
    };
    let correctness = vec![
        run_core_differential(),
        run_interoperability(),
        run_wal_reopen(),
        run_snapshot_backup(),
        run_session_cross_apply(),
        run_timeout_probe(exe),
    ];
    let correctness_passed = correctness.iter().all(|check| check.status == Status::Pass);
    let (microbenchmark, benchmark_skipped_reason) = if correctness_passed {
        match run_microbenchmark(bench_iterations) {
            Ok(metrics) => (metrics, None),
            Err(e) => (Vec::new(), Some(format!("benchmark failed: {e}"))),
        }
    } else {
        (
            Vec::new(),
            Some("one or more correctness gates did not pass".into()),
        )
    };
    let policy_and_cancellation = run_policy_capabilities();
    let hard_stop = !correctness_passed
        || benchmark_skipped_reason.is_some()
        || policy_and_cancellation
            .iter()
            .any(|check| check.status != Status::Pass);
    Summary {
        label: "NON-PRODUCTION conformance spike",
        production_ready: false,
        provenance: provenance(),
        correctness,
        policy_and_cancellation,
        benchmark_methodology,
        microbenchmark,
        benchmark_skipped_reason,
        hard_stop,
        hard_stop_criteria: "hard stop if any correctness or policy/cancellation check is not PASS; benchmark data never overrides correctness or missing controls",
    }
}

pub fn hard_stop_exit_code(hard_stop: bool, allow_hard_stop: bool) -> i32 {
    if hard_stop && !allow_hard_stop { 2 } else { 0 }
}

pub fn print_table(summary: &Summary) {
    println!("NON-PRODUCTION Rust SQLite conformance spike");
    println!("{:<42} {:<20} DETAIL", "CHECK", "STATUS");
    for check in summary
        .correctness
        .iter()
        .chain(summary.policy_and_cancellation.iter())
    {
        println!(
            "{:<42} {:<20} {}",
            check.name,
            match check.status {
                Status::Pass => "PASS",
                Status::Fail => "FAIL",
                Status::BlockedCapability => "BLOCKED_CAPABILITY",
            },
            check.detail
        );
    }
    for metric in &summary.microbenchmark {
        println!(
            "bench/{:<36} {:<20} median {} ns/op; range {}..{} ({} samples × {} iterations, {} warmup)",
            format!("{}/{}", metric.engine, metric.operation),
            "MEASURED",
            metric.median_ns_per_operation,
            metric.min_ns_per_operation,
            metric.max_ns_per_operation,
            metric.samples,
            metric.iterations_per_sample,
            metric.warmup_iterations,
        );
    }
    println!("hard_stop={}", summary.hard_stop);
}

#[cfg(test)]
mod tests {
    use super::normalize_optional_changeset_value;

    #[test]
    fn changeset_normalization_suppresses_only_omitted_values() {
        assert_eq!(
            normalize_optional_changeset_value(Err(rusqlite::Error::InvalidColumnIndex(2)))
                .unwrap(),
            None
        );
        let error = normalize_optional_changeset_value(Err(rusqlite::Error::InvalidQuery))
            .expect_err("unrelated rusqlite errors must propagate");
        assert!(error.contains("Query is not read-only"));
    }
}
