use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use rhiza_core::{
    ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, LogIndex, RecoveryAnchor,
    Snapshot, SnapshotIdentity, SnapshotManifest,
};
use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    params, params_from_iter,
    types::{ToSql, ToSqlOutput, Value, ValueRef},
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::qwal::diff_closed_databases_from_candidates;

mod control;
mod qwal;
mod qwal_vfs;

pub use control::{ControlIdentity, ControlStore, PendingApply, RequestReceipt};
pub use qwal::{
    apply_qwal_to_file, decode_qwal_v2, diff_closed_databases, encode_qwal_v2, file_digest,
    sqlite_page_size, QwalEnvelopeV2, QwalPageV2, QwalReceiptV2, MAX_QWAL_V2_BYTES,
    MAX_QWAL_V2_RECEIPTS, QWAL_V2_MAGIC,
};
pub use qwal_vfs::{
    PageRange, QwalRecordingSession, QwalVfsError, SealedQwalRecording, QWAL_RECORDING_VFS_NAME,
};

const CREATE_KV_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS __rhiza_kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;

const SQL_COMMAND_V2_MAGIC: &[u8] = b"QSQL\0\x02";
const SQL_RESULT_V1_MAGIC: &[u8] = b"QRES\0\x01";
const QWAL_SNAPSHOT_V2_MAGIC: &[u8] = b"QSNP\0\x03";
const SQL_EXECUTOR_POLICY_VERSION: &str = "rhiza-sql-qwal-batch-v2-policy-v6-quorum-authoritative";
const SQL_CONNECTION_PROFILE: &str = "qwal_batch_v2;wal_autocheckpoint=0;canonical_synchronous=OFF;control_synchronous=OFF;local_cache=rebuildable;staging_synchronous=OFF;foreign_keys=ON;trusted_schema=OFF;temp=denied;attach=denied;vtable=denied";
pub const MAX_SQL_STATEMENTS: usize = 64;
pub const MAX_SQL_PARAMETERS: usize = 999;
pub const MAX_SQL_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_RETURNING_ROWS: usize = 1_024;
pub const MAX_RETURNING_BYTES: usize = 1024 * 1024;
pub const MAX_SQL_EFFECT_BYTES: usize = 512 * 1024;
pub const DEFAULT_SQL_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const SQL_PROGRESS_HANDLER_OPS: i32 = 1_000;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QwalSnapshotV2 {
    user_db: Vec<u8>,
    replicated_control: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Null => ToSqlOutput::Owned(Value::Null),
            Self::Integer(value) => ToSqlOutput::Owned(Value::Integer(*value)),
            Self::Real(value) => ToSqlOutput::Owned(Value::Real(*value)),
            Self::Text(value) => ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes())),
            Self::Blob(value) => ToSqlOutput::Borrowed(ValueRef::Blob(value)),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStatement {
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<SqlValue>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlCommand {
    pub request_id: String,
    pub statements: Vec<SqlStatement>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStatementResult {
    pub rows_affected: u64,
    pub returning: Option<SqlQueryResult>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlCommandResult {
    pub statement_results: Vec<SqlStatementResult>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SqlCommandV2Envelope {
    executor_fingerprint: LogHash,
    command: SqlCommand,
}

#[derive(Clone, Copy, Debug)]
pub struct SqlBatchMember<'a> {
    pub command: &'a SqlCommand,
    pub request_payload: &'a [u8],
}

#[derive(Clone, Debug, PartialEq)]
pub struct SqlBatchPreparation {
    pub effect: Option<Vec<u8>>,
    pub results: Vec<Result<SqlCommandResult>>,
}

struct PreparedQwalMutation {
    receipts: Vec<QwalReceiptV2>,
    results: Vec<Result<SqlCommandResult>>,
}

pub fn sql_executor_fingerprint() -> Result<LogHash> {
    static FINGERPRINT: OnceLock<std::result::Result<LogHash, String>> = OnceLock::new();
    FINGERPRINT
        .get_or_init(compute_sql_executor_fingerprint)
        .clone()
        .map_err(Error::Sqlite)
}

fn compute_sql_executor_fingerprint() -> std::result::Result<LogHash, String> {
    let conn = Connection::open_in_memory().map_err(|error| error.to_string())?;
    let source_id: String = conn
        .query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let mut statement = conn
        .prepare("PRAGMA compile_options")
        .map_err(|error| error.to_string())?;
    let mut compile_options = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    compile_options.sort_unstable();
    let canonical = format!(
        "{SQL_EXECUTOR_POLICY_VERSION}\n{SQL_CONNECTION_PROFILE}\n{}\n{}\n{}",
        env!("CARGO_PKG_VERSION"),
        source_id,
        compile_options.join("\n")
    );
    Ok(LogHash::digest(&[canonical.as_bytes()]))
}

pub fn encode_sql_command(command: &SqlCommand) -> Result<Vec<u8>> {
    validate_sql_command(command)?;
    let encoded = serde_json::to_vec(&SqlCommandV2Envelope {
        executor_fingerprint: sql_executor_fingerprint()?,
        command: command.clone(),
    })
    .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL command: {error}")))?;
    let mut payload = Vec::with_capacity(SQL_COMMAND_V2_MAGIC.len() + encoded.len());
    payload.extend_from_slice(SQL_COMMAND_V2_MAGIC);
    payload.extend_from_slice(&encoded);
    Ok(payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyProgress {
    applied_index: LogIndex,
    applied_hash: LogHash,
}

impl ApplyProgress {
    pub const fn new(applied_index: LogIndex, applied_hash: LogHash) -> Self {
        Self {
            applied_index,
            applied_hash,
        }
    }

    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApplyOutcome {
    progress: ApplyProgress,
    sql_result: Option<SqlCommandResult>,
}

impl ApplyOutcome {
    pub const fn progress(&self) -> ApplyProgress {
        self.progress
    }

    pub const fn sql_result(&self) -> Option<&SqlCommandResult> {
        self.sql_result.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestOutcome {
    original_log_index: LogIndex,
    original_log_hash: LogHash,
}

impl RequestOutcome {
    pub const fn new(original_log_index: LogIndex, original_log_hash: LogHash) -> Self {
        Self {
            original_log_index,
            original_log_hash,
        }
    }

    pub const fn original_log_index(&self) -> LogIndex {
        self.original_log_index
    }

    pub const fn original_log_hash(&self) -> LogHash {
        self.original_log_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestConflict {
    request_id: String,
    original_outcome: RequestOutcome,
}

impl RequestConflict {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub const fn original_outcome(&self) -> RequestOutcome {
        self.original_outcome
    }
}

impl std::fmt::Display for RequestConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request id reused with different payload: {}",
            self.request_id
        )
    }
}

impl std::error::Error for RequestConflict {}

pub type Result<T> = std::result::Result<T, Error>;
pub type SqlRequestLookup = Result<Option<(RequestOutcome, Option<SqlCommandResult>)>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    ApplyFailed,
    RestoreFailed,
    Io(String),
    Sqlite(String),
    ResourceExhausted(String),
    InvalidCommand(String),
    IdentityMismatch(String),
    InvalidEntry(String),
    RequestConflict(RequestConflict),
    InvalidSnapshot(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApplyFailed => write!(f, "SQLite apply failed"),
            Self::RestoreFailed => write!(f, "SQLite restore failed"),
            Self::Io(message) => write!(f, "SQLite io failed: {message}"),
            Self::Sqlite(message) => write!(f, "SQLite error: {message}"),
            Self::ResourceExhausted(message) => write!(f, "SQLite resource exhausted: {message}"),
            Self::InvalidCommand(message) => write!(f, "invalid deterministic command: {message}"),
            Self::IdentityMismatch(field) => {
                write!(f, "SQLite database identity mismatch for {field}")
            }
            Self::InvalidEntry(message) => write!(f, "invalid log entry: {message}"),
            Self::RequestConflict(conflict) => conflict.fmt(f),
            Self::InvalidSnapshot(message) => write!(f, "invalid SQLite snapshot: {message}"),
        }
    }
}

impl std::error::Error for Error {}

pub trait StateMachine {
    fn applied_index(&self) -> Result<LogIndex>;
    fn apply(&self, entry: &LogEntry) -> Result<ApplyProgress>;
    fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot>;
}

pub struct SqliteStateMachine {
    path: PathBuf,
    conn: Mutex<Option<Connection>>,
    lifecycle: Mutex<()>,
    control: ControlStore,
    prepared_target: Mutex<Option<PreparedTarget>>,
}

struct PreparedTarget {
    artifact: NamedTempFile,
    target_seal: Option<PreparedBaseSeal>,
    base_file: File,
    base_seal: Option<PreparedBaseSeal>,
    cluster_id: String,
    node_id: String,
    epoch: u64,
    configuration_id: u64,
    recovery_generation: u64,
    materializer_fingerprint: String,
    base_index: LogIndex,
    base_hash: LogHash,
    base_db_digest: LogHash,
    base_file_bytes: u64,
    target_db_digest: LogHash,
    target_file_bytes: u64,
    effect_digest: LogHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedBaseSeal {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}

impl PreparedTarget {
    fn matches(
        &self,
        effect: &QwalEnvelopeV2,
        effect_payload: &[u8],
        identity: &ControlIdentity,
    ) -> bool {
        self.cluster_id == effect.cluster_id
            && self.cluster_id == identity.cluster_id()
            && self.node_id == identity.node_id()
            && self.epoch == effect.epoch
            && self.epoch == identity.epoch()
            && self.configuration_id == effect.configuration_id
            && self.recovery_generation == effect.recovery_generation
            && self.materializer_fingerprint == effect.materializer_fingerprint
            && self.base_index == effect.base_index
            && self.base_hash == effect.base_hash
            && self.base_db_digest == effect.base_db_digest
            && self.base_file_bytes == effect.base_file_bytes
            && self.target_db_digest == effect.target_db_digest
            && self.target_file_bytes == effect.target_file_bytes
            && self.effect_digest == LogHash::digest(&[effect_payload])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverySnapshot {
    snapshot: Snapshot,
    anchor: RecoveryAnchor,
}

impl RecoverySnapshot {
    pub const fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn db_bytes(&self) -> &[u8] {
        self.snapshot.db_bytes()
    }

    pub const fn anchor(&self) -> &RecoveryAnchor {
        &self.anchor
    }
}

impl SqliteStateMachine {
    pub fn open(
        path: impl AsRef<Path>,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        config_id: u64,
    ) -> Result<Self> {
        Self::open_with_configuration(
            path,
            cluster_id,
            node_id,
            epoch,
            ConfigurationState::active(config_id, LogHash::ZERO),
        )
    }

    pub fn open_with_configuration(
        path: impl AsRef<Path>,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: ConfigurationState,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;

        let control_path = control_sidecar_path(&path);
        match (path.exists(), control_path.exists()) {
            (false, false) => {
                Self::create_new(&path, cluster_id, node_id, epoch, configuration_state)
            }
            (true, true) => {
                let db = Self::open_existing_file(&path)?;
                db.validate_control_identity(cluster_id, node_id, epoch)?;
                Ok(db)
            }
            (true, false) => Err(Error::IdentityMismatch(
                "QWAL control sidecar is missing; install a QWAL snapshot instead of auto-migrating"
                    .into(),
            )),
            (false, true) => Err(Error::IdentityMismatch(
                "canonical SQLite database is missing beside its QWAL control sidecar".into(),
            )),
        }
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_existing_file(path.as_ref())
    }

    fn create_new(
        path: &Path,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: ConfigurationState,
    ) -> Result<Self> {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| Error::Io(err.to_string()))?;

        let control_path = control_sidecar_path(path);
        let created = (|| {
            let conn = open_connection(path)?;
            conn.execute_batch(CREATE_KV_TABLE_SQL)
                .map_err(sqlite_error)?;
            conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
                .map_err(sqlite_error)?;
            conn.close()
                .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
            let digest = file_digest(path)?;
            let identity = ControlIdentity::new(
                cluster_id,
                node_id,
                epoch,
                configuration_state,
                1,
                sql_executor_fingerprint()?,
                digest,
            );
            let control = ControlStore::create(&control_path, &identity)?;
            let conn = open_connection(path)?;
            Ok(Self {
                path: path.to_path_buf(),
                conn: Mutex::new(Some(conn)),
                lifecycle: Mutex::new(()),
                control,
                prepared_target: Mutex::new(None),
            })
        })();
        if created.is_err() {
            let _ = fs::remove_file(path);
            let _ = fs::remove_file(&control_path);
        }
        created
    }

    fn open_existing_file(path: &Path) -> Result<Self> {
        let control_path = control_sidecar_path(path);
        if !path.exists() || !control_path.exists() {
            return Err(Error::IdentityMismatch(
                "QWAL database and control sidecar must both exist".into(),
            ));
        }
        reject_legacy_user_database(path)?;
        let control = ControlStore::open_existing_unchecked(&control_path)?;
        validate_control_database_pair(path, &control)?;
        let conn = open_connection(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            conn: Mutex::new(Some(conn)),
            lifecycle: Mutex::new(()),
            control,
            prepared_target: Mutex::new(None),
        })
    }

    fn validate_control_identity(&self, cluster_id: &str, node_id: &str, epoch: u64) -> Result<()> {
        let identity = self.control.identity()?;
        if identity.cluster_id() != cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if identity.node_id() != node_id {
            return Err(Error::IdentityMismatch("node_id".into()));
        }
        if identity.epoch() != epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if identity.materializer_fingerprint() != sql_executor_fingerprint()? {
            return Err(Error::IdentityMismatch(
                "SQLite QWAL materializer fingerprint".into(),
            ));
        }
        Ok(())
    }

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?;
        let conn = guard
            .as_ref()
            .ok_or_else(|| Error::Sqlite("SQLite connection is closed".into()))?;
        operation(conn)
    }

    fn lock_lifecycle(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.lifecycle
            .lock()
            .map_err(|_| Error::Sqlite("SQLite lifecycle lock is poisoned".into()))
    }

    fn ensure_no_pending_apply(&self) -> Result<()> {
        if self.control.pending()?.is_some() {
            return Err(Error::InvalidEntry(
                "canonical SQLite state is unavailable while a QWAL apply is pending".into(),
            ));
        }
        Ok(())
    }

    fn close_connection(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?
            .take();
        if let Some(conn) = conn {
            conn.close()
                .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
        }
        Ok(())
    }

    fn reopen_connection(&self) -> Result<()> {
        let reopened = open_connection(&self.path)?;
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?;
        if guard.is_some() {
            return Err(Error::Sqlite(
                "refusing to replace an open SQLite connection".into(),
            ));
        }
        *guard = Some(reopened);
        Ok(())
    }

    pub fn apply_entry(&self, entry: &LogEntry) -> Result<ApplyProgress> {
        Ok(self.apply_entry_with_result(entry)?.progress())
    }

    pub fn apply_entry_with_result(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        let _lifecycle = self.lock_lifecycle()?;
        if entry.recompute_hash() != entry.hash {
            return Err(Error::InvalidEntry(
                "hash does not match entry contents".into(),
            ));
        }
        self.apply_qwal_entry(entry)
    }

    fn apply_qwal_entry(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        let identity = self.control.identity()?;
        if entry.cluster_id != identity.cluster_id() {
            return Err(Error::InvalidEntry(
                "cluster_id does not match sidecar".into(),
            ));
        }
        if entry.epoch != identity.epoch() {
            return Err(Error::InvalidEntry("epoch does not match sidecar".into()));
        }
        let tip = self.control.applied_tip()?;
        if entry.index == tip.applied_index() {
            if entry.hash != tip.applied_hash() {
                return Err(Error::InvalidEntry(
                    "current index was reapplied with a different hash".into(),
                ));
            }
            let sql_result = if entry.entry_type == EntryType::Command {
                let effect = decode_qwal_command(&entry.payload)?;
                if let [effect_receipt] = effect.receipts.as_slice() {
                    self.control
                        .lookup_request(&effect_receipt.request_id, effect_receipt.request_digest)?
                        .map(|receipt| decode_sql_result(receipt.result_blob()))
                        .transpose()?
                } else {
                    None
                }
            } else {
                None
            };
            return Ok(ApplyOutcome {
                progress: tip,
                sql_result,
            });
        }
        let next_index = tip
            .applied_index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index != next_index || entry.prev_hash != tip.applied_hash() {
            return Err(Error::InvalidEntry(
                "entry does not extend the QWAL applied tip".into(),
            ));
        }
        let current_configuration = self.control.configuration_state()?;
        let next_configuration = current_configuration
            .validate_entry(entry)
            .map_err(|error| Error::InvalidEntry(error.to_string()))?;
        let base_anchor = LogAnchor::new(tip.applied_index(), tip.applied_hash());
        let entry_anchor = LogAnchor::new(entry.index, entry.hash);

        if entry.entry_type != EntryType::Command {
            self.discard_prepared_target()?;
            match entry.entry_type {
                EntryType::Noop if entry.payload.is_empty() => {}
                EntryType::ConfigChange => {}
                EntryType::Noop => {
                    return Err(Error::InvalidEntry("Noop payload must be empty".into()))
                }
                _ => {
                    return Err(Error::InvalidEntry(format!(
                        "entry type {:?} is unsupported by QWAL",
                        entry.entry_type
                    )))
                }
            }
            let digest = self.control.user_db_digest()?;
            if entry.entry_type == EntryType::Noop && next_configuration == current_configuration {
                self.control.commit_metadata_only_entry_with_log(
                    base_anchor,
                    entry,
                    &current_configuration,
                    digest,
                )?;
            } else {
                let bytes = fs::metadata(&self.path).map_err(io_error)?.len();
                let pending = PendingApply::new(base_anchor, entry_anchor, digest, digest, bytes);
                self.control.begin_pending_with_entry(&pending, entry)?;
                self.control
                    .commit_applied(&pending, &next_configuration, &[])?;
            }
            return Ok(ApplyOutcome {
                progress: ApplyProgress::new(entry.index, entry.hash),
                sql_result: None,
            });
        }

        let effect = match decode_qwal_command(&entry.payload) {
            Ok(effect) => effect,
            Err(error) => {
                self.discard_prepared_target()?;
                return Err(error);
            }
        };
        self.discard_prepared_target_unless(&effect, &entry.payload, &identity)?;
        validate_qwal_identity(&effect, &identity, &current_configuration)?;
        if effect.base_index != tip.applied_index() || effect.base_hash != tip.applied_hash() {
            return Err(Error::InvalidEntry(
                "QWAL effect base does not match the applied tip".into(),
            ));
        }
        let mut results = Vec::with_capacity(effect.receipts.len());
        let mut receipts = Vec::with_capacity(effect.receipts.len());
        for effect_receipt in &effect.receipts {
            let result = decode_sql_result(&effect_receipt.result_blob)?;
            if encode_sql_result(&result)? != effect_receipt.result_blob {
                return Err(Error::InvalidEntry(
                    "QWAL result is not canonically encoded".into(),
                ));
            }
            results.push(result);
            receipts.push(RequestReceipt::new(
                effect_receipt.request_id.clone(),
                effect_receipt.request_digest,
                entry_anchor,
                effect_receipt.result_blob.clone(),
            ));
        }
        let lookup_keys = effect
            .receipts
            .iter()
            .map(|receipt| (receipt.request_id.as_str(), receipt.request_digest))
            .collect::<Vec<_>>();
        let existing = self.control.lookup_requests(&lookup_keys)?;
        for (expected, existing) in receipts.iter().zip(existing) {
            match existing? {
                None => {}
                Some(existing) if existing == *expected => {}
                Some(_) => {
                    return Err(Error::InvalidEntry(
                        "QWAL request receipt already belongs to another entry or result".into(),
                    ));
                }
            }
        }
        let pending = PendingApply::new(
            base_anchor,
            entry_anchor,
            effect.base_db_digest,
            effect.target_db_digest,
            effect.target_file_bytes,
        );
        self.install_qwal_effect(&effect, &entry.payload)?;
        self.control
            .commit_rebuildable_apply(&pending, entry, &next_configuration, &receipts)?;
        Ok(ApplyOutcome {
            progress: ApplyProgress::new(entry.index, entry.hash),
            sql_result: if results.len() == 1 {
                results.pop()
            } else {
                None
            },
        })
    }

    fn install_qwal_effect(&self, effect: &QwalEnvelopeV2, effect_payload: &[u8]) -> Result<()> {
        #[cfg(test)]
        begin_prepared_base_reuse_audit(&self.path);
        if let Some(prepared) = self.take_matching_prepared_target(effect, effect_payload)? {
            self.close_connection()?;
            match self.promote_prepared_target(&prepared, effect) {
                Ok(true) => {
                    #[cfg(test)]
                    note_prepared_install(&self.path, PreparedInstallPath::Promoted);
                    return self.reopen_connection();
                }
                Ok(false) => self.reopen_connection()?,
                Err(error) => {
                    let _ = self.reopen_connection();
                    return Err(error);
                }
            }
        }
        #[cfg(test)]
        note_second_checkpoint(&self.path);
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let install = (|| {
            #[cfg(test)]
            note_second_base_full_hash(&self.path);
            let current_digest = file_digest(&self.path)?;
            if current_digest == effect.target_db_digest {
                self.discard_prepared_target()?;
                return Ok(());
            }
            if current_digest != effect.base_db_digest {
                self.discard_prepared_target()?;
                return Err(Error::InvalidEntry(
                    "canonical SQLite digest matches neither QWAL base nor target".into(),
                ));
            }
            if fs::metadata(&self.path).map_err(io_error)?.len() != effect.base_file_bytes {
                self.discard_prepared_target()?;
                return Err(Error::InvalidEntry(
                    "canonical SQLite size does not match the QWAL base".into(),
                ));
            }
            let temp_dir = tempfile::tempdir_in(parent_dir(&self.path)).map_err(io_error)?;
            let temp_path = temp_dir.path().join("target.sqlite");
            (|| {
                apply_qwal_to_file(&self.path, &temp_path, effect)?;
                let verify = Connection::open_with_flags(
                    &temp_path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(sqlite_error)?;
                integrity_check(&verify)?;
                verify
                    .close()
                    .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
                fs::rename(&temp_path, &self.path).map_err(io_error)?;
                #[cfg(test)]
                note_prepared_install(&self.path, PreparedInstallPath::Rebuilt);
                Ok(())
            })()
        })();
        let reopen = self.reopen_connection();
        match (install, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn discard_prepared_target(&self) -> Result<()> {
        self.prepared_target
            .lock()
            .map_err(|_| Error::Sqlite("prepared SQLite target lock is poisoned".into()))?
            .take();
        Ok(())
    }

    fn discard_prepared_target_unless(
        &self,
        effect: &QwalEnvelopeV2,
        effect_payload: &[u8],
        identity: &ControlIdentity,
    ) -> Result<()> {
        let mut prepared = self
            .prepared_target
            .lock()
            .map_err(|_| Error::Sqlite("prepared SQLite target lock is poisoned".into()))?;
        if prepared
            .as_ref()
            .is_some_and(|prepared| !prepared.matches(effect, effect_payload, identity))
        {
            prepared.take();
        }
        Ok(())
    }

    fn take_matching_prepared_target(
        &self,
        effect: &QwalEnvelopeV2,
        effect_payload: &[u8],
    ) -> Result<Option<PreparedTarget>> {
        let identity = self.control.identity()?;
        let mut prepared = self
            .prepared_target
            .lock()
            .map_err(|_| Error::Sqlite("prepared SQLite target lock is poisoned".into()))?;
        Ok(prepared
            .take()
            .filter(|prepared| prepared.matches(effect, effect_payload, &identity)))
    }

    fn promote_prepared_target(
        &self,
        prepared: &PreparedTarget,
        effect: &QwalEnvelopeV2,
    ) -> Result<bool> {
        if !prepared_base_still_sealed(&self.path, prepared)?
            || !sqlite_sidecars_absent(prepared.artifact.path())?
        {
            return Ok(false);
        }
        let owned_metadata = prepared.artifact.as_file().metadata().map_err(io_error)?;
        let Some(named_metadata) = symlink_metadata_if_exists(prepared.artifact.path())? else {
            return Ok(false);
        };
        if !prepared_base_metadata_matches(
            prepared.target_seal.as_ref(),
            &owned_metadata,
            &named_metadata,
        ) {
            return Ok(false);
        }
        if owned_metadata.len() != effect.target_file_bytes {
            return Ok(false);
        }
        let Some(rename_metadata) = symlink_metadata_if_exists(prepared.artifact.path())? else {
            return Ok(false);
        };
        if !prepared_base_metadata_matches(
            prepared.target_seal.as_ref(),
            &owned_metadata,
            &rename_metadata,
        ) || !prepared_base_still_sealed(&self.path, prepared)?
        {
            return Ok(false);
        }
        // The lifecycle lock excludes in-process renames. std has no portable
        // rename-by-handle primitive, so an external actor with write access to
        // this private directory could still race this final lstat and rename.
        if let Err(error) = fs::rename(prepared.artifact.path(), &self.path) {
            if error.kind() == std::io::ErrorKind::NotFound {
                return Ok(false);
            }
            return Err(io_error(error));
        }
        Ok(true)
    }

    pub fn get_value(&self, key: &str) -> Result<Option<String>> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        self.with_connection(|conn| {
            conn.query_row(
                "SELECT value FROM __rhiza_kv WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)
        })
    }

    pub fn query_sql(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SqlQueryResult> {
        self.query_sql_with_timeout(query, max_rows, max_bytes, DEFAULT_SQL_QUERY_TIMEOUT)
    }

    pub fn query_sql_with_timeout(
        &self,
        query: &SqlStatement,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<SqlQueryResult> {
        let _lifecycle = self.lock_lifecycle()?;
        validate_sql_statement(query)?;
        self.ensure_no_pending_apply()?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(Error::InvalidCommand(
                "SQL query limits must be positive".into(),
            ));
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        self.with_connection(|conn| {
            conn.progress_handler(
                SQL_PROGRESS_HANDLER_OPS,
                Some(move || Instant::now() >= deadline),
            )
            .map_err(sqlite_error)?;
            let result = with_sql_authorizer(conn, SqlAuthorizationMode::ReadOnly, || {
                let mut statement = conn.prepare(&query.sql).map_err(sql_query_error)?;
                if !statement.readonly() {
                    return Err(Error::InvalidCommand("SQL query must be read-only".into()));
                }
                let columns = statement
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let column_count = columns.len();
                let mut rows = statement
                    .query(params_from_iter(query.parameters.iter()))
                    .map_err(sql_query_error)?;
                let mut result_rows = Vec::new();
                let mut result_bytes = columns.iter().map(String::len).sum::<usize>();
                while let Some(row) = rows.next().map_err(sql_query_error)? {
                    if result_rows.len() == max_rows {
                        return Err(Error::InvalidCommand(format!(
                            "SQL query exceeds {max_rows} rows"
                        )));
                    }
                    let mut values = Vec::with_capacity(column_count);
                    for column in 0..column_count {
                        let value = sql_value(row.get_ref(column).map_err(sql_query_error)?)?;
                        result_bytes = result_bytes
                            .checked_add(sql_value_size(&value))
                            .ok_or_else(|| {
                                Error::InvalidCommand("SQL result size overflow".into())
                            })?;
                        if result_bytes > max_bytes {
                            return Err(Error::InvalidCommand(format!(
                                "SQL query exceeds {max_bytes} result bytes"
                            )));
                        }
                        values.push(value);
                    }
                    result_rows.push(values);
                }
                Ok(SqlQueryResult {
                    columns,
                    rows: result_rows,
                })
            });
            let clear_result = conn
                .progress_handler(0, None::<fn() -> bool>)
                .map_err(sqlite_error);
            match (result, clear_result) {
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
                (Ok(result), Ok(())) => Ok(result),
            }
        })
    }

    pub fn validate_sql_write(&self, command: &SqlCommand) -> Result<()> {
        let request = encode_sql_command(command)?;
        let tip = self.control.applied_tip()?;
        let preparation = self.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: &request,
            }],
            tip.applied_index(),
            tip.applied_hash(),
        )?;
        preparation
            .results
            .into_iter()
            .next()
            .expect("one-member batch returns one result")
            .map(|_| ())
    }

    /// Prepares one physical QWAL v2 effect for the successful subset of an
    /// ordered SQL batch.
    ///
    /// Each member runs inside its own savepoint nested under one outer SQLite
    /// transaction. A failed member is rolled back and reported at the same
    /// input position; later members still observe all prior successful
    /// members. Every successful member is bound into the effect as an ordered
    /// receipt template and is committed at the effect's single log anchor.
    pub fn prepare_sql_batch_effect(
        &self,
        members: &[SqlBatchMember<'_>],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<SqlBatchPreparation> {
        if members.is_empty() || members.len() > MAX_QWAL_V2_RECEIPTS {
            return Err(Error::InvalidCommand(format!(
                "SQL batch must contain 1..={MAX_QWAL_V2_RECEIPTS} members"
            )));
        }
        self.prepare_qwal_effect(base_index, base_hash, |staging| {
            let mut preflight = std::iter::repeat_with(|| None)
                .take(members.len())
                .collect::<Vec<Option<Result<Option<RequestReceipt>>>>>();
            let mut lookup_members: Vec<usize> = Vec::with_capacity(members.len());
            let mut lookup_keys = Vec::with_capacity(members.len());
            let mut seen_request_ids = HashSet::with_capacity(members.len());
            for (index, member) in members.iter().enumerate() {
                let request_digest = LogHash::digest(&[member.request_payload]);
                let validation = validate_sql_command(member.command).and_then(|()| {
                    if decode_sql_command(member.request_payload)? != *member.command {
                        return Err(Error::InvalidCommand(
                            "SQL batch member is not the canonical QSQL v2 command".into(),
                        ));
                    }
                    if !seen_request_ids.insert(member.command.request_id.as_str()) {
                        return Err(Error::InvalidCommand(
                            "SQL batch member repeats a request_id".into(),
                        ));
                    }
                    Ok(())
                });
                if let Err(error) = validation {
                    preflight[index] = Some(Err(error));
                    continue;
                }
                lookup_members.push(index);
                lookup_keys.push((member.command.request_id.as_str(), request_digest));
            }
            for (member_index, lookup) in lookup_members
                .iter()
                .copied()
                .zip(self.control.lookup_requests(&lookup_keys)?)
            {
                preflight[member_index] = Some(lookup);
            }

            let mut tx = Transaction::new_unchecked(staging, TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            let mut receipts = Vec::with_capacity(members.len());
            let mut results = Vec::with_capacity(members.len());
            for (member, preflight) in members.iter().zip(preflight) {
                let request_digest = LogHash::digest(&[member.request_payload]);
                match preflight.expect("every SQL batch member has one preflight result") {
                    Err(error) => {
                        results.push(Err(error));
                        continue;
                    }
                    Ok(Some(_)) => {
                        results.push(Err(Error::InvalidCommand(
                            "request was already materialized; return its stored receipt".into(),
                        )));
                        continue;
                    }
                    Ok(None) => {}
                }

                let savepoint = tx.savepoint().map_err(sqlite_error)?;
                match execute_sql_statements(&savepoint, &member.command.statements)
                    .and_then(|result| encode_sql_result(&result).map(|blob| (result, blob)))
                {
                    Ok((result, result_blob)) => {
                        savepoint.commit().map_err(sqlite_error)?;
                        receipts.push(QwalReceiptV2 {
                            request_id: member.command.request_id.clone(),
                            request_digest,
                            result_blob,
                        });
                        results.push(Ok(result));
                    }
                    Err(error) => {
                        savepoint.finish().map_err(sqlite_error)?;
                        results.push(Err(error));
                    }
                }
            }
            if receipts.is_empty() {
                tx.rollback().map_err(sqlite_error)?;
            } else {
                tx.commit().map_err(sqlite_error)?;
            }
            Ok(PreparedQwalMutation { receipts, results })
        })
    }

    pub fn prepare_put_effect(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>> {
        let canonical_request = encode_put_request(request_id, key, value)?;
        if request_payload != canonical_request {
            return Err(Error::InvalidCommand(
                "put effect request is not the canonical put command".into(),
            ));
        }
        let preparation = self.prepare_qwal_effect(base_index, base_hash, |staging| {
            let request_digest = LogHash::digest(&[request_payload]);
            if self
                .control
                .lookup_request(request_id, request_digest)?
                .is_some()
            {
                return Err(Error::InvalidCommand(
                    "request was already materialized; return its stored receipt".into(),
                ));
            }
            let tx = Transaction::new_unchecked(staging, TransactionBehavior::Immediate)
                .map_err(sqlite_error)?;
            tx.execute(
                "INSERT INTO __rhiza_kv(key, value) VALUES (?1, ?2)\n                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            let result = SqlCommandResult {
                statement_results: Vec::new(),
            };
            Ok(PreparedQwalMutation {
                receipts: vec![QwalReceiptV2 {
                    request_id: request_id.to_owned(),
                    request_digest,
                    result_blob: encode_sql_result(&result)?,
                }],
                results: vec![Ok(result)],
            })
        })?;
        preparation.effect.ok_or_else(|| {
            Error::InvalidCommand("put effect unexpectedly produced no successful member".into())
        })
    }

    fn prepare_qwal_effect(
        &self,
        base_index: LogIndex,
        base_hash: LogHash,
        mutation: impl FnOnce(&mut Connection) -> Result<PreparedQwalMutation>,
    ) -> Result<SqlBatchPreparation> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        let tip = self.control.applied_tip()?;
        if tip != ApplyProgress::new(base_index, base_hash) {
            return Err(Error::InvalidEntry(
                "QWAL effect base does not match the materialized SQLite tip".into(),
            ));
        }
        let identity = self.control.identity()?;

        let prepare_result = (|| {
            self.close_connection()?;

            let (base_file, actual_base_digest, base_seal) = open_hashed_prepared_base(&self.path)?;
            if actual_base_digest != identity.user_db_digest() {
                return Err(Error::InvalidEntry(
                    "closed SQLite base digest does not match the control sidecar".into(),
                ));
            }
            let base_file_bytes = fs::metadata(&self.path).map_err(io_error)?.len();
            let staging_artifact = clone_or_copy_to_temp(&self.path)?;
            let staging_path = staging_artifact.path();
            let copied = fs::metadata(staging_path).map_err(io_error)?.len();
            if copied != base_file_bytes {
                return Err(Error::Io(
                    "speculative SQLite clone did not reproduce the closed base size".into(),
                ));
            }
            #[cfg(test)]
            note_speculative_copy(&self.path);
            let page_size = sqlite_page_size(&self.path)?;
            let mut recording = QwalRecordingSession::begin(staging_path, page_size).ok();
            let mut staging = if recording.is_some() {
                match open_connection_with_vfs(staging_path, Some(QWAL_RECORDING_VFS_NAME)) {
                    Ok(connection) => connection,
                    Err(_) => {
                        recording = None;
                        open_connection(staging_path)?
                    }
                }
            } else {
                open_connection(staging_path)?
            };
            staging
                .pragma_update(None, "synchronous", "OFF")
                .map_err(sqlite_error)?;
            #[cfg(test)]
            note_speculative_synchronous(&self.path, &staging)?;
            let mutation = mutation(&mut staging)?;
            if mutation.receipts.is_empty() {
                staging
                    .close()
                    .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
                return Ok((None, mutation.results, None, None, None));
            }
            if let Some(recording) = &recording {
                let _ = recording.mark_commit_observed();
            }
            checkpoint_truncate(&staging)?;
            if let Some(recording) = &recording {
                let _ = recording.mark_checkpoint_succeeded();
            }
            staging
                .close()
                .map_err(|(_, error)| Error::Sqlite(error.to_string()))?;
            let recording = recording.and_then(|recording| recording.seal().ok());
            let (pages, target_db_digest, _recorder_covers_diff) = if let Some(recording) =
                recording
                    .as_ref()
                    .filter(|recording| recording.is_complete())
            {
                let (pages, target_db_digest) = diff_closed_databases_from_candidates(
                    &self.path,
                    staging_path,
                    &recording.candidate_pages,
                )?;
                (pages, target_db_digest, true)
            } else {
                let (pages, target_db_digest) = diff_closed_databases(&self.path, staging_path)?;
                (pages, target_db_digest, false)
            };
            #[cfg(test)]
            if _recorder_covers_diff {
                note_recorder_audit(&self.path);
            }

            let effect = QwalEnvelopeV2 {
                cluster_id: identity.cluster_id().to_owned(),
                epoch: identity.epoch(),
                configuration_id: identity.configuration_state().config_id(),
                recovery_generation: identity.recovery_generation(),
                base_index,
                base_hash,
                base_db_digest: actual_base_digest,
                base_file_bytes,
                target_db_digest,
                target_file_bytes: fs::metadata(staging_path).map_err(io_error)?.len(),
                materializer_fingerprint: identity.materializer_fingerprint().to_hex(),
                page_size,
                receipts: mutation.receipts,
                pages,
            };
            let encoded = encode_qwal_v2(&effect)?;
            Ok((
                Some(encoded),
                mutation.results,
                Some(effect),
                Some((base_file, base_seal)),
                Some(staging_artifact),
            ))
        })();

        if self
            .conn
            .lock()
            .map_err(|_| Error::Sqlite("SQLite connection lock is poisoned".into()))?
            .is_none()
        {
            let reopen_result = self.reopen_connection();
            if prepare_result.is_ok() {
                reopen_result?;
            }
        }
        let (encoded, results, effect, prepared_base, staging_artifact) = prepare_result?;
        let (Some(encoded), Some(effect)) = (encoded, effect) else {
            self.discard_prepared_target()?;
            return Ok(SqlBatchPreparation {
                effect: None,
                results,
            });
        };
        let (base_file, base_seal) =
            prepared_base.expect("a prepared QWAL effect retains its hashed canonical base");
        let staging_artifact =
            staging_artifact.expect("a prepared QWAL effect retains its speculative target");
        let target_owned = staging_artifact.as_file().metadata().map_err(io_error)?;
        let target_named = fs::symlink_metadata(staging_artifact.path()).map_err(io_error)?;
        let target_seal = prepared_base_seal(&target_owned, &target_named)?;
        let prepared = PreparedTarget {
            artifact: staging_artifact,
            target_seal,
            base_file,
            base_seal,
            cluster_id: effect.cluster_id.clone(),
            node_id: identity.node_id().to_owned(),
            epoch: effect.epoch,
            configuration_id: effect.configuration_id,
            recovery_generation: effect.recovery_generation,
            materializer_fingerprint: effect.materializer_fingerprint.clone(),
            base_index: effect.base_index,
            base_hash: effect.base_hash,
            base_db_digest: effect.base_db_digest,
            base_file_bytes: effect.base_file_bytes,
            target_db_digest: effect.target_db_digest,
            target_file_bytes: effect.target_file_bytes,
            effect_digest: LogHash::digest(&[&encoded]),
        };
        *self
            .prepared_target
            .lock()
            .map_err(|_| Error::Sqlite("prepared SQLite target lock is poisoned".into()))? =
            Some(prepared);
        Ok(SqlBatchPreparation {
            effect: Some(encoded),
            results,
        })
    }

    pub fn check_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<RequestOutcome>> {
        self.ensure_no_pending_apply()?;
        let Some(receipt) = self
            .control
            .lookup_request(request_id, LogHash::digest(&[command_payload]))?
        else {
            return Ok(None);
        };
        Ok(Some(RequestOutcome::new(
            receipt.original_anchor().index(),
            receipt.original_anchor().hash(),
        )))
    }

    pub fn connection_pragmas(&self) -> Result<(String, i64)> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        self.with_connection(|conn| {
            let journal_mode = conn
                .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
                .map_err(sqlite_error)?;
            let synchronous = conn
                .query_row("PRAGMA synchronous;", [], |row| row.get(0))
                .map_err(sqlite_error)?;
            Ok((journal_mode, synchronous))
        })
    }

    pub fn check_sql_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<(RequestOutcome, Option<SqlCommandResult>)>> {
        self.check_sql_requests(&[(request_id, command_payload)])?
            .pop()
            .expect("one SQL request produces one aligned lookup")
    }

    /// Checks unique SQL request ids with one bounded control-sidecar query.
    ///
    /// Results preserve input order. A missing receipt is `Ok(None)`, an exact
    /// receipt is decoded into its original outcome and result, and a digest
    /// conflict or invalid payload is returned in that member's aligned slot.
    /// Duplicate request ids are rejected before querying; callers that accept
    /// aliases must deduplicate by id and fan the aligned result back out.
    pub fn check_sql_requests(&self, requests: &[(&str, &[u8])]) -> Result<Vec<SqlRequestLookup>> {
        self.ensure_no_pending_apply()?;
        let mut validations = Vec::with_capacity(requests.len());
        let mut lookup_keys = Vec::with_capacity(requests.len());
        for (request_id, command_payload) in requests {
            let validation = decode_sql_command(command_payload).and_then(|command| {
                if command.request_id != *request_id {
                    return Err(Error::InvalidCommand(
                        "SQL payload request_id does not match lookup request_id".into(),
                    ));
                }
                Ok(())
            });
            validations.push(validation);
            lookup_keys.push((*request_id, LogHash::digest(&[command_payload])));
        }
        let receipts = self.control.lookup_requests(&lookup_keys)?;
        let mut aligned = Vec::with_capacity(requests.len());
        for (validation, receipt) in validations.into_iter().zip(receipts) {
            let checked = match validation {
                Err(error) => Err(error),
                Ok(()) => match receipt {
                    Err(error) => Err(error),
                    Ok(None) => Ok(None),
                    Ok(Some(receipt)) => decode_sql_result(receipt.result_blob()).map(|result| {
                        Some((
                            RequestOutcome::new(
                                receipt.original_anchor().index(),
                                receipt.original_anchor().hash(),
                            ),
                            Some(result),
                        ))
                    }),
                },
            };
            aligned.push(checked);
        }
        Ok(aligned)
    }

    pub fn applied_index_value(&self) -> Result<LogIndex> {
        Ok(self.control.applied_tip()?.applied_index())
    }

    pub fn applied_hash_value(&self) -> Result<LogHash> {
        Ok(self.control.applied_tip()?.applied_hash())
    }

    /// Returns the applied index and hash observed by one control-store snapshot.
    pub fn applied_tip(&self) -> Result<ApplyProgress> {
        self.control.applied_tip()
    }

    pub fn applied_tip_value(&self) -> Result<(LogIndex, LogHash)> {
        let tip = self.applied_tip()?;
        Ok((tip.applied_index(), tip.applied_hash()))
    }

    pub fn configuration_state_value(&self) -> Result<ConfigurationState> {
        self.control.configuration_state()
    }

    pub fn embedded_log_entries(
        &self,
        from_index: LogIndex,
        through_index: LogIndex,
    ) -> Result<Vec<LogEntry>> {
        self.control.embedded_log_entries(from_index, through_index)
    }

    pub fn compact_embedded_log_before(&self, anchor_index: LogIndex) -> Result<()> {
        self.control.compact_embedded_log_before(anchor_index)
    }

    pub fn canonical_db_digest(&self) -> Result<LogHash> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let digest = file_digest(&self.path);
        let reopen = self.reopen_connection();
        match (digest, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(digest), Ok(())) => Ok(digest),
        }
    }

    pub fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot> {
        let _lifecycle = self.lock_lifecycle()?;
        self.ensure_no_pending_apply()?;
        let tip = self.control.applied_tip()?;
        if tip.applied_index() != target {
            return Err(Error::InvalidSnapshot(format!(
                "snapshot target {target} does not match applied index {}",
                tip.applied_index()
            )));
        }
        let identity = self.control.identity()?;
        let manifest = SnapshotManifest::new_with_configuration(
            identity.cluster_id(),
            identity.configuration_state().clone(),
            identity.epoch(),
            target,
            tip.applied_hash(),
            1,
            identity.node_id(),
        )
        .with_executor_fingerprint(sql_executor_fingerprint()?);
        self.with_connection(checkpoint_truncate)?;
        self.close_connection()?;
        let snapshot = (|| {
            let user_db = fs::read(&self.path).map_err(io_error)?;
            if LogHash::digest(&[&user_db]) != identity.user_db_digest() {
                return Err(Error::InvalidSnapshot(
                    "canonical database digest does not match control sidecar".into(),
                ));
            }
            let container = QwalSnapshotV2 {
                user_db,
                replicated_control: self.control.export_replicated_snapshot()?,
            };
            encode_qwal_snapshot(&container).map(|bytes| Snapshot::new(manifest, bytes))
        })();
        let reopen = self.reopen_connection();
        match (snapshot, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(snapshot), Ok(())) => Ok(snapshot),
        }
    }

    pub fn create_recovery_snapshot(&self, recovery_generation: u64) -> Result<RecoverySnapshot> {
        if recovery_generation == 0 {
            return Err(Error::InvalidSnapshot(
                "recovery_generation must be positive".into(),
            ));
        }
        let target = self.applied_index_value()?;
        if target == 0 {
            return Err(Error::InvalidSnapshot(
                "recovery snapshot requires an applied entry".into(),
            ));
        }
        let snapshot = self.create_snapshot(target)?;
        let manifest = snapshot.manifest();
        let size_bytes = u64::try_from(snapshot.db_bytes().len())
            .map_err(|_| Error::InvalidSnapshot("snapshot size exceeds u64".into()))?;
        let anchor = RecoveryAnchor::new_with_configuration(
            manifest.cluster_id(),
            manifest.epoch(),
            manifest.configuration_state().clone(),
            recovery_generation,
            LogAnchor::new(manifest.index(), manifest.applied_hash()),
            SnapshotIdentity::new(
                manifest.snapshot_id(),
                LogHash::digest(&[snapshot.db_bytes()]),
                size_bytes,
            )
            .with_executor_fingerprint(
                manifest
                    .executor_fingerprint()
                    .expect("new snapshots always bind the executor fingerprint"),
            ),
        );
        Ok(RecoverySnapshot { snapshot, anchor })
    }
}

pub fn encode_put_request(request_id: &str, key: &str, value: &str) -> Result<Vec<u8>> {
    if request_id.is_empty() || request_id.len() > 256 || key.is_empty() {
        return Err(Error::InvalidCommand(
            "put request_id and key must be non-empty and request_id at most 256 bytes".into(),
        ));
    }
    if [request_id, key, value]
        .iter()
        .any(|field| field.as_bytes().contains(&b'\t'))
    {
        return Err(Error::InvalidCommand(
            "put request fields must not contain a tab".into(),
        ));
    }
    Ok(format!("put\t{request_id}\t{key}\t{value}").into_bytes())
}

impl StateMachine for SqliteStateMachine {
    fn applied_index(&self) -> Result<LogIndex> {
        self.applied_index_value()
    }

    fn apply(&self, entry: &LogEntry) -> Result<ApplyProgress> {
        self.apply_entry(entry)
    }

    fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot> {
        self.create_snapshot(target)
    }
}

pub fn restore_snapshot_file(
    path: impl AsRef<Path>,
    snapshot: &Snapshot,
    target_node_id: &str,
) -> Result<()> {
    restore_snapshot_file_with_recovery_generation(path, snapshot, target_node_id, None)
}

fn restore_snapshot_file_with_recovery_generation(
    path: impl AsRef<Path>,
    snapshot: &Snapshot,
    target_node_id: &str,
    recovery_generation: Option<u64>,
) -> Result<()> {
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node_id is empty".into()));
    }
    let path = path.as_ref();
    ensure_parent(path)?;
    let parent = parent_dir(path);
    let control_path = control_sidecar_path(path);
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    let mut shm_path = path.as_os_str().to_os_string();
    shm_path.push("-shm");
    if path.exists()
        || control_path.exists()
        || Path::new(&wal_path).exists()
        || Path::new(&shm_path).exists()
    {
        return Err(Error::InvalidSnapshot(
            "QWAL restore destination and SQLite sidecars must not exist".into(),
        ));
    }
    let container = decode_qwal_snapshot(snapshot.db_bytes())?;
    if snapshot.manifest().executor_fingerprint() != Some(sql_executor_fingerprint()?) {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot materializer fingerprint does not match local SQLite".into(),
        ));
    }
    let mut restore_file = NamedTempFile::new_in(parent).map_err(io_error)?;
    restore_file
        .write_all(&container.user_db)
        .map_err(io_error)?;
    restore_file.as_file().sync_all().map_err(io_error)?;

    {
        let restore_conn = Connection::open_with_flags(
            restore_file.path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        integrity_check(&restore_conn)?;
    }

    let digest = LogHash::digest(&[&container.user_db]);
    let control_temp_dir = tempfile::tempdir_in(parent).map_err(io_error)?;
    let control_temp_path = control_temp_dir.path().join("control.sqlite");
    let control_identity = ControlIdentity::new(
        snapshot.manifest().cluster_id(),
        target_node_id,
        snapshot.manifest().epoch(),
        snapshot.manifest().configuration_state().clone(),
        1,
        sql_executor_fingerprint()?,
        digest,
    );
    let control = ControlStore::create(&control_temp_path, &control_identity)?;
    control.import_replicated_snapshot_with_recovery_generation(
        &container.replicated_control,
        recovery_generation,
    )?;
    let imported_tip = control.applied_tip()?;
    if imported_tip.applied_index() != snapshot.manifest().index()
        || imported_tip.applied_hash() != snapshot.manifest().applied_hash()
        || control.configuration_state()? != *snapshot.manifest().configuration_state()
        || control.user_db_digest()? != digest
        || recovery_generation
            .is_some_and(|expected| control.recovery_generation().ok() != Some(expected))
    {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot manifest does not match replicated control state".into(),
        ));
    }
    drop(control);
    File::open(&control_temp_path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)?;

    let restored = restore_file
        .persist_noclobber(path)
        .map_err(|err| Error::Io(err.error.to_string()))?;
    restored.sync_all().map_err(io_error)?;
    if let Err(error) = fs::hard_link(&control_temp_path, &control_path) {
        drop(restored);
        let cleanup = fs::remove_file(path);
        let synced = sync_parent(parent);
        if let Err(cleanup_error) = cleanup {
            return Err(Error::Io(format!(
                "control publish failed ({error}); database cleanup failed ({cleanup_error})"
            )));
        }
        synced?;
        return Err(io_error(error));
    }
    sync_parent(parent)
}

pub fn restore_recovery_snapshot_file(
    path: impl AsRef<Path>,
    db_bytes: &[u8],
    anchor: &RecoveryAnchor,
    target_node_id: &str,
) -> Result<()> {
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node_id is empty".into()));
    }
    if anchor.snapshot().size_bytes() != db_bytes.len() as u64
        || anchor.snapshot().digest() != LogHash::digest(&[db_bytes])
    {
        return Err(Error::InvalidSnapshot(
            "recovery anchor does not match snapshot bytes".into(),
        ));
    }
    if let Some(fingerprint) = anchor.executor_fingerprint() {
        let expected = sql_executor_fingerprint()?;
        if fingerprint != expected {
            return Err(Error::InvalidSnapshot(format!(
                "recovery snapshot executor fingerprint {} does not match local {}",
                fingerprint.to_hex(),
                expected.to_hex()
            )));
        }
    } else {
        return Err(Error::InvalidSnapshot(
            "QWAL recovery snapshot is missing a materializer fingerprint".into(),
        ));
    }
    let manifest = SnapshotManifest::new_with_configuration(
        anchor.cluster_id(),
        anchor.configuration_state().clone(),
        anchor.epoch(),
        anchor.compacted().index(),
        anchor.compacted().hash(),
        1,
        target_node_id,
    )
    .with_executor_fingerprint(sql_executor_fingerprint()?);
    if manifest.snapshot_id() != anchor.snapshot().snapshot_id() {
        return Err(Error::InvalidSnapshot(
            "recovery snapshot id does not match compacted index".into(),
        ));
    }
    restore_snapshot_file_with_recovery_generation(
        path,
        &Snapshot::new(manifest, db_bytes.to_vec()),
        target_node_id,
        Some(anchor.recovery_generation()),
    )
}

fn open_connection(path: &Path) -> Result<Connection> {
    open_connection_with_vfs(path, None)
}

fn open_connection_with_vfs(path: &Path, vfs: Option<&str>) -> Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = match vfs {
        Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs),
        None => Connection::open_with_flags(path, flags),
    }
    .map_err(sqlite_error)?;
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(Error::Sqlite(format!(
            "SQLite refused WAL journal mode: {journal_mode}"
        )));
    }
    conn.pragma_update(None, "synchronous", "OFF")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "trusted_schema", "OFF")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(sqlite_error)?;
    Ok(conn)
}

fn checkpoint_truncate(conn: &Connection) -> Result<()> {
    let (busy, _, _): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(sqlite_error)?;
    if busy != 0 {
        return Err(Error::Sqlite("SQLite WAL checkpoint is busy".into()));
    }
    Ok(())
}

#[cfg(test)]
fn recorder_audits() -> &'static Mutex<Vec<PathBuf>> {
    static AUDITS: OnceLock<Mutex<Vec<PathBuf>>> = OnceLock::new();
    AUDITS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn note_recorder_audit(path: &Path) {
    if let Ok(mut audits) = recorder_audits().lock() {
        audits.push(path.to_path_buf());
    }
}

#[cfg(test)]
fn recorder_audited(path: &Path) -> bool {
    recorder_audits()
        .lock()
        .is_ok_and(|audits| audits.iter().any(|audited| audited == path))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SpeculativePrepareAudit {
    copy_count: usize,
    synchronous: i64,
}

#[cfg(test)]
fn speculative_prepare_audits() -> &'static Mutex<Vec<(PathBuf, SpeculativePrepareAudit)>> {
    static AUDITS: OnceLock<Mutex<Vec<(PathBuf, SpeculativePrepareAudit)>>> = OnceLock::new();
    AUDITS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn note_speculative_copy(path: &Path) {
    if let Ok(mut audits) = speculative_prepare_audits().lock() {
        audits.push((
            path.to_path_buf(),
            SpeculativePrepareAudit {
                copy_count: 1,
                synchronous: -1,
            },
        ));
    }
}

#[cfg(test)]
fn note_speculative_synchronous(path: &Path, staging: &Connection) -> Result<()> {
    let synchronous = staging
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if let Ok(mut audits) = speculative_prepare_audits().lock() {
        let Some((_, audit)) = audits.iter_mut().rev().find(|(audited, _)| audited == path) else {
            return Err(Error::Sqlite(
                "speculative SQLite synchronous audit is missing its copy".into(),
            ));
        };
        audit.synchronous = synchronous;
    }
    Ok(())
}

#[cfg(test)]
fn speculative_prepare_audit(path: &Path) -> Option<SpeculativePrepareAudit> {
    speculative_prepare_audits().lock().ok().and_then(|audits| {
        audits
            .iter()
            .rev()
            .find_map(|(audited, audit)| (audited == path).then_some(*audit))
    })
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedInstallPath {
    Promoted,
    Rebuilt,
}

#[cfg(test)]
fn prepared_installs() -> &'static Mutex<Vec<(PathBuf, PreparedInstallPath)>> {
    static INSTALLS: OnceLock<Mutex<Vec<(PathBuf, PreparedInstallPath)>>> = OnceLock::new();
    INSTALLS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn note_prepared_install(path: &Path, install: PreparedInstallPath) {
    if let Ok(mut installs) = prepared_installs().lock() {
        installs.push((path.to_path_buf(), install));
    }
}

#[cfg(test)]
fn prepared_install_path(path: &Path) -> Option<PreparedInstallPath> {
    prepared_installs().lock().ok().and_then(|installs| {
        installs
            .iter()
            .rev()
            .find_map(|(installed, method)| (installed == path).then_some(*method))
    })
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PreparedBaseReuseAudit {
    second_checkpoint_count: usize,
    second_base_full_hash_count: usize,
}

#[cfg(test)]
fn prepared_base_reuse_audits() -> &'static Mutex<Vec<(PathBuf, PreparedBaseReuseAudit)>> {
    static AUDITS: OnceLock<Mutex<Vec<(PathBuf, PreparedBaseReuseAudit)>>> = OnceLock::new();
    AUDITS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn begin_prepared_base_reuse_audit(path: &Path) {
    if let Ok(mut audits) = prepared_base_reuse_audits().lock() {
        audits.push((path.to_path_buf(), PreparedBaseReuseAudit::default()));
    }
}

#[cfg(test)]
fn note_second_checkpoint(path: &Path) {
    if let Ok(mut audits) = prepared_base_reuse_audits().lock() {
        if let Some((_, audit)) = audits.iter_mut().rev().find(|(audited, _)| audited == path) {
            audit.second_checkpoint_count += 1;
        }
    }
}

#[cfg(test)]
fn note_second_base_full_hash(path: &Path) {
    if let Ok(mut audits) = prepared_base_reuse_audits().lock() {
        if let Some((_, audit)) = audits.iter_mut().rev().find(|(audited, _)| audited == path) {
            audit.second_base_full_hash_count += 1;
        }
    }
}

#[cfg(test)]
fn prepared_base_reuse_audit(path: &Path) -> Option<PreparedBaseReuseAudit> {
    prepared_base_reuse_audits().lock().ok().and_then(|audits| {
        audits
            .iter()
            .rev()
            .find_map(|(audited, audit)| (audited == path).then_some(*audit))
    })
}

fn sqlite_sidecars_absent(path: &Path) -> Result<bool> {
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        if Path::new(&sidecar).try_exists().map_err(io_error)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn open_file_digest(file: &File) -> Result<LogHash> {
    let mut file = file.try_clone().map_err(io_error)?;
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(LogHash::from_bytes(hasher.finalize().into()))
}

fn clone_or_copy_to_temp(base: &Path) -> Result<NamedTempFile> {
    const COW_CLONE_MIN_BYTES: u64 = 256 * 1024;
    let placeholder = NamedTempFile::new_in(parent_dir(base)).map_err(io_error)?;
    let (placeholder_file, temp_path) = placeholder.into_parts();
    drop(placeholder_file);
    fs::remove_file(&temp_path).map_err(io_error)?;

    let clone_result = if fs::metadata(base).map_err(io_error)?.len() >= COW_CLONE_MIN_BYTES {
        try_platform_clone(base, &temp_path)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "small SQLite bases are cheaper to copy than clone",
        ))
    };
    let file = match clone_result {
        Ok(file) => file,
        Err(_) => {
            if temp_path.exists() {
                fs::remove_file(&temp_path).map_err(io_error)?;
            }
            fs::copy(base, &temp_path).map_err(io_error)?;
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&temp_path)
                .map_err(io_error)?
        }
    };
    Ok(NamedTempFile::from_parts(file, temp_path))
}

#[cfg(target_os = "macos")]
fn try_platform_clone(base: &Path, target: &Path) -> io::Result<File> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    unsafe extern "C" {
        fn clonefile(
            source: *const std::os::raw::c_char,
            target: *const std::os::raw::c_char,
            flags: u32,
        ) -> i32;
    }

    let source = CString::new(base.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"))?;
    let target_c = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "target path contains NUL"))?;
    // SAFETY: both C strings are NUL-terminated and remain alive for the call.
    if unsafe { clonefile(source.as_ptr(), target_c.as_ptr(), 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    OpenOptions::new().read(true).write(true).open(target)
}

#[cfg(target_os = "linux")]
fn try_platform_clone(base: &Path, target: &Path) -> io::Result<File> {
    use std::os::{fd::AsRawFd, raw::c_ulong};

    unsafe extern "C" {
        fn ioctl(fd: std::os::raw::c_int, request: c_ulong, ...) -> std::os::raw::c_int;
    }

    const FICLONE: c_ulong = 0x4004_9409;
    let source = File::open(base)?;
    let cloned = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(target)?;
    // SAFETY: FICLONE expects a valid destination fd and source fd argument.
    if unsafe { ioctl(cloned.as_raw_fd(), FICLONE, source.as_raw_fd()) } == 0 {
        return Ok(cloned);
    }
    let error = io::Error::last_os_error();
    drop(cloned);
    let _ = fs::remove_file(target);
    Err(error)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn try_platform_clone(_base: &Path, _target: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "copy-on-write cloning is not supported on this platform",
    ))
}

fn open_hashed_prepared_base(path: &Path) -> Result<(File, LogHash, Option<PreparedBaseSeal>)> {
    let named_before = fs::symlink_metadata(path).map_err(io_error)?;
    if !named_before.file_type().is_file() {
        return Err(Error::InvalidEntry(
            "closed SQLite base is not a regular file".into(),
        ));
    }
    let file = File::open(path).map_err(io_error)?;
    let owned_before = file.metadata().map_err(io_error)?;
    if !owned_before.file_type().is_file() {
        return Err(Error::InvalidEntry(
            "owned SQLite base is not a regular file".into(),
        ));
    }
    let seal = prepared_base_seal(&owned_before, &named_before)?;
    if !sqlite_sidecars_absent(path)? {
        return Err(Error::InvalidEntry(
            "closed SQLite base still has WAL sidecars".into(),
        ));
    }
    let digest = open_file_digest(&file)?;
    let owned_after = file.metadata().map_err(io_error)?;
    let named_after = fs::symlink_metadata(path).map_err(io_error)?;
    if !prepared_base_metadata_matches(seal.as_ref(), &owned_after, &named_after)
        || !sqlite_sidecars_absent(path)?
    {
        return Err(Error::InvalidEntry(
            "closed SQLite base changed while its digest was computed".into(),
        ));
    }
    Ok((file, digest, seal))
}

#[cfg(unix)]
fn prepared_base_seal(
    owned: &fs::Metadata,
    named: &fs::Metadata,
) -> Result<Option<PreparedBaseSeal>> {
    if !owned_regular_file_still_named(owned, named) {
        return Err(Error::InvalidEntry(
            "owned SQLite base inode is no longer named by its path".into(),
        ));
    }
    let seal = unix_metadata_seal(owned);
    if unix_metadata_seal(named) != seal {
        return Err(Error::InvalidEntry(
            "owned SQLite base metadata is not stable".into(),
        ));
    }
    Ok(Some(seal))
}

#[cfg(not(unix))]
fn prepared_base_seal(
    _owned: &fs::Metadata,
    _named: &fs::Metadata,
) -> Result<Option<PreparedBaseSeal>> {
    Ok(None)
}

#[cfg(unix)]
fn unix_metadata_seal(metadata: &fs::Metadata) -> PreparedBaseSeal {
    use std::os::unix::fs::MetadataExt;

    PreparedBaseSeal {
        dev: metadata.dev(),
        ino: metadata.ino(),
        len: metadata.len(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
    }
}

#[cfg(unix)]
fn prepared_base_metadata_matches(
    seal: Option<&PreparedBaseSeal>,
    owned: &fs::Metadata,
    named: &fs::Metadata,
) -> bool {
    seal.is_some_and(|seal| {
        owned_regular_file_still_named(owned, named)
            && unix_metadata_seal(owned) == *seal
            && unix_metadata_seal(named) == *seal
    })
}

#[cfg(not(unix))]
fn prepared_base_metadata_matches(
    _seal: Option<&PreparedBaseSeal>,
    _owned: &fs::Metadata,
    _named: &fs::Metadata,
) -> bool {
    false
}

fn prepared_base_still_sealed(path: &Path, prepared: &PreparedTarget) -> Result<bool> {
    let Some(named) = symlink_metadata_if_exists(path)? else {
        return Ok(false);
    };
    let owned = prepared.base_file.metadata().map_err(io_error)?;
    Ok(
        prepared_base_metadata_matches(prepared.base_seal.as_ref(), &owned, &named)
            && sqlite_sidecars_absent(path)?,
    )
}

fn symlink_metadata_if_exists(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file(_left: &fs::Metadata, _right: &fs::Metadata) -> bool {
    // The optimization is intentionally disabled when the platform cannot
    // prove that the owned temporary inode is still the one named by its path.
    false
}

fn owned_regular_file_still_named(owned: &fs::Metadata, named: &fs::Metadata) -> bool {
    owned.file_type().is_file() && named.file_type().is_file() && same_file(owned, named)
}

fn control_sidecar_path(path: &Path) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(".control");
    PathBuf::from(sidecar)
}

fn reject_legacy_user_database(path: &Path) -> Result<()> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(sqlite_error)?;
    let legacy: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_schema WHERE name IN ('__rhiza_meta', '__rhiza_requests') LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some(table) = legacy {
        return Err(Error::IdentityMismatch(format!(
            "legacy table {table} requires snapshot bootstrap into QWAL storage"
        )));
    }
    Ok(())
}

fn validate_control_database_pair(path: &Path, control: &ControlStore) -> Result<()> {
    let digest = file_digest(path)?;
    if let Some(pending) = control.pending()? {
        let tip = control.applied_tip()?;
        let expected_entry_index = tip
            .applied_index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("pending QWAL entry index is exhausted".into()))?;
        if pending.base() != LogAnchor::new(tip.applied_index(), tip.applied_hash())
            || pending.base_db_digest() != control.user_db_digest()?
            || pending.entry().index() != expected_entry_index
        {
            return Err(Error::InvalidEntry(
                "pending QWAL intent does not extend the committed control state".into(),
            ));
        }
        if digest == pending.base_db_digest() {
            return Ok(());
        }
        if digest == pending.target_db_digest() {
            let bytes = fs::metadata(path).map_err(io_error)?.len();
            if bytes != pending.target_file_bytes() {
                return Err(Error::InvalidEntry(
                    "pending QWAL target size does not match the canonical database".into(),
                ));
            }
            return Ok(());
        }
        return Err(Error::InvalidEntry(
            "pending QWAL database digest matches neither base nor target".into(),
        ));
    }
    if digest != control.user_db_digest()? {
        return Err(Error::InvalidEntry(
            "canonical SQLite digest does not match the control sidecar".into(),
        ));
    }
    Ok(())
}

fn decode_qwal_command(payload: &[u8]) -> Result<QwalEnvelopeV2> {
    if !payload.starts_with(QWAL_V2_MAGIC) {
        return Err(Error::InvalidCommand(
            "QWAL-only SQLite apply requires a QWAL v2 payload".into(),
        ));
    }
    decode_qwal_v2(payload)
}

fn validate_qwal_identity(
    effect: &QwalEnvelopeV2,
    identity: &ControlIdentity,
    configuration: &ConfigurationState,
) -> Result<()> {
    if effect.cluster_id != identity.cluster_id()
        || effect.epoch != identity.epoch()
        || effect.configuration_id != configuration.config_id()
        || effect.recovery_generation != identity.recovery_generation()
        || effect.materializer_fingerprint != identity.materializer_fingerprint().to_hex()
    {
        return Err(Error::InvalidEntry(
            "QWAL effect identity or materializer fingerprint mismatch".into(),
        ));
    }
    Ok(())
}

fn encode_qwal_snapshot(snapshot: &QwalSnapshotV2) -> Result<Vec<u8>> {
    let body = postcard::to_allocvec(snapshot)
        .map_err(|error| Error::InvalidSnapshot(format!("QSNP encode failed: {error}")))?;
    let mut encoded = Vec::with_capacity(QWAL_SNAPSHOT_V2_MAGIC.len() + body.len());
    encoded.extend_from_slice(QWAL_SNAPSHOT_V2_MAGIC);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_qwal_snapshot(encoded: &[u8]) -> Result<QwalSnapshotV2> {
    let body = encoded
        .strip_prefix(QWAL_SNAPSHOT_V2_MAGIC)
        .ok_or_else(|| Error::InvalidSnapshot("QWAL snapshot magic is missing".into()))?;
    let snapshot: QwalSnapshotV2 = postcard::from_bytes(body)
        .map_err(|error| Error::InvalidSnapshot(format!("QSNP decode failed: {error}")))?;
    if postcard::to_allocvec(&snapshot)
        .map_err(|error| Error::InvalidSnapshot(format!("QSNP re-encode failed: {error}")))?
        != body
    {
        return Err(Error::InvalidSnapshot(
            "QWAL snapshot is not canonically encoded".into(),
        ));
    }
    Ok(snapshot)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlAuthorizationMode {
    ReadOnly,
    PhysicalWrite,
}

fn execute_sql_statements(
    conn: &Connection,
    statements: &[SqlStatement],
) -> Result<SqlCommandResult> {
    let result = with_sql_authorizer(conn, SqlAuthorizationMode::PhysicalWrite, || {
        let mut statement_results = Vec::with_capacity(statements.len());
        let mut returning_rows = 0usize;
        let mut returning_bytes = 0usize;
        for operation in statements {
            let mut statement = conn.prepare(&operation.sql).map_err(sqlite_error)?;
            if statement.readonly() {
                return Err(Error::InvalidCommand(
                    "replicated SQL statements must mutate the database".into(),
                ));
            }
            let column_count = statement.column_count();
            let total_changes_before = conn.total_changes();
            let returning = if column_count == 0 {
                statement
                    .execute(params_from_iter(operation.parameters.iter()))
                    .map_err(sqlite_error)?;
                None
            } else {
                let columns = statement
                    .column_names()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                for column in &columns {
                    add_returning_bytes(&mut returning_bytes, column.len())?;
                }
                let mut result_rows = Vec::new();
                {
                    let mut rows = statement
                        .query(params_from_iter(operation.parameters.iter()))
                        .map_err(sqlite_error)?;
                    while let Some(row) = rows.next().map_err(sqlite_error)? {
                        returning_rows = returning_rows.checked_add(1).ok_or_else(|| {
                            Error::InvalidCommand("SQL RETURNING row count overflow".into())
                        })?;
                        if returning_rows > MAX_RETURNING_ROWS {
                            return Err(Error::InvalidCommand(format!(
                                "SQL RETURNING exceeds {MAX_RETURNING_ROWS} rows"
                            )));
                        }
                        let mut values = Vec::with_capacity(column_count);
                        for column in 0..column_count {
                            let value = sql_value(row.get_ref(column).map_err(sqlite_error)?)?;
                            add_returning_bytes(&mut returning_bytes, sql_value_size(&value))?;
                            values.push(value);
                        }
                        result_rows.push(values);
                    }
                }
                Some(SqlQueryResult {
                    columns,
                    rows: result_rows,
                })
            };
            let total_changes = conn
                .total_changes()
                .checked_sub(total_changes_before)
                .ok_or_else(|| Error::Sqlite("SQLite total_changes moved backwards".into()))?;
            let rows_affected = if total_changes == 0 {
                0
            } else {
                conn.changes()
            };
            statement_results.push(SqlStatementResult {
                rows_affected,
                returning,
            });
        }
        Ok(SqlCommandResult { statement_results })
    })?;
    validate_reserved_schema(conn)?;
    Ok(result)
}

fn add_returning_bytes(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| Error::InvalidCommand("SQL RETURNING result size overflow".into()))?;
    if *total > MAX_RETURNING_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL RETURNING exceeds {MAX_RETURNING_BYTES} result bytes"
        )));
    }
    Ok(())
}

fn decode_sql_command(payload: &[u8]) -> Result<SqlCommand> {
    let encoded = payload
        .strip_prefix(SQL_COMMAND_V2_MAGIC)
        .ok_or_else(|| Error::InvalidCommand("canonical QSQL v2 magic is missing".into()))?;
    let envelope: SqlCommandV2Envelope = serde_json::from_slice(encoded)
        .map_err(|error| Error::InvalidCommand(format!("invalid SQL command: {error}")))?;
    let expected = sql_executor_fingerprint()?;
    if envelope.executor_fingerprint != expected {
        return Err(Error::InvalidCommand(format!(
            "SQL executor fingerprint {} does not match local {}",
            envelope.executor_fingerprint.to_hex(),
            expected.to_hex()
        )));
    }
    let command = envelope.command;
    validate_sql_command(&command)?;
    if encode_sql_command(&command)? != payload {
        return Err(Error::InvalidCommand(
            "QSQL v2 command is not canonically encoded".into(),
        ));
    }
    Ok(command)
}

fn validate_sql_command(command: &SqlCommand) -> Result<()> {
    if command.request_id.is_empty() || command.request_id.len() > 256 {
        return Err(Error::InvalidCommand(
            "SQL request_id must contain 1..=256 bytes".into(),
        ));
    }
    if command.statements.is_empty() || command.statements.len() > MAX_SQL_STATEMENTS {
        return Err(Error::InvalidCommand(format!(
            "SQL command must contain 1..={MAX_SQL_STATEMENTS} statements"
        )));
    }
    for statement in &command.statements {
        validate_sql_statement(statement)?;
    }
    Ok(())
}

fn validate_sql_statement(statement: &SqlStatement) -> Result<()> {
    if statement.sql.trim().is_empty() || statement.sql.len() > MAX_SQL_TEXT_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL text must contain 1..={MAX_SQL_TEXT_BYTES} bytes"
        )));
    }
    if statement.sql.contains('\0') {
        return Err(Error::InvalidCommand(
            "SQL text must not contain NUL".into(),
        ));
    }
    if statement.parameters.len() > MAX_SQL_PARAMETERS {
        return Err(Error::InvalidCommand(format!(
            "SQL statement exceeds {MAX_SQL_PARAMETERS} parameters"
        )));
    }
    if statement
        .parameters
        .iter()
        .any(|value| matches!(value, SqlValue::Real(number) if !number.is_finite()))
    {
        return Err(Error::InvalidCommand(
            "SQL real parameters must be finite".into(),
        ));
    }
    Ok(())
}

fn with_sql_authorizer<T>(
    conn: &Connection,
    mode: SqlAuthorizationMode,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        authorize_sql(context, mode)
    }))
    .map_err(sqlite_error)?;
    let result = operation();
    let cleared = conn
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(sqlite_error);
    match (result, cleared) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn authorize_sql(context: AuthContext<'_>, mode: SqlAuthorizationMode) -> Authorization {
    if context.database_name.is_some_and(|name| name != "main") {
        return Authorization::Deny;
    }
    match context.action {
        AuthAction::Unknown { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. }
        | AuthAction::DropTempIndex { .. }
        | AuthAction::DropTempTable { .. }
        | AuthAction::DropTempTrigger { .. }
        | AuthAction::DropTempView { .. }
        | AuthAction::Transaction { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::CreateVtable { .. }
        | AuthAction::DropVtable { .. }
        | AuthAction::Savepoint { .. } => Authorization::Deny,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } if mode == SqlAuthorizationMode::ReadOnly
            && observational_pragma(pragma_name, pragma_value) =>
        {
            Authorization::Allow
        }
        AuthAction::Pragma { .. } => Authorization::Deny,
        AuthAction::Function { function_name } if unsafe_sql_function(function_name) => {
            Authorization::Deny
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        } if reserved_name(index_name) || reserved_name(table_name) => Authorization::Deny,
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name,
            table_name,
        } if reserved_name(trigger_name) || reserved_name(table_name) => Authorization::Deny,
        AuthAction::CreateTable { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::Read { table_name, .. }
        | AuthAction::Update { table_name, .. }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::Analyze { table_name }
            if reserved_name(table_name) =>
        {
            Authorization::Deny
        }
        AuthAction::CreateView { view_name } | AuthAction::DropView { view_name }
            if reserved_name(view_name) =>
        {
            Authorization::Deny
        }
        AuthAction::Reindex { index_name } if reserved_name(index_name) => Authorization::Deny,
        _ => Authorization::Allow,
    }
}

fn observational_pragma(name: &str, value: Option<&str>) -> bool {
    const ARGUMENT_SAFE: &[&str] = &[
        "foreign_key_check",
        "foreign_key_list",
        "index_info",
        "index_list",
        "index_xinfo",
        "integrity_check",
        "quick_check",
        "table_info",
        "table_list",
        "table_xinfo",
    ];
    const NO_ARGUMENT_ONLY: &[&str] = &[
        "application_id",
        "collation_list",
        "compile_options",
        "data_version",
        "encoding",
        "freelist_count",
        "function_list",
        "module_list",
        "page_count",
        "pragma_list",
        "schema_version",
        "user_version",
    ];

    if value.is_some_and(reserved_name) {
        return false;
    }
    ARGUMENT_SAFE
        .iter()
        .any(|allowed| name.eq_ignore_ascii_case(allowed))
        || value.is_none()
            && NO_ARGUMENT_ONLY
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
}

fn reserved_name(name: &str) -> bool {
    name.to_ascii_lowercase().starts_with("__rhiza_")
}

fn validate_reserved_schema(conn: &Connection) -> Result<()> {
    let unexpected: Option<String> = conn
        .query_row(
            "SELECT name
             FROM sqlite_schema
             WHERE (lower(name) GLOB '__rhiza_*' OR lower(tbl_name) GLOB '__rhiza_*')
               AND NOT (
                   (type = 'table' AND name = '__rhiza_kv' AND tbl_name = '__rhiza_kv')
                   OR
                   (type = 'index'
                    AND name GLOB 'sqlite_autoindex___rhiza_kv_*'
                    AND tbl_name = '__rhiza_kv')
               )
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some(name) = unexpected {
        return Err(Error::InvalidCommand(format!(
            "SQL object uses reserved rhiza namespace: {name}"
        )));
    }
    Ok(())
}

fn unsafe_sql_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("load_extension")
}

fn sql_value(value: ValueRef<'_>) -> Result<SqlValue> {
    Ok(match value {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(value) => SqlValue::Integer(value),
        ValueRef::Real(value) if value.is_finite() => SqlValue::Real(value),
        ValueRef::Real(_) => {
            return Err(Error::InvalidCommand(
                "SQL real result must be finite".into(),
            ));
        }
        ValueRef::Text(value) => SqlValue::Text(
            String::from_utf8(value.to_vec())
                .map_err(|_| Error::InvalidCommand("SQL TEXT result is not valid UTF-8".into()))?,
        ),
        ValueRef::Blob(value) => SqlValue::Blob(value.to_vec()),
    })
}

fn sql_value_size(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 0,
        SqlValue::Integer(_) | SqlValue::Real(_) => 8,
        SqlValue::Text(value) => value.len(),
        SqlValue::Blob(value) => value.len(),
    }
}

fn encode_sql_result(result: &SqlCommandResult) -> Result<Vec<u8>> {
    validate_sql_result_bounds(result)?;
    let encoded = serde_json::to_vec(result)
        .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL result: {error}")))?;
    let mut blob = Vec::with_capacity(SQL_RESULT_V1_MAGIC.len() + encoded.len());
    blob.extend_from_slice(SQL_RESULT_V1_MAGIC);
    blob.extend_from_slice(&encoded);
    Ok(blob)
}

fn decode_sql_result(blob: &[u8]) -> Result<SqlCommandResult> {
    let encoded = blob
        .strip_prefix(SQL_RESULT_V1_MAGIC)
        .ok_or_else(|| Error::Sqlite("unsupported SQL result encoding".into()))?;
    let result: SqlCommandResult = serde_json::from_slice(encoded)
        .map_err(|error| Error::Sqlite(format!("invalid SQL result: {error}")))?;
    validate_sql_result_bounds(&result)?;
    if encode_sql_result(&result)? != blob {
        return Err(Error::Sqlite("SQL result blob is not canonical".into()));
    }
    Ok(result)
}

pub(crate) fn validate_sql_result_blob_bounds(blob: &[u8]) -> Result<()> {
    decode_sql_result(blob).map(|_| ())
}

fn validate_sql_result_bounds(result: &SqlCommandResult) -> Result<()> {
    let mut row_count = 0usize;
    let mut bytes = 0usize;
    for statement in &result.statement_results {
        let Some(returning) = &statement.returning else {
            continue;
        };
        row_count = row_count
            .checked_add(returning.rows.len())
            .ok_or_else(|| Error::InvalidCommand("SQL RETURNING row count overflow".into()))?;
        if row_count > MAX_RETURNING_ROWS {
            return Err(Error::InvalidCommand(format!(
                "SQL RETURNING exceeds {MAX_RETURNING_ROWS} rows"
            )));
        }
        for column in &returning.columns {
            add_returning_bytes(&mut bytes, column.len())?;
        }
        for row in &returning.rows {
            if row.len() != returning.columns.len() {
                return Err(Error::InvalidCommand(
                    "SQL RETURNING row width does not match its columns".into(),
                ));
            }
            for value in row {
                add_returning_bytes(&mut bytes, sql_value_size(value))?;
            }
        }
    }
    Ok(())
}

fn integrity_check(conn: &Connection) -> Result<()> {
    let mut statement = conn
        .prepare("PRAGMA integrity_check;")
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    let mut rows = statement
        .query([])
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    let mut messages = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| Error::InvalidSnapshot(err.to_string()))?
    {
        messages.push(
            row.get::<_, String>(0)
                .map_err(|err| Error::InvalidSnapshot(err.to_string()))?,
        );
    }
    if messages == ["ok"] {
        return Ok(());
    }
    Err(Error::InvalidSnapshot(if messages.is_empty() {
        "integrity_check returned no result".into()
    } else {
        messages.join("; ")
    }))
}

fn ensure_parent(path: &Path) -> Result<()> {
    let parent = parent_dir(path);
    fs::create_dir_all(parent).map_err(io_error)
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sync_parent(parent: &Path) -> Result<()> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)
}

fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::Sqlite(error.to_string())
}

fn sql_query_error(error: rusqlite::Error) -> Error {
    match &error {
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ffi::ErrorCode::OperationInterrupted =>
        {
            Error::ResourceExhausted("SQL query execution timed out".into())
        }
        _ => sqlite_error(error),
    }
}

fn io_error(error: std::io::Error) -> Error {
    Error::Io(error.to_string())
}

#[cfg(test)]
mod query_policy_tests {
    use super::*;

    fn prepare_single_sql_effect(
        database: &SqliteStateMachine,
        command: &SqlCommand,
        request: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>> {
        let preparation = database.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: request,
            }],
            base_index,
            base_hash,
        )?;
        preparation
            .results
            .into_iter()
            .next()
            .expect("one-member batch returns one result")?;
        preparation
            .effect
            .ok_or_else(|| Error::InvalidCommand("successful batch produced no effect".into()))
    }

    #[test]
    fn speculative_prepare_uses_one_copy_and_non_durable_sqlite_without_weakening_canonical() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "speculative-durability".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE speculative(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let base_digest = database.canonical_db_digest().unwrap();

        prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();

        let audit = speculative_prepare_audit(&path).expect("prepare records its test audit");
        assert_eq!(audit.copy_count, 1);
        assert_eq!(audit.synchronous, 0);
        assert_eq!(database.connection_pragmas().unwrap(), ("wal".into(), 0));
        assert_eq!(database.canonical_db_digest().unwrap(), base_digest);
        let prepared = database.prepared_target.lock().unwrap();
        assert!(
            prepared
                .as_ref()
                .unwrap()
                .artifact
                .as_file()
                .metadata()
                .unwrap()
                .len()
                > 0
        );
    }

    #[test]
    fn batch_capacity_rejects_1025_members_before_speculative_io() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "over-capacity".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE must_not_run(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let payload = encode_sql_command(&command).unwrap();
        let member = SqlBatchMember {
            command: &command,
            request_payload: &payload,
        };

        assert!(matches!(
            database.prepare_sql_batch_effect(&vec![member; 1025], 0, LogHash::ZERO),
            Err(Error::InvalidCommand(_))
        ));
        assert_eq!(speculative_prepare_audit(&path), None);
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
    }

    #[test]
    fn capacity_sized_batch_rejects_a_duplicate_request_id_at_the_tail() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let commands = (0usize..MAX_QWAL_V2_RECEIPTS)
            .map(|index| SqlCommand {
                request_id: if index + 1 == MAX_QWAL_V2_RECEIPTS {
                    "request-0000".into()
                } else {
                    format!("request-{index:04}")
                },
                statements: vec![SqlStatement {
                    sql: "INSERT INTO absent_table(value) VALUES (?1)".into(),
                    parameters: vec![SqlValue::Integer(index as i64)],
                }],
            })
            .collect::<Vec<_>>();
        let payloads = commands
            .iter()
            .map(|command| encode_sql_command(command).unwrap())
            .collect::<Vec<_>>();
        let members = commands
            .iter()
            .zip(&payloads)
            .map(|(command, request_payload)| SqlBatchMember {
                command,
                request_payload,
            })
            .collect::<Vec<_>>();

        let preparation = database
            .prepare_sql_batch_effect(&members, 0, LogHash::ZERO)
            .unwrap();

        assert!(preparation.effect.is_none());
        assert_eq!(preparation.results.len(), MAX_QWAL_V2_RECEIPTS);
        assert!(matches!(
            preparation.results.last().unwrap(),
            Err(Error::InvalidCommand(message)) if message.contains("repeats a request_id")
        ));
        assert_eq!(database.applied_tip_value().unwrap(), (0, LogHash::ZERO));
    }

    #[test]
    fn applied_tip_returns_index_and_hash_from_the_same_database_state() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let request = b"put\trequest-1\tkey-1\tvalue-1";
        let payload = database
            .prepare_put_effect("request-1", "key-1", "value-1", request, 0, LogHash::ZERO)
            .unwrap();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        database
            .apply_entry(&LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Command,
                payload: payload.to_vec(),
                prev_hash: LogHash::ZERO,
                hash,
            })
            .unwrap();

        assert_eq!(database.applied_tip().unwrap(), ApplyProgress::new(1, hash));
        assert_eq!(database.applied_tip_value().unwrap(), (1, hash));
    }

    #[test]
    fn bulk_sql_request_check_aligns_exact_absent_and_conflict_results() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let committed = SqlCommand {
            request_id: "committed".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE committed(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let committed_payload = encode_sql_command(&committed).unwrap();
        let effect =
            prepare_single_sql_effect(&database, &committed, &committed_payload, 0, LogHash::ZERO)
                .unwrap();
        let entry = command_entry(1, LogHash::ZERO, effect);
        database.apply_entry(&entry).unwrap();
        let absent = SqlCommand {
            request_id: "absent".into(),
            statements: vec![SqlStatement {
                sql: "SELECT 1".into(),
                parameters: vec![],
            }],
        };
        let absent_payload = encode_sql_command(&absent).unwrap();

        let aligned = database
            .check_sql_requests(&[
                ("committed", committed_payload.as_slice()),
                ("absent", absent_payload.as_slice()),
            ])
            .unwrap();
        assert!(matches!(
            aligned.as_slice(),
            [Ok(Some((outcome, Some(_)))), Ok(None)]
                if *outcome == RequestOutcome::new(1, entry.hash)
        ));

        let conflicting = SqlCommand {
            request_id: "committed".into(),
            statements: vec![SqlStatement {
                sql: "SELECT 2".into(),
                parameters: vec![],
            }],
        };
        let conflicting_payload = encode_sql_command(&conflicting).unwrap();
        assert!(matches!(
            database
                .check_sql_requests(&[("committed", conflicting_payload.as_slice())])
                .unwrap()
                .pop()
                .unwrap(),
            Err(Error::RequestConflict(_))
        ));
        assert!(matches!(
            database.check_sql_requests(&[
                ("committed", committed_payload.as_slice()),
                ("committed", committed_payload.as_slice()),
            ]),
            Err(Error::InvalidCommand(message)) if message.contains("duplicate")
        ));
    }

    #[test]
    fn read_query_timeout_interrupts_work_and_releases_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let expensive = SqlStatement {
            sql: "WITH RECURSIVE count(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM count WHERE value < 100000000) SELECT sum(value) FROM count".into(),
            parameters: vec![],
        };

        assert_eq!(
            database
                .query_sql_with_timeout(&expensive, 1, 1024, Duration::ZERO)
                .unwrap_err(),
            Error::ResourceExhausted("SQL query execution timed out".into())
        );

        let quick = database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT 1".into(),
                    parameters: vec![],
                },
                1,
                1024,
            )
            .unwrap();
        assert_eq!(quick.rows, vec![vec![SqlValue::Integer(1)]]);
    }

    #[test]
    fn read_query_allows_nondeterministic_and_runtime_introspection_functions() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();

        let result = database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT random(), datetime('now'), sqlite_version()".into(),
                    parameters: vec![],
                },
                1,
                4096,
            )
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].len(), 3);
    }

    #[test]
    fn physical_effect_preparation_accepts_nondeterministic_functions() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let command = SqlCommand {
            request_id: "nondeterministic-write".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE generated(value INTEGER DEFAULT (random()))".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO generated DEFAULT VALUES".into(),
                    parameters: vec![],
                },
            ],
        };

        let payload = encode_sql_command(&command).unwrap();
        let effect =
            prepare_single_sql_effect(&database, &command, &payload, 0, LogHash::ZERO).unwrap();
        assert!(effect.starts_with(QWAL_V2_MAGIC));
        assert_eq!(database.applied_index_value().unwrap(), 0);
    }

    #[test]
    fn non_durable_staging_uses_complete_candidate_page_capture() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "recorded-write".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE recorded(value TEXT NOT NULL)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO recorded(value) VALUES ('shadow')".into(),
                    parameters: vec![],
                },
            ],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();

        // The speculative database is disposable, so logical commit and
        // checkpoint completion are sufficient for candidate-page capture.
        assert!(recorder_audited(&path));
        let effect = decode_qwal_v2(&payload).unwrap();
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        database
            .apply_entry(&LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Command,
                payload,
                prev_hash: LogHash::ZERO,
                hash,
            })
            .unwrap();
        assert_eq!(
            database.canonical_db_digest().unwrap(),
            effect.target_db_digest
        );
    }

    #[test]
    fn exact_consensus_winner_promotes_the_prepared_target_without_rebuilding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "prepared-winner".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE promoted(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        assert_eq!(
            database
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT 1".into(),
                        parameters: vec![],
                    },
                    1,
                    1024,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Integer(1)]]
        );
        let entry = command_entry(1, LogHash::ZERO, payload);

        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Promoted)
        );
        assert_eq!(
            prepared_base_reuse_audit(&path),
            Some(PreparedBaseReuseAudit {
                second_checkpoint_count: 0,
                second_base_full_hash_count: 0,
            })
        );
        assert_eq!(outcome.progress(), ApplyProgress::new(1, entry.hash));
        assert_eq!(
            database
                .check_sql_request("prepared-winner", &request)
                .unwrap()
                .unwrap()
                .0,
            RequestOutcome::new(1, entry.hash)
        );
    }

    #[test]
    fn foreign_consensus_winner_discards_the_prepared_target_and_rebuilds() {
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let foreign_path = dir.path().join("foreign.sqlite");
        let local = SqliteStateMachine::open(&local_path, "cluster-a", "node-1", 1, 1).unwrap();
        let foreign = SqliteStateMachine::open(&foreign_path, "cluster-a", "node-2", 1, 1).unwrap();
        let local_command = SqlCommand {
            request_id: "local-loser".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE local_only(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let local_request = encode_sql_command(&local_command).unwrap();
        let _ = prepare_single_sql_effect(&local, &local_command, &local_request, 0, LogHash::ZERO)
            .unwrap();
        let foreign_command = SqlCommand {
            request_id: "foreign-winner".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE foreign_only(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let foreign_request = encode_sql_command(&foreign_command).unwrap();
        let foreign_effect = prepare_single_sql_effect(
            &foreign,
            &foreign_command,
            &foreign_request,
            0,
            LogHash::ZERO,
        )
        .unwrap();

        local
            .apply_entry(&command_entry(1, LogHash::ZERO, foreign_effect))
            .unwrap();

        assert_eq!(
            prepared_install_path(&local_path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert_eq!(
            prepared_base_reuse_audit(&local_path),
            Some(PreparedBaseReuseAudit {
                second_checkpoint_count: 1,
                second_base_full_hash_count: 1,
            })
        );
        assert_eq!(
            local
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT name FROM sqlite_schema WHERE name = 'foreign_only'".into(),
                        parameters: vec![],
                    },
                    1,
                    1024,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Text("foreign_only".into())]]
        );
    }

    #[cfg(unix)]
    #[test]
    fn prepared_target_promotion_rejects_a_symlink_to_the_owned_inode() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "symlinked-target".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE safe_fallback(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let staging_path = database
            .prepared_target
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .artifact
            .path()
            .to_path_buf();
        let backing_path = staging_path.with_extension("owned-inode");
        fs::rename(&staging_path, &backing_path).unwrap();
        symlink(&backing_path, &staging_path).unwrap();

        database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert_eq!(
            prepared_base_reuse_audit(&path),
            Some(PreparedBaseReuseAudit {
                second_checkpoint_count: 1,
                second_base_full_hash_count: 1,
            })
        );
        assert!(!fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            database
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT name FROM sqlite_schema WHERE name = 'safe_fallback'".into(),
                        parameters: vec![],
                    },
                    1,
                    1024,
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Text("safe_fallback".into())]]
        );
    }

    #[cfg(unix)]
    #[test]
    fn prepared_base_new_inode_falls_back_to_a_full_rebuild() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "new-base-inode".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE rebuilt_from_new_inode(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let original_inode = fs::metadata(&path).unwrap().ino();
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            let replacement = path.with_extension("replacement");
            fs::copy(&path, &replacement).unwrap();
            fs::rename(&replacement, &path).unwrap();
        }
        assert_ne!(fs::metadata(&path).unwrap().ino(), original_inode);

        database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert_eq!(
            prepared_base_reuse_audit(&path),
            Some(PreparedBaseReuseAudit {
                second_checkpoint_count: 1,
                second_base_full_hash_count: 1,
            })
        );
    }

    #[cfg(unix)]
    #[test]
    fn prepared_base_symlink_falls_back_without_promoting_through_it() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "symlinked-base".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE rebuilt_from_symlink(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let backing = path.with_extension("backing");
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            fs::rename(&path, &backing).unwrap();
            symlink(&backing, &path).unwrap();
        }

        database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert!(!fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn prepared_base_same_inode_mutation_rejects_rebuildable_apply() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "mutated-base".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE should_not_promote(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        let original_inode = fs::metadata(&path).unwrap().ino();
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.close_connection().unwrap();
            let mutation = open_connection(&path).unwrap();
            mutation
                .execute_batch(
                    "CREATE TABLE external_mutation(value INTEGER NOT NULL); PRAGMA wal_checkpoint(TRUNCATE);",
                )
                .unwrap();
            mutation.close().unwrap();
        }
        assert_eq!(fs::metadata(&path).unwrap().ino(), original_inode);

        assert!(database.apply_entry(&entry).is_err());

        assert_eq!(database.control.pending().unwrap(), None);
        assert_ne!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Promoted)
        );
    }

    #[cfg(unix)]
    #[test]
    fn missing_prepared_target_falls_back_to_a_full_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "missing-target".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE rebuilt_missing_target(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let staging_path = database
            .prepared_target
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .artifact
            .path()
            .to_path_buf();
        fs::remove_file(staging_path).unwrap();

        database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
    }

    #[cfg(unix)]
    #[test]
    fn mutated_prepared_target_falls_back_to_a_full_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "mutated-target".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE rebuilt_mutated_target(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v2(&payload).unwrap();
        let staging_path = database
            .prepared_target
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .artifact
            .path()
            .to_path_buf();
        let mutation = open_connection(&staging_path).unwrap();
        mutation
            .execute_batch(
                "CREATE TABLE injected_target(value INTEGER NOT NULL); PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .unwrap();
        mutation.close().unwrap();
        assert_ne!(file_digest(&staging_path).unwrap(), effect.target_db_digest);

        database
            .apply_entry(&command_entry(1, LogHash::ZERO, payload))
            .unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert!(database
            .query_sql(
                &SqlStatement {
                    sql: "SELECT name FROM sqlite_schema WHERE name = 'injected_target'".into(),
                    parameters: vec![],
                },
                1,
                1024,
            )
            .unwrap()
            .rows
            .is_empty());
    }

    #[test]
    fn stale_prepared_entry_cannot_promote_after_the_tip_moves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let command = SqlCommand {
            request_id: "stale-prepared".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE stale_target(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let stale = command_entry(1, LogHash::ZERO, payload);
        let noop_hash =
            LogEntry::calculate_hash("cluster-a", 1, 1, 1, EntryType::Noop, LogHash::ZERO, &[]);
        database
            .apply_entry(&LogEntry {
                cluster_id: "cluster-a".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Noop,
                payload: vec![],
                prev_hash: LogHash::ZERO,
                hash: noop_hash,
            })
            .unwrap();

        assert!(database.apply_entry(&stale).is_err());
        assert_ne!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Promoted)
        );
    }

    #[test]
    fn reopening_forgets_the_prepared_target_and_preserves_fallback_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "restart-fallback".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE restarted(value INTEGER NOT NULL)".into(),
                parameters: vec![],
            }],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        drop(database);
        let database = SqliteStateMachine::open_existing(&path).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);

        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert_eq!(
            prepared_base_reuse_audit(&path),
            Some(PreparedBaseReuseAudit {
                second_checkpoint_count: 1,
                second_base_full_hash_count: 1,
            })
        );
        assert_eq!(
            database
                .check_sql_request("restart-fallback", &request)
                .unwrap()
                .unwrap(),
            (
                RequestOutcome::new(1, entry.hash),
                outcome.sql_result().cloned()
            )
        );
    }

    #[test]
    fn replay_after_durable_pending_with_canonical_base_rebuilds_target_and_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "pending-base-replay".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE base_recovery(value TEXT NOT NULL)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO base_recovery VALUES ('recovered') RETURNING value".into(),
                    parameters: vec![],
                },
            ],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v2(&payload).unwrap();
        let expected_result = decode_sql_result(&effect.receipts[0].result_blob).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        let pending = pending_for(&entry, &effect);
        database.control.begin_pending(&pending).unwrap();
        drop(database);

        let database = SqliteStateMachine::open_existing(&path).unwrap();
        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert_eq!(outcome.sql_result(), Some(&expected_result));
        assert_eq!(
            database.canonical_db_digest().unwrap(),
            effect.target_db_digest
        );
        assert_eq!(
            prepared_install_path(&path),
            Some(PreparedInstallPath::Rebuilt)
        );
        assert_eq!(
            prepared_base_reuse_audit(&path),
            Some(PreparedBaseReuseAudit {
                second_checkpoint_count: 1,
                second_base_full_hash_count: 1,
            })
        );
        assert_eq!(
            database
                .check_sql_request("pending-base-replay", &request)
                .unwrap()
                .unwrap(),
            (RequestOutcome::new(1, entry.hash), Some(expected_result))
        );
    }

    #[test]
    fn replay_after_promoted_target_without_receipt_commits_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.sqlite");
        let command = SqlCommand {
            request_id: "pending-target-replay".into(),
            statements: vec![
                SqlStatement {
                    sql: "CREATE TABLE target_recovery(value TEXT NOT NULL)".into(),
                    parameters: vec![],
                },
                SqlStatement {
                    sql: "INSERT INTO target_recovery VALUES ('already-promoted') RETURNING value"
                        .into(),
                    parameters: vec![],
                },
            ],
        };
        let request = encode_sql_command(&command).unwrap();
        let database = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        let payload =
            prepare_single_sql_effect(&database, &command, &request, 0, LogHash::ZERO).unwrap();
        let effect = decode_qwal_v2(&payload).unwrap();
        let expected_result = decode_sql_result(&effect.receipts[0].result_blob).unwrap();
        let entry = command_entry(1, LogHash::ZERO, payload);
        let pending = pending_for(&entry, &effect);
        database.control.begin_pending(&pending).unwrap();
        {
            let _lifecycle = database.lock_lifecycle().unwrap();
            database.with_connection(checkpoint_truncate).unwrap();
            database.close_connection().unwrap();
            let prepared = database
                .take_matching_prepared_target(&effect, &entry.payload)
                .unwrap()
                .unwrap();
            assert!(database
                .promote_prepared_target(&prepared, &effect)
                .unwrap());
        }
        drop(database);

        let database = SqliteStateMachine::open_existing(&path).unwrap();
        let outcome = database.apply_entry_with_result(&entry).unwrap();

        assert_eq!(outcome.sql_result(), Some(&expected_result));
        assert_eq!(
            database.canonical_db_digest().unwrap(),
            effect.target_db_digest
        );
        assert_eq!(prepared_install_path(&path), None);
        assert_eq!(
            database
                .check_sql_request("pending-target-replay", &request)
                .unwrap()
                .unwrap(),
            (RequestOutcome::new(1, entry.hash), Some(expected_result))
        );
    }

    fn pending_for(entry: &LogEntry, effect: &QwalEnvelopeV2) -> PendingApply {
        PendingApply::new(
            LogAnchor::new(effect.base_index, effect.base_hash),
            LogAnchor::new(entry.index, entry.hash),
            effect.base_db_digest,
            effect.target_db_digest,
            effect.target_file_bytes,
        )
    }

    fn command_entry(index: u64, prev_hash: LogHash, payload: Vec<u8>) -> LogEntry {
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            index,
            1,
            1,
            EntryType::Command,
            prev_hash,
            &payload,
        );
        LogEntry {
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            index,
            entry_type: EntryType::Command,
            payload,
            prev_hash,
            hash,
        }
    }

    #[test]
    fn result_decoder_rejects_semantically_oversized_returning_rows() {
        let result = SqlCommandResult {
            statement_results: vec![SqlStatementResult {
                rows_affected: 0,
                returning: Some(SqlQueryResult {
                    columns: vec!["value".into()],
                    rows: (0..=MAX_RETURNING_ROWS)
                        .map(|_| vec![SqlValue::Integer(1)])
                        .collect(),
                }),
            }],
        };
        let mut blob = SQL_RESULT_V1_MAGIC.to_vec();
        blob.extend_from_slice(&serde_json::to_vec(&result).unwrap());

        assert!(matches!(
            decode_sql_result(&blob),
            Err(Error::InvalidCommand(_))
        ));
    }
}
