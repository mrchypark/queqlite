use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Cursor, Error as IoError, ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use fallible_streaming_iterator::FallibleStreamingIterator;
use rhiza_core::{
    ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, LogIndex, RecoveryAnchor,
    Snapshot, SnapshotIdentity, SnapshotManifest,
};
use rusqlite::{
    hooks::{Action as UpdateAction, AuthAction, AuthContext, Authorization},
    params, params_from_iter,
    session::{ChangesetIter, ConflictAction, Session},
    types::{ToSql, ToSqlOutput, Value, ValueRef},
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, MAIN_DB,
};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

pub const CREATE_META_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS __rhiza_meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS __rhiza_migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);
"#;

pub const CREATE_REQUESTS_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS __rhiza_requests (
    request_id TEXT PRIMARY KEY,
    original_log_index INTEGER NOT NULL,
    original_log_hash BLOB NOT NULL CHECK(length(original_log_hash) = 32),
    command_digest BLOB NOT NULL CHECK(length(command_digest) = 32),
    result_blob BLOB
);
"#;

const CREATE_KV_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS __rhiza_kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"#;

const LEGACY_SCHEMA_VERSION: u64 = 1;
const REQUEST_RESULTS_SCHEMA_VERSION: u64 = 2;
const SCHEMA_VERSION: u64 = 3;
const CONFIGURATION_STATE_V2_MAGIC: &[u8] = b"QCST\0\x02";
const SQL_COMMAND_V1_MAGIC: &[u8] = b"QSQL\0\x01";
const SQL_COMMAND_V2_MAGIC: &[u8] = b"QSQL\0\x02";
const SQL_EFFECT_V1_MAGIC: &[u8] = b"QEFX\0\x01";
const SQL_RESULT_V1_MAGIC: &[u8] = b"QRES\0\x01";
const WRITE_BATCH_V1_MAGIC: &[u8] = b"QBCH\0\x01";
const SQL_EXECUTOR_POLICY_VERSION: &str = "rhiza-sql-policy-v3";
const SQL_CONNECTION_PROFILE: &str =
    "foreign_keys=ON;trusted_schema=OFF;temp=denied;attach=denied;vtable=denied";
pub const MAX_SQL_STATEMENTS: usize = 64;
pub const MAX_SQL_PARAMETERS: usize = 999;
pub const MAX_SQL_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_RETURNING_ROWS: usize = 1_024;
pub const MAX_RETURNING_BYTES: usize = 1024 * 1024;
pub const MAX_SQL_EFFECT_BYTES: usize = 256 * 1024;
pub const MAX_WRITE_BATCH_MEMBERS: usize = 64;
pub const DEFAULT_SQL_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const SQL_PROGRESS_HANDLER_OPS: i32 = 1_000;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlEffectPreparation {
    Effect(Vec<u8>),
    StatementReplay,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SqlEffectEnvelope {
    base_index: LogIndex,
    base_hash: LogHash,
    executor_fingerprint: LogHash,
    request_id: String,
    request_digest: LogHash,
    result_blob: Vec<u8>,
    changeset: Vec<u8>,
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
    encode_sql_command_with_magic(command, SQL_COMMAND_V2_MAGIC)
}

pub fn encode_write_batch(member_payloads: &[Vec<u8>]) -> Result<Vec<u8>> {
    if member_payloads.is_empty() || member_payloads.len() > MAX_WRITE_BATCH_MEMBERS {
        return Err(Error::InvalidCommand(format!(
            "write batch must contain 1..={MAX_WRITE_BATCH_MEMBERS} members"
        )));
    }
    let count = u16::try_from(member_payloads.len())
        .map_err(|_| Error::InvalidCommand("write batch member count is exhausted".into()))?;
    let mut payload = Vec::with_capacity(
        WRITE_BATCH_V1_MAGIC.len()
            + 2
            + member_payloads
                .iter()
                .map(|member| 4usize.saturating_add(member.len()))
                .sum::<usize>(),
    );
    payload.extend_from_slice(WRITE_BATCH_V1_MAGIC);
    payload.extend_from_slice(&count.to_be_bytes());
    for member in member_payloads {
        if member.is_empty() {
            return Err(Error::InvalidCommand(
                "write batch members must not be empty".into(),
            ));
        }
        let len = u32::try_from(member.len())
            .map_err(|_| Error::InvalidCommand("write batch member is too large".into()))?;
        payload.extend_from_slice(&len.to_be_bytes());
        payload.extend_from_slice(member);
    }
    Ok(payload)
}

fn decode_write_batch(payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    let encoded = payload
        .strip_prefix(WRITE_BATCH_V1_MAGIC)
        .ok_or_else(|| Error::InvalidCommand("write batch magic is missing".into()))?;
    if encoded.len() < 2 {
        return Err(Error::InvalidCommand(
            "write batch count is truncated".into(),
        ));
    }
    let count = usize::from(u16::from_be_bytes([encoded[0], encoded[1]]));
    if count == 0 || count > MAX_WRITE_BATCH_MEMBERS {
        return Err(Error::InvalidCommand(format!(
            "write batch must contain 1..={MAX_WRITE_BATCH_MEMBERS} members"
        )));
    }
    let mut offset = 2usize;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| Error::InvalidCommand("write batch offset overflow".into()))?;
        let length_bytes: [u8; 4] = encoded
            .get(offset..length_end)
            .ok_or_else(|| Error::InvalidCommand("write batch member length is truncated".into()))?
            .try_into()
            .expect("four-byte slice");
        offset = length_end;
        let length = usize::try_from(u32::from_be_bytes(length_bytes))
            .map_err(|_| Error::InvalidCommand("write batch member length is invalid".into()))?;
        if length == 0 {
            return Err(Error::InvalidCommand(
                "write batch members must not be empty".into(),
            ));
        }
        let member_end = offset
            .checked_add(length)
            .ok_or_else(|| Error::InvalidCommand("write batch member offset overflow".into()))?;
        let member = encoded
            .get(offset..member_end)
            .ok_or_else(|| Error::InvalidCommand("write batch member is truncated".into()))?;
        members.push(member.to_vec());
        offset = member_end;
    }
    if offset != encoded.len() || encode_write_batch(&members)? != payload {
        return Err(Error::InvalidCommand(
            "write batch encoding is not canonical".into(),
        ));
    }
    Ok(members)
}

pub fn encode_sql_command_v1(command: &SqlCommand) -> Result<Vec<u8>> {
    encode_sql_command_with_magic(command, SQL_COMMAND_V1_MAGIC)
}

fn encode_sql_command_with_magic(command: &SqlCommand, magic: &[u8]) -> Result<Vec<u8>> {
    validate_sql_command(command)?;
    let encoded = if magic == SQL_COMMAND_V2_MAGIC {
        serde_json::to_vec(&SqlCommandV2Envelope {
            executor_fingerprint: sql_executor_fingerprint()?,
            command: command.clone(),
        })
    } else {
        serde_json::to_vec(command)
    }
    .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL command: {error}")))?;
    let mut payload = Vec::with_capacity(magic.len() + encoded.len());
    payload.extend_from_slice(magic);
    payload.extend_from_slice(&encoded);
    Ok(payload)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaKey {
    ClusterId,
    NodeId,
    Epoch,
    ConfigId,
    ConfigurationState,
    AppliedIndex,
    AppliedHash,
    SchemaVersion,
    SnapshotId,
    CreatedAt,
}

impl MetaKey {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClusterId => "cluster_id",
            Self::NodeId => "node_id",
            Self::Epoch => "epoch",
            Self::ConfigId => "config_id",
            Self::ConfigurationState => "configuration_state",
            Self::AppliedIndex => "applied_index",
            Self::AppliedHash => "applied_hash",
            Self::SchemaVersion => "schema_version",
            Self::SnapshotId => "snapshot_id",
            Self::CreatedAt => "created_at",
        }
    }
}

pub const REQUIRED_META_KEYS: [MetaKey; 10] = [
    MetaKey::ClusterId,
    MetaKey::NodeId,
    MetaKey::Epoch,
    MetaKey::ConfigId,
    MetaKey::ConfigurationState,
    MetaKey::AppliedIndex,
    MetaKey::AppliedHash,
    MetaKey::SchemaVersion,
    MetaKey::SnapshotId,
    MetaKey::CreatedAt,
];

pub const fn required_meta_keys() -> &'static [MetaKey] {
    &REQUIRED_META_KEYS
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
    conn: Connection,
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
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;
        if path.exists() {
            let db = Self::open_existing_file(&path)?;
            db.validate_scalar_identity(cluster_id, node_id, epoch, config_id)?;
            db.migrate()?;
            db.validate_initialized()?;
            return Ok(db);
        }
        Self::create_new(
            &path,
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

        if path.exists() {
            let db = Self::open_existing_file(&path)?;
            db.validate_scalar_identity(
                cluster_id,
                node_id,
                epoch,
                configuration_state.config_id(),
            )?;
            db.migrate()?;
            db.validate_initialized()?;
            db.validate_identity(cluster_id, node_id, epoch, &configuration_state)?;
            return Ok(db);
        }

        Self::create_new(&path, cluster_id, node_id, epoch, configuration_state)
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let db = Self::open_existing_file(path.as_ref())?;
        db.migrate()?;
        db.validate_initialized()?;
        Ok(db)
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

        let db = match Self::open_existing_file(path) {
            Ok(db) => db,
            Err(err) => {
                let _ = fs::remove_file(path);
                return Err(err);
            }
        };
        if let Err(err) = db.initialize(cluster_id, node_id, epoch, &configuration_state) {
            drop(db);
            let _ = fs::remove_file(path);
            return Err(err);
        }
        Ok(db)
    }

    fn open_existing_file(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|err| Error::Sqlite(err.to_string()))?;
        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode = WAL;", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Sqlite(format!(
                "SQLite refused WAL journal mode: {journal_mode}"
            )));
        }
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(sqlite_error)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(sqlite_error)?;
        Ok(Self {
            path: path.to_path_buf(),
            conn,
        })
    }

    fn initialize(
        &self,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: &ConfigurationState,
    ) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_META_TABLE_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_REQUESTS_TABLE_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_KV_TABLE_SQL)
            .map_err(sqlite_error)?;
        put_meta(&tx, MetaKey::ClusterId, cluster_id.as_bytes())?;
        put_meta(&tx, MetaKey::NodeId, node_id.as_bytes())?;
        put_meta(&tx, MetaKey::Epoch, epoch.to_string().as_bytes())?;
        put_meta(
            &tx,
            MetaKey::ConfigId,
            configuration_state.config_id().to_string().as_bytes(),
        )?;
        put_configuration_state(&tx, configuration_state)?;
        put_meta(
            &tx,
            MetaKey::SchemaVersion,
            SCHEMA_VERSION.to_string().as_bytes(),
        )?;
        put_meta(&tx, MetaKey::CreatedAt, b"0")?;
        put_meta(&tx, MetaKey::AppliedIndex, b"0")?;
        put_meta(&tx, MetaKey::AppliedHash, LogHash::ZERO.to_hex().as_bytes())?;
        put_meta(&tx, MetaKey::SnapshotId, b"")?;
        tx.commit().map_err(sqlite_error)
    }

    fn validate_initialized(&self) -> Result<()> {
        validate_initialized(&self.conn)
    }

    fn validate_identity(
        &self,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: &ConfigurationState,
    ) -> Result<()> {
        validate_text_identity(&self.conn, MetaKey::ClusterId, cluster_id)?;
        validate_text_identity(&self.conn, MetaKey::NodeId, node_id)?;
        validate_integer_identity(&self.conn, MetaKey::Epoch, epoch)?;
        if self.configuration_state_value()? == *configuration_state {
            Ok(())
        } else {
            Err(Error::IdentityMismatch("configuration_state".into()))
        }
    }

    fn validate_scalar_identity(
        &self,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        config_id: u64,
    ) -> Result<()> {
        validate_text_identity(&self.conn, MetaKey::ClusterId, cluster_id)?;
        validate_text_identity(&self.conn, MetaKey::NodeId, node_id)?;
        validate_integer_identity(&self.conn, MetaKey::Epoch, epoch)?;
        validate_integer_identity(&self.conn, MetaKey::ConfigId, config_id)
    }

    fn migrate_configuration_state(&self) -> Result<()> {
        let existing = get_meta(&self.conn, MetaKey::ConfigurationState)?;
        let state = match existing.as_deref() {
            Some(value) => decode_configuration_state(value)?,
            None => {
                ConfigurationState::active(meta_u64(&self.conn, MetaKey::ConfigId)?, LogHash::ZERO)
            }
        };
        if existing
            .as_deref()
            .is_some_and(|value| value.starts_with(CONFIGURATION_STATE_V2_MAGIC))
        {
            return Ok(());
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        put_configuration_state(&tx, &state)?;
        tx.commit().map_err(sqlite_error)
    }

    fn migrate(&self) -> Result<()> {
        self.migrate_configuration_state()?;
        self.migrate_request_results()
    }

    fn migrate_request_results(&self) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let schema_version = meta_u64(&tx, MetaKey::SchemaVersion)?;
        match schema_version {
            SCHEMA_VERSION => {
                validate_requests_schema(&tx, SCHEMA_VERSION)?;
                tx.commit().map_err(sqlite_error)
            }
            REQUEST_RESULTS_SCHEMA_VERSION => {
                validate_requests_schema(&tx, REQUEST_RESULTS_SCHEMA_VERSION)?;
                put_meta(
                    &tx,
                    MetaKey::SchemaVersion,
                    SCHEMA_VERSION.to_string().as_bytes(),
                )?;
                tx.commit().map_err(sqlite_error)
            }
            LEGACY_SCHEMA_VERSION => {
                validate_requests_schema(&tx, LEGACY_SCHEMA_VERSION)?;
                tx.execute(
                    "ALTER TABLE __rhiza_requests ADD COLUMN result_blob BLOB",
                    [],
                )
                .map_err(sqlite_error)?;
                put_meta(
                    &tx,
                    MetaKey::SchemaVersion,
                    SCHEMA_VERSION.to_string().as_bytes(),
                )?;
                tx.commit().map_err(sqlite_error)
            }
            version => Err(Error::Sqlite(format!(
                "unsupported schema version {version}"
            ))),
        }
    }

    pub fn apply_entry(&self, entry: &LogEntry) -> Result<ApplyProgress> {
        Ok(self.apply_entry_with_result(entry)?.progress())
    }

    pub fn apply_entry_with_result(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        if entry.recompute_hash() != entry.hash {
            return Err(Error::InvalidEntry(
                "hash does not match entry contents".into(),
            ));
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;

        validate_text_identity(&tx, MetaKey::ClusterId, &entry.cluster_id)
            .map_err(identity_as_entry_error)?;
        validate_integer_identity(&tx, MetaKey::Epoch, entry.epoch)
            .map_err(identity_as_entry_error)?;
        let current_index = meta_u64(&tx, MetaKey::AppliedIndex)?;
        let current_hash = meta_hash(&tx, MetaKey::AppliedHash)?;

        if entry.index == current_index {
            if entry.hash == current_hash {
                let operation = parse_operation(entry)?;
                let sql_result = replay_operation(&tx, &operation, &entry.payload)?;
                return Ok(ApplyOutcome {
                    progress: ApplyProgress::new(current_index, current_hash),
                    sql_result,
                });
            }
            return Err(Error::InvalidEntry(
                "current index was reapplied with a different hash".into(),
            ));
        }
        let next_index = current_index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index != next_index {
            return Err(Error::InvalidEntry(format!(
                "expected index {next_index}, got {}",
                entry.index
            )));
        }
        if entry.prev_hash != current_hash {
            return Err(Error::InvalidEntry(
                "prev_hash does not match the applied tip".into(),
            ));
        }

        let current_configuration = meta_configuration_state(&tx)?;
        let next_configuration = current_configuration
            .validate_entry(entry)
            .map_err(|err| Error::InvalidEntry(err.to_string()))?;
        let operation = parse_operation(entry)?;
        let sql_result = apply_operation(&tx, &operation, entry.index, entry.hash, &entry.payload)?;
        put_configuration_state(&tx, &next_configuration)?;
        put_meta(
            &tx,
            MetaKey::ConfigId,
            next_configuration.config_id().to_string().as_bytes(),
        )?;
        put_meta(
            &tx,
            MetaKey::AppliedIndex,
            entry.index.to_string().as_bytes(),
        )?;
        put_meta(&tx, MetaKey::AppliedHash, entry.hash.to_hex().as_bytes())?;
        tx.commit().map_err(sqlite_error)?;
        Ok(ApplyOutcome {
            progress: ApplyProgress::new(entry.index, entry.hash),
            sql_result,
        })
    }

    pub fn get_value(&self, key: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT value FROM __rhiza_kv WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)
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
        validate_sql_statement(query)?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(Error::InvalidCommand(
                "SQL query limits must be positive".into(),
            ));
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        self.conn
            .progress_handler(
                SQL_PROGRESS_HANDLER_OPS,
                Some(move || Instant::now() >= deadline),
            )
            .map_err(sqlite_error)?;
        let result = with_sql_authorizer(&self.conn, None, SqlAuthorizationMode::ReadOnly, || {
            let mut statement = self.conn.prepare(&query.sql).map_err(sql_query_error)?;
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
                        .ok_or_else(|| Error::InvalidCommand("SQL result size overflow".into()))?;
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
        let clear_result = self
            .conn
            .progress_handler(0, None::<fn() -> bool>)
            .map_err(sqlite_error);
        match (result, clear_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(result), Ok(())) => Ok(result),
        }
    }

    pub fn validate_sql_write(&self, command: &SqlCommand) -> Result<()> {
        validate_sql_command(command)?;
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        execute_sql_statements(
            &tx,
            &command.statements,
            SqlCommandVersion::V2,
            SqlExecutionMode::StatementReplay,
        )?;
        tx.rollback().map_err(sqlite_error)
    }

    pub fn prepare_sql_effect(
        &self,
        command: &SqlCommand,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<SqlEffectPreparation> {
        validate_sql_command(command)?;
        let (version, decoded) = decode_sql_command(request_payload)?;
        if version != SqlCommandVersion::V2 || decoded != *command {
            return Err(Error::InvalidCommand(
                "SQL effect request is not the canonical QSQL v2 command".into(),
            ));
        }

        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if meta_u64(&tx, MetaKey::AppliedIndex)? != base_index
            || meta_hash(&tx, MetaKey::AppliedHash)? != base_hash
        {
            return Err(Error::InvalidEntry(
                "SQL effect base does not match the materialized SQLite tip".into(),
            ));
        }
        let attempted =
            prepare_sql_effect_in_transaction(&tx, command, request_payload, base_index, base_hash);
        tx.rollback().map_err(sqlite_error)?;
        attempted.map(|(preparation, _)| preparation)
    }

    pub fn prepare_write_batch(
        &self,
        member_payloads: &[Vec<u8>],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>> {
        self.prepare_write_batch_prefix(member_payloads, base_index, base_hash, usize::MAX)?
            .map(|(_, payload)| payload)
            .ok_or_else(|| Error::InvalidCommand("write batch has no encodable members".into()))
    }

    /// Prepares the largest ordered prefix whose canonical batch payload fits `max_payload_bytes`.
    ///
    /// Each candidate member is executed at most once in the rollback-only preview transaction.
    /// The first member that would overflow the encoded batch is previewed to determine its
    /// canonical size, but is not included in the returned payload.
    pub fn prepare_write_batch_prefix(
        &self,
        member_payloads: &[Vec<u8>],
        base_index: LogIndex,
        base_hash: LogHash,
        max_payload_bytes: usize,
    ) -> Result<Option<(usize, Vec<u8>)>> {
        if member_payloads.is_empty() || member_payloads.len() > MAX_WRITE_BATCH_MEMBERS {
            return Err(Error::InvalidCommand(format!(
                "write batch must contain 1..={MAX_WRITE_BATCH_MEMBERS} members"
            )));
        }
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if meta_u64(&tx, MetaKey::AppliedIndex)? != base_index
            || meta_hash(&tx, MetaKey::AppliedHash)? != base_hash
        {
            return Err(Error::InvalidEntry(
                "write batch base does not match the materialized SQLite tip".into(),
            ));
        }
        let preview_index = base_index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        let mut proposals = Vec::with_capacity(member_payloads.len());
        let mut encoded_len = WRITE_BATCH_V1_MAGIC.len() + 2;
        for member_payload in member_payloads {
            let proposal = match parse_command_operation(member_payload)? {
                Operation::Put { .. } => {
                    let operation = parse_command_operation(member_payload)?;
                    apply_operation(
                        &tx,
                        &operation,
                        preview_index,
                        LogHash::ZERO,
                        member_payload,
                    )?;
                    member_payload.clone()
                }
                Operation::Sql { version, command } => {
                    if version != SqlCommandVersion::V2 {
                        return Err(Error::InvalidCommand(
                            "write batches require canonical QSQL v2 commands".into(),
                        ));
                    }
                    if matching_request(&tx, &command.request_id, member_payload)?.is_some() {
                        member_payload.clone()
                    } else {
                        let (preparation, result) = prepare_sql_effect_in_transaction(
                            &tx,
                            &command,
                            member_payload,
                            base_index,
                            base_hash,
                        )?;
                        let result_blob = encode_sql_result(&result)?;
                        record_request(
                            &tx,
                            &command.request_id,
                            preview_index,
                            LogHash::ZERO,
                            member_payload,
                            Some(&result_blob),
                        )?;
                        match preparation {
                            SqlEffectPreparation::Effect(payload) => payload,
                            SqlEffectPreparation::StatementReplay => member_payload.clone(),
                        }
                    }
                }
                Operation::SqlEffect(_) | Operation::Batch(_) | Operation::Noop => {
                    return Err(Error::InvalidCommand(
                        "write batch members must be original put or QSQL commands".into(),
                    ));
                }
            };
            u32::try_from(proposal.len())
                .map_err(|_| Error::InvalidCommand("write batch member is too large".into()))?;
            let next_encoded_len = encoded_len
                .checked_add(4)
                .and_then(|len| len.checked_add(proposal.len()))
                .ok_or_else(|| Error::InvalidCommand("write batch size is exhausted".into()))?;
            if next_encoded_len > max_payload_bytes {
                tx.rollback().map_err(sqlite_error)?;
                return if proposals.is_empty() {
                    Ok(None)
                } else {
                    let member_count = proposals.len();
                    encode_write_batch(&proposals).map(|payload| Some((member_count, payload)))
                };
            }
            encoded_len = next_encoded_len;
            proposals.push(proposal);
        }
        tx.rollback().map_err(sqlite_error)?;
        let member_count = proposals.len();
        encode_write_batch(&proposals).map(|payload| Some((member_count, payload)))
    }

    pub fn check_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<RequestOutcome>> {
        let Some(record) = matching_request(&self.conn, request_id, command_payload)? else {
            return Ok(None);
        };
        Ok(Some(record.outcome))
    }

    pub fn connection_pragmas(&self) -> Result<(String, i64)> {
        let journal_mode = self
            .conn
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        let synchronous = self
            .conn
            .query_row("PRAGMA synchronous;", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        Ok((journal_mode, synchronous))
    }

    pub fn check_sql_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<(RequestOutcome, Option<SqlCommandResult>)>> {
        let Some(record) = matching_request(&self.conn, request_id, command_payload)? else {
            return Ok(None);
        };
        let (version, command) = decode_sql_command(command_payload)?;
        if command.request_id != request_id {
            return Err(Error::InvalidCommand(
                "SQL payload request_id does not match lookup request_id".into(),
            ));
        }
        let result = stored_sql_result(version, &record)?;
        Ok(Some((record.outcome, result)))
    }

    pub fn applied_index_value(&self) -> Result<LogIndex> {
        meta_u64(&self.conn, MetaKey::AppliedIndex)
    }

    pub fn applied_hash_value(&self) -> Result<LogHash> {
        meta_hash(&self.conn, MetaKey::AppliedHash)
    }

    pub fn applied_tip_value(&self) -> Result<(LogIndex, LogHash)> {
        let (applied_index, applied_hash) = self
            .conn
            .query_row(
                "SELECT
                    (SELECT value FROM __rhiza_meta WHERE key = ?1),
                    (SELECT value FROM __rhiza_meta WHERE key = ?2)",
                params![
                    MetaKey::AppliedIndex.as_str(),
                    MetaKey::AppliedHash.as_str()
                ],
                |row| {
                    Ok((
                        row.get::<_, Option<Vec<u8>>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                    ))
                },
            )
            .map_err(sqlite_error)?;
        Ok((
            decode_meta_u64(MetaKey::AppliedIndex, applied_index)?,
            decode_meta_hash(MetaKey::AppliedHash, applied_hash)?,
        ))
    }

    pub fn configuration_state_value(&self) -> Result<ConfigurationState> {
        meta_configuration_state(&self.conn)
    }

    pub fn create_snapshot(&self, target: LogIndex) -> Result<Snapshot> {
        let applied_index = self.applied_index_value()?;
        if applied_index != target {
            return Err(Error::InvalidSnapshot(format!(
                "snapshot target {target} does not match applied index {applied_index}"
            )));
        }
        let applied_hash = self.applied_hash_value()?;
        let manifest = SnapshotManifest::new_with_configuration(
            meta_string(&self.conn, MetaKey::ClusterId)?,
            self.configuration_state_value()?,
            meta_u64(&self.conn, MetaKey::Epoch)?,
            target,
            applied_hash,
            meta_u64(&self.conn, MetaKey::SchemaVersion)?,
            meta_string(&self.conn, MetaKey::NodeId)?,
        )
        .with_executor_fingerprint(sql_executor_fingerprint()?);

        let parent = parent_dir(&self.path);
        let snapshot_file = NamedTempFile::new_in(parent).map_err(io_error)?;
        self.conn
            .backup(MAIN_DB, snapshot_file.path(), None)
            .map_err(sqlite_error)?;

        {
            let snapshot_conn = Connection::open_with_flags(
                snapshot_file.path(),
                OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .map_err(sqlite_error)?;
            let journal_mode: String = snapshot_conn
                .query_row("PRAGMA journal_mode = DELETE;", [], |row| row.get(0))
                .map_err(sqlite_error)?;
            if !journal_mode.eq_ignore_ascii_case("delete") {
                return Err(Error::InvalidSnapshot(format!(
                    "temporary snapshot journal mode is {journal_mode}"
                )));
            }
            put_meta(
                &snapshot_conn,
                MetaKey::SnapshotId,
                manifest.snapshot_id().as_bytes(),
            )?;
            validate_snapshot(&snapshot_conn, &manifest)?;
        }

        snapshot_file.as_file().sync_all().map_err(io_error)?;
        let db_bytes = fs::read(snapshot_file.path()).map_err(io_error)?;
        Ok(Snapshot::new(manifest, db_bytes))
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
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node_id is empty".into()));
    }
    let path = path.as_ref();
    ensure_parent(path)?;
    let parent = parent_dir(path);
    let mut restore_file = NamedTempFile::new_in(parent).map_err(io_error)?;
    restore_file
        .write_all(snapshot.db_bytes())
        .map_err(io_error)?;
    restore_file.as_file().sync_all().map_err(io_error)?;

    {
        let restore_conn =
            Connection::open_with_flags(restore_file.path(), OpenFlags::SQLITE_OPEN_READ_WRITE)
                .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        validate_snapshot(&restore_conn, snapshot.manifest())?;
        let tx = Transaction::new_unchecked(&restore_conn, TransactionBehavior::Immediate)
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        put_meta(&tx, MetaKey::NodeId, target_node_id.as_bytes())
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        tx.commit()
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        validate_text_identity(&restore_conn, MetaKey::NodeId, target_node_id)
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    }
    restore_file.as_file().sync_all().map_err(io_error)?;

    let restored = restore_file
        .persist(path)
        .map_err(|err| Error::Io(err.error.to_string()))?;
    restored.sync_all().map_err(io_error)?;
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
    }

    let path = path.as_ref();
    ensure_parent(path)?;
    let parent = parent_dir(path);
    let mut restore_file = NamedTempFile::new_in(parent).map_err(io_error)?;
    restore_file.write_all(db_bytes).map_err(io_error)?;
    restore_file.as_file().sync_all().map_err(io_error)?;

    {
        let restore_conn =
            Connection::open_with_flags(restore_file.path(), OpenFlags::SQLITE_OPEN_READ_WRITE)
                .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        integrity_check(&restore_conn)?;
        validate_initialized(&restore_conn)
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        validate_snapshot_text(&restore_conn, MetaKey::ClusterId, anchor.cluster_id())?;
        validate_snapshot_integer(&restore_conn, MetaKey::Epoch, anchor.epoch())?;
        validate_snapshot_integer(&restore_conn, MetaKey::ConfigId, anchor.config_id())?;
        let actual_configuration =
            meta_configuration_state(&restore_conn).map_err(snapshot_validation_error)?;
        if actual_configuration != *anchor.configuration_state() {
            return Err(Error::InvalidSnapshot(
                "recovery anchor configuration_state does not match database value".into(),
            ));
        }
        validate_snapshot_integer(
            &restore_conn,
            MetaKey::AppliedIndex,
            anchor.compacted().index(),
        )?;
        validate_snapshot_hash(
            &restore_conn,
            MetaKey::AppliedHash,
            anchor.compacted().hash(),
        )?;
        validate_snapshot_text(
            &restore_conn,
            MetaKey::SnapshotId,
            anchor.snapshot().snapshot_id(),
        )?;

        let tx = Transaction::new_unchecked(&restore_conn, TransactionBehavior::Immediate)
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        put_meta(&tx, MetaKey::NodeId, target_node_id.as_bytes())
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        tx.commit()
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
        validate_text_identity(&restore_conn, MetaKey::NodeId, target_node_id)
            .map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    }
    restore_file.as_file().sync_all().map_err(io_error)?;

    let restored = restore_file
        .persist(path)
        .map_err(|err| Error::Io(err.error.to_string()))?;
    restored.sync_all().map_err(io_error)?;
    sync_parent(parent)
}

enum Operation<'a> {
    Put {
        request_id: Option<&'a str>,
        key: &'a str,
        value: &'a str,
    },
    Sql {
        version: SqlCommandVersion,
        command: SqlCommand,
    },
    SqlEffect(SqlEffectEnvelope),
    Batch(Vec<Vec<u8>>),
    Noop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlCommandVersion {
    V1,
    V2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlExecutionMode {
    StatementReplay,
    EffectPreparation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlAuthorizationMode {
    ReadOnly,
    DeterministicWrite,
}

fn parse_operation(entry: &LogEntry) -> Result<Operation<'_>> {
    match entry.entry_type {
        EntryType::Command => parse_command_operation(&entry.payload),
        EntryType::Noop if entry.payload.is_empty() => Ok(Operation::Noop),
        EntryType::Noop => Err(Error::InvalidEntry("Noop payload must be empty".into())),
        EntryType::ConfigChange => Ok(Operation::Noop),
        _ => Err(Error::InvalidEntry(format!(
            "entry type {:?} is unsupported in QuePaxa v1",
            entry.entry_type
        ))),
    }
}

fn parse_command_operation(payload: &[u8]) -> Result<Operation<'_>> {
    if payload.starts_with(WRITE_BATCH_V1_MAGIC) {
        return decode_write_batch(payload).map(Operation::Batch);
    }
    if payload.starts_with(SQL_EFFECT_V1_MAGIC) {
        return decode_sql_effect(payload).map(Operation::SqlEffect);
    }
    if payload.starts_with(SQL_COMMAND_V1_MAGIC) || payload.starts_with(SQL_COMMAND_V2_MAGIC) {
        let (version, command) = decode_sql_command(payload)?;
        return Ok(Operation::Sql { version, command });
    }
    let command =
        std::str::from_utf8(payload).map_err(|err| Error::InvalidCommand(err.to_string()))?;
    let fields: Vec<&str> = command.split('\t').collect();
    match fields.as_slice() {
        ["put", key, value] if !key.is_empty() => Ok(Operation::Put {
            request_id: None,
            key,
            value,
        }),
        ["put", request_id, key, value] if !request_id.is_empty() && !key.is_empty() => {
            Ok(Operation::Put {
                request_id: Some(request_id),
                key,
                value,
            })
        }
        _ => Err(Error::InvalidCommand(
            "expected `put<TAB>key<TAB>value` or `put<TAB>request_id<TAB>key<TAB>value`".into(),
        )),
    }
}

fn apply_operation(
    conn: &Connection,
    operation: &Operation<'_>,
    log_index: LogIndex,
    log_hash: LogHash,
    payload: &[u8],
) -> Result<Option<SqlCommandResult>> {
    match operation {
        Operation::Sql { version, command } => {
            return apply_sql_command(conn, *version, command, log_index, log_hash, payload);
        }
        Operation::SqlEffect(effect) => return apply_sql_effect(conn, effect, log_index, log_hash),
        Operation::Batch(members) => {
            for member in members {
                let member_operation = parse_command_operation(member)?;
                if matches!(member_operation, Operation::Batch(_)) {
                    return Err(Error::InvalidCommand(
                        "nested write batches are not supported".into(),
                    ));
                }
                apply_operation(conn, &member_operation, log_index, log_hash, member)?;
            }
            return Ok(None);
        }
        _ => {}
    }
    let Operation::Put {
        request_id,
        key,
        value,
    } = operation
    else {
        return Ok(None);
    };

    if let Some(request_id) = request_id {
        if matching_request(conn, request_id, payload)?.is_some() {
            return Ok(None);
        }

        put_value(conn, key, value)?;
        record_request(conn, request_id, log_index, log_hash, payload, None)?;
        return Ok(None);
    }

    put_value(conn, key, value)?;
    Ok(None)
}

fn replay_operation(
    conn: &Connection,
    operation: &Operation<'_>,
    payload: &[u8],
) -> Result<Option<SqlCommandResult>> {
    match operation {
        Operation::Put {
            request_id: Some(request_id),
            ..
        } => {
            matching_request(conn, request_id, payload)?.ok_or_else(|| {
                Error::Sqlite("decided request is missing its request record".into())
            })?;
            Ok(None)
        }
        Operation::Sql { version, command } => {
            let record =
                matching_request(conn, &command.request_id, payload)?.ok_or_else(|| {
                    Error::Sqlite("decided SQL request is missing its request record".into())
                })?;
            stored_sql_result(*version, &record)
        }
        Operation::SqlEffect(effect) => {
            let record = matching_request_digest(conn, &effect.request_id, effect.request_digest)?
                .ok_or_else(|| {
                    Error::Sqlite("decided SQL effect is missing its request record".into())
                })?;
            let stored = stored_sql_result(SqlCommandVersion::V2, &record)?
                .ok_or_else(|| Error::Sqlite("decided SQL effect result is missing".into()))?;
            let expected = decode_sql_result(&effect.result_blob)?;
            if stored != expected {
                return Err(Error::Sqlite(
                    "decided SQL effect result differs from its request record".into(),
                ));
            }
            Ok(Some(stored))
        }
        Operation::Batch(members) => {
            for member in members {
                let member_operation = parse_command_operation(member)?;
                if matches!(member_operation, Operation::Batch(_)) {
                    return Err(Error::InvalidCommand(
                        "nested write batches are not supported".into(),
                    ));
                }
                replay_operation(conn, &member_operation, member)?;
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn apply_sql_command(
    conn: &Connection,
    version: SqlCommandVersion,
    command: &SqlCommand,
    log_index: LogIndex,
    log_hash: LogHash,
    payload: &[u8],
) -> Result<Option<SqlCommandResult>> {
    if let Some(record) = matching_request(conn, &command.request_id, payload)? {
        return stored_sql_result(version, &record);
    }
    let result = execute_sql_statements(
        conn,
        &command.statements,
        version,
        SqlExecutionMode::StatementReplay,
    )?;
    let result_blob = match version {
        SqlCommandVersion::V1 => None,
        SqlCommandVersion::V2 => Some(encode_sql_result(&result)?),
    };
    record_request(
        conn,
        &command.request_id,
        log_index,
        log_hash,
        payload,
        result_blob.as_deref(),
    )?;
    Ok((version == SqlCommandVersion::V2).then_some(result))
}

fn attach_session_tables(conn: &Connection, session: &mut Session<'_>) -> Result<()> {
    let mut statement = conn
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type = 'table'
               AND lower(name) NOT GLOB 'sqlite_*'
               AND lower(name) NOT GLOB '__rhiza_*'
             ORDER BY name",
        )
        .map_err(sqlite_error)?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    for table in tables {
        session.attach(Some(table.as_str())).map_err(sqlite_error)?;
    }
    Ok(())
}

struct SqlEffectBuild<'a> {
    touched_tables: BTreeSet<String>,
    base_index: LogIndex,
    base_hash: LogHash,
    command: &'a SqlCommand,
    request_payload: &'a [u8],
    result: &'a SqlCommandResult,
}

fn build_sql_effect(
    conn: &Connection,
    session: &mut Session<'_>,
    audit: Arc<Mutex<SqlEffectAudit>>,
    build: SqlEffectBuild<'_>,
) -> Result<Vec<u8>> {
    let SqlEffectBuild {
        touched_tables,
        base_index,
        base_hash,
        command,
        request_payload,
        result,
    } = build;
    let audit = audit
        .lock()
        .map_err(|_| Error::Sqlite("SQL effect audit mutex is poisoned".into()))?;
    if audit.saw_ddl {
        return Err(Error::InvalidCommand(
            "SQL effect batches do not support DDL".into(),
        ));
    }
    validate_effect_environment(conn)?;
    for table in &audit.write_tables {
        effect_table_shape(conn, table)?;
    }
    for table in &touched_tables {
        effect_table_shape(conn, table)?;
    }

    conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(sqlite_error)?;
    let mut changeset_writer = BoundedChangesetWriter::new(MAX_SQL_EFFECT_BYTES);
    if let Err(error) = session.changeset_strm(&mut changeset_writer) {
        if changeset_writer.overflowed {
            return Err(Error::InvalidCommand(format!(
                "SQL changeset exceeds {MAX_SQL_EFFECT_BYTES} bytes"
            )));
        }
        return Err(Error::Sqlite(format!(
            "cannot generate SQL changeset: {error}"
        )));
    }
    let changeset = changeset_writer.bytes;
    let changed_tables = validate_changeset_tables(conn, &changeset)?;
    if let Some(missing) = touched_tables.difference(&changed_tables).next() {
        return Err(Error::InvalidCommand(format!(
            "SQL session did not capture every write to table {missing}"
        )));
    }

    let result_blob = encode_sql_result(result)?;
    let envelope = SqlEffectEnvelope {
        base_index,
        base_hash,
        executor_fingerprint: sql_executor_fingerprint()?,
        request_id: command.request_id.clone(),
        request_digest: LogHash::digest(&[request_payload]),
        result_blob,
        changeset,
    };
    let encoded = serde_json::to_vec(&envelope)
        .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL effect: {error}")))?;
    let mut payload = Vec::with_capacity(SQL_EFFECT_V1_MAGIC.len() + encoded.len());
    payload.extend_from_slice(SQL_EFFECT_V1_MAGIC);
    payload.extend_from_slice(&encoded);
    if payload.len() > MAX_SQL_EFFECT_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL effect envelope exceeds {MAX_SQL_EFFECT_BYTES} bytes"
        )));
    }
    Ok(payload)
}

fn with_update_audit<T>(
    conn: &Connection,
    operation: impl FnOnce() -> Result<T>,
) -> Result<(T, BTreeSet<String>)> {
    let touched = Arc::new(Mutex::new(BTreeSet::new()));
    let hook_touched = Arc::clone(&touched);
    conn.update_hook(Some(
        move |_action: UpdateAction, database: &str, table: &str, _rowid: i64| {
            if database == "main" {
                if let Ok(mut tables) = hook_touched.lock() {
                    tables.insert(table.to_owned());
                }
            }
        },
    ))
    .map_err(sqlite_error)?;
    let result = operation();
    let cleared = conn
        .update_hook(None::<fn(UpdateAction, &str, &str, i64)>)
        .map_err(sqlite_error);
    let touched = touched
        .lock()
        .map_err(|_| Error::Sqlite("SQL update audit mutex is poisoned".into()))?
        .clone();
    match (result, cleared) {
        (Ok(value), Ok(())) => Ok((value, touched)),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

struct BoundedChangesetWriter {
    bytes: Vec<u8>,
    limit: usize,
    overflowed: bool,
}

impl BoundedChangesetWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            overflowed: false,
        }
    }
}

impl Write for BoundedChangesetWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|size| size > self.limit)
        {
            self.overflowed = true;
            return Err(IoError::new(
                ErrorKind::FileTooLarge,
                "SQL changeset size limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn decode_sql_effect(payload: &[u8]) -> Result<SqlEffectEnvelope> {
    if payload.len() > MAX_SQL_EFFECT_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL effect envelope exceeds {MAX_SQL_EFFECT_BYTES} bytes"
        )));
    }
    let encoded = payload
        .strip_prefix(SQL_EFFECT_V1_MAGIC)
        .ok_or_else(|| Error::InvalidCommand("SQL effect magic is missing".into()))?;
    let effect: SqlEffectEnvelope = serde_json::from_slice(encoded)
        .map_err(|error| Error::InvalidCommand(format!("invalid SQL effect: {error}")))?;
    if effect.request_id.is_empty() || effect.request_id.len() > 256 {
        return Err(Error::InvalidCommand(
            "SQL effect request_id must contain 1..=256 bytes".into(),
        ));
    }
    if effect.executor_fingerprint != sql_executor_fingerprint()? {
        return Err(Error::InvalidCommand(
            "SQL effect executor fingerprint does not match the local executor".into(),
        ));
    }
    if effect.changeset.len() > MAX_SQL_EFFECT_BYTES {
        return Err(Error::InvalidCommand(format!(
            "SQL changeset exceeds {MAX_SQL_EFFECT_BYTES} bytes"
        )));
    }
    if effect.changeset.is_empty() {
        return Err(Error::InvalidCommand(
            "SQL effect changeset is empty".into(),
        ));
    }
    decode_sql_result(&effect.result_blob)?;
    let canonical = serde_json::to_vec(&effect)
        .map_err(|error| Error::InvalidCommand(format!("cannot encode SQL effect: {error}")))?;
    if canonical != encoded {
        return Err(Error::InvalidCommand(
            "SQL effect envelope is not canonical".into(),
        ));
    }
    Ok(effect)
}

fn apply_sql_effect(
    conn: &Connection,
    effect: &SqlEffectEnvelope,
    log_index: LogIndex,
    log_hash: LogHash,
) -> Result<Option<SqlCommandResult>> {
    let result = decode_sql_result(&effect.result_blob)?;
    if let Some(record) = matching_request_digest(conn, &effect.request_id, effect.request_digest)?
    {
        let stored = stored_sql_result(SqlCommandVersion::V2, &record)?
            .ok_or_else(|| Error::Sqlite("SQL effect request result is missing".into()))?;
        if stored != result {
            return Err(Error::Sqlite(
                "SQL effect result differs from the stored request result".into(),
            ));
        }
        return Ok(Some(stored));
    }
    if meta_u64(conn, MetaKey::AppliedIndex)? != effect.base_index
        || meta_hash(conn, MetaKey::AppliedHash)? != effect.base_hash
    {
        return Err(Error::InvalidEntry(
            "SQL effect base does not match the materialized SQLite tip".into(),
        ));
    }
    if effect.executor_fingerprint != sql_executor_fingerprint()? {
        return Err(Error::InvalidEntry(
            "SQL effect executor fingerprint does not match the local executor".into(),
        ));
    }
    validate_effect_environment(conn)?;
    validate_changeset_tables(conn, &effect.changeset)?;
    let mut changeset = Cursor::new(effect.changeset.as_slice());
    conn.apply_strm(&mut changeset, None::<fn(&str) -> bool>, |_, _| {
        ConflictAction::SQLITE_CHANGESET_ABORT
    })
    .map_err(sqlite_error)?;
    validate_reserved_schema(conn)?;
    record_request_digest(
        conn,
        &effect.request_id,
        log_index,
        log_hash,
        effect.request_digest,
        Some(&effect.result_blob),
    )?;
    Ok(Some(result))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EffectTableShape {
    column_count: usize,
    primary_key: Vec<u8>,
}

fn effect_table_shape(conn: &Connection, table: &str) -> Result<EffectTableShape> {
    if reserved_name(table) || table.to_ascii_lowercase().starts_with("sqlite_") {
        return Err(Error::InvalidCommand(format!(
            "SQL effect cannot change internal table {table}"
        )));
    }
    let schema_sql = conn
        .query_row(
            "SELECT sql FROM sqlite_schema
             WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
            params![table],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .flatten()
        .ok_or_else(|| Error::InvalidCommand(format!("SQL effect table {table} is missing")))?;
    let normalized = schema_sql.trim_start().to_ascii_uppercase();
    if normalized.starts_with("CREATE VIRTUAL TABLE") {
        return Err(Error::InvalidCommand(format!(
            "SQL effect does not support virtual table {table}"
        )));
    }
    if normalized.contains("AUTOINCREMENT") {
        return Err(Error::InvalidCommand(format!(
            "SQL effect does not support AUTOINCREMENT table {table}"
        )));
    }

    let mut statement = conn
        .prepare(
            "SELECT type, \"notnull\", pk, hidden
             FROM pragma_table_xinfo(?1)
             ORDER BY cid",
        )
        .map_err(sqlite_error)?;
    let columns = statement
        .query_map(params![table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    if columns.is_empty() {
        return Err(Error::InvalidCommand(format!(
            "SQL effect table {table} has no columns"
        )));
    }
    if columns.iter().any(|column| column.3 != 0) {
        return Err(Error::InvalidCommand(format!(
            "SQL effect does not support generated or hidden columns in table {table}"
        )));
    }
    let primary_key_count = columns.iter().filter(|column| column.2 != 0).count();
    if primary_key_count == 0 {
        return Err(Error::InvalidCommand(format!(
            "SQL effect table {table} has no explicit primary key"
        )));
    }
    let primary_key_indexes: i64 = conn
        .query_row(
            "SELECT count(*) FROM pragma_index_list(?1) WHERE origin = 'pk'",
            params![table],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    for (declared_type, not_null, primary_key, _) in &columns {
        if *primary_key == 0 || *not_null != 0 {
            continue;
        }
        let rowid_alias = primary_key_count == 1
            && primary_key_indexes == 0
            && declared_type.trim().eq_ignore_ascii_case("INTEGER");
        if !rowid_alias {
            return Err(Error::InvalidCommand(format!(
                "SQL effect table {table} has a nullable primary-key column"
            )));
        }
    }
    let primary_key = columns
        .iter()
        .map(|column| u8::from(column.2 != 0))
        .collect();
    Ok(EffectTableShape {
        column_count: columns.len(),
        primary_key,
    })
}

fn validate_changeset_tables(conn: &Connection, changeset: &[u8]) -> Result<BTreeSet<String>> {
    if changeset.is_empty() {
        return Err(Error::InvalidCommand(
            "SQL effect produced an empty changeset".into(),
        ));
    }
    let mut input = Cursor::new(changeset);
    let input_ref: &mut dyn std::io::Read = &mut input;
    let mut iterator = ChangesetIter::start_strm(&input_ref).map_err(sqlite_error)?;
    let mut schemas = BTreeMap::<String, EffectTableShape>::new();
    while let Some(item) = iterator.next().map_err(sqlite_error)? {
        let operation = item.op().map_err(sqlite_error)?;
        if operation.indirect() {
            return Err(Error::InvalidCommand(
                "SQL effect contains an indirect trigger or foreign-key change".into(),
            ));
        }
        let table = operation.table_name().to_owned();
        let column_count = usize::try_from(operation.number_of_columns())
            .map_err(|_| Error::InvalidCommand("negative changeset column count".into()))?;
        let encoded = EffectTableShape {
            column_count,
            primary_key: item
                .pk()
                .map_err(sqlite_error)?
                .iter()
                .map(|column| u8::from(*column != 0))
                .collect(),
        };
        let expected = effect_table_shape(conn, &table)?;
        if encoded != expected {
            return Err(Error::InvalidCommand(format!(
                "SQL changeset does not exactly match table {table}"
            )));
        }
        if schemas
            .insert(table.clone(), encoded.clone())
            .is_some_and(|known| known != encoded)
        {
            return Err(Error::InvalidCommand(format!(
                "SQL changeset schema changes within table {table}"
            )));
        }
    }
    if schemas.is_empty() {
        return Err(Error::InvalidCommand(
            "SQL effect produced no complete row changes".into(),
        ));
    }
    Ok(schemas.into_keys().collect())
}

fn validate_effect_environment(conn: &Connection) -> Result<()> {
    let trigger: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_schema WHERE type = 'trigger' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some(trigger) = trigger {
        return Err(Error::InvalidCommand(format!(
            "SQL effect mode is gated while trigger {trigger} exists"
        )));
    }
    let mut statement = conn
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type = 'table' AND lower(name) NOT GLOB 'sqlite_*'",
        )
        .map_err(sqlite_error)?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    for table in tables {
        let foreign_keys: i64 = conn
            .query_row(
                "SELECT count(*) FROM pragma_foreign_key_list(?1)",
                params![table],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        if foreign_keys != 0 {
            return Err(Error::InvalidCommand(format!(
                "SQL effect mode is gated while table {table} has foreign keys"
            )));
        }
    }
    Ok(())
}

#[derive(Default)]
struct SqlEffectAudit {
    saw_ddl: bool,
    write_tables: BTreeSet<String>,
}

fn prepare_sql_effect_in_transaction(
    conn: &Connection,
    command: &SqlCommand,
    request_payload: &[u8],
    base_index: LogIndex,
    base_hash: LogHash,
) -> Result<(SqlEffectPreparation, SqlCommandResult)> {
    let mut session = Session::new(conn).map_err(sqlite_error)?;
    attach_session_tables(conn, &mut session)?;
    let audit = Arc::new(Mutex::new(SqlEffectAudit::default()));
    let (result, touched_tables) = with_update_audit(conn, || {
        execute_sql_statements_with_audit(
            conn,
            &command.statements,
            SqlCommandVersion::V2,
            Arc::clone(&audit),
        )
    })?;
    let has_returning = result
        .statement_results
        .iter()
        .any(|statement| statement.returning.is_some());
    let effect = build_sql_effect(
        conn,
        &mut session,
        audit,
        SqlEffectBuild {
            touched_tables,
            base_index,
            base_hash,
            command,
            request_payload,
            result: &result,
        },
    );
    drop(session);
    let preparation = match effect {
        Ok(payload) => SqlEffectPreparation::Effect(payload),
        Err(_) if !has_returning => SqlEffectPreparation::StatementReplay,
        Err(error) => {
            return Err(Error::InvalidCommand(format!(
                "SQL RETURNING requires a complete changeset effect: {error}"
            )))
        }
    };
    Ok((preparation, result))
}

fn execute_sql_statements(
    conn: &Connection,
    statements: &[SqlStatement],
    version: SqlCommandVersion,
    mode: SqlExecutionMode,
) -> Result<SqlCommandResult> {
    execute_sql_statements_inner(conn, statements, version, mode, None)
}

fn execute_sql_statements_with_audit(
    conn: &Connection,
    statements: &[SqlStatement],
    version: SqlCommandVersion,
    audit: Arc<Mutex<SqlEffectAudit>>,
) -> Result<SqlCommandResult> {
    execute_sql_statements_inner(
        conn,
        statements,
        version,
        SqlExecutionMode::EffectPreparation,
        Some(audit),
    )
}

fn execute_sql_statements_inner(
    conn: &Connection,
    statements: &[SqlStatement],
    version: SqlCommandVersion,
    mode: SqlExecutionMode,
    audit: Option<Arc<Mutex<SqlEffectAudit>>>,
) -> Result<SqlCommandResult> {
    let result = with_sql_authorizer(
        conn,
        audit,
        SqlAuthorizationMode::DeterministicWrite,
        || {
            let mut statement_results = Vec::with_capacity(statements.len());
            let mut returning_rows = 0usize;
            let mut returning_bytes = 0usize;
            for operation in statements {
                validate_deterministic_bytecode(conn, operation)?;
                let mut statement = conn.prepare(&operation.sql).map_err(sqlite_error)?;
                if statement.readonly() {
                    return Err(Error::InvalidCommand(
                        "replicated SQL statements must mutate the database".into(),
                    ));
                }
                let column_count = statement.column_count();
                if column_count != 0 {
                    match (version, mode) {
                        (SqlCommandVersion::V1, _) => {
                            return Err(Error::InvalidCommand(
                                "replicated SQL does not support RETURNING; query separately"
                                    .into(),
                            ));
                        }
                        (SqlCommandVersion::V2, SqlExecutionMode::StatementReplay) => {
                            return Err(Error::InvalidCommand(
                                "QSQL v2 RETURNING must be prepared as a QEFX effect".into(),
                            ));
                        }
                        (SqlCommandVersion::V2, SqlExecutionMode::EffectPreparation) => {}
                    }
                }
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
        },
    )?;
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

fn record_request(
    conn: &Connection,
    request_id: &str,
    log_index: LogIndex,
    log_hash: LogHash,
    payload: &[u8],
    result_blob: Option<&[u8]>,
) -> Result<()> {
    record_request_digest(
        conn,
        request_id,
        log_index,
        log_hash,
        LogHash::digest(&[payload]),
        result_blob,
    )
}

fn record_request_digest(
    conn: &Connection,
    request_id: &str,
    log_index: LogIndex,
    log_hash: LogHash,
    digest: LogHash,
    result_blob: Option<&[u8]>,
) -> Result<()> {
    let log_index = i64::try_from(log_index)
        .map_err(|_| Error::InvalidEntry("request log index exceeds SQLite INTEGER".into()))?;
    conn.execute(
        "INSERT INTO __rhiza_requests(
             request_id,
             original_log_index,
             original_log_hash,
             command_digest,
             result_blob
         )
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            request_id,
            log_index,
            log_hash.as_bytes().as_slice(),
            digest.as_bytes().as_slice(),
            result_blob,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn decode_sql_command(payload: &[u8]) -> Result<(SqlCommandVersion, SqlCommand)> {
    let (version, command) = if let Some(encoded) = payload.strip_prefix(SQL_COMMAND_V1_MAGIC) {
        let command = serde_json::from_slice(encoded)
            .map_err(|error| Error::InvalidCommand(format!("invalid SQL command: {error}")))?;
        (SqlCommandVersion::V1, command)
    } else if let Some(encoded) = payload.strip_prefix(SQL_COMMAND_V2_MAGIC) {
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
        (SqlCommandVersion::V2, envelope.command)
    } else {
        return Err(Error::InvalidCommand("SQL command magic is missing".into()));
    };
    validate_sql_command(&command)?;
    Ok((version, command))
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
    audit: Option<Arc<Mutex<SqlEffectAudit>>>,
    mode: SqlAuthorizationMode,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let effect_capture = audit.is_some();
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        if let Some(audit) = audit.as_ref() {
            record_sql_effect_action(audit, &context.action);
        }
        let session_schema_read = effect_capture
            && matches!(
                &context.action,
                AuthAction::Pragma { pragma_name, .. }
                    if pragma_name.eq_ignore_ascii_case("table_xinfo")
            );
        if session_schema_read {
            Authorization::Allow
        } else {
            authorize_sql(context, mode)
        }
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

fn record_sql_effect_action(audit: &Arc<Mutex<SqlEffectAudit>>, action: &AuthAction<'_>) {
    let Ok(mut audit) = audit.lock() else {
        return;
    };
    match action {
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. } => {
            audit.write_tables.insert((*table_name).to_owned());
        }
        AuthAction::CreateIndex { .. }
        | AuthAction::CreateTable { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. }
        | AuthAction::CreateTrigger { .. }
        | AuthAction::CreateView { .. }
        | AuthAction::DropIndex { .. }
        | AuthAction::DropTable { .. }
        | AuthAction::DropTempIndex { .. }
        | AuthAction::DropTempTable { .. }
        | AuthAction::DropTempTrigger { .. }
        | AuthAction::DropTempView { .. }
        | AuthAction::DropTrigger { .. }
        | AuthAction::DropView { .. }
        | AuthAction::AlterTable { .. }
        | AuthAction::Reindex { .. }
        | AuthAction::Analyze { .. }
        | AuthAction::CreateVtable { .. }
        | AuthAction::DropVtable { .. } => audit.saw_ddl = true,
        _ => {}
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
        | AuthAction::Pragma { .. }
        | AuthAction::Transaction { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::CreateVtable { .. }
        | AuthAction::DropVtable { .. }
        | AuthAction::Savepoint { .. } => Authorization::Deny,
        AuthAction::Function { function_name } if unsafe_sql_function(function_name) => {
            Authorization::Deny
        }
        AuthAction::Function { function_name }
            if mode == SqlAuthorizationMode::DeterministicWrite
                && nondeterministic_function(function_name) =>
        {
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
        AuthAction::CreateTable { table_name }
        | AuthAction::CreateTrigger { table_name, .. }
        | AuthAction::Delete { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropTrigger { table_name, .. }
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

fn reserved_name(name: &str) -> bool {
    name.to_ascii_lowercase().starts_with("__rhiza_")
}

fn validate_reserved_schema(conn: &Connection) -> Result<()> {
    let unexpected: Option<String> = conn
        .query_row(
            "SELECT name
             FROM sqlite_schema
             WHERE lower(name) GLOB '__rhiza_*'
               AND name NOT IN (
                   '__rhiza_meta',
                   '__rhiza_migrations',
                   '__rhiza_requests',
                   '__rhiza_kv'
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

fn nondeterministic_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "random"
            | "randomblob"
            | "current_date"
            | "current_time"
            | "current_timestamp"
            | "date"
            | "time"
            | "datetime"
            | "julianday"
            | "unixepoch"
            | "strftime"
            | "timediff"
            | "changes"
            | "total_changes"
            | "last_insert_rowid"
            | "load_extension"
            | "sqlite_compileoption_get"
            | "sqlite_compileoption_used"
            | "sqlite_offset"
            | "sqlite_source_id"
            | "sqlite_version"
    )
}

fn unsafe_sql_function(name: &str) -> bool {
    name.eq_ignore_ascii_case("load_extension")
}

fn validate_deterministic_bytecode(conn: &Connection, statement: &SqlStatement) -> Result<()> {
    let mut explain = conn
        .prepare(&format!("EXPLAIN {}", statement.sql))
        .map_err(sqlite_error)?;
    let mut rows = explain
        .query(params_from_iter(statement.parameters.iter()))
        .map_err(sqlite_error)?;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let opcode: String = row.get(1).map_err(sqlite_error)?;
        if opcode == "Function" || opcode == "PureFunc" {
            let detail: Option<String> = row.get(5).map_err(sqlite_error)?;
            let function = detail
                .as_deref()
                .unwrap_or_default()
                .split('(')
                .next()
                .unwrap_or_default();
            if nondeterministic_function(function) {
                return Err(Error::InvalidCommand(format!(
                    "nondeterministic SQL function is forbidden: {function}"
                )));
            }
        }
    }
    Ok(())
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

struct StoredRequest {
    outcome: RequestOutcome,
    command_digest: LogHash,
    result_blob: Option<Vec<u8>>,
}

fn request_record(conn: &Connection, request_id: &str) -> Result<Option<StoredRequest>> {
    let record = conn
        .query_row(
            "SELECT original_log_index, original_log_hash, command_digest, result_blob
             FROM __rhiza_requests
             WHERE request_id = ?1",
            params![request_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;
    let Some((log_index, log_hash, command_digest, result_blob)) = record else {
        return Ok(None);
    };
    let log_index = u64::try_from(log_index)
        .map_err(|_| Error::Sqlite("invalid request original_log_index".into()))?;
    let log_hash = request_hash(log_hash, "original_log_hash")?;
    let command_digest = request_hash(command_digest, "command_digest")?;
    Ok(Some(StoredRequest {
        outcome: RequestOutcome::new(log_index, log_hash),
        command_digest,
        result_blob,
    }))
}

fn matching_request(
    conn: &Connection,
    request_id: &str,
    payload: &[u8],
) -> Result<Option<StoredRequest>> {
    let Some(record) = request_record(conn, request_id)? else {
        return Ok(None);
    };
    if record.command_digest == LogHash::digest(&[payload]) {
        return Ok(Some(record));
    }
    Err(Error::RequestConflict(RequestConflict {
        request_id: request_id.into(),
        original_outcome: record.outcome,
    }))
}

fn matching_request_digest(
    conn: &Connection,
    request_id: &str,
    digest: LogHash,
) -> Result<Option<StoredRequest>> {
    let Some(record) = request_record(conn, request_id)? else {
        return Ok(None);
    };
    if record.command_digest == digest {
        return Ok(Some(record));
    }
    Err(Error::RequestConflict(RequestConflict {
        request_id: request_id.into(),
        original_outcome: record.outcome,
    }))
}

fn stored_sql_result(
    version: SqlCommandVersion,
    record: &StoredRequest,
) -> Result<Option<SqlCommandResult>> {
    match version {
        SqlCommandVersion::V1 => Ok(None),
        SqlCommandVersion::V2 => record
            .result_blob
            .as_deref()
            .ok_or_else(|| Error::Sqlite("QSQL v2 request result is missing".into()))
            .and_then(decode_sql_result)
            .map(Some),
    }
}

fn encode_sql_result(result: &SqlCommandResult) -> Result<Vec<u8>> {
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
    if encode_sql_result(&result)? != blob {
        return Err(Error::Sqlite("SQL result blob is not canonical".into()));
    }
    Ok(result)
}

fn request_hash(value: Vec<u8>, column: &str) -> Result<LogHash> {
    let bytes = value
        .try_into()
        .map_err(|_| Error::Sqlite(format!("invalid request {column}")))?;
    Ok(LogHash::from_bytes(bytes))
}

fn put_value(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO __rhiza_kv(key, value)
         VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn validate_initialized(conn: &Connection) -> Result<()> {
    for key in REQUIRED_META_KEYS {
        if get_meta(conn, key)?.is_none() {
            return Err(Error::Sqlite(format!("missing meta key {}", key.as_str())));
        }
    }
    if meta_string(conn, MetaKey::ClusterId)?.is_empty() {
        return Err(Error::Sqlite("cluster_id meta value is empty".into()));
    }
    if meta_string(conn, MetaKey::NodeId)?.is_empty() {
        return Err(Error::Sqlite("node_id meta value is empty".into()));
    }
    meta_u64(conn, MetaKey::Epoch)?;
    meta_u64(conn, MetaKey::ConfigId)?;
    let configuration_state = meta_configuration_state(conn)?;
    if configuration_state.config_id() != meta_u64(conn, MetaKey::ConfigId)? {
        return Err(Error::Sqlite(
            "configuration_state does not match config_id".into(),
        ));
    }
    meta_u64(conn, MetaKey::AppliedIndex)?;
    meta_hash(conn, MetaKey::AppliedHash)?;
    meta_string(conn, MetaKey::SnapshotId)?;
    meta_u64(conn, MetaKey::CreatedAt)?;
    let schema_version = meta_u64(conn, MetaKey::SchemaVersion)?;
    if schema_version != SCHEMA_VERSION {
        return Err(Error::Sqlite(format!(
            "unsupported schema version {schema_version}"
        )));
    }
    conn.prepare("SELECT key, value FROM __rhiza_kv LIMIT 0")
        .map_err(sqlite_error)?;
    validate_requests_schema(conn, schema_version)?;
    Ok(())
}

fn validate_requests_schema(conn: &Connection, schema_version: u64) -> Result<()> {
    let mut statement = conn
        .prepare("PRAGMA table_info(__rhiza_requests)")
        .map_err(sqlite_error)?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?.to_ascii_uppercase(),
                row.get::<_, i64>(3)? != 0,
                row.get::<_, i64>(5)? != 0,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    let v1_columns = [
        ("request_id".into(), "TEXT".into(), false, true),
        ("original_log_index".into(), "INTEGER".into(), true, false),
        ("original_log_hash".into(), "BLOB".into(), true, false),
        ("command_digest".into(), "BLOB".into(), true, false),
    ];
    let valid = match schema_version {
        LEGACY_SCHEMA_VERSION => columns == v1_columns,
        REQUEST_RESULTS_SCHEMA_VERSION | SCHEMA_VERSION => {
            let mut expected = v1_columns.to_vec();
            expected.push(("result_blob".into(), "BLOB".into(), false, false));
            columns == expected
        }
        _ => false,
    };
    if valid {
        return Ok(());
    }
    Err(Error::Sqlite(
        "incompatible __rhiza_requests schema; recreate this prototype database".into(),
    ))
}

fn validate_snapshot(conn: &Connection, manifest: &SnapshotManifest) -> Result<()> {
    integrity_check(conn)?;
    let expected_fingerprint = sql_executor_fingerprint().map_err(snapshot_validation_error)?;
    if manifest.executor_fingerprint() != Some(expected_fingerprint) {
        return Err(Error::InvalidSnapshot(format!(
            "snapshot executor fingerprint {:?} does not match local {}",
            manifest.executor_fingerprint().map(|hash| hash.to_hex()),
            expected_fingerprint.to_hex()
        )));
    }
    validate_initialized(conn).map_err(|err| Error::InvalidSnapshot(err.to_string()))?;
    validate_snapshot_text(conn, MetaKey::ClusterId, manifest.cluster_id())?;
    validate_snapshot_integer(conn, MetaKey::Epoch, manifest.epoch())?;
    validate_snapshot_integer(conn, MetaKey::ConfigId, manifest.config_id())?;
    let actual_configuration = meta_configuration_state(conn).map_err(snapshot_validation_error)?;
    if actual_configuration != *manifest.configuration_state() {
        return Err(Error::InvalidSnapshot(
            "manifest configuration_state does not match database value".into(),
        ));
    }
    validate_snapshot_integer(conn, MetaKey::SchemaVersion, manifest.schema_version())?;
    validate_snapshot_integer(conn, MetaKey::AppliedIndex, manifest.index())?;
    validate_snapshot_hash(conn, MetaKey::AppliedHash, manifest.applied_hash())?;
    validate_snapshot_text(conn, MetaKey::NodeId, manifest.created_by())?;
    validate_snapshot_text(conn, MetaKey::SnapshotId, manifest.snapshot_id())
}

fn validate_snapshot_text(conn: &Connection, key: MetaKey, expected: &str) -> Result<()> {
    let actual = meta_string(conn, key).map_err(snapshot_validation_error)?;
    if actual == expected {
        Ok(())
    } else {
        Err(snapshot_identity_error(key, expected, &actual))
    }
}

fn validate_snapshot_integer(conn: &Connection, key: MetaKey, expected: u64) -> Result<()> {
    let actual = meta_u64(conn, key).map_err(snapshot_validation_error)?;
    if actual == expected {
        Ok(())
    } else {
        Err(snapshot_identity_error(
            key,
            &expected.to_string(),
            &actual.to_string(),
        ))
    }
}

fn validate_snapshot_hash(conn: &Connection, key: MetaKey, expected: LogHash) -> Result<()> {
    let actual = meta_hash(conn, key).map_err(snapshot_validation_error)?;
    if actual == expected {
        Ok(())
    } else {
        Err(snapshot_identity_error(
            key,
            &expected.to_hex(),
            &actual.to_hex(),
        ))
    }
}

fn snapshot_validation_error(error: Error) -> Error {
    Error::InvalidSnapshot(error.to_string())
}

fn snapshot_identity_error(key: MetaKey, expected: &str, actual: &str) -> Error {
    Error::InvalidSnapshot(format!(
        "manifest {} {expected} does not match database value {actual}",
        key.as_str()
    ))
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

fn validate_text_identity(conn: &Connection, key: MetaKey, expected: &str) -> Result<()> {
    if meta_string(conn, key)? == expected {
        Ok(())
    } else {
        Err(Error::IdentityMismatch(key.as_str().into()))
    }
}

fn validate_integer_identity(conn: &Connection, key: MetaKey, expected: u64) -> Result<()> {
    if meta_u64(conn, key)? == expected {
        Ok(())
    } else {
        Err(Error::IdentityMismatch(key.as_str().into()))
    }
}

fn identity_as_entry_error(error: Error) -> Error {
    match error {
        Error::IdentityMismatch(field) => {
            Error::InvalidEntry(format!("{field} does not match database identity"))
        }
        error => error,
    }
}

fn put_meta(conn: &Connection, key: MetaKey, value: &[u8]) -> Result<()> {
    conn.execute(
        "INSERT INTO __rhiza_meta(key, value)
         VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key.as_str(), value],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn get_meta(conn: &Connection, key: MetaKey) -> Result<Option<Vec<u8>>> {
    conn.query_row(
        "SELECT value FROM __rhiza_meta WHERE key = ?1",
        params![key.as_str()],
        |row| row.get(0),
    )
    .optional()
    .map_err(sqlite_error)
}

fn put_configuration_state(conn: &Connection, state: &ConfigurationState) -> Result<()> {
    let encoded = serde_json::to_vec(state)
        .map_err(|error| Error::Sqlite(format!("cannot encode configuration_state: {error}")))?;
    let mut value = Vec::with_capacity(CONFIGURATION_STATE_V2_MAGIC.len() + encoded.len());
    value.extend_from_slice(CONFIGURATION_STATE_V2_MAGIC);
    value.extend_from_slice(&encoded);
    put_meta(conn, MetaKey::ConfigurationState, &value)
}

fn meta_configuration_state(conn: &Connection) -> Result<ConfigurationState> {
    let value = get_meta(conn, MetaKey::ConfigurationState)?
        .ok_or_else(|| Error::Sqlite("missing meta key configuration_state".into()))?;
    decode_configuration_state(&value)
}

fn decode_configuration_state(value: &[u8]) -> Result<ConfigurationState> {
    if let Some(encoded) = value.strip_prefix(CONFIGURATION_STATE_V2_MAGIC) {
        return serde_json::from_slice(encoded).map_err(|error| {
            Error::Sqlite(format!("invalid configuration_state meta value: {error}"))
        });
    }
    let config_id = read_state_u64(value, 1)?;
    let digest = read_state_hash(value, 9)?;
    match value.first() {
        Some(1) if value.len() == 41 => Ok(ConfigurationState::active(config_id, digest)),
        Some(2) if value.len() == 81 => Err(Error::Sqlite(
            "ambiguous legacy stopped configuration_state cannot be migrated safely".into(),
        )),
        _ => Err(Error::Sqlite(
            "invalid configuration_state meta value".into(),
        )),
    }
}

fn read_state_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| Error::Sqlite("invalid configuration_state meta value".into()))?;
    Ok(u64::from_be_bytes(
        value.try_into().expect("u64 slice length"),
    ))
}

fn read_state_hash(bytes: &[u8], offset: usize) -> Result<LogHash> {
    let value = bytes
        .get(offset..offset + 32)
        .ok_or_else(|| Error::Sqlite("invalid configuration_state meta value".into()))?;
    Ok(LogHash::from_bytes(
        value.try_into().expect("hash slice length"),
    ))
}

fn meta_string(conn: &Connection, key: MetaKey) -> Result<String> {
    decode_meta_string(key, get_meta(conn, key)?)
}

fn decode_meta_string(key: MetaKey, value: Option<Vec<u8>>) -> Result<String> {
    let value = value.ok_or_else(|| Error::Sqlite(format!("missing meta key {}", key.as_str())))?;
    String::from_utf8(value)
        .map_err(|err| Error::Sqlite(format!("invalid {} utf8: {err}", key.as_str())))
}

fn meta_u64(conn: &Connection, key: MetaKey) -> Result<u64> {
    decode_meta_u64(key, get_meta(conn, key)?)
}

fn decode_meta_u64(key: MetaKey, value: Option<Vec<u8>>) -> Result<u64> {
    decode_meta_string(key, value)?
        .parse()
        .map_err(|err| Error::Sqlite(format!("invalid {} integer value: {err}", key.as_str())))
}

fn meta_hash(conn: &Connection, key: MetaKey) -> Result<LogHash> {
    decode_meta_hash(key, get_meta(conn, key)?)
}

fn decode_meta_hash(key: MetaKey, value: Option<Vec<u8>>) -> Result<LogHash> {
    let value = decode_meta_string(key, value)?;
    LogHash::from_hex(&value)
        .ok_or_else(|| Error::Sqlite(format!("invalid {} hash value", key.as_str())))
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

    #[test]
    fn applied_tip_returns_index_and_hash_from_the_same_database_state() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let payload = b"put\trequest-1\tkey-1\tvalue-1";
        let hash = LogEntry::calculate_hash(
            "cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            payload,
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

        assert_eq!(database.applied_tip_value().unwrap(), (1, hash));
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
    fn bounded_batch_preparation_visits_each_candidate_once() {
        let dir = tempfile::tempdir().unwrap();
        let database =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let value = "x".repeat(1024);
        let members = (0..8)
            .map(|index| format!("put\trequest-{index}\tkey-{index}\t{value}").into_bytes())
            .collect::<Vec<_>>();
        let max_payload_bytes = encode_write_batch(&members[..4]).unwrap().len();
        let changes_before = database.conn.total_changes();

        let (member_count, payload) = database
            .prepare_write_batch_prefix(
                &members,
                0,
                database.applied_hash_value().unwrap(),
                max_payload_bytes,
            )
            .unwrap()
            .unwrap();

        assert_eq!(member_count, 4);
        assert_eq!(payload, encode_write_batch(&members[..4]).unwrap());
        assert_eq!(database.conn.total_changes() - changes_before, 10);
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
    fn replicated_write_still_rejects_nondeterministic_functions() {
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

        assert!(matches!(
            database.validate_sql_write(&command),
            Err(Error::InvalidCommand(_)) | Err(Error::Sqlite(_))
        ));
        assert_eq!(database.applied_index_value().unwrap(), 0);
    }
}
