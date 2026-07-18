//! Durable control-plane state for physical SQLite effects.
//!
//! This database is deliberately separate from the canonical user database so
//! node-local identity and qlog progress cannot change replicated user pages.

use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use rhiza_core::{ConfigurationState, LogAnchor, LogHash};
use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};

use super::{ApplyProgress, Error, RequestConflict, RequestOutcome, Result};

const CONTROL_MAGIC: &[u8] = b"RHIZA-SQL-CONTROL\0\x01";
const CONTROL_SCHEMA_VERSION: u64 = 1;
const SNAPSHOT_MAGIC: &[u8] = b"QCTL\0\x01";
const MAX_RESULT_BLOB_BYTES: usize = super::MAX_SQL_EFFECT_BYTES;

const CREATE_CONTROL_META_SQL: &str = r#"CREATE TABLE control_meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
) WITHOUT ROWID;"#;
const CREATE_REQUEST_RECEIPTS_SQL: &str = r#"CREATE TABLE request_receipts (
    request_id TEXT PRIMARY KEY,
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    original_log_index INTEGER NOT NULL CHECK(original_log_index >= 0),
    original_log_hash BLOB NOT NULL CHECK(length(original_log_hash) = 32),
    result_blob BLOB NOT NULL
) WITHOUT ROWID;"#;
const CREATE_PENDING_APPLY_SQL: &str = r#"CREATE TABLE pending_apply (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    base_index INTEGER NOT NULL CHECK(base_index >= 0),
    base_hash BLOB NOT NULL CHECK(length(base_hash) = 32),
    entry_index INTEGER NOT NULL CHECK(entry_index > 0),
    entry_hash BLOB NOT NULL CHECK(length(entry_hash) = 32),
    base_db_digest BLOB NOT NULL CHECK(length(base_db_digest) = 32),
    target_db_digest BLOB NOT NULL CHECK(length(target_db_digest) = 32),
    target_file_bytes INTEGER NOT NULL CHECK(target_file_bytes >= 0)
);"#;

const REQUIRED_META_KEYS: [&str; 10] = [
    "magic",
    "schema_version",
    "cluster_id",
    "node_id",
    "epoch",
    "configuration_state",
    "recovery_generation",
    "materializer_fingerprint",
    "user_db_digest",
    "applied_tip",
];

type ExpectedColumn = (&'static str, &'static str, bool, i64);

const CONTROL_META_COLUMNS: &[ExpectedColumn] =
    &[("key", "TEXT", true, 1), ("value", "BLOB", true, 0)];
const REQUEST_RECEIPT_COLUMNS: &[ExpectedColumn] = &[
    ("request_id", "TEXT", true, 1),
    ("request_digest", "BLOB", true, 0),
    ("original_log_index", "INTEGER", true, 0),
    ("original_log_hash", "BLOB", true, 0),
    ("result_blob", "BLOB", true, 0),
];
const PENDING_APPLY_COLUMNS: &[ExpectedColumn] = &[
    ("singleton", "INTEGER", false, 1),
    ("base_index", "INTEGER", true, 0),
    ("base_hash", "BLOB", true, 0),
    ("entry_index", "INTEGER", true, 0),
    ("entry_hash", "BLOB", true, 0),
    ("base_db_digest", "BLOB", true, 0),
    ("target_db_digest", "BLOB", true, 0),
    ("target_file_bytes", "INTEGER", true, 0),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlIdentity {
    cluster_id: String,
    node_id: String,
    epoch: u64,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    materializer_fingerprint: LogHash,
    user_db_digest: LogHash,
}

impl ControlIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        epoch: u64,
        configuration_state: ConfigurationState,
        recovery_generation: u64,
        materializer_fingerprint: LogHash,
        user_db_digest: LogHash,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            epoch,
            configuration_state,
            recovery_generation,
            materializer_fingerprint,
            user_db_digest,
        }
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }
    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }
    pub const fn materializer_fingerprint(&self) -> LogHash {
        self.materializer_fingerprint
    }
    pub const fn user_db_digest(&self) -> LogHash {
        self.user_db_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RequestReceipt {
    request_id: String,
    request_digest: LogHash,
    original_anchor: LogAnchor,
    result_blob: Vec<u8>,
}

impl RequestReceipt {
    pub fn new(
        request_id: impl Into<String>,
        request_digest: LogHash,
        original_anchor: LogAnchor,
        result_blob: Vec<u8>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            request_digest,
            original_anchor,
            result_blob,
        }
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub const fn request_digest(&self) -> LogHash {
        self.request_digest
    }
    pub const fn original_anchor(&self) -> LogAnchor {
        self.original_anchor
    }
    pub fn result_blob(&self) -> &[u8] {
        &self.result_blob
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingApply {
    base: LogAnchor,
    entry: LogAnchor,
    base_db_digest: LogHash,
    target_db_digest: LogHash,
    target_file_bytes: u64,
}

impl PendingApply {
    pub const fn new(
        base: LogAnchor,
        entry: LogAnchor,
        base_db_digest: LogHash,
        target_db_digest: LogHash,
        target_file_bytes: u64,
    ) -> Self {
        Self {
            base,
            entry,
            base_db_digest,
            target_db_digest,
            target_file_bytes,
        }
    }

    pub const fn base(&self) -> LogAnchor {
        self.base
    }
    pub const fn entry(&self) -> LogAnchor {
        self.entry
    }
    pub const fn base_db_digest(&self) -> LogHash {
        self.base_db_digest
    }
    pub const fn target_db_digest(&self) -> LogHash {
        self.target_db_digest
    }
    pub const fn target_file_bytes(&self) -> u64 {
        self.target_file_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicatedSnapshot {
    cluster_id: String,
    epoch: u64,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    materializer_fingerprint: LogHash,
    user_db_digest: LogHash,
    applied_tip: LogAnchor,
    receipts: Vec<RequestReceipt>,
}

pub struct ControlStore {
    path: PathBuf,
    conn: Connection,
}

impl ControlStore {
    /// Opens and validates an existing sidecar, or creates it if absent.
    pub fn open(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            Self::open_existing(path, identity)
        } else {
            Self::create(path, identity)
        }
    }

    pub fn create(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        validate_new_identity(identity)?;
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| Error::Io(error.to_string()))?;
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| Error::Io(error.to_string()))?;

        let store = match Self::open_file(path) {
            Ok(store) => store,
            Err(error) => {
                let _ = std::fs::remove_file(path);
                return Err(error);
            }
        };
        if let Err(error) = store.initialize(identity) {
            drop(store);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        sync_parent(path)?;
        Ok(store)
    }

    pub fn open_existing(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        validate_new_identity(identity)?;
        let store = Self::open_existing_unchecked(path)?;
        store.validate_identity(identity)?;
        Ok(store)
    }

    /// Opens a sidecar and validates its durable format without imposing an
    /// expected runtime identity. This is used by recovery paths that must load
    /// the persisted identity before they can validate the paired user DB.
    pub fn open_existing_unchecked(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self::open_file(path.as_ref())?;
        store.validate_schema()?;
        Ok(store)
    }

    pub fn read_identity(path: impl AsRef<Path>) -> Result<ControlIdentity> {
        Self::open_existing_unchecked(path)?.identity()
    }

    fn open_file(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sqlite_error)?;
        let journal: String = conn
            .query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if !journal.eq_ignore_ascii_case("delete") {
            return Err(Error::Sqlite(format!(
                "control sidecar refused DELETE journal mode: {journal}"
            )));
        }
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite_error)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(sqlite_error)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sqlite_error)?;
        Ok(Self {
            path: path.to_path_buf(),
            conn,
        })
    }

    fn initialize(&self, identity: &ControlIdentity) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_CONTROL_META_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_REQUEST_RECEIPTS_SQL)
            .map_err(sqlite_error)?;
        tx.execute_batch(CREATE_PENDING_APPLY_SQL)
            .map_err(sqlite_error)?;
        put_meta(&tx, "magic", CONTROL_MAGIC)?;
        put_u64(&tx, "schema_version", CONTROL_SCHEMA_VERSION)?;
        put_meta(&tx, "cluster_id", identity.cluster_id.as_bytes())?;
        put_meta(&tx, "node_id", identity.node_id.as_bytes())?;
        put_u64(&tx, "epoch", identity.epoch)?;
        put_configuration(&tx, &identity.configuration_state)?;
        put_u64(&tx, "recovery_generation", identity.recovery_generation)?;
        put_hash(
            &tx,
            "materializer_fingerprint",
            identity.materializer_fingerprint,
        )?;
        put_hash(&tx, "user_db_digest", identity.user_db_digest)?;
        put_anchor(&tx, "applied_tip", LogAnchor::new(0, LogHash::ZERO))?;
        tx.commit().map_err(sqlite_error)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn identity(&self) -> Result<ControlIdentity> {
        Ok(ControlIdentity::new(
            meta_text(&self.conn, "cluster_id")?,
            meta_text(&self.conn, "node_id")?,
            meta_u64(&self.conn, "epoch")?,
            meta_configuration(&self.conn)?,
            meta_u64(&self.conn, "recovery_generation")?,
            meta_hash(&self.conn, "materializer_fingerprint")?,
            meta_hash(&self.conn, "user_db_digest")?,
        ))
    }

    pub fn applied_tip(&self) -> Result<ApplyProgress> {
        let anchor = meta_anchor(&self.conn, "applied_tip")?;
        Ok(ApplyProgress::new(anchor.index(), anchor.hash()))
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        meta_configuration(&self.conn)
    }

    pub fn recovery_generation(&self) -> Result<u64> {
        meta_u64(&self.conn, "recovery_generation")
    }

    pub fn materializer_fingerprint(&self) -> Result<LogHash> {
        meta_hash(&self.conn, "materializer_fingerprint")
    }

    pub fn user_db_digest(&self) -> Result<LogHash> {
        meta_hash(&self.conn, "user_db_digest")
    }

    pub fn lookup_request(
        &self,
        request_id: &str,
        request_digest: LogHash,
    ) -> Result<Option<RequestReceipt>> {
        let receipt = self.conn.query_row(
            "SELECT request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts WHERE request_id = ?1",
            params![request_id],
            |row| {
                let digest = hash_from_blob(row.get(0)?)?;
                let index = u64_from_sql(row.get(1)?)?;
                let hash = hash_from_blob(row.get(2)?)?;
                Ok(RequestReceipt::new(request_id, digest, LogAnchor::new(index, hash), row.get(3)?))
            },
        ).optional().map_err(sqlite_error)?;

        match receipt {
            Some(receipt) if receipt.request_digest != request_digest => {
                Err(Error::RequestConflict(RequestConflict {
                    request_id: request_id.to_owned(),
                    original_outcome: RequestOutcome::new(
                        receipt.original_anchor.index(),
                        receipt.original_anchor.hash(),
                    ),
                }))
            }
            value => Ok(value),
        }
    }

    pub fn begin_pending(&self, pending: &PendingApply) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if let Some(existing) = pending_from(&tx)? {
            return if existing == *pending {
                tx.commit().map_err(sqlite_error)
            } else {
                Err(Error::InvalidEntry(
                    "a different physical apply is already pending".into(),
                ))
            };
        }
        let tip = meta_anchor(&tx, "applied_tip")?;
        let db_digest = meta_hash(&tx, "user_db_digest")?;
        if pending.base != tip || pending.base_db_digest != db_digest {
            return Err(Error::InvalidEntry(
                "pending apply does not match the committed base".into(),
            ));
        }
        if pending.entry.index()
            != tip
                .index()
                .checked_add(1)
                .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?
        {
            return Err(Error::InvalidEntry(
                "pending apply entry is not the next slot".into(),
            ));
        }
        insert_pending(&tx, pending)?;
        tx.commit().map_err(sqlite_error)
    }

    pub fn pending(&self) -> Result<Option<PendingApply>> {
        pending_from(&self.conn)
    }

    pub fn clear_pending(&self, expected: &PendingApply) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        match pending_from(&tx)? {
            Some(actual) if actual == *expected => {
                tx.execute("DELETE FROM pending_apply WHERE singleton = 1", [])
                    .map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)
            }
            Some(_) => Err(Error::InvalidEntry(
                "refusing to clear a different pending apply".into(),
            )),
            None => tx.commit().map_err(sqlite_error),
        }
    }

    pub fn commit_applied(
        &self,
        pending: &PendingApply,
        configuration_state: &ConfigurationState,
        receipt: Option<&RequestReceipt>,
    ) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if pending_from(&tx)?.as_ref() != Some(pending) {
            return Err(Error::InvalidEntry(
                "pending apply intent is missing or different".into(),
            ));
        }
        if configuration_state.config_id() < meta_configuration(&tx)?.config_id() {
            return Err(Error::InvalidEntry(
                "configuration state moved backwards".into(),
            ));
        }
        if let Some(receipt) = receipt {
            validate_receipt(receipt)?;
            if receipt.original_anchor != pending.entry {
                return Err(Error::InvalidEntry(
                    "request receipt anchor does not match applied entry".into(),
                ));
            }
            insert_or_validate_receipt(&tx, receipt)?;
        }
        put_anchor(&tx, "applied_tip", pending.entry)?;
        put_configuration(&tx, configuration_state)?;
        put_hash(&tx, "user_db_digest", pending.target_db_digest)?;
        tx.execute("DELETE FROM pending_apply WHERE singleton = 1", [])
            .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)
    }

    pub fn export_replicated_snapshot(&self) -> Result<Vec<u8>> {
        if self.pending()?.is_some() {
            return Err(Error::InvalidSnapshot(
                "cannot export control state while apply is pending".into(),
            ));
        }
        let mut statement = self.conn.prepare(
            "SELECT request_id, request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts ORDER BY request_id",
        ).map_err(sqlite_error)?;
        let receipts = statement
            .query_map([], |row| {
                let request_id: String = row.get(0)?;
                Ok(RequestReceipt::new(
                    request_id,
                    hash_from_blob(row.get(1)?)?,
                    LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
                    row.get(4)?,
                ))
            })
            .map_err(sqlite_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?;
        let tip = meta_anchor(&self.conn, "applied_tip")?;
        let snapshot = ReplicatedSnapshot {
            cluster_id: meta_text(&self.conn, "cluster_id")?,
            epoch: meta_u64(&self.conn, "epoch")?,
            configuration_state: meta_configuration(&self.conn)?,
            recovery_generation: meta_u64(&self.conn, "recovery_generation")?,
            materializer_fingerprint: meta_hash(&self.conn, "materializer_fingerprint")?,
            user_db_digest: meta_hash(&self.conn, "user_db_digest")?,
            applied_tip: tip,
            receipts,
        };
        encode_snapshot(&snapshot)
    }

    pub fn import_replicated_snapshot(&self, encoded: &[u8]) -> Result<()> {
        self.import_replicated_snapshot_with_recovery_generation(encoded, None)
    }

    /// Atomically imports replicated state while optionally rebinding it to a
    /// recovery anchor generation. The destination node identity is retained.
    pub fn import_replicated_snapshot_with_recovery_generation(
        &self,
        encoded: &[u8],
        recovery_generation: Option<u64>,
    ) -> Result<()> {
        let snapshot = decode_snapshot(encoded)?;
        let recovery_generation = recovery_generation.unwrap_or(snapshot.recovery_generation);
        if recovery_generation == 0 {
            return Err(Error::InvalidSnapshot(
                "control snapshot recovery generation must be positive".into(),
            ));
        }
        let local = self.identity()?;
        if snapshot.cluster_id != local.cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if snapshot.epoch != local.epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if snapshot.materializer_fingerprint != local.materializer_fingerprint {
            return Err(Error::IdentityMismatch("materializer_fingerprint".into()));
        }
        for receipt in &snapshot.receipts {
            validate_receipt(receipt)?;
        }

        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        tx.execute("DELETE FROM request_receipts", [])
            .map_err(sqlite_error)?;
        for receipt in &snapshot.receipts {
            insert_or_validate_receipt(&tx, receipt)?;
        }
        put_configuration(&tx, &snapshot.configuration_state)?;
        put_u64(&tx, "recovery_generation", recovery_generation)?;
        put_hash(&tx, "user_db_digest", snapshot.user_db_digest)?;
        put_anchor(&tx, "applied_tip", snapshot.applied_tip)?;
        tx.execute("DELETE FROM pending_apply", [])
            .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)
    }

    fn validate_schema(&self) -> Result<()> {
        let magic = get_meta(&self.conn, "magic")?;
        if magic != CONTROL_MAGIC {
            return Err(Error::Sqlite("invalid control sidecar magic".into()));
        }
        let version = meta_u64(&self.conn, "schema_version")?;
        if version != CONTROL_SCHEMA_VERSION {
            return Err(Error::Sqlite(format!(
                "unsupported control sidecar schema version {version}"
            )));
        }
        for key in REQUIRED_META_KEYS {
            let _: Vec<u8> = get_meta(&self.conn, key)?;
        }
        for (table, expected_sql, expected_columns) in [
            (
                "control_meta",
                CREATE_CONTROL_META_SQL,
                CONTROL_META_COLUMNS,
            ),
            (
                "request_receipts",
                CREATE_REQUEST_RECEIPTS_SQL,
                REQUEST_RECEIPT_COLUMNS,
            ),
            (
                "pending_apply",
                CREATE_PENDING_APPLY_SQL,
                PENDING_APPLY_COLUMNS,
            ),
        ] {
            validate_table_schema(&self.conn, table, expected_sql, expected_columns)?;
        }
        let unexpected_objects: i64 = self.conn.query_row(
            "SELECT count(*) FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
               AND (type <> 'table' OR name NOT IN ('control_meta','request_receipts','pending_apply'))",
            [],
            |row| row.get(0),
        ).map_err(sqlite_error)?;
        if unexpected_objects != 0 {
            return Err(Error::Sqlite(
                "control sidecar contains unexpected schema objects".into(),
            ));
        }
        let meta_count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM control_meta", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if meta_count != i64::try_from(REQUIRED_META_KEYS.len()).expect("small key count") {
            return Err(Error::Sqlite(
                "control sidecar has unknown or duplicate metadata".into(),
            ));
        }
        // Decode every typed value here so corrupt state fails before serving.
        let identity = self.identity()?;
        validate_new_identity(&identity)?;
        let _ = meta_anchor(&self.conn, "applied_tip")?;
        let pending = self.pending()?;
        let pending_rows: i64 = self
            .conn
            .query_row("SELECT count(*) FROM pending_apply", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        let expected_pending_rows = if pending.is_some() { 1 } else { 0 };
        if pending_rows != expected_pending_rows {
            return Err(Error::Sqlite(
                "control sidecar has invalid pending apply rows".into(),
            ));
        }
        validate_all_receipts(&self.conn)?;
        Ok(())
    }

    fn validate_identity(&self, expected: &ControlIdentity) -> Result<()> {
        let actual = self.identity()?;
        if actual.cluster_id != expected.cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if actual.node_id != expected.node_id {
            return Err(Error::IdentityMismatch("node_id".into()));
        }
        if actual.epoch != expected.epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if actual.configuration_state != expected.configuration_state {
            return Err(Error::IdentityMismatch("configuration_state".into()));
        }
        if actual.recovery_generation != expected.recovery_generation {
            return Err(Error::IdentityMismatch("recovery_generation".into()));
        }
        if actual.materializer_fingerprint != expected.materializer_fingerprint {
            return Err(Error::IdentityMismatch("materializer_fingerprint".into()));
        }
        if actual.user_db_digest != expected.user_db_digest {
            return Err(Error::IdentityMismatch("user_db_digest".into()));
        }
        Ok(())
    }
}

fn validate_new_identity(identity: &ControlIdentity) -> Result<()> {
    if identity.cluster_id.is_empty() {
        return Err(Error::IdentityMismatch("cluster_id".into()));
    }
    if identity.node_id.is_empty() {
        return Err(Error::IdentityMismatch("node_id".into()));
    }
    if identity.epoch == 0 {
        return Err(Error::IdentityMismatch("epoch".into()));
    }
    Ok(())
}

fn validate_table_schema(
    conn: &Connection,
    table: &str,
    expected_sql: &str,
    expected_columns: &[ExpectedColumn],
) -> Result<()> {
    let declared_sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| Error::Sqlite(format!("control sidecar is missing table {table}")))?;
    if normalize_schema_sql(&declared_sql) != normalize_schema_sql(expected_sql) {
        return Err(Error::Sqlite(format!(
            "invalid control sidecar declaration for table {table}"
        )));
    }

    let pragma =
        format!("SELECT name, type, [notnull], pk FROM pragma_table_info('{table}') ORDER BY cid");
    let mut statement = conn.prepare(&pragma).map_err(sqlite_error)?;
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    let matches = actual.len() == expected_columns.len()
        && actual.iter().zip(expected_columns).all(
            |((name, declared_type, not_null, primary_key), expected)| {
                name == expected.0
                    && declared_type.eq_ignore_ascii_case(expected.1)
                    && *not_null == expected.2
                    && *primary_key == expected.3
            },
        );
    if !matches {
        return Err(Error::Sqlite(format!(
            "invalid control sidecar columns for table {table}"
        )));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect()
}

fn validate_receipt(receipt: &RequestReceipt) -> Result<()> {
    if receipt.request_id.is_empty() || receipt.request_id.len() > usize::from(u16::MAX) {
        return Err(Error::InvalidCommand(
            "request id must contain 1..=65535 bytes".into(),
        ));
    }
    if receipt.result_blob.len() > MAX_RESULT_BLOB_BYTES {
        return Err(Error::ResourceExhausted(format!(
            "request result exceeds {MAX_RESULT_BLOB_BYTES} bytes"
        )));
    }
    super::validate_sql_result_blob_bounds(receipt.result_blob())
}

fn validate_all_receipts(conn: &Connection) -> Result<()> {
    let mut statement = conn
        .prepare(
            "SELECT request_id, request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts ORDER BY request_id",
        )
        .map_err(sqlite_error)?;
    let receipts = statement
        .query_map([], |row| {
            Ok(RequestReceipt::new(
                row.get::<_, String>(0)?,
                hash_from_blob(row.get(1)?)?,
                LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
                row.get(4)?,
            ))
        })
        .map_err(sqlite_error)?;
    for receipt in receipts {
        validate_receipt(&receipt.map_err(sqlite_error)?)?;
    }
    Ok(())
}

fn insert_or_validate_receipt(conn: &Connection, receipt: &RequestReceipt) -> Result<()> {
    if let Some(existing) = conn.query_row(
        "SELECT request_digest, original_log_index, original_log_hash, result_blob FROM request_receipts WHERE request_id=?1",
        params![receipt.request_id],
        |row| Ok(RequestReceipt::new(
            &receipt.request_id,
            hash_from_blob(row.get(0)?)?,
            LogAnchor::new(u64_from_sql(row.get(1)?)?, hash_from_blob(row.get(2)?)?),
            row.get(3)?,
        )),
    ).optional().map_err(sqlite_error)? {
        if existing == *receipt { return Ok(()); }
        return Err(Error::RequestConflict(RequestConflict {
            request_id: receipt.request_id.clone(),
            original_outcome: RequestOutcome::new(existing.original_anchor.index(), existing.original_anchor.hash()),
        }));
    }
    conn.execute(
        "INSERT INTO request_receipts(request_id,request_digest,original_log_index,original_log_hash,result_blob) VALUES(?1,?2,?3,?4,?5)",
        params![receipt.request_id, receipt.request_digest.as_bytes().as_slice(), u64_to_sql(receipt.original_anchor.index())?, receipt.original_anchor.hash().as_bytes().as_slice(), receipt.result_blob],
    ).map_err(sqlite_error)?;
    Ok(())
}

fn insert_pending(conn: &Connection, value: &PendingApply) -> Result<()> {
    conn.execute(
        "INSERT INTO pending_apply(singleton,base_index,base_hash,entry_index,entry_hash,base_db_digest,target_db_digest,target_file_bytes) VALUES(1,?1,?2,?3,?4,?5,?6,?7)",
        params![u64_to_sql(value.base.index())?, value.base.hash().as_bytes().as_slice(), u64_to_sql(value.entry.index())?, value.entry.hash().as_bytes().as_slice(), value.base_db_digest.as_bytes().as_slice(), value.target_db_digest.as_bytes().as_slice(), u64_to_sql(value.target_file_bytes)?],
    ).map_err(sqlite_error)?;
    Ok(())
}

fn pending_from(conn: &Connection) -> Result<Option<PendingApply>> {
    conn.query_row(
        "SELECT base_index,base_hash,entry_index,entry_hash,base_db_digest,target_db_digest,target_file_bytes FROM pending_apply WHERE singleton=1",
        [],
        |row| Ok(PendingApply::new(
            LogAnchor::new(u64_from_sql(row.get(0)?)?, hash_from_blob(row.get(1)?)?),
            LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
            hash_from_blob(row.get(4)?)?,
            hash_from_blob(row.get(5)?)?,
            u64_from_sql(row.get(6)?)?,
        )),
    ).optional().map_err(sqlite_error)
}

fn encode_snapshot(snapshot: &ReplicatedSnapshot) -> Result<Vec<u8>> {
    let body =
        serde_json::to_vec(snapshot).map_err(|error| Error::InvalidSnapshot(error.to_string()))?;
    let digest = LogHash::digest(&[&body]);
    let mut encoded = Vec::with_capacity(SNAPSHOT_MAGIC.len() + 32 + body.len());
    encoded.extend_from_slice(SNAPSHOT_MAGIC);
    encoded.extend_from_slice(digest.as_bytes());
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_snapshot(encoded: &[u8]) -> Result<ReplicatedSnapshot> {
    let payload = encoded.strip_prefix(SNAPSHOT_MAGIC).ok_or_else(|| {
        Error::InvalidSnapshot("control snapshot magic/version is invalid".into())
    })?;
    if payload.len() < 32 {
        return Err(Error::InvalidSnapshot(
            "control snapshot is truncated".into(),
        ));
    }
    let expected = LogHash::from_bytes(payload[..32].try_into().expect("32-byte checked slice"));
    let body = &payload[32..];
    if LogHash::digest(&[body]) != expected {
        return Err(Error::InvalidSnapshot(
            "control snapshot digest mismatch".into(),
        ));
    }
    let snapshot: ReplicatedSnapshot =
        serde_json::from_slice(body).map_err(|error| Error::InvalidSnapshot(error.to_string()))?;
    let canonical =
        serde_json::to_vec(&snapshot).map_err(|error| Error::InvalidSnapshot(error.to_string()))?;
    if canonical != body {
        return Err(Error::InvalidSnapshot(
            "control snapshot encoding is not canonical".into(),
        ));
    }
    let mut previous = None;
    for receipt in &snapshot.receipts {
        validate_receipt(receipt)?;
        if previous.is_some_and(|id: &str| id >= receipt.request_id.as_str()) {
            return Err(Error::InvalidSnapshot(
                "control snapshot receipts are not uniquely sorted".into(),
            ));
        }
        previous = Some(receipt.request_id.as_str());
    }
    Ok(snapshot)
}

fn put_meta(conn: &Connection, key: &str, value: &[u8]) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO control_meta(key,value) VALUES(?1,?2)",
        params![key, value],
    )
    .map_err(sqlite_error)?;
    Ok(())
}
fn get_meta(conn: &Connection, key: &str) -> Result<Vec<u8>> {
    conn.query_row(
        "SELECT value FROM control_meta WHERE key=?1",
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(sqlite_error)?
    .ok_or_else(|| Error::Sqlite(format!("control sidecar is missing {key}")))
}
fn put_u64(conn: &Connection, key: &str, value: u64) -> Result<()> {
    put_meta(conn, key, &value.to_be_bytes())
}
fn meta_u64(conn: &Connection, key: &str) -> Result<u64> {
    let bytes = get_meta(conn, key)?;
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Sqlite(format!("invalid control u64 {key}")))?;
    Ok(u64::from_be_bytes(bytes))
}
fn put_hash(conn: &Connection, key: &str, value: LogHash) -> Result<()> {
    put_meta(conn, key, value.as_bytes())
}
fn meta_hash(conn: &Connection, key: &str) -> Result<LogHash> {
    let bytes = get_meta(conn, key)?;
    Ok(LogHash::from_bytes(bytes.try_into().map_err(|_| {
        Error::Sqlite(format!("invalid control hash {key}"))
    })?))
}
fn meta_text(conn: &Connection, key: &str) -> Result<String> {
    String::from_utf8(get_meta(conn, key)?)
        .map_err(|_| Error::Sqlite(format!("invalid control text {key}")))
}
fn put_configuration(conn: &Connection, value: &ConfigurationState) -> Result<()> {
    let encoded = serde_json::to_vec(value).map_err(|error| Error::Sqlite(error.to_string()))?;
    put_meta(conn, "configuration_state", &encoded)
}
fn meta_configuration(conn: &Connection) -> Result<ConfigurationState> {
    serde_json::from_slice(&get_meta(conn, "configuration_state")?)
        .map_err(|error| Error::Sqlite(format!("invalid control configuration: {error}")))
}
fn put_anchor(conn: &Connection, key: &str, value: LogAnchor) -> Result<()> {
    let mut encoded = Vec::with_capacity(40);
    encoded.extend_from_slice(&value.index().to_be_bytes());
    encoded.extend_from_slice(value.hash().as_bytes());
    put_meta(conn, key, &encoded)
}
fn meta_anchor(conn: &Connection, key: &str) -> Result<LogAnchor> {
    let bytes = get_meta(conn, key)?;
    if bytes.len() != 40 {
        return Err(Error::Sqlite(format!("invalid control anchor {key}")));
    }
    Ok(LogAnchor::new(
        u64::from_be_bytes(bytes[..8].try_into().expect("length checked")),
        LogHash::from_bytes(bytes[8..].try_into().expect("length checked")),
    ))
}

fn u64_to_sql(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::ResourceExhausted("control integer exceeds SQLite i64".into()))
}
fn u64_from_sql(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, value))
}
fn hash_from_blob(bytes: Vec<u8>) -> rusqlite::Result<LogHash> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected 32-byte hash, got {} bytes", bytes.len()),
            )),
        )
    })?;
    Ok(LogHash::from_bytes(bytes))
}
fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::Sqlite(error.to_string())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::Io(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(label: &[u8]) -> LogHash {
        LogHash::digest(&[label])
    }
    fn identity(node: &str) -> ControlIdentity {
        ControlIdentity::new(
            "cluster",
            node,
            7,
            ConfigurationState::active(3, hash(b"config")),
            11,
            hash(b"fingerprint"),
            hash(b"base-db"),
        )
    }
    fn pending() -> PendingApply {
        PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, hash(b"entry")),
            hash(b"base-db"),
            hash(b"target-db"),
            4096,
        )
    }
    fn receipt(digest: LogHash) -> RequestReceipt {
        RequestReceipt::new(
            "request-1",
            digest,
            LogAnchor::new(1, hash(b"entry")),
            crate::encode_sql_result(&crate::SqlCommandResult {
                statement_results: Vec::new(),
            })
            .unwrap(),
        )
    }

    #[test]
    fn open_rejects_identity_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        ControlStore::create(&path, &identity("node-a")).unwrap();
        let error = match ControlStore::open_existing(&path, &identity("node-b")) {
            Ok(_) => panic!("mismatched node identity must fail"),
            Err(error) => error,
        };
        assert_eq!(error, Error::IdentityMismatch("node_id".into()));
    }

    #[test]
    fn open_rejects_request_table_with_same_columns_but_no_primary_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        drop(ControlStore::create(&path, &identity("node-a")).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP TABLE request_receipts;
             CREATE TABLE request_receipts (
                 request_id TEXT NOT NULL,
                 request_digest BLOB NOT NULL,
                 original_log_index INTEGER NOT NULL,
                 original_log_hash BLOB NOT NULL,
                 result_blob BLOB NOT NULL
             );",
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            ControlStore::open_existing_unchecked(&path),
            Err(Error::Sqlite(_))
        ));
    }

    #[test]
    fn open_rejects_pending_table_without_singleton_constraint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        drop(ControlStore::create(&path, &identity("node-a")).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP TABLE pending_apply;
             CREATE TABLE pending_apply (
                 singleton INTEGER PRIMARY KEY,
                 base_index INTEGER NOT NULL,
                 base_hash BLOB NOT NULL,
                 entry_index INTEGER NOT NULL,
                 entry_hash BLOB NOT NULL,
                 base_db_digest BLOB NOT NULL,
                 target_db_digest BLOB NOT NULL,
                 target_file_bytes INTEGER NOT NULL
             );",
        )
        .unwrap();
        drop(conn);

        assert!(matches!(
            ControlStore::open_existing_unchecked(&path),
            Err(Error::Sqlite(_))
        ));
    }

    #[test]
    fn lookup_returns_duplicate_and_rejects_conflicting_digest() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let receipt = receipt(hash(b"request"));
        store.begin_pending(&pending).unwrap();
        store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                Some(&receipt),
            )
            .unwrap();
        assert_eq!(
            store.lookup_request("request-1", hash(b"request")).unwrap(),
            Some(receipt)
        );
        assert!(matches!(
            store.lookup_request("request-1", hash(b"other")),
            Err(Error::RequestConflict(_))
        ));
    }

    #[test]
    fn commit_rejects_result_larger_than_inline_qwal_limit_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let oversized = RequestReceipt::new(
            "request-1",
            hash(b"request"),
            pending.entry(),
            vec![0; MAX_RESULT_BLOB_BYTES + 1],
        );
        store.begin_pending(&pending).unwrap();

        assert!(matches!(
            store.commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                Some(&oversized),
            ),
            Err(Error::ResourceExhausted(_))
        ));
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }

    #[test]
    fn commit_rejects_noncanonical_sql_result_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("sqlite.control"), &identity("node-a")).unwrap();
        let pending = pending();
        let malformed = RequestReceipt::new(
            "request-1",
            hash(b"request"),
            pending.entry(),
            b"not-qres".to_vec(),
        );
        store.begin_pending(&pending).unwrap();

        assert!(store
            .commit_applied(
                &pending,
                identity("node-a").configuration_state(),
                Some(&malformed),
            )
            .is_err());
        assert_eq!(store.pending().unwrap(), Some(pending));
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }

    #[test]
    fn pending_lifecycle_is_idempotent_and_guarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let store = ControlStore::create(&path, &identity("node-a")).unwrap();
        let pending = pending();
        store.begin_pending(&pending).unwrap();
        store.begin_pending(&pending).unwrap();
        drop(store);
        let store = ControlStore::open_existing(&path, &identity("node-a")).unwrap();
        assert_eq!(store.pending().unwrap(), Some(pending.clone()));
        let different = PendingApply::new(
            pending.base(),
            LogAnchor::new(1, hash(b"different")),
            pending.base_db_digest(),
            pending.target_db_digest(),
            4096,
        );
        assert!(store.clear_pending(&different).is_err());
        store.clear_pending(&pending).unwrap();
        store.clear_pending(&pending).unwrap();
        assert_eq!(store.pending().unwrap(), None);
    }

    #[test]
    fn committed_state_and_receipt_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sqlite.control");
        let original = identity("node-a");
        let pending = pending();
        let receipt = receipt(hash(b"request"));
        let next_config = ConfigurationState::active(4, hash(b"next-config"));
        {
            let store = ControlStore::create(&path, &original).unwrap();
            store.begin_pending(&pending).unwrap();
            store
                .commit_applied(&pending, &next_config, Some(&receipt))
                .unwrap();
        }
        let reopened_identity = ControlIdentity::new(
            "cluster",
            "node-a",
            7,
            next_config.clone(),
            11,
            hash(b"fingerprint"),
            hash(b"target-db"),
        );
        let store = ControlStore::open_existing(&path, &reopened_identity).unwrap();
        assert_eq!(
            store.applied_tip().unwrap(),
            ApplyProgress::new(1, hash(b"entry"))
        );
        assert_eq!(store.configuration_state().unwrap(), next_config);
        assert_eq!(
            store.lookup_request("request-1", hash(b"request")).unwrap(),
            Some(receipt)
        );
    }

    #[test]
    fn snapshot_import_keeps_destination_node_and_excludes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let source =
            ControlStore::create(dir.path().join("source.control"), &identity("node-a")).unwrap();
        let source_pending = pending();
        source.begin_pending(&source_pending).unwrap();
        source
            .commit_applied(
                &source_pending,
                identity("node-a").configuration_state(),
                Some(&receipt(hash(b"request"))),
            )
            .unwrap();
        let snapshot = source.export_replicated_snapshot().unwrap();

        let destination =
            ControlStore::create(dir.path().join("destination.control"), &identity("node-b"))
                .unwrap();
        destination.begin_pending(&pending()).unwrap();
        destination
            .import_replicated_snapshot_with_recovery_generation(&snapshot, Some(42))
            .unwrap();
        assert_eq!(destination.identity().unwrap().node_id(), "node-b");
        assert_eq!(destination.recovery_generation().unwrap(), 42);
        assert_eq!(
            destination.applied_tip().unwrap(),
            ApplyProgress::new(1, hash(b"entry"))
        );
        assert_eq!(destination.pending().unwrap(), None);
    }

    #[test]
    fn snapshot_import_rejects_result_larger_than_inline_qwal_limit() {
        let dir = tempfile::tempdir().unwrap();
        let destination =
            ControlStore::create(dir.path().join("destination.control"), &identity("node-b"))
                .unwrap();
        let snapshot = ReplicatedSnapshot {
            cluster_id: "cluster".into(),
            epoch: 7,
            configuration_state: ConfigurationState::active(3, hash(b"config")),
            recovery_generation: 11,
            materializer_fingerprint: hash(b"fingerprint"),
            user_db_digest: hash(b"base-db"),
            applied_tip: LogAnchor::new(1, hash(b"entry")),
            receipts: vec![RequestReceipt::new(
                "oversized",
                hash(b"request"),
                LogAnchor::new(1, hash(b"entry")),
                vec![0; MAX_RESULT_BLOB_BYTES + 1],
            )],
        };
        let encoded = encode_snapshot(&snapshot).unwrap();

        assert!(matches!(
            destination.import_replicated_snapshot(&encoded),
            Err(Error::ResourceExhausted(_))
        ));
        assert_eq!(
            destination.applied_tip().unwrap(),
            ApplyProgress::new(0, LogHash::ZERO)
        );
    }
}
