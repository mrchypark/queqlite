//! Durable graph control state kept outside the canonical Ladybug file.

use std::{
    fs::{self, File, OpenOptions},
    path::Path,
    sync::Mutex,
    time::Duration,
};

use rhiza_core::{ConfigurationState, LogAnchor, LogHash, StopBinding, SuccessorDescriptor};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};

use super::{Error, Result, MAX_LGFX_V1_BYTES};

const CONTROL_MAGIC: &[u8] = b"RHIZA-GRAPH-CONTROL-SQLITE\0\x01";
const CONTROL_SCHEMA_VERSION: u64 = 1;
const SNAPSHOT_MAGIC: &[u8] = b"RGCT\0\x01";
/// Finite in-memory RHGS transport bound. Receipt rows remain durable beyond
/// this size; export returns `ResourceExhausted` and never evicts history.
pub(crate) const MAX_CONTROL_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
const MAX_ID_BYTES: usize = 256;
const MAX_CONFIGURATION_BYTES: usize = 1024 * 1024;
const MAX_CONFIGURATION_MEMBERS: usize = 4096;
const MIN_ENCODED_RECEIPT_BYTES: usize = 8 + 1 + 32 + 40 + 8 + 8;
const MAX_CONTROL_SNAPSHOT_RECEIPTS: usize = MAX_CONTROL_SNAPSHOT_BYTES / MIN_ENCODED_RECEIPT_BYTES;

const CREATE_CONTROL_META_SQL: &str = r#"CREATE TABLE control_meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL CHECK(length(value) <= 1048576)
) WITHOUT ROWID;"#;
const CREATE_REQUEST_RECEIPTS_SQL: &str = r#"CREATE TABLE request_receipts (
    request_id TEXT PRIMARY KEY CHECK(length(request_id) BETWEEN 1 AND 256),
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    original_log_index INTEGER NOT NULL CHECK(original_log_index >= 0),
    original_log_hash BLOB NOT NULL CHECK(length(original_log_hash) = 32),
    result_blob BLOB NOT NULL CHECK(length(result_blob) <= 262144)
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

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicatedSnapshot {
    cluster_id: String,
    source_node_id: String,
    epoch: u64,
    configuration_state: ConfigurationState,
    recovery_generation: u64,
    materializer_fingerprint: LogHash,
    user_db_digest: LogHash,
    applied_tip: LogAnchor,
    receipts: Vec<RequestReceipt>,
}

pub struct ControlStore {
    connection: Mutex<Connection>,
}

impl ControlStore {
    pub fn create(path: impl AsRef<Path>, identity: &ControlIdentity) -> Result<Self> {
        validate_identity(identity)?;
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(super::io_error)?;
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(super::io_error)?;
        let store = match Self::open_file(path) {
            Ok(store) => store,
            Err(error) => {
                let _ = fs::remove_file(path);
                return Err(error);
            }
        };
        if let Err(error) = store.initialize(identity) {
            drop(store);
            let _ = fs::remove_file(path);
            return Err(error);
        }
        sync_parent(path)?;
        Ok(store)
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        require_regular_file(path, "graph control sidecar")?;
        let store = Self::open_file(path)?;
        store.validate_schema()?;
        Ok(store)
    }

    fn open_file(path: &Path) -> Result<Self> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sqlite_error)?;
        let journal: String = connection
            .query_row("PRAGMA journal_mode = DELETE", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if !journal.eq_ignore_ascii_case("delete") {
            return Err(Error::Io(format!(
                "graph control refused DELETE journal mode: {journal}"
            )));
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "trusted_schema", "OFF")
            .map_err(sqlite_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(sqlite_error)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn initialize(&self, identity: &ControlIdentity) -> Result<()> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        transaction
            .execute_batch(CREATE_CONTROL_META_SQL)
            .map_err(sqlite_error)?;
        transaction
            .execute_batch(CREATE_REQUEST_RECEIPTS_SQL)
            .map_err(sqlite_error)?;
        transaction
            .execute_batch(CREATE_PENDING_APPLY_SQL)
            .map_err(sqlite_error)?;
        put_meta(&transaction, "magic", CONTROL_MAGIC)?;
        put_u64(&transaction, "schema_version", CONTROL_SCHEMA_VERSION)?;
        put_meta(&transaction, "cluster_id", identity.cluster_id.as_bytes())?;
        put_meta(&transaction, "node_id", identity.node_id.as_bytes())?;
        put_u64(&transaction, "epoch", identity.epoch)?;
        put_configuration(&transaction, &identity.configuration_state)?;
        put_u64(
            &transaction,
            "recovery_generation",
            identity.recovery_generation,
        )?;
        put_hash(
            &transaction,
            "materializer_fingerprint",
            identity.materializer_fingerprint,
        )?;
        put_hash(&transaction, "user_db_digest", identity.user_db_digest)?;
        put_anchor(
            &transaction,
            "applied_tip",
            LogAnchor::new(0, LogHash::ZERO),
        )?;
        transaction.commit().map_err(sqlite_error)
    }

    pub fn identity(&self) -> Result<ControlIdentity> {
        let connection = self.lock()?;
        Ok(ControlIdentity::new(
            meta_text(&connection, "cluster_id")?,
            meta_text(&connection, "node_id")?,
            meta_u64(&connection, "epoch")?,
            meta_configuration(&connection)?,
            meta_u64(&connection, "recovery_generation")?,
            meta_hash(&connection, "materializer_fingerprint")?,
            meta_hash(&connection, "user_db_digest")?,
        ))
    }

    pub fn applied_tip(&self) -> Result<LogAnchor> {
        meta_anchor(&*self.lock()?, "applied_tip")
    }
    pub fn configuration_state(&self) -> Result<ConfigurationState> {
        meta_configuration(&*self.lock()?)
    }
    pub fn recovery_generation(&self) -> Result<u64> {
        meta_u64(&*self.lock()?, "recovery_generation")
    }
    pub fn user_db_digest(&self) -> Result<LogHash> {
        meta_hash(&*self.lock()?, "user_db_digest")
    }
    pub fn pending(&self) -> Result<Option<PendingApply>> {
        pending_from(&*self.lock()?)
    }

    pub fn lookup_request(
        &self,
        request_id: &str,
        request_digest: LogHash,
    ) -> Result<Option<RequestReceipt>> {
        let connection = self.lock()?;
        let receipt = connection
            .query_row(
                "SELECT request_digest,original_log_index,original_log_hash,result_blob
                 FROM request_receipts WHERE request_id=?1",
                params![request_id],
                |row| {
                    Ok(RequestReceipt::new(
                        request_id,
                        hash_from_blob(row.get(0)?)?,
                        LogAnchor::new(u64_from_sql(row.get(1)?)?, hash_from_blob(row.get(2)?)?),
                        row.get(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error)?;
        match receipt {
            Some(receipt) if receipt.request_digest != request_digest => {
                Err(Error::RequestConflict {
                    request_id: request_id.to_owned(),
                    original_log_index: receipt.original_anchor.index(),
                    original_log_hash: receipt.original_anchor.hash(),
                })
            }
            value => Ok(value),
        }
    }

    pub fn begin_pending(&self, pending: &PendingApply) -> Result<()> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if let Some(existing) = pending_from(&transaction)? {
            return if existing == *pending {
                transaction.commit().map_err(sqlite_error)
            } else {
                Err(Error::InvalidEntry(
                    "a different LGFX apply is already pending".into(),
                ))
            };
        }
        let tip = meta_anchor(&transaction, "applied_tip")?;
        if pending.base != tip
            || pending.base_db_digest != meta_hash(&transaction, "user_db_digest")?
        {
            return Err(Error::InvalidEntry(
                "pending LGFX apply does not match the committed base".into(),
            ));
        }
        if pending.entry.index()
            != tip
                .index()
                .checked_add(1)
                .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?
        {
            return Err(Error::InvalidEntry(
                "pending LGFX entry is not the next slot".into(),
            ));
        }
        insert_pending(&transaction, pending)?;
        transaction.commit().map_err(sqlite_error)
    }

    pub fn commit_applied(
        &self,
        pending: &PendingApply,
        configuration_state: &ConfigurationState,
        receipt: Option<&RequestReceipt>,
    ) -> Result<()> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        if pending_from(&transaction)?.as_ref() != Some(pending) {
            return Err(Error::InvalidEntry(
                "pending LGFX intent is missing or different".into(),
            ));
        }
        if configuration_state.config_id() < meta_configuration(&transaction)?.config_id() {
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
            insert_or_validate_receipt(&transaction, receipt)?;
        }
        put_anchor(&transaction, "applied_tip", pending.entry)?;
        put_configuration(&transaction, configuration_state)?;
        put_hash(&transaction, "user_db_digest", pending.target_db_digest)?;
        transaction
            .execute("DELETE FROM pending_apply WHERE singleton=1", [])
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)
    }

    /// Exports receipt history without mutating it. A history larger than the
    /// finite transport bound remains applicable/queryable but cannot be put in
    /// one RHGS object and returns `ResourceExhausted` explicitly.
    pub fn export_replicated_snapshot(&self) -> Result<Vec<u8>> {
        let connection = self.lock()?;
        if pending_from(&connection)?.is_some() {
            return Err(Error::InvalidSnapshot(
                "cannot snapshot graph control while apply is pending".into(),
            ));
        }
        let receipt_count: i64 = connection
            .query_row("SELECT count(*) FROM request_receipts", [], |row| {
                row.get(0)
            })
            .map_err(sqlite_error)?;
        let receipt_count = usize::try_from(receipt_count)
            .map_err(|_| Error::InvalidSnapshot("negative receipt count".into()))?;
        if receipt_count > MAX_CONTROL_SNAPSHOT_RECEIPTS {
            return Err(Error::ResourceExhausted(
                "graph control snapshot receipt count exceeds bound".into(),
            ));
        }
        encode_snapshot_from_connection(&connection, receipt_count)
    }

    pub fn import_replicated_snapshot(
        &self,
        encoded: &[u8],
        expected_source_node_id: &str,
    ) -> Result<()> {
        let imported = decode_snapshot(encoded)?;
        if imported.source_node_id != expected_source_node_id {
            return Err(Error::InvalidSnapshot(
                "replicated graph control source node does not match RHGS created_by".into(),
            ));
        }
        let local = self.identity()?;
        if imported.cluster_id != local.cluster_id
            || imported.epoch != local.epoch
            || imported.materializer_fingerprint != local.materializer_fingerprint
        {
            return Err(Error::IdentityMismatch(
                "snapshot graph control identity".into(),
            ));
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        transaction
            .execute("DELETE FROM request_receipts", [])
            .map_err(sqlite_error)?;
        for receipt in &imported.receipts {
            insert_or_validate_receipt(&transaction, receipt)?;
        }
        put_configuration(&transaction, &imported.configuration_state)?;
        put_u64(
            &transaction,
            "recovery_generation",
            imported.recovery_generation,
        )?;
        put_hash(&transaction, "user_db_digest", imported.user_db_digest)?;
        put_anchor(&transaction, "applied_tip", imported.applied_tip)?;
        transaction
            .execute("DELETE FROM pending_apply", [])
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)
    }

    fn validate_schema(&self) -> Result<()> {
        let connection = self.lock()?;
        let integrity: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if integrity != "ok" {
            return Err(Error::IdentityMismatch(
                "graph control SQLite integrity".into(),
            ));
        }
        if get_meta(&connection, "magic")? != CONTROL_MAGIC {
            return Err(Error::IdentityMismatch(
                "graph control magic/version".into(),
            ));
        }
        if meta_u64(&connection, "schema_version")? != CONTROL_SCHEMA_VERSION {
            return Err(Error::IdentityMismatch(
                "graph control schema version".into(),
            ));
        }
        for key in REQUIRED_META_KEYS {
            let _ = get_meta(&connection, key)?;
        }
        for (table, expected) in [
            ("control_meta", CREATE_CONTROL_META_SQL),
            ("request_receipts", CREATE_REQUEST_RECEIPTS_SQL),
            ("pending_apply", CREATE_PENDING_APPLY_SQL),
        ] {
            let declared: String = connection
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
                    params![table],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sqlite_error)?
                .ok_or_else(|| {
                    Error::IdentityMismatch(format!("graph control missing table {table}"))
                })?;
            if normalize_schema_sql(&declared) != normalize_schema_sql(expected) {
                return Err(Error::IdentityMismatch(format!(
                    "graph control table schema {table}"
                )));
            }
        }
        let unexpected: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE name NOT LIKE 'sqlite_%'
                   AND (type <> 'table' OR name NOT IN ('control_meta','request_receipts','pending_apply'))",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        if unexpected != 0 {
            return Err(Error::IdentityMismatch(
                "graph control contains unexpected schema objects".into(),
            ));
        }
        let meta_count: i64 = connection
            .query_row("SELECT count(*) FROM control_meta", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if meta_count != REQUIRED_META_KEYS.len() as i64 {
            return Err(Error::IdentityMismatch(
                "graph control metadata keys".into(),
            ));
        }
        validate_identity(&ControlIdentity::new(
            meta_text(&connection, "cluster_id")?,
            meta_text(&connection, "node_id")?,
            meta_u64(&connection, "epoch")?,
            meta_configuration(&connection)?,
            meta_u64(&connection, "recovery_generation")?,
            meta_hash(&connection, "materializer_fingerprint")?,
            meta_hash(&connection, "user_db_digest")?,
        ))?;
        let _ = meta_anchor(&connection, "applied_tip")?;
        let _ = pending_from(&connection)?;
        validate_all_receipts(&connection)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| Error::Io("graph control lock is poisoned".into()))
    }
}

pub(crate) fn validate_replicated_snapshot_source(
    encoded: &[u8],
    expected_source_node_id: &str,
) -> Result<()> {
    let snapshot = decode_snapshot(encoded)?;
    if snapshot.source_node_id != expected_source_node_id {
        return Err(Error::InvalidSnapshot(
            "replicated graph control source node does not match RHGS created_by".into(),
        ));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect()
}

fn validate_identity(identity: &ControlIdentity) -> Result<()> {
    if identity.cluster_id.is_empty()
        || identity.cluster_id.len() > MAX_ID_BYTES
        || identity.node_id.is_empty()
        || identity.node_id.len() > MAX_ID_BYTES
        || identity.epoch == 0
        || identity.recovery_generation == 0
    {
        return Err(Error::IdentityMismatch(
            "invalid graph control identity".into(),
        ));
    }
    Ok(())
}

fn validate_receipt(receipt: &RequestReceipt) -> Result<()> {
    if receipt.request_id.is_empty() || receipt.request_id.len() > 256 {
        return Err(Error::InvalidCommand(
            "request id must contain 1..=256 bytes".into(),
        ));
    }
    if receipt.result_blob.len() > MAX_LGFX_V1_BYTES {
        return Err(Error::ResourceExhausted(
            "graph request result exceeds LGFX bound".into(),
        ));
    }
    let result = super::GraphCommandResultV1::decode(&receipt.result_blob)?;
    if result.encode() != receipt.result_blob {
        return Err(Error::InvalidCommand(
            "graph request result is not canonical".into(),
        ));
    }
    Ok(())
}

fn put_meta(connection: &Connection, key: &str, value: &[u8]) -> Result<()> {
    connection
        .execute(
            "INSERT OR REPLACE INTO control_meta(key,value) VALUES(?1,?2)",
            params![key, value],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn get_meta(connection: &Connection, key: &str) -> Result<Vec<u8>> {
    connection
        .query_row(
            "SELECT value FROM control_meta WHERE key=?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| Error::IdentityMismatch(format!("graph control missing {key}")))
}

fn put_u64(connection: &Connection, key: &str, value: u64) -> Result<()> {
    put_meta(connection, key, &value.to_be_bytes())
}

fn meta_u64(connection: &Connection, key: &str) -> Result<u64> {
    let bytes: [u8; 8] = get_meta(connection, key)?
        .try_into()
        .map_err(|_| Error::IdentityMismatch(format!("graph control u64 {key}")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn put_hash(connection: &Connection, key: &str, value: LogHash) -> Result<()> {
    put_meta(connection, key, value.as_bytes())
}

fn meta_hash(connection: &Connection, key: &str) -> Result<LogHash> {
    let bytes: [u8; 32] = get_meta(connection, key)?
        .try_into()
        .map_err(|_| Error::IdentityMismatch(format!("graph control hash {key}")))?;
    Ok(LogHash::from_bytes(bytes))
}

fn meta_text(connection: &Connection, key: &str) -> Result<String> {
    let bytes = get_meta(connection, key)?;
    if bytes.is_empty() || bytes.len() > MAX_ID_BYTES {
        return Err(Error::IdentityMismatch(format!(
            "graph control text {key} exceeds bound"
        )));
    }
    String::from_utf8(bytes)
        .map_err(|_| Error::IdentityMismatch(format!("graph control text {key}")))
}

fn put_configuration(connection: &Connection, value: &ConfigurationState) -> Result<()> {
    let mut encoded = Vec::new();
    encode_configuration(&mut encoded, value)?;
    if encoded.len() > MAX_CONFIGURATION_BYTES {
        return Err(Error::ResourceExhausted(
            "graph configuration exceeds bound".into(),
        ));
    }
    put_meta(connection, "configuration_state", &encoded)
}

fn meta_configuration(connection: &Connection) -> Result<ConfigurationState> {
    let encoded = get_meta(connection, "configuration_state")?;
    if encoded.len() > MAX_CONFIGURATION_BYTES {
        return Err(Error::IdentityMismatch(
            "graph control configuration exceeds bound".into(),
        ));
    }
    let mut decoder = BoundedDecoder::new(&encoded);
    let value = decode_configuration(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}

fn put_anchor(connection: &Connection, key: &str, value: LogAnchor) -> Result<()> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(40)
        .map_err(|_| Error::ResourceExhausted("graph control anchor allocation failed".into()))?;
    encoded.extend_from_slice(&value.index().to_be_bytes());
    encoded.extend_from_slice(value.hash().as_bytes());
    put_meta(connection, key, &encoded)
}

fn meta_anchor(connection: &Connection, key: &str) -> Result<LogAnchor> {
    let bytes = get_meta(connection, key)?;
    if bytes.len() != 40 {
        return Err(Error::IdentityMismatch(format!(
            "graph control anchor {key}"
        )));
    }
    Ok(LogAnchor::new(
        u64::from_be_bytes(bytes[..8].try_into().expect("length checked")),
        LogHash::from_bytes(bytes[8..].try_into().expect("length checked")),
    ))
}

fn insert_pending(connection: &Connection, pending: &PendingApply) -> Result<()> {
    connection
        .execute(
            "INSERT INTO pending_apply(singleton,base_index,base_hash,entry_index,entry_hash,base_db_digest,target_db_digest,target_file_bytes)
             VALUES(1,?1,?2,?3,?4,?5,?6,?7)",
            params![
                u64_to_sql(pending.base.index())?,
                pending.base.hash().as_bytes(),
                u64_to_sql(pending.entry.index())?,
                pending.entry.hash().as_bytes(),
                pending.base_db_digest.as_bytes(),
                pending.target_db_digest.as_bytes(),
                u64_to_sql(pending.target_file_bytes)?,
            ],
        )
        .map_err(sqlite_error)?;
    Ok(())
}

fn pending_from(connection: &Connection) -> Result<Option<PendingApply>> {
    connection
        .query_row(
            "SELECT base_index,base_hash,entry_index,entry_hash,base_db_digest,target_db_digest,target_file_bytes
             FROM pending_apply WHERE singleton=1",
            [],
            |row| {
                Ok(PendingApply::new(
                    LogAnchor::new(u64_from_sql(row.get(0)?)?, hash_from_blob(row.get(1)?)?),
                    LogAnchor::new(u64_from_sql(row.get(2)?)?, hash_from_blob(row.get(3)?)?),
                    hash_from_blob(row.get(4)?)?,
                    hash_from_blob(row.get(5)?)?,
                    u64_from_sql(row.get(6)?)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)
}

fn insert_or_validate_receipt(connection: &Connection, receipt: &RequestReceipt) -> Result<()> {
    let existing = connection
        .query_row(
            "SELECT request_digest,original_log_index,original_log_hash,result_blob
             FROM request_receipts WHERE request_id=?1",
            params![receipt.request_id],
            |row| {
                Ok(RequestReceipt::new(
                    &receipt.request_id,
                    hash_from_blob(row.get(0)?)?,
                    LogAnchor::new(u64_from_sql(row.get(1)?)?, hash_from_blob(row.get(2)?)?),
                    row.get(3)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;
    match existing {
        Some(existing) if existing != *receipt => Err(Error::RequestConflict {
            request_id: receipt.request_id.clone(),
            original_log_index: existing.original_anchor.index(),
            original_log_hash: existing.original_anchor.hash(),
        }),
        Some(_) => Ok(()),
        None => {
            connection
                .execute(
                    "INSERT INTO request_receipts(request_id,request_digest,original_log_index,original_log_hash,result_blob)
                     VALUES(?1,?2,?3,?4,?5)",
                    params![
                        receipt.request_id,
                        receipt.request_digest.as_bytes(),
                        u64_to_sql(receipt.original_anchor.index())?,
                        receipt.original_anchor.hash().as_bytes(),
                        receipt.result_blob,
                    ],
                )
                .map_err(sqlite_error)?;
            Ok(())
        }
    }
}

fn validate_all_receipts(connection: &Connection) -> Result<()> {
    let invalid: i64 = connection
        .query_row(
            "SELECT count(*) FROM request_receipts
             WHERE typeof(request_id) <> 'text'
                OR length(request_id) NOT BETWEEN 1 AND 256
                OR typeof(request_digest) <> 'blob' OR length(request_digest) <> 32
                OR original_log_index < 0
                OR typeof(original_log_hash) <> 'blob' OR length(original_log_hash) <> 32
                OR typeof(result_blob) <> 'blob' OR length(result_blob) > 262144",
            [],
            |row| row.get(0),
        )
        .map_err(sqlite_error)?;
    if invalid != 0 {
        return Err(Error::IdentityMismatch(
            "graph control contains invalid receipt fields".into(),
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT request_id,request_digest,original_log_index,original_log_hash,result_blob
             FROM request_receipts ORDER BY request_id",
        )
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let receipt = RequestReceipt::new(
            row.get::<_, String>(0).map_err(sqlite_error)?,
            hash_from_blob(row.get(1).map_err(sqlite_error)?).map_err(sqlite_error)?,
            LogAnchor::new(
                u64_from_sql(row.get(2).map_err(sqlite_error)?).map_err(sqlite_error)?,
                hash_from_blob(row.get(3).map_err(sqlite_error)?).map_err(sqlite_error)?,
            ),
            row.get(4).map_err(sqlite_error)?,
        );
        validate_receipt(&receipt)?;
    }
    Ok(())
}

fn encode_snapshot_from_connection(
    connection: &Connection,
    receipt_count: usize,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    write_bytes(
        &mut body,
        &get_meta(connection, "cluster_id")?,
        MAX_ID_BYTES,
        "cluster id",
    )?;
    write_bytes(
        &mut body,
        &get_meta(connection, "node_id")?,
        MAX_ID_BYTES,
        "source node id",
    )?;
    try_extend_control(&mut body, &meta_u64(connection, "epoch")?.to_be_bytes())?;
    encode_configuration(&mut body, &meta_configuration(connection)?)?;
    try_extend_control(
        &mut body,
        &meta_u64(connection, "recovery_generation")?.to_be_bytes(),
    )?;
    try_extend_control(
        &mut body,
        meta_hash(connection, "materializer_fingerprint")?.as_bytes(),
    )?;
    try_extend_control(
        &mut body,
        meta_hash(connection, "user_db_digest")?.as_bytes(),
    )?;
    encode_anchor(&mut body, meta_anchor(connection, "applied_tip")?)?;
    try_extend_control(&mut body, &(receipt_count as u64).to_be_bytes())?;

    let mut statement = connection
        .prepare(
            "SELECT request_id,request_digest,original_log_index,original_log_hash,result_blob
             FROM request_receipts ORDER BY request_id",
        )
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let mut encoded_receipts = 0usize;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let receipt = RequestReceipt::new(
            row.get::<_, String>(0).map_err(sqlite_error)?,
            hash_from_blob(row.get(1).map_err(sqlite_error)?).map_err(sqlite_error)?,
            LogAnchor::new(
                u64_from_sql(row.get(2).map_err(sqlite_error)?).map_err(sqlite_error)?,
                hash_from_blob(row.get(3).map_err(sqlite_error)?).map_err(sqlite_error)?,
            ),
            row.get(4).map_err(sqlite_error)?,
        );
        validate_receipt(&receipt)?;
        write_bytes(
            &mut body,
            receipt.request_id.as_bytes(),
            MAX_ID_BYTES,
            "request id",
        )?;
        try_extend_control(&mut body, receipt.request_digest.as_bytes())?;
        encode_anchor(&mut body, receipt.original_anchor)?;
        write_bytes(&mut body, &receipt.result_blob, MAX_LGFX_V1_BYTES, "result")?;
        encoded_receipts += 1;
    }
    if encoded_receipts != receipt_count {
        return Err(Error::InvalidSnapshot(
            "graph control receipt count changed during snapshot".into(),
        ));
    }
    finish_snapshot_encoding(body)
}

fn finish_snapshot_encoding(body: Vec<u8>) -> Result<Vec<u8>> {
    let digest = LogHash::digest(&[&body]);
    let capacity = SNAPSHOT_MAGIC
        .len()
        .checked_add(32)
        .and_then(|value| value.checked_add(body.len()))
        .ok_or_else(|| Error::ResourceExhausted("graph control snapshot size overflow".into()))?;
    ensure_snapshot_bound(capacity)?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(capacity)
        .map_err(|_| Error::ResourceExhausted("graph control snapshot allocation failed".into()))?;
    try_extend_control(&mut encoded, SNAPSHOT_MAGIC)?;
    try_extend_control(&mut encoded, digest.as_bytes())?;
    try_extend_control(&mut encoded, &body)?;
    ensure_snapshot_bound(encoded.len())?;
    Ok(encoded)
}

fn decode_snapshot(encoded: &[u8]) -> Result<ReplicatedSnapshot> {
    ensure_snapshot_bound(encoded.len())?;
    let payload = encoded
        .strip_prefix(SNAPSHOT_MAGIC)
        .ok_or_else(|| Error::InvalidSnapshot("graph control snapshot magic/version".into()))?;
    if payload.len() < 32 {
        return Err(Error::InvalidSnapshot(
            "graph control snapshot is truncated".into(),
        ));
    }
    let expected = LogHash::from_bytes(payload[..32].try_into().expect("length checked"));
    let body = &payload[32..];
    if LogHash::digest(&[body]) != expected {
        return Err(Error::InvalidSnapshot(
            "graph control snapshot digest mismatch".into(),
        ));
    }
    let mut decoder = BoundedDecoder::new(body);
    let cluster_id = decoder.string(MAX_ID_BYTES, "cluster id")?;
    let source_node_id = decoder.string(MAX_ID_BYTES, "source node id")?;
    let epoch = decoder.u64()?;
    let configuration_state = decode_configuration(&mut decoder)?;
    let recovery_generation = decoder.u64()?;
    let materializer_fingerprint = decoder.hash()?;
    let user_db_digest = decoder.hash()?;
    let applied_tip = decoder.anchor()?;
    let receipt_count = decoder.count(MAX_CONTROL_SNAPSHOT_RECEIPTS, "receipt count")?;
    if receipt_count > decoder.remaining() / MIN_ENCODED_RECEIPT_BYTES {
        return Err(Error::InvalidSnapshot(
            "graph control receipt count exceeds the remaining bounded payload".into(),
        ));
    }
    let mut receipts: Vec<RequestReceipt> = Vec::new();
    receipts
        .try_reserve_exact(receipt_count)
        .map_err(|_| Error::ResourceExhausted("graph control receipt allocation failed".into()))?;
    for _ in 0..receipt_count {
        let request_id = decoder.string(MAX_ID_BYTES, "request id")?;
        if receipts
            .last()
            .is_some_and(|receipt| receipt.request_id() >= request_id.as_str())
        {
            return Err(Error::InvalidSnapshot(
                "graph control receipts are not uniquely sorted".into(),
            ));
        }
        let receipt = RequestReceipt::new(
            request_id,
            decoder.hash()?,
            decoder.anchor()?,
            try_copy_control(
                decoder.bytes(MAX_LGFX_V1_BYTES, "result")?,
                "graph control result",
            )?,
        );
        validate_receipt(&receipt).map_err(super::invalid_snapshot_error)?;
        receipts.push(receipt);
    }
    decoder.finish()?;
    let snapshot = ReplicatedSnapshot {
        cluster_id,
        source_node_id,
        epoch,
        configuration_state,
        recovery_generation,
        materializer_fingerprint,
        user_db_digest,
        applied_tip,
        receipts,
    };
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn validate_snapshot(snapshot: &ReplicatedSnapshot) -> Result<()> {
    if snapshot.cluster_id.is_empty()
        || snapshot.cluster_id.len() > MAX_ID_BYTES
        || snapshot.source_node_id.is_empty()
        || snapshot.source_node_id.len() > MAX_ID_BYTES
        || snapshot.epoch == 0
        || snapshot.recovery_generation == 0
        || snapshot.receipts.len() > MAX_CONTROL_SNAPSHOT_RECEIPTS
    {
        return Err(Error::InvalidSnapshot(
            "invalid bounded graph control snapshot identity or receipt count".into(),
        ));
    }
    let mut previous = None;
    for receipt in &snapshot.receipts {
        validate_receipt(receipt).map_err(super::invalid_snapshot_error)?;
        if previous.is_some_and(|id: &str| id >= receipt.request_id()) {
            return Err(Error::InvalidSnapshot(
                "graph control receipts are not uniquely sorted".into(),
            ));
        }
        previous = Some(receipt.request_id());
    }
    Ok(())
}

fn encode_configuration(output: &mut Vec<u8>, value: &ConfigurationState) -> Result<()> {
    match value {
        ConfigurationState::Active { config_id, digest } => {
            try_push_control(output, 0)?;
            try_extend_control(output, &config_id.to_be_bytes())?;
            try_extend_control(output, digest.as_bytes())?;
        }
        ConfigurationState::Stopped {
            config_id,
            digest,
            stop,
            binding,
        } => {
            try_push_control(output, 1)?;
            try_extend_control(output, &config_id.to_be_bytes())?;
            try_extend_control(output, digest.as_bytes())?;
            encode_anchor(output, *stop)?;
            match binding {
                StopBinding::Unknown => try_push_control(output, 0)?,
                StopBinding::Unbound => try_push_control(output, 1)?,
                StopBinding::Bound {
                    successor,
                    stop_command_hash,
                } => {
                    try_push_control(output, 2)?;
                    write_bytes(
                        output,
                        successor.cluster_id().as_bytes(),
                        MAX_ID_BYTES,
                        "successor cluster id",
                    )?;
                    try_extend_control(output, &successor.predecessor_config_id().to_be_bytes())?;
                    try_extend_control(output, successor.predecessor_config_digest().as_bytes())?;
                    try_extend_control(output, &successor.config_id().to_be_bytes())?;
                    if successor.members().len() > MAX_CONFIGURATION_MEMBERS {
                        return Err(Error::ResourceExhausted(
                            "configuration members exceed bound".into(),
                        ));
                    }
                    try_extend_control(output, &(successor.members().len() as u64).to_be_bytes())?;
                    for member in successor.members() {
                        write_bytes(output, member.as_bytes(), MAX_ID_BYTES, "member id")?;
                    }
                    try_extend_control(output, stop_command_hash.as_bytes())?;
                }
            }
        }
    }
    if output.len() > MAX_CONTROL_SNAPSHOT_BYTES {
        return Err(Error::ResourceExhausted(
            "encoded graph configuration exceeds bound".into(),
        ));
    }
    Ok(())
}

fn decode_configuration(decoder: &mut BoundedDecoder<'_>) -> Result<ConfigurationState> {
    match decoder.u8()? {
        0 => Ok(ConfigurationState::active(decoder.u64()?, decoder.hash()?)),
        1 => {
            let config_id = decoder.u64()?;
            let digest = decoder.hash()?;
            let stop = decoder.anchor()?;
            let binding = match decoder.u8()? {
                0 => StopBinding::Unknown,
                1 => StopBinding::Unbound,
                2 => {
                    let cluster_id = decoder.string(MAX_ID_BYTES, "successor cluster id")?;
                    let predecessor_config_id = decoder.u64()?;
                    let predecessor_config_digest = decoder.hash()?;
                    let successor_config_id = decoder.u64()?;
                    let member_count =
                        decoder.count(MAX_CONFIGURATION_MEMBERS, "configuration members")?;
                    if member_count > decoder.remaining() / 9 {
                        return Err(Error::InvalidSnapshot(
                            "configuration member count exceeds the remaining bounded payload"
                                .into(),
                        ));
                    }
                    let mut members = Vec::new();
                    members.try_reserve_exact(member_count).map_err(|_| {
                        Error::ResourceExhausted(
                            "graph configuration member allocation failed".into(),
                        )
                    })?;
                    for _ in 0..member_count {
                        members.push(decoder.string(MAX_ID_BYTES, "member id")?);
                    }
                    StopBinding::Bound {
                        successor: SuccessorDescriptor::new(
                            cluster_id,
                            predecessor_config_id,
                            predecessor_config_digest,
                            successor_config_id,
                            members,
                        )
                        .map_err(|_| {
                            Error::InvalidSnapshot("invalid successor configuration".into())
                        })?,
                        stop_command_hash: decoder.hash()?,
                    }
                }
                value => {
                    return Err(Error::InvalidSnapshot(format!(
                        "invalid stop binding tag {value}"
                    )))
                }
            };
            Ok(ConfigurationState::Stopped {
                config_id,
                digest,
                stop,
                binding,
            })
        }
        value => Err(Error::InvalidSnapshot(format!(
            "invalid configuration tag {value}"
        ))),
    }
}

fn encode_anchor(output: &mut Vec<u8>, anchor: LogAnchor) -> Result<()> {
    try_extend_control(output, &anchor.index().to_be_bytes())?;
    try_extend_control(output, anchor.hash().as_bytes())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8], maximum: usize, label: &str) -> Result<()> {
    if value.len() > maximum {
        return Err(Error::ResourceExhausted(format!(
            "graph control {label} exceeds bound"
        )));
    }
    try_extend_control(output, &(value.len() as u64).to_be_bytes())?;
    try_extend_control(output, value)
}

fn try_extend_control(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let next = output
        .len()
        .checked_add(value.len())
        .ok_or_else(|| Error::ResourceExhausted("graph control encoding overflow".into()))?;
    ensure_snapshot_bound(next)?;
    output
        .try_reserve(value.len())
        .map_err(|_| Error::ResourceExhausted("graph control allocation failed".into()))?;
    output.extend_from_slice(value);
    Ok(())
}

fn try_push_control(output: &mut Vec<u8>, value: u8) -> Result<()> {
    try_extend_control(output, &[value])
}

fn try_copy_control(value: &[u8], label: &str) -> Result<Vec<u8>> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(|_| Error::ResourceExhausted(format!("{label} allocation failed")))?;
    copied.extend_from_slice(value);
    Ok(copied)
}

fn ensure_snapshot_bound(length: usize) -> Result<()> {
    if length > MAX_CONTROL_SNAPSHOT_BYTES {
        return Err(Error::ResourceExhausted(
            "replicated graph control exceeds snapshot bound".into(),
        ));
    }
    Ok(())
}

struct BoundedDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BoundedDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::InvalidSnapshot("graph control length overflow".into()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::InvalidSnapshot("graph control is truncated".into()))?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }
    fn hash(&mut self) -> Result<LogHash> {
        Ok(LogHash::from_bytes(
            self.take(32)?.try_into().expect("length checked"),
        ))
    }
    fn anchor(&mut self) -> Result<LogAnchor> {
        Ok(LogAnchor::new(self.u64()?, self.hash()?))
    }
    fn count(&mut self, maximum: usize, label: &str) -> Result<usize> {
        let count = usize::try_from(self.u64()?)
            .map_err(|_| Error::InvalidSnapshot(format!("{label} exceeds platform")))?;
        if count > maximum {
            return Err(Error::ResourceExhausted(format!(
                "graph control {label} exceeds bound"
            )));
        }
        Ok(count)
    }
    fn bytes(&mut self, maximum: usize, label: &str) -> Result<&'a [u8]> {
        let length = self.count(maximum, label)?;
        self.take(length)
    }
    fn string(&mut self, maximum: usize, label: &str) -> Result<String> {
        let value = self.bytes(maximum, label)?;
        let value = std::str::from_utf8(value)
            .map_err(|_| Error::InvalidSnapshot(format!("{label} is not UTF-8")))?;
        if value.is_empty() {
            return Err(Error::InvalidSnapshot(format!("{label} is empty")));
        }
        let mut copied = String::new();
        copied.try_reserve_exact(value.len()).map_err(|_| {
            Error::ResourceExhausted(format!("graph control {label} allocation failed"))
        })?;
        copied.push_str(value);
        Ok(copied)
    }
    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(Error::InvalidSnapshot(
                "graph control has trailing bytes".into(),
            ));
        }
        Ok(())
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

fn u64_to_sql(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::ResourceExhausted("graph control integer exceeds i64".into()))
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
                format!("expected 32-byte hash, got {}", bytes.len()),
            )),
        )
    })?;
    Ok(LogHash::from_bytes(bytes))
}

fn sqlite_error(error: rusqlite::Error) -> Error {
    Error::Io(format!("graph control SQLite error: {error}"))
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(super::io_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::IdentityMismatch(format!(
            "{label} is not a regular file"
        )));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(super::io_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(value: &[u8]) -> LogHash {
        LogHash::digest(&[value])
    }

    fn identity(node: &str) -> ControlIdentity {
        ControlIdentity::new(
            "cluster-1",
            node,
            7,
            ConfigurationState::active(3, hash(b"config")),
            1,
            hash(b"fingerprint"),
            hash(b"db-0"),
        )
    }

    #[test]
    fn transactional_receipts_accumulate_without_rewriting_or_silent_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.control");
        let store = ControlStore::create(&path, &identity("node-1")).unwrap();
        let mut base = LogAnchor::new(0, LogHash::ZERO);
        let mut db_digest = hash(b"db-0");
        for index in 1_u64..=128 {
            let entry = LogAnchor::new(index, hash(&index.to_be_bytes()));
            let target_digest = hash(&[b'd', index as u8]);
            let pending = PendingApply::new(base, entry, db_digest, target_digest, 4096);
            let result = super::super::GraphCommandResultV1::PutDocument { created: true };
            let receipt = RequestReceipt::new(
                format!("request-{index:03}"),
                hash(&index.to_be_bytes()),
                entry,
                result.encode(),
            );
            store.begin_pending(&pending).unwrap();
            store
                .commit_applied(
                    &pending,
                    &ConfigurationState::active(3, hash(b"config")),
                    Some(&receipt),
                )
                .unwrap();
            base = entry;
            db_digest = target_digest;
        }

        drop(store);
        let store = ControlStore::open_existing(&path).unwrap();
        for index in 1_u64..=128 {
            assert!(store
                .lookup_request(&format!("request-{index:03}"), hash(&index.to_be_bytes()))
                .unwrap()
                .is_some());
        }
        let snapshot = store.export_replicated_snapshot().unwrap();
        let destination =
            ControlStore::create(dir.path().join("restored.control"), &identity("node-2")).unwrap();
        destination
            .import_replicated_snapshot(&snapshot, "node-1")
            .unwrap();
        assert_eq!(destination.applied_tip().unwrap(), base);
        for index in 1_u64..=128 {
            assert!(destination
                .lookup_request(&format!("request-{index:03}"), hash(&index.to_be_bytes()))
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn bounded_control_decoder_rejects_oversized_and_truncated_fields_before_copy() {
        let oversized_length = (MAX_LGFX_V1_BYTES as u64 + 1).to_be_bytes();
        let mut oversized = BoundedDecoder::new(&oversized_length);
        assert!(matches!(
            oversized.bytes(MAX_LGFX_V1_BYTES, "result"),
            Err(Error::ResourceExhausted(_))
        ));

        let mut truncated = Vec::new();
        truncated.extend_from_slice(&8_u64.to_be_bytes());
        truncated.extend_from_slice(b"short");
        let mut decoder = BoundedDecoder::new(&truncated);
        assert!(matches!(
            decoder.bytes(MAX_LGFX_V1_BYTES, "result"),
            Err(Error::InvalidSnapshot(message)) if message.contains("truncated")
        ));

        assert!(matches!(
            ensure_snapshot_bound(MAX_CONTROL_SNAPSHOT_BYTES + 1),
            Err(Error::ResourceExhausted(_))
        ));

        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("graph.control"), &identity("node-1")).unwrap();
        let encoded = store.export_replicated_snapshot().unwrap();
        let body_start = SNAPSHOT_MAGIC.len() + 32;
        let mut body = encoded[body_start..].to_vec();
        let mut locator = BoundedDecoder::new(&body);
        locator.string(MAX_ID_BYTES, "cluster id").unwrap();
        locator.string(MAX_ID_BYTES, "source node id").unwrap();
        locator.u64().unwrap();
        decode_configuration(&mut locator).unwrap();
        locator.u64().unwrap();
        locator.hash().unwrap();
        locator.hash().unwrap();
        locator.anchor().unwrap();
        let receipt_count_offset = locator.offset;
        body[receipt_count_offset..receipt_count_offset + 8]
            .copy_from_slice(&1000_u64.to_be_bytes());
        let mut forged = Vec::new();
        forged.extend_from_slice(SNAPSHOT_MAGIC);
        forged.extend_from_slice(LogHash::digest(&[&body]).as_bytes());
        forged.extend_from_slice(&body);
        assert!(matches!(
            decode_snapshot(&forged),
            Err(Error::InvalidSnapshot(message)) if message.contains("remaining bounded payload")
        ));
    }

    #[test]
    fn snapshot_transport_overflow_is_typed_and_never_evicts_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            ControlStore::create(dir.path().join("graph.control"), &identity("node-1")).unwrap();
        let pending = PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, hash(b"entry")),
            hash(b"db-0"),
            hash(b"db-1"),
            4096,
        );
        let request_digest = hash(b"request");
        let receipt = RequestReceipt::new(
            "request-1",
            request_digest,
            pending.entry(),
            super::super::GraphCommandResultV1::PutDocument { created: true }.encode(),
        );
        store.begin_pending(&pending).unwrap();
        store
            .commit_applied(
                &pending,
                &ConfigurationState::active(3, hash(b"config")),
                Some(&receipt),
            )
            .unwrap();

        assert!(matches!(
            ensure_snapshot_bound(MAX_CONTROL_SNAPSHOT_BYTES + 1),
            Err(Error::ResourceExhausted(_))
        ));
        assert_eq!(
            store.lookup_request("request-1", request_digest).unwrap(),
            Some(receipt)
        );
    }
}
