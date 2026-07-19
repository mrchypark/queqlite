//! LadybugDB materialization for deterministic QuePaxa log entries.
//!
//! The write surface is deliberately semantic: callers encode [`GraphCommandV1`]
//! values instead of submitting arbitrary Cypher. This keeps generated values,
//! external I/O, transaction control, and other non-replayable behavior outside
//! the authoritative state machine.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use lbug::{Connection, Database, LogicalType, SystemConfig, Value};
use rhiza_core::{
    ConfigurationState, EntryType, ExecutionProfile, LogAnchor, LogEntry, LogHash, LogIndex,
    ReplicatedCommandEnvelope,
};
use tempfile::NamedTempFile;

#[cfg(feature = "vfs")]
use tracking_file_system::{DirtyTracker, TrackingFileSystem};

mod control;
mod lgfx;
#[cfg(feature = "vfs")]
mod tracking_file_system;

pub use control::{ControlIdentity, ControlStore, PendingApply, RequestReceipt};
pub use lgfx::{
    apply_lgfx_to_exact_base, capture_graph_entry_native_wal, diff_closed_ladybug_files,
    lgfx_chunks_digest, open_lgfx_readback, replay_native_ladybug_wal, CapturedNativeLadybugWal,
    LadybugFileChunkV1, LadybugFileEffectV1, LGFX_CHUNK_BYTES, LGFX_V1_MAGIC, MAX_LGFX_V1_BYTES,
};

const COMMAND_MAGIC: &[u8; 6] = b"RHGC\0\x01";
const BATCH_COMMAND_MAGIC: &[u8; 6] = b"RHGB\0\x01";
const RESULT_MAGIC: &[u8; 6] = b"RHGR\0\x01";
const SNAPSHOT_DOMAIN: &[u8] = b"rhiza-ladybug-snapshot-v2\0";
const SNAPSHOT_WIRE_MAGIC: &[u8; 4] = b"RHGS";
const SNAPSHOT_WIRE_VERSION: u16 = 2;
const RESTORE_INTENT_MAGIC: &[u8] = b"RHIZA-GRAPH-RESTORE\0\x01";
const RESTORE_INTENT_HASHES: usize = 4;
const RESTORE_INTENT_BYTES: usize =
    RESTORE_INTENT_MAGIC.len() + 1 + 32 * RESTORE_INTENT_HASHES + 32;
const MATERIALIZER_DOMAIN: &[u8] = b"rhiza-graph-materializer-v1\0";
const SCHEMA_VERSION: &str = "1";
const MAX_REQUEST_ID_BYTES: usize = 256;
const MAX_RHGS_ID_BYTES: usize = 256;
const MAX_RHGS_DB_BYTES: usize = 1024 * 1024 * 1024;
const MAX_RHGS_CONTROL_BYTES: usize = control::MAX_CONTROL_SNAPSHOT_BYTES;
const MAX_RHGS_V2_BYTES: usize = MAX_RHGS_DB_BYTES + MAX_RHGS_CONTROL_BYTES + 1024 * 1024;
const MAX_DOCUMENT_ID_BYTES: usize = 1024;
const MAX_STRING_BYTES: usize = 256 * 1024;
const MAX_BLOB_BYTES: usize = 4096;
pub const MAX_GRAPH_QUERY_BYTES: usize = 64 * 1024;
pub const MAX_GRAPH_PARAMETERS: usize = 999;
pub const MAX_GRAPH_PARAMETER_DEPTH: usize = 16;
const MAX_GRAPH_PARAMETER_VALUES: usize = 4096;
const MAX_GRAPH_PARAMETER_CONTAINER_VALUES: usize = 1024;
const MAX_GRAPH_PARAMETER_NAME_BYTES: usize = 256;
const LADYBUG_BUFFER_POOL_BYTES: u64 = 512 * 1024 * 1024;
const LADYBUG_MAX_NUM_THREADS: u64 = 2;
const LADYBUG_BUFFER_POOL_EXHAUSTED: &str =
    "Buffer manager exception: Unable to allocate memory! The buffer pool is full and no memory could be freed!";
const LADYBUG_CONVERSION_ERROR_PREFIX: &str = "Conversion exception:";
const BATCH_COMMAND_VERSION: u16 = 2;
const BATCH_REQUEST_ID: &str = "__rhiza_graph_batch_v1";
pub const MAX_GRAPH_BATCH_MEMBERS: usize = 64;

const CREATE_DOCUMENT_TABLE: &str = r#"
CREATE NODE TABLE IF NOT EXISTS RhizaDocument(
    id STRING PRIMARY KEY,
    kind UINT8,
    bool_value BOOL,
    i64_value INT64,
    u64_value UINT64,
    f64_value DOUBLE,
    string_value STRING,
    bytes_value BLOB
)
"#;

pub type Result<T> = std::result::Result<T, Error>;

/// Stable compatibility identity for Ladybug bytes and deterministic graph semantics.
pub fn graph_materializer_fingerprint() -> LogHash {
    LogHash::digest(&[
        MATERIALIZER_DOMAIN,
        b"lbug=0.18.1",
        &lbug::get_storage_version().to_be_bytes(),
        SCHEMA_VERSION.as_bytes(),
        COMMAND_MAGIC,
        BATCH_COMMAND_MAGIC,
        &BATCH_COMMAND_VERSION.to_be_bytes(),
    ])
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Closed,
    Codec(String),
    InvalidCommand(String),
    InvalidEntry(String),
    IdentityMismatch(String),
    Ladybug(String),
    ResourceExhausted(String),
    Io(String),
    RequestConflict {
        request_id: String,
        original_log_index: LogIndex,
        original_log_hash: LogHash,
    },
    InvalidSnapshot(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "Ladybug state machine is closed"),
            Self::Codec(message) => write!(f, "invalid graph command encoding: {message}"),
            Self::InvalidCommand(message) => write!(f, "invalid graph command: {message}"),
            Self::InvalidEntry(message) => write!(f, "invalid log entry: {message}"),
            Self::IdentityMismatch(field) => {
                write!(f, "Ladybug database identity mismatch for {field}")
            }
            Self::Ladybug(message) => write!(f, "Ladybug error: {message}"),
            Self::ResourceExhausted(message) => {
                write!(f, "Ladybug query resources exhausted: {message}")
            }
            Self::Io(message) => write!(f, "Ladybug snapshot I/O failed: {message}"),
            Self::RequestConflict { request_id, .. } => {
                write!(
                    f,
                    "request id reused with a different graph command: {request_id}"
                )
            }
            Self::InvalidSnapshot(message) => write!(f, "invalid Ladybug snapshot: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// A finite, canonical IEEE-754 double.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF64(u64);

impl CanonicalF64 {
    pub fn new(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(Error::InvalidCommand(
                "floating graph values must be finite".into(),
            ));
        }
        // Canonicalize negative zero so equal numeric inputs have one encoding.
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        Ok(Self(bits))
    }

    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }
}

/// Canonical scalar values accepted by the first rhiza graph command format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphValueV1 {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(CanonicalF64),
    String(String),
    Bytes(Vec<u8>),
}

/// Canonical values accepted by the direct read-only graph query boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphParameterValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(CanonicalF64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GraphParameterValue>),
    Struct(BTreeMap<String, GraphParameterValue>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphInternalId {
    pub offset: u64,
    pub table_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphNode {
    pub id: GraphInternalId,
    pub label: String,
    pub properties: Vec<(String, GraphResultValue)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRel {
    pub src: GraphInternalId,
    pub dst: GraphInternalId,
    pub label: String,
    pub properties: Vec<(String, GraphResultValue)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphLogicalType {
    Any,
    Bool,
    Serial,
    I64,
    I32,
    I16,
    I8,
    U64,
    U32,
    U16,
    U8,
    I128,
    F64,
    F32,
    Date,
    Interval,
    Timestamp,
    TimestampTz,
    TimestampNs,
    TimestampMs,
    TimestampSec,
    InternalId,
    String,
    Json,
    Bytes,
    List(Box<GraphLogicalType>),
    Array {
        element_type: Box<GraphLogicalType>,
        length: u64,
    },
    Struct(Vec<(String, GraphLogicalType)>),
    Node,
    Rel,
    RecursiveRel,
    Map {
        key_type: Box<GraphLogicalType>,
        value_type: Box<GraphLogicalType>,
    },
    Union(Vec<(String, GraphLogicalType)>),
    Uuid,
    Decimal {
        precision: u32,
        scale: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphColumn {
    pub name: String,
    pub logical_type: GraphLogicalType,
}

/// Lossless, transport-neutral values returned by direct graph queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphResultValue {
    Null(GraphLogicalType),
    Bool(bool),
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    U64(u64),
    U32(u32),
    U16(u16),
    U8(u8),
    I128(String),
    F64(CanonicalF64),
    F32(String),
    Date(String),
    Interval(String),
    Timestamp(String),
    TimestampTz(String),
    TimestampNs(String),
    TimestampMs(String),
    TimestampSec(String),
    InternalId(GraphInternalId),
    String(String),
    Json(String),
    Bytes(Vec<u8>),
    List {
        element_type: GraphLogicalType,
        values: Vec<GraphResultValue>,
    },
    Array {
        element_type: GraphLogicalType,
        values: Vec<GraphResultValue>,
    },
    Struct(Vec<(String, GraphResultValue)>),
    Node(GraphNode),
    Rel(GraphRel),
    RecursiveRel {
        nodes: Vec<GraphNode>,
        rels: Vec<GraphRel>,
    },
    Map {
        key_type: GraphLogicalType,
        value_type: GraphLogicalType,
        entries: Vec<(GraphResultValue, GraphResultValue)>,
    },
    Union {
        variants: Vec<(String, GraphLogicalType)>,
        value: Box<GraphResultValue>,
    },
    Uuid(String),
    Decimal(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphQueryResult {
    pub columns: Vec<GraphColumn>,
    pub rows: Vec<Vec<GraphResultValue>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

impl GraphValueV1 {
    pub fn from_f64(value: f64) -> Result<Self> {
        Ok(Self::F64(CanonicalF64::new(value)?))
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::String(value) if value.len() > MAX_STRING_BYTES => Err(Error::InvalidCommand(
                format!("string graph values cannot exceed {MAX_STRING_BYTES} bytes"),
            )),
            Self::Bytes(value) if value.len() > MAX_BLOB_BYTES => Err(Error::InvalidCommand(
                format!("byte graph values cannot exceed {MAX_BLOB_BYTES} bytes"),
            )),
            _ => Ok(()),
        }
    }

    fn encode_into(&self, output: &mut Vec<u8>) {
        match self {
            Self::Null => output.push(0),
            Self::Bool(false) => output.push(1),
            Self::Bool(true) => output.push(2),
            Self::I64(value) => {
                output.push(3);
                output.extend_from_slice(&value.to_be_bytes());
            }
            Self::U64(value) => {
                output.push(4);
                output.extend_from_slice(&value.to_be_bytes());
            }
            Self::F64(value) => {
                output.push(5);
                output.extend_from_slice(&value.bits().to_be_bytes());
            }
            Self::String(value) => {
                output.push(6);
                write_bytes(output, value.as_bytes());
            }
            Self::Bytes(value) => {
                output.push(7);
                write_bytes(output, value);
            }
        }
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let value = match decoder.u8()? {
            0 => Self::Null,
            1 => Self::Bool(false),
            2 => Self::Bool(true),
            3 => Self::I64(i64::from_be_bytes(decoder.array()?)),
            4 => Self::U64(u64::from_be_bytes(decoder.array()?)),
            5 => {
                let bits = u64::from_be_bytes(decoder.array()?);
                let canonical = CanonicalF64::new(f64::from_bits(bits))?;
                if canonical.bits() != bits {
                    return Err(Error::Codec("noncanonical floating value".into()));
                }
                Self::F64(canonical)
            }
            6 => Self::String(decoder.string(MAX_STRING_BYTES)?),
            7 => Self::Bytes(decoder.bytes(MAX_BLOB_BYTES)?.to_vec()),
            tag => return Err(Error::Codec(format!("unknown graph value tag {tag}"))),
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GraphOperationV1 {
    PutDocument { id: String, value: GraphValueV1 },
    DeleteDocument { id: String },
}

/// Versioned semantic write command. It cannot carry raw write-Cypher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCommandV1 {
    request_id: String,
    operation: GraphOperationV1,
}

impl GraphCommandV1 {
    pub fn put_document(
        request_id: impl Into<String>,
        id: impl Into<String>,
        value: GraphValueV1,
    ) -> Result<Self> {
        let command = Self {
            request_id: request_id.into(),
            operation: GraphOperationV1::PutDocument {
                id: id.into(),
                value,
            },
        };
        command.validate()?;
        Ok(command)
    }

    pub fn delete_document(request_id: impl Into<String>, id: impl Into<String>) -> Result<Self> {
        let command = Self {
            request_id: request_id.into(),
            operation: GraphOperationV1::DeleteDocument { id: id.into() },
        };
        command.validate()?;
        Ok(command)
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(COMMAND_MAGIC);
        write_bytes(&mut output, self.request_id.as_bytes());
        match &self.operation {
            GraphOperationV1::PutDocument { id, value } => {
                output.push(1);
                write_bytes(&mut output, id.as_bytes());
                value.encode_into(&mut output);
            }
            GraphOperationV1::DeleteDocument { id } => {
                output.push(2);
                write_bytes(&mut output, id.as_bytes());
            }
        }
        output
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        if decoder.take(COMMAND_MAGIC.len())? != COMMAND_MAGIC {
            return Err(Error::Codec("wrong graph command magic or version".into()));
        }
        let request_id = decoder.string(MAX_REQUEST_ID_BYTES)?;
        let operation = match decoder.u8()? {
            1 => GraphOperationV1::PutDocument {
                id: decoder.string(MAX_DOCUMENT_ID_BYTES)?,
                value: GraphValueV1::decode(&mut decoder)?,
            },
            2 => GraphOperationV1::DeleteDocument {
                id: decoder.string(MAX_DOCUMENT_ID_BYTES)?,
            },
            tag => return Err(Error::Codec(format!("unknown graph command tag {tag}"))),
        };
        if !decoder.is_empty() {
            return Err(Error::Codec("trailing graph command bytes".into()));
        }
        let command = Self {
            request_id,
            operation,
        };
        command.validate()?;
        if command.encode() != bytes {
            return Err(Error::Codec("noncanonical graph command".into()));
        }
        Ok(command)
    }

    fn validate(&self) -> Result<()> {
        validate_nonempty_bytes("request_id", &self.request_id, MAX_REQUEST_ID_BYTES)?;
        match &self.operation {
            GraphOperationV1::PutDocument { id, value } => {
                validate_nonempty_bytes("document id", id, MAX_DOCUMENT_ID_BYTES)?;
                value.validate()
            }
            GraphOperationV1::DeleteDocument { id } => {
                validate_nonempty_bytes("document id", id, MAX_DOCUMENT_ID_BYTES)
            }
        }
    }
}

/// Wraps a canonical RHGC v1 body in the common replicated-command envelope.
pub fn encode_replicated_graph_command(command: &GraphCommandV1) -> Result<Vec<u8>> {
    ReplicatedCommandEnvelope::new(
        ExecutionProfile::Graph,
        1,
        command.request_id(),
        command.encode(),
    )
    .and_then(|envelope| envelope.encode())
    .map_err(|error| Error::InvalidCommand(error.to_string()))
}

/// Encodes ordered semantic graph mutations as one canonical replicated batch.
pub fn encode_replicated_graph_batch(commands: &[GraphCommandV1]) -> Result<Vec<u8>> {
    if commands.is_empty() || commands.len() > MAX_GRAPH_BATCH_MEMBERS {
        return Err(Error::InvalidCommand(format!(
            "graph batch must contain 1..={MAX_GRAPH_BATCH_MEMBERS} commands"
        )));
    }
    let mut request_ids = BTreeSet::new();
    let mut body = Vec::from(BATCH_COMMAND_MAGIC.as_slice());
    body.extend_from_slice(&(commands.len() as u16).to_be_bytes());
    for command in commands {
        command.validate()?;
        if !request_ids.insert(command.request_id()) {
            return Err(Error::InvalidCommand(format!(
                "graph batch repeats request id {:?}",
                command.request_id()
            )));
        }
        write_bytes(&mut body, &command.encode());
    }
    ReplicatedCommandEnvelope::new(
        ExecutionProfile::Graph,
        BATCH_COMMAND_VERSION,
        BATCH_REQUEST_ID,
        body,
    )
    .and_then(|envelope| envelope.encode())
    .map_err(|error| Error::InvalidCommand(error.to_string()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphCommandResultV1 {
    PutDocument { created: bool },
    DeleteDocument { existed: bool },
}

impl GraphCommandResultV1 {
    fn encode(&self) -> Vec<u8> {
        let mut output = Vec::from(RESULT_MAGIC.as_slice());
        match self {
            Self::PutDocument { created } => {
                output.push(1);
                output.push(u8::from(*created));
            }
            Self::DeleteDocument { existed } => {
                output.push(2);
                output.push(u8::from(*existed));
            }
        }
        output
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        if decoder.take(RESULT_MAGIC.len())? != RESULT_MAGIC {
            return Err(Error::Codec("wrong graph result magic or version".into()));
        }
        let tag = decoder.u8()?;
        let flag = match decoder.u8()? {
            0 => false,
            1 => true,
            value => return Err(Error::Codec(format!("invalid graph result flag {value}"))),
        };
        if !decoder.is_empty() {
            return Err(Error::Codec("trailing graph result bytes".into()));
        }
        match tag {
            1 => Ok(Self::PutDocument { created: flag }),
            2 => Ok(Self::DeleteDocument { existed: flag }),
            value => Err(Error::Codec(format!("unknown graph result tag {value}"))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestRecord {
    original_log_index: LogIndex,
    original_log_hash: LogHash,
    result: GraphCommandResultV1,
}

impl RequestRecord {
    pub const fn original_log_index(&self) -> LogIndex {
        self.original_log_index
    }

    pub const fn original_log_hash(&self) -> LogHash {
        self.original_log_hash
    }

    pub const fn result(&self) -> &GraphCommandResultV1 {
        &self.result
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyOutcome {
    applied_index: LogIndex,
    applied_hash: LogHash,
    result: Option<GraphCommandResultV1>,
}

impl ApplyOutcome {
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }

    pub const fn result(&self) -> Option<&GraphCommandResultV1> {
        self.result.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LadybugSnapshot {
    cluster_id: String,
    created_by: String,
    epoch: u64,
    config_id: u64,
    applied_index: LogIndex,
    applied_hash: LogHash,
    storage_version: u64,
    materializer_fingerprint: LogHash,
    digest: LogHash,
    db_bytes: Vec<u8>,
    replicated_control: Vec<u8>,
}

impl LadybugSnapshot {
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn created_by(&self) -> &str {
        &self.created_by
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn config_id(&self) -> u64 {
        self.config_id
    }

    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }

    pub const fn storage_version(&self) -> u64 {
        self.storage_version
    }

    pub const fn materializer_fingerprint(&self) -> LogHash {
        self.materializer_fingerprint
    }

    pub const fn digest(&self) -> LogHash {
        self.digest
    }

    pub fn db_bytes(&self) -> &[u8] {
        &self.db_bytes
    }

    pub fn replicated_control(&self) -> &[u8] {
        &self.replicated_control
    }

    fn recompute_digest(&self) -> LogHash {
        let cluster_id_length = (self.cluster_id.len() as u64).to_be_bytes();
        let created_by_length = (self.created_by.len() as u64).to_be_bytes();
        let database_length = u64::try_from(self.db_bytes.len()).expect("usize fits in u64");
        let control_length =
            u64::try_from(self.replicated_control.len()).expect("usize fits in u64");
        LogHash::digest(&[
            SNAPSHOT_DOMAIN,
            &cluster_id_length,
            self.cluster_id.as_bytes(),
            &created_by_length,
            self.created_by.as_bytes(),
            &self.epoch.to_be_bytes(),
            &self.config_id.to_be_bytes(),
            &self.storage_version.to_be_bytes(),
            &self.applied_index.to_be_bytes(),
            self.applied_hash.as_bytes(),
            self.materializer_fingerprint.as_bytes(),
            &database_length.to_be_bytes(),
            &self.db_bytes,
            &control_length.to_be_bytes(),
            &self.replicated_control,
        ])
    }
}

/// Encodes a complete Ladybug snapshot as one canonical, versioned archive object.
pub fn encode_snapshot(snapshot: &LadybugSnapshot) -> Result<Vec<u8>> {
    validate_snapshot_envelope(snapshot)?;
    let capacity = rhgs_encoded_len(snapshot)?;
    ensure_rhgs_total_bound(capacity)?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(capacity)
        .map_err(|_| Error::ResourceExhausted("RHGS v2 allocation failed".into()))?;
    try_extend_rhgs(&mut encoded, SNAPSHOT_WIRE_MAGIC)?;
    try_extend_rhgs(&mut encoded, &SNAPSHOT_WIRE_VERSION.to_be_bytes())?;
    encode_snapshot_bytes(&mut encoded, snapshot.cluster_id.as_bytes())?;
    encode_snapshot_bytes(&mut encoded, snapshot.created_by.as_bytes())?;
    try_extend_rhgs(&mut encoded, &snapshot.epoch.to_be_bytes())?;
    try_extend_rhgs(&mut encoded, &snapshot.config_id.to_be_bytes())?;
    try_extend_rhgs(&mut encoded, &snapshot.applied_index.to_be_bytes())?;
    try_extend_rhgs(&mut encoded, snapshot.applied_hash.as_bytes())?;
    try_extend_rhgs(&mut encoded, &snapshot.storage_version.to_be_bytes())?;
    try_extend_rhgs(&mut encoded, snapshot.materializer_fingerprint.as_bytes())?;
    try_extend_rhgs(&mut encoded, snapshot.digest.as_bytes())?;
    encode_snapshot_bytes(&mut encoded, &snapshot.db_bytes)?;
    encode_snapshot_bytes(&mut encoded, &snapshot.replicated_control)?;
    ensure_rhgs_total_bound(encoded.len())?;
    Ok(encoded)
}

/// Decodes and verifies a canonical Ladybug snapshot archive object.
pub fn decode_snapshot(encoded: &[u8]) -> Result<LadybugSnapshot> {
    ensure_rhgs_total_bound(encoded.len())?;
    let mut decoder = SnapshotDecoder::new(encoded);
    if decoder.take(SNAPSHOT_WIRE_MAGIC.len())? != SNAPSHOT_WIRE_MAGIC {
        return Err(Error::InvalidSnapshot(
            "snapshot envelope magic does not match RHGS".into(),
        ));
    }
    let version = decoder.u16()?;
    if version != SNAPSHOT_WIRE_VERSION {
        return Err(Error::InvalidSnapshot(format!(
            "unsupported snapshot envelope version {version}"
        )));
    }
    let snapshot = LadybugSnapshot {
        cluster_id: decoder.string(MAX_RHGS_ID_BYTES, "cluster id")?,
        created_by: decoder.string(MAX_RHGS_ID_BYTES, "source node id")?,
        epoch: decoder.u64()?,
        config_id: decoder.u64()?,
        applied_index: decoder.u64()?,
        applied_hash: LogHash::from_bytes(decoder.array()?),
        storage_version: decoder.u64()?,
        materializer_fingerprint: LogHash::from_bytes(decoder.array()?),
        digest: LogHash::from_bytes(decoder.array()?),
        db_bytes: try_copy_bounded(
            decoder.bytes(MAX_RHGS_DB_BYTES, "Ladybug database")?,
            "RHGS v2 Ladybug database",
        )?,
        replicated_control: try_copy_bounded(
            decoder.bytes(MAX_RHGS_CONTROL_BYTES, "replicated graph control")?,
            "RHGS v2 replicated graph control",
        )?,
    };
    if !decoder.is_empty() {
        return Err(Error::InvalidSnapshot(
            "snapshot envelope has trailing bytes".into(),
        ));
    }
    validate_snapshot_envelope(&snapshot)?;
    Ok(snapshot)
}

fn validate_snapshot_envelope(snapshot: &LadybugSnapshot) -> Result<()> {
    if snapshot.cluster_id.is_empty()
        || snapshot.cluster_id.len() > MAX_RHGS_ID_BYTES
        || snapshot.created_by.is_empty()
        || snapshot.created_by.len() > MAX_RHGS_ID_BYTES
        || snapshot.epoch == 0
    {
        return Err(Error::InvalidSnapshot(
            "snapshot identity must contain bounded cluster and source node ids".into(),
        ));
    }
    if snapshot.db_bytes.len() > MAX_RHGS_DB_BYTES {
        return Err(Error::ResourceExhausted(
            "RHGS v2 Ladybug database exceeds bound".into(),
        ));
    }
    if snapshot.replicated_control.len() > MAX_RHGS_CONTROL_BYTES {
        return Err(Error::ResourceExhausted(
            "RHGS v2 replicated graph control exceeds bound".into(),
        ));
    }
    if snapshot.storage_version != lbug::get_storage_version() {
        return Err(Error::InvalidSnapshot(format!(
            "storage version {} does not match local {}",
            snapshot.storage_version,
            lbug::get_storage_version()
        )));
    }
    if snapshot.materializer_fingerprint != graph_materializer_fingerprint() {
        return Err(Error::InvalidSnapshot(
            "materializer fingerprint does not match this binary".into(),
        ));
    }
    if snapshot.recompute_digest() != snapshot.digest {
        return Err(Error::InvalidSnapshot(
            "snapshot digest does not match its contents".into(),
        ));
    }
    Ok(())
}

fn rhgs_encoded_len(snapshot: &LadybugSnapshot) -> Result<usize> {
    [
        SNAPSHOT_WIRE_MAGIC.len(),
        2,
        8,
        snapshot.cluster_id.len(),
        8,
        snapshot.created_by.len(),
        8,
        8,
        8,
        32,
        8,
        32,
        32,
        8,
        snapshot.db_bytes.len(),
        8,
        snapshot.replicated_control.len(),
    ]
    .into_iter()
    .try_fold(0usize, |total, value| total.checked_add(value))
    .ok_or_else(|| Error::ResourceExhausted("RHGS v2 size overflows".into()))
}

fn encode_snapshot_bytes(encoded: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    try_extend_rhgs(encoded, &(value.len() as u64).to_be_bytes())?;
    try_extend_rhgs(encoded, value)
}

fn try_extend_rhgs(encoded: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let next = encoded
        .len()
        .checked_add(value.len())
        .ok_or_else(|| Error::ResourceExhausted("RHGS v2 size overflows".into()))?;
    ensure_rhgs_total_bound(next)?;
    encoded
        .try_reserve(value.len())
        .map_err(|_| Error::ResourceExhausted("RHGS v2 allocation failed".into()))?;
    encoded.extend_from_slice(value);
    Ok(())
}

fn try_copy_bounded(value: &[u8], label: &str) -> Result<Vec<u8>> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(|_| Error::ResourceExhausted(format!("{label} allocation failed")))?;
    copied.extend_from_slice(value);
    Ok(copied)
}

struct SnapshotDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::InvalidSnapshot("snapshot envelope length overflow".into()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::InvalidSnapshot("snapshot envelope is truncated".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn bytes(&mut self, maximum: usize, label: &str) -> Result<&'a [u8]> {
        let length = usize::try_from(self.u64()?).map_err(|_| {
            Error::InvalidSnapshot("snapshot envelope length exceeds this platform".into())
        })?;
        if length > maximum {
            return Err(Error::ResourceExhausted(format!(
                "RHGS v2 {label} exceeds bound"
            )));
        }
        self.take(length)
    }

    fn string(&mut self, maximum: usize, label: &str) -> Result<String> {
        let bytes = self.bytes(maximum, label)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| Error::InvalidSnapshot("snapshot identity is not valid UTF-8".into()))?;
        let mut copied = String::new();
        copied
            .try_reserve_exact(value.len())
            .map_err(|_| Error::ResourceExhausted(format!("RHGS v2 {label} allocation failed")))?;
        copied.push_str(value);
        Ok(copied)
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn ensure_rhgs_total_bound(length: usize) -> Result<()> {
    if length > MAX_RHGS_V2_BYTES {
        return Err(Error::ResourceExhausted(
            "RHGS v2 envelope exceeds bound".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct Identity {
    cluster_id: String,
    node_id: String,
    epoch: u64,
}

/// Authoritative LadybugDB materialized state guarded by a single local writer.
pub struct LadybugStateMachine {
    path: PathBuf,
    identity: Identity,
    database: RwLock<Option<Database>>,
    writer: Mutex<()>,
    control: ControlStore,
}

impl LadybugStateMachine {
    pub fn open(
        path: impl AsRef<Path>,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        config_id: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;
        Self::open_with_configuration(
            path,
            cluster_id,
            node_id,
            epoch,
            ConfigurationState::active(config_id, LogHash::ZERO),
            1,
        )
    }

    pub fn open_with_configuration(
        path: impl AsRef<Path>,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: ConfigurationState,
        recovery_generation: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure_parent(&path)?;
        recover_interrupted_snapshot_publish(&path)?;
        let control_path = control_sidecar_path(&path);
        match (path_present(&path)?, path_present(&control_path)?) {
            (false, false) => Self::create_new(
                &path,
                cluster_id,
                node_id,
                epoch,
                configuration_state,
                recovery_generation,
            ),
            (true, true) => Self::open_existing_pair(
                &path,
                cluster_id,
                node_id,
                epoch,
                recovery_generation,
            ),
            (true, false) => Err(Error::IdentityMismatch(
                "graph control sidecar is missing; restore an RHGS v2 snapshot instead of auto-migrating"
                    .into(),
            )),
            (false, true) => Err(Error::IdentityMismatch(
                "canonical Ladybug database is missing beside its graph control sidecar".into(),
            )),
        }
    }

    fn create_new(
        path: &Path,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        configuration_state: ConfigurationState,
        recovery_generation: u64,
    ) -> Result<Self> {
        let control_path = control_sidecar_path(path);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(io_error)?;
        let mut owns_control = false;
        let created = (|| {
            let identity = Identity {
                cluster_id: cluster_id.into(),
                node_id: node_id.into(),
                epoch,
            };
            let database = open_database(path)?;
            let connection = Connection::new(&database).map_err(ladybug_error)?;
            connection
                .query(CREATE_DOCUMENT_TABLE)
                .map_err(ladybug_error)?;
            connection.query("CHECKPOINT").map_err(ladybug_error)?;
            drop(connection);
            drop(database);
            reject_legacy_database(path)?;
            let digest = lgfx::file_digest(path)?;
            let control = ControlStore::create(
                &control_path,
                &ControlIdentity::new(
                    cluster_id,
                    node_id,
                    epoch,
                    configuration_state,
                    recovery_generation,
                    graph_materializer_fingerprint(),
                    digest,
                ),
            )?;
            owns_control = true;
            Ok(Self {
                path: path.to_path_buf(),
                identity,
                database: RwLock::new(Some(open_database(path)?)),
                writer: Mutex::new(()),
                control,
            })
        })();
        if created.is_err() {
            remove_sidecars(path);
            let _ = fs::remove_file(path);
            if owns_control {
                let _ = fs::remove_file(control_path);
            }
        }
        created
    }

    fn open_existing_pair(
        path: &Path,
        cluster_id: &str,
        node_id: &str,
        epoch: u64,
        recovery_generation: u64,
    ) -> Result<Self> {
        require_regular_file(path, "canonical Ladybug database")?;
        require_regular_file(
            &control_sidecar_path(path),
            "canonical graph control sidecar",
        )?;
        reject_legacy_database(path)?;
        let control = ControlStore::open_existing(control_sidecar_path(path))?;
        validate_control_database_pair(path, &control)?;
        let persisted = control.identity()?;
        if persisted.cluster_id() != cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if persisted.node_id() != node_id {
            return Err(Error::IdentityMismatch("node_id".into()));
        }
        if persisted.epoch() != epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if persisted.recovery_generation() != recovery_generation {
            return Err(Error::IdentityMismatch("recovery_generation".into()));
        }
        if persisted.materializer_fingerprint() != graph_materializer_fingerprint() {
            return Err(Error::IdentityMismatch(
                "graph materializer fingerprint".into(),
            ));
        }
        Ok(Self {
            path: path.to_path_buf(),
            identity: Identity {
                cluster_id: cluster_id.into(),
                node_id: node_id.into(),
                epoch,
            },
            database: RwLock::new(Some(open_database(path)?)),
            writer: Mutex::new(()),
            control,
        })
    }

    pub fn apply_entry(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        if entry.recompute_hash() != entry.hash {
            return Err(Error::InvalidEntry(
                "hash does not match entry contents".into(),
            ));
        }
        let _writer = self
            .writer
            .lock()
            .map_err(|_| Error::Ladybug("state machine writer lock is poisoned".into()))?;
        self.apply_lgfx_entry(entry)
    }

    pub fn applied_index(&self) -> Result<LogIndex> {
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        Ok(self.control.applied_tip()?.index())
    }

    pub fn applied_hash(&self) -> Result<LogHash> {
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        Ok(self.control.applied_tip()?.hash())
    }

    pub fn materialized_tip(&self) -> Result<LogAnchor> {
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        self.control.applied_tip()
    }

    /// Returns the committed control base for startup qlog reconciliation.
    ///
    /// Unlike normal read APIs, this deliberately remains available while an
    /// LGFX apply is pending so startup can redeliver the next committed qlog
    /// entry. It must not be used for serving reads or creating snapshots.
    pub fn reconciliation_base_state(&self) -> Result<(LogAnchor, ConfigurationState)> {
        let _writer = self.lock_writer()?;
        self.control.committed_state()
    }

    pub fn configuration_state_value(&self) -> Result<ConfigurationState> {
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        self.control.configuration_state()
    }

    pub fn canonical_db_digest(&self) -> Result<LogHash> {
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        self.close_database_cleanly()?;
        let digest = lgfx::file_digest(&self.path);
        let reopen = self.reopen_database();
        match (digest, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(digest), Ok(())) => Ok(digest),
        }
    }

    /// Safe read boundary for the fixed document projection. No raw Cypher is accepted.
    pub fn get_document(&self, id: &str) -> Result<Option<GraphValueV1>> {
        validate_nonempty_bytes("document id", id, MAX_DOCUMENT_ID_BYTES)?;
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        document(&connection, id)
    }

    /// Reads one fixed document projection and the materialized log tip from
    /// one Ladybug query snapshot, rechecking misses in an explicit read transaction.
    pub fn get_document_with_tip(
        &self,
        id: &str,
    ) -> Result<(Option<GraphValueV1>, LogIndex, LogHash)> {
        validate_nonempty_bytes("document id", id, MAX_DOCUMENT_ID_BYTES)?;
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        let value = document(&connection, id)?;
        let tip = self.control.applied_tip()?;
        Ok((value, tip.index(), tip.hash()))
    }

    /// Executes one admitted read-only Cypher statement and returns rows with the
    /// materialized log tip observed under the same database lock.
    pub fn query_read_only(
        &self,
        statement: &str,
        parameters: &BTreeMap<String, GraphParameterValue>,
        max_rows: usize,
        max_bytes: usize,
        timeout_ms: u64,
    ) -> Result<GraphQueryResult> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(Error::InvalidCommand(
                "graph query row and byte limits must be positive".into(),
            ));
        }
        if timeout_ms == 0 {
            return Err(Error::InvalidCommand(
                "graph query timeout must be positive".into(),
            ));
        }
        let admitted = admit_read_only_query(statement, parameters, max_rows, max_bytes)?;
        validate_query_parameter_contract(parameters, &admitted.referenced_parameters)?;
        let parameters = query_parameters(parameters)?;
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        connection.set_query_timeout(timeout_ms);
        read_transaction(&connection, || {
            let mut prepared = connection
                .prepare(&admitted.statement)
                .map_err(ladybug_prepare_error)?;
            if !prepared.is_read_only() {
                return Err(Error::InvalidCommand(
                    "graph query must be read-only".into(),
                ));
            }
            let mut result = connection
                .execute(&mut prepared, parameters)
                .map_err(ladybug_execution_error)?;
            let column_names = result.get_column_names();
            let column_types = result.get_column_data_types();
            if column_names.len() != column_types.len() {
                return Err(Error::Ladybug(
                    "Ladybug query column names and types have different lengths".into(),
                ));
            }
            let mut budget = GraphResultBudget::new(max_bytes);
            budget.ensure_elements(column_names.len())?;
            let columns = column_names
                .into_iter()
                .zip(column_types)
                .map(|(name, logical_type)| {
                    budget.reserve_column(&name, &logical_type)?;
                    Ok(GraphColumn {
                        name,
                        logical_type: graph_logical_type(logical_type)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let tuple_count = usize::try_from(result.get_num_tuples()).map_err(|_| {
                Error::InvalidCommand("graph query row count exceeds this platform".into())
            })?;
            if tuple_count > max_rows {
                return Err(Error::InvalidCommand(format!(
                    "graph query exceeds {max_rows} rows"
                )));
            }
            let mut rows = Vec::with_capacity(tuple_count);
            loop {
                let next =
                    panic::catch_unwind(AssertUnwindSafe(|| result.next())).map_err(|_| {
                        Error::Ladybug("Ladybug result value conversion panicked".into())
                    })?;
                let Some(row) = next else { break };
                budget.reserve_row(&row)?;
                let row = row
                    .into_iter()
                    .map(graph_result_value)
                    .collect::<Result<Vec<_>>>()?;
                rows.push(row);
            }
            let tip = self.control.applied_tip()?;
            Ok(GraphQueryResult {
                columns,
                rows,
                applied_index: tip.index(),
                hash: tip.hash(),
            })
        })
    }

    pub fn check_request(
        &self,
        request_id: &str,
        command_payload: &[u8],
    ) -> Result<Option<RequestRecord>> {
        let command = decode_replicated_graph_command(command_payload)?;
        if command.request_id() != request_id {
            return Err(Error::InvalidCommand(
                "request id does not match the encoded graph command".into(),
            ));
        }
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        let request_digest = LogHash::digest(&[command_payload]);
        self.control
            .lookup_request(request_id, request_digest)?
            .map(|receipt| {
                Ok(RequestRecord {
                    original_log_index: receipt.original_anchor().index(),
                    original_log_hash: receipt.original_anchor().hash(),
                    result: GraphCommandResultV1::decode(receipt.result_blob())?,
                })
            })
            .transpose()
    }

    /// Materializes one semantic RHGC request against a closed clone without
    /// changing the authoritative database or control sidecar.
    pub fn prepare_graph_effect(
        &self,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>> {
        let command = decode_replicated_graph_command(request_payload)?;
        let _writer = self.lock_writer()?;
        self.ensure_no_pending_apply()?;
        let tip = self.control.applied_tip()?;
        if tip != LogAnchor::new(base_index, base_hash) {
            return Err(Error::InvalidEntry(
                "LGFX preparation base does not match the materialized tip".into(),
            ));
        }
        let request_digest = LogHash::digest(&[request_payload]);
        if self
            .control
            .lookup_request(command.request_id(), request_digest)?
            .is_some()
        {
            return Err(Error::InvalidCommand(
                "graph request was already materialized; use its receipt".into(),
            ));
        }
        let first_graph_command = !self.control.has_receipts()?;
        if first_graph_command {
            self.validate_ladybug_logical_genesis()?;
        }
        let identity = self.control.identity()?;
        let base_artifact = NamedTempFile::new_in(parent_dir(&self.path)).map_err(io_error)?;
        let staging_artifact = NamedTempFile::new_in(parent_dir(&self.path)).map_err(io_error)?;
        let base_path = base_artifact.path();
        let staging_path = staging_artifact.path();

        self.close_database_cleanly()?;
        let prepared = (|| {
            fs::copy(&self.path, base_path).map_err(io_error)?;
            File::open(base_path)
                .and_then(|file| file.sync_all())
                .map_err(io_error)?;
            // Base-snapshot/staging copies and closed-file digests remain O(file
            // size). VFS dirty tracking narrows only the LGFX comparison reads.
            fs::copy(base_path, staging_path).map_err(io_error)?;
            #[cfg(feature = "vfs")]
            let (staging, tracker) = open_tracking_database(staging_path)?;
            #[cfg(not(feature = "vfs"))]
            let staging = open_database(staging_path)?;
            let connection = Connection::new(&staging).map_err(ladybug_error)?;
            #[cfg(feature = "vfs")]
            tracker.reset();
            let result = transaction(&connection, || apply_command(&connection, &command))?;
            connection.query("CHECKPOINT").map_err(ladybug_error)?;
            drop(connection);
            drop(staging);
            ensure_clean_database(staging_path)?;
            let actual_base_digest = lgfx::file_digest(base_path)?;
            if actual_base_digest != identity.user_db_digest() {
                return Err(Error::InvalidEntry(
                    "closed Ladybug base digest does not match graph control".into(),
                ));
            }
            let base_file_bytes = fs::metadata(base_path).map_err(io_error)?.len();
            let target_db_digest = lgfx::file_digest(staging_path)?;
            let target_file_bytes = fs::metadata(staging_path).map_err(io_error)?.len();
            let chunks = if first_graph_command {
                lgfx::full_closed_ladybug_file(staging_path)?
            } else {
                #[cfg(feature = "vfs")]
                {
                    let dirty = tracker.dirty_snapshot();
                    if dirty.needs_full_diff(
                        target_db_digest != actual_base_digest
                            || target_file_bytes != base_file_bytes,
                    ) {
                        diff_closed_ladybug_files(base_path, staging_path)?
                    } else {
                        lgfx::diff_closed_ladybug_file_chunks(
                            base_path,
                            staging_path,
                            &dirty.chunk_indices,
                        )?
                    }
                }
                #[cfg(not(feature = "vfs"))]
                {
                    diff_closed_ladybug_files(base_path, staging_path)?
                }
            };
            let effect = LadybugFileEffectV1 {
                cluster_id: identity.cluster_id().to_owned(),
                epoch: identity.epoch(),
                configuration_id: identity.configuration_state().config_id(),
                recovery_generation: identity.recovery_generation(),
                base_log_index: base_index,
                base_log_hash: base_hash,
                base_db_digest: actual_base_digest,
                base_file_bytes,
                target_db_digest,
                target_file_bytes,
                storage_version: lbug::get_storage_version(),
                materializer_fingerprint: graph_materializer_fingerprint(),
                request_id: command.request_id().to_owned(),
                request_digest,
                result_encoding_version: 1,
                bounded_result: result.encode(),
                chunks_digest: lgfx_chunks_digest(&chunks),
                chunks,
            };
            effect.encode()
        })();
        let reopen = self.reopen_database();
        match (prepared, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(effect), Ok(())) => Ok(effect),
        }
    }

    /// Drains crate-owned operations, closes Ladybug, copies the clean database
    /// file plus replicated control state, and reopens it before returning.
    pub fn create_snapshot(&self, target: LogIndex) -> Result<LadybugSnapshot> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| Error::Ladybug("state machine writer lock is poisoned".into()))?;
        self.ensure_no_pending_apply()?;
        let tip = self.control.applied_tip()?;
        if tip.index() != target {
            return Err(Error::InvalidSnapshot(format!(
                "snapshot target {target} does not match applied index {}",
                tip.index()
            )));
        }
        let mut guard = self.write_database()?;
        let checkpoint_wal = ladybug_sidecar(&self.path, ".wal.checkpoint");
        if path_present(&checkpoint_wal)? {
            return Err(Error::InvalidSnapshot(format!(
                "checkpoint found stale sidecar file {}",
                checkpoint_wal.display()
            )));
        }
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        let applied_index = tip.index();
        let applied_hash = tip.hash();
        drop(connection);
        let database = guard.take().ok_or(Error::Closed)?;
        drop(database);
        for sidecar in ladybug_sidecars(&self.path) {
            if path_present(&sidecar)? {
                let reopened = open_database(&self.path)?;
                *guard = Some(reopened);
                return Err(Error::InvalidSnapshot(format!(
                    "checkpoint left sidecar file {}",
                    sidecar.display()
                )));
            }
        }
        let read_result =
            read_bounded_file(&self.path, MAX_RHGS_DB_BYTES, "RHGS v2 Ladybug database");
        *guard = Some(open_database(&self.path)?);
        let db_bytes = read_result?;
        if LogHash::digest(&[&db_bytes]) != self.control.user_db_digest()? {
            return Err(Error::InvalidSnapshot(
                "canonical Ladybug digest does not match graph control".into(),
            ));
        }
        let storage_version = lbug::get_storage_version();
        let control_identity = self.control.identity()?;
        let mut snapshot = LadybugSnapshot {
            cluster_id: self.identity.cluster_id.clone(),
            created_by: self.identity.node_id.clone(),
            epoch: self.identity.epoch,
            config_id: control_identity.configuration_state().config_id(),
            applied_index,
            applied_hash,
            storage_version,
            materializer_fingerprint: graph_materializer_fingerprint(),
            digest: LogHash::ZERO,
            db_bytes,
            replicated_control: self.control.export_replicated_snapshot()?,
        };
        snapshot.digest = snapshot.recompute_digest();
        Ok(snapshot)
    }

    fn apply_lgfx_entry(&self, entry: &LogEntry) -> Result<ApplyOutcome> {
        let identity = self.control.identity()?;
        if entry.cluster_id != self.identity.cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if entry.epoch != self.identity.epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        let tip = self.control.applied_tip()?;
        if entry.index == tip.index() {
            if entry.hash != tip.hash() {
                return Err(Error::InvalidEntry(
                    "current index was reapplied with a different hash".into(),
                ));
            }
            let result = if entry.entry_type == EntryType::Command {
                let effect = decode_lgfx_command(&entry.payload)?;
                self.control
                    .lookup_request(&effect.request_id, effect.request_digest)?
                    .map(|receipt| GraphCommandResultV1::decode(receipt.result_blob()))
                    .transpose()?
            } else {
                None
            };
            return Ok(ApplyOutcome {
                applied_index: tip.index(),
                applied_hash: tip.hash(),
                result,
            });
        }
        let expected = tip
            .index()
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index != expected {
            return Err(Error::InvalidEntry(format!(
                "expected index {expected}, got {}",
                entry.index
            )));
        }
        if entry.prev_hash != tip.hash() {
            return Err(Error::InvalidEntry(
                "prev_hash does not match the materialized graph tip".into(),
            ));
        }

        let current_configuration = self.control.configuration_state()?;
        let next_configuration = current_configuration
            .validate_entry(entry)
            .map_err(|error| Error::InvalidEntry(error.to_string()))?;
        let base = LogAnchor::new(tip.index(), tip.hash());
        let target = LogAnchor::new(entry.index, entry.hash);
        let result = if entry.entry_type == EntryType::Command {
            let effect = decode_lgfx_command(&entry.payload)?;
            validate_lgfx_identity(&effect, &identity, &current_configuration)?;
            if effect.base_log_index != tip.index() || effect.base_log_hash != tip.hash() {
                return Err(Error::InvalidEntry(
                    "LGFX effect base does not match the graph tip".into(),
                ));
            }
            let result = GraphCommandResultV1::decode(&effect.bounded_result)?;
            if result.encode() != effect.bounded_result {
                return Err(Error::InvalidEntry(
                    "LGFX result is not canonically encoded".into(),
                ));
            }
            if self
                .control
                .lookup_request(&effect.request_id, effect.request_digest)?
                .is_some()
            {
                return Err(Error::InvalidEntry(
                    "LGFX request receipt already belongs to an earlier entry".into(),
                ));
            }
            let divergent_base = effect.base_db_digest != identity.user_db_digest();
            let first_graph_command = !self.control.has_receipts()?;
            let pending = PendingApply::new(
                base,
                target,
                identity.user_db_digest(),
                effect.target_db_digest,
                effect.target_file_bytes,
            );
            self.validate_lgfx_apply_mode(&effect, &pending, first_graph_command, divergent_base)?;
            self.control.begin_pending(&pending)?;
            self.install_lgfx_effect(&effect, divergent_base)?;
            let receipt = RequestReceipt::new(
                &effect.request_id,
                effect.request_digest,
                target,
                effect.bounded_result.clone(),
            );
            self.control
                .commit_applied(&pending, &next_configuration, Some(&receipt))?;
            Some(result)
        } else {
            match entry.entry_type {
                EntryType::Noop if !entry.payload.is_empty() => {
                    return Err(Error::InvalidEntry("Noop payload must be empty".into()))
                }
                EntryType::Noop
                | EntryType::ConfigChange
                | EntryType::SnapshotBarrier
                | EntryType::SnapshotPublished => {}
                EntryType::Command => unreachable!(),
            }
            let digest = self.control.user_db_digest()?;
            let bytes = fs::metadata(&self.path).map_err(io_error)?.len();
            let pending = PendingApply::new(base, target, digest, digest, bytes);
            self.control.begin_pending(&pending)?;
            self.control
                .commit_applied(&pending, &next_configuration, None)?;
            None
        };
        Ok(ApplyOutcome {
            applied_index: entry.index,
            applied_hash: entry.hash,
            result,
        })
    }

    fn validate_lgfx_apply_mode(
        &self,
        effect: &LadybugFileEffectV1,
        pending: &PendingApply,
        first_graph_command: bool,
        divergent_base: bool,
    ) -> Result<()> {
        let existing_pending = self.control.pending()?;
        if existing_pending
            .as_ref()
            .is_some_and(|existing| existing != pending)
        {
            return Err(Error::InvalidEntry(
                "a different LGFX apply is already pending".into(),
            ));
        }
        if first_graph_command && !effect.fully_covers_target() {
            return Err(Error::InvalidEntry(
                "first LGFX command requires a full target image".into(),
            ));
        }
        if !first_graph_command && divergent_base {
            return Err(Error::InvalidEntry(
                "LGFX after bootstrap requires the exact committed base".into(),
            ));
        }

        self.close_database_cleanly()?;
        let inspected = (|| {
            Ok((
                lgfx::file_digest(&self.path)?,
                fs::metadata(&self.path).map_err(io_error)?.len(),
            ))
        })();
        let reopen = self.reopen_database();
        let (digest, bytes) = match (inspected, reopen) {
            (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
            (Ok(identity), Ok(())) => identity,
        };

        if digest == effect.target_db_digest && bytes == effect.target_file_bytes {
            if existing_pending.as_ref() == Some(pending) {
                return Ok(());
            }
            return Err(Error::InvalidEntry(
                "LGFX target bytes exist without the matching pending intent".into(),
            ));
        }
        if !first_graph_command {
            if digest == effect.base_db_digest && bytes == effect.base_file_bytes {
                return Ok(());
            }
            return Err(Error::InvalidEntry(
                "canonical Ladybug file does not match the exact LGFX base".into(),
            ));
        }
        if digest != pending.base_db_digest() {
            return Err(Error::InvalidEntry(
                "first LGFX command does not match fresh local control".into(),
            ));
        }
        self.validate_ladybug_logical_genesis()
    }

    fn validate_ladybug_logical_genesis(&self) -> Result<()> {
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        let tables = connection
            .query("CALL show_tables() RETURN name, type")
            .map_err(ladybug_error)?
            .collect::<Vec<_>>();
        if tables
            != vec![vec![
                Value::String("RhizaDocument".into()),
                Value::String("NODE".into()),
            ]]
        {
            return Err(Error::InvalidEntry(
                "divergent LGFX bootstrap requires exactly the RhizaDocument table".into(),
            ));
        }
        let columns = connection
            .query(
                "CALL table_info('RhizaDocument') RETURN name, type, `default expression`, `primary key`",
            )
            .map_err(ladybug_error)?
            .collect::<Vec<_>>();
        let expected = [
            ("id", "STRING", true),
            ("kind", "UINT8", false),
            ("bool_value", "BOOL", false),
            ("i64_value", "INT64", false),
            ("u64_value", "UINT64", false),
            ("f64_value", "DOUBLE", false),
            ("string_value", "STRING", false),
            ("bytes_value", "BLOB", false),
        ]
        .into_iter()
        .map(|(name, kind, primary)| {
            vec![
                Value::String(name.into()),
                Value::String(kind.into()),
                Value::String("NULL".into()),
                Value::Bool(primary),
            ]
        })
        .collect::<Vec<_>>();
        if columns != expected {
            return Err(Error::InvalidEntry(
                "divergent LGFX bootstrap requires the exact RhizaDocument schema".into(),
            ));
        }
        let rows = connection
            .query("MATCH (d:RhizaDocument) RETURN count(d)")
            .map_err(ladybug_error)?
            .collect::<Vec<_>>();
        if rows != vec![vec![Value::Int64(0)]] {
            return Err(Error::InvalidEntry(
                "divergent LGFX bootstrap requires zero RhizaDocument rows".into(),
            ));
        }
        Ok(())
    }

    fn install_lgfx_effect(
        &self,
        effect: &LadybugFileEffectV1,
        divergent_base: bool,
    ) -> Result<()> {
        self.close_database_cleanly()?;
        let install = (|| {
            let digest = lgfx::file_digest(&self.path)?;
            if digest == effect.target_db_digest {
                let actual_bytes = fs::metadata(&self.path).map_err(io_error)?.len();
                if actual_bytes != effect.target_file_bytes {
                    return Err(Error::InvalidEntry(
                        "canonical Ladybug target size does not match LGFX target_file_bytes"
                            .into(),
                    ));
                }
                return Ok(());
            }
            if !divergent_base && digest != effect.base_db_digest {
                return Err(Error::InvalidEntry(
                    "canonical Ladybug digest matches neither LGFX base nor target".into(),
                ));
            }
            let temp_dir = tempfile::tempdir_in(parent_dir(&self.path)).map_err(io_error)?;
            let target = temp_dir.path().join("target.lbug");
            if divergent_base {
                lgfx::apply_lgfx_full_image(&target, effect)?;
            } else {
                apply_lgfx_to_exact_base(&self.path, &target, effect)?;
            }
            let verify = open_database(&target)?;
            let connection = Connection::new(&verify).map_err(ladybug_error)?;
            connection.query("RETURN 1").map_err(ladybug_error)?;
            drop(connection);
            drop(verify);
            ensure_clean_database(&target)?;
            fs::rename(&target, &self.path).map_err(io_error)?;
            sync_parent(parent_dir(&self.path))
        })();
        let reopen = self.reopen_database();
        match (install, reopen) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn close_database_cleanly(&self) -> Result<()> {
        let mut guard = self.write_database()?;
        guard.as_ref().ok_or(Error::Closed)?;
        drop(guard.take().ok_or(Error::Closed)?);
        ensure_clean_database(&self.path)
    }

    fn reopen_database(&self) -> Result<()> {
        let reopened = open_database(&self.path)?;
        let mut guard = self.write_database()?;
        if guard.is_some() {
            return Err(Error::Ladybug(
                "refusing to replace an open Ladybug database".into(),
            ));
        }
        *guard = Some(reopened);
        Ok(())
    }

    fn ensure_no_pending_apply(&self) -> Result<()> {
        if self.control.pending()?.is_some() {
            return Err(Error::InvalidEntry(
                "canonical graph state is unavailable while an LGFX apply is pending".into(),
            ));
        }
        Ok(())
    }

    fn lock_writer(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.writer
            .lock()
            .map_err(|_| Error::Ladybug("state machine writer lock is poisoned".into()))
    }

    fn read_database(&self) -> Result<RwLockReadGuard<'_, Option<Database>>> {
        self.database
            .read()
            .map_err(|_| Error::Ladybug("state machine lock is poisoned".into()))
    }

    fn write_database(&self) -> Result<RwLockWriteGuard<'_, Option<Database>>> {
        self.database
            .write()
            .map_err(|_| Error::Ladybug("state machine lock is poisoned".into()))
    }
}

fn decode_replicated_graph_command(payload: &[u8]) -> Result<GraphCommandV1> {
    let envelope = ReplicatedCommandEnvelope::decode(payload)
        .map_err(|error| Error::InvalidCommand(error.to_string()))?;
    if envelope.profile() != ExecutionProfile::Graph {
        return Err(Error::InvalidCommand(format!(
            "expected graph execution profile, got {}",
            envelope.profile()
        )));
    }
    if envelope.command_version() != 1 {
        return Err(Error::InvalidCommand(format!(
            "unsupported graph command version {}",
            envelope.command_version()
        )));
    }
    let command = GraphCommandV1::decode(envelope.body())?;
    if envelope.request_id() != command.request_id() {
        return Err(Error::InvalidCommand(
            "replicated envelope request id does not match RHGC request id".into(),
        ));
    }
    Ok(command)
}

struct DecodedGraphCommand {
    command: GraphCommandV1,
}

fn decode_replicated_graph_commands(payload: &[u8]) -> Result<Vec<DecodedGraphCommand>> {
    let envelope = ReplicatedCommandEnvelope::decode(payload)
        .map_err(|error| Error::InvalidCommand(error.to_string()))?;
    if envelope.profile() != ExecutionProfile::Graph {
        return Err(Error::InvalidCommand(format!(
            "expected graph execution profile, got {}",
            envelope.profile()
        )));
    }
    match envelope.command_version() {
        1 => {
            let command = decode_replicated_graph_command(payload)?;
            Ok(vec![DecodedGraphCommand { command }])
        }
        BATCH_COMMAND_VERSION => {
            if envelope.request_id() != BATCH_REQUEST_ID {
                return Err(Error::InvalidCommand(
                    "graph batch envelope request id is invalid".into(),
                ));
            }
            let mut decoder = Decoder::new(envelope.body());
            if decoder.take(BATCH_COMMAND_MAGIC.len())? != BATCH_COMMAND_MAGIC {
                return Err(Error::Codec(
                    "wrong graph batch command magic or version".into(),
                ));
            }
            let count = usize::from(u16::from_be_bytes(decoder.array()?));
            if count == 0 || count > MAX_GRAPH_BATCH_MEMBERS {
                return Err(Error::InvalidCommand(format!(
                    "graph batch must contain 1..={MAX_GRAPH_BATCH_MEMBERS} commands"
                )));
            }
            let mut request_ids = BTreeSet::new();
            let mut commands = Vec::with_capacity(count);
            for _ in 0..count {
                let encoded = decoder.bytes(usize::MAX)?;
                let command = GraphCommandV1::decode(encoded)?;
                if !request_ids.insert(command.request_id().to_owned()) {
                    return Err(Error::InvalidCommand(format!(
                        "graph batch repeats request id {:?}",
                        command.request_id()
                    )));
                }
                commands.push(DecodedGraphCommand { command });
            }
            if !decoder.is_empty() {
                return Err(Error::Codec("trailing graph batch command bytes".into()));
            }
            let canonical = commands
                .iter()
                .map(|member| member.command.clone())
                .collect::<Vec<_>>();
            if encode_replicated_graph_batch(&canonical)? != payload {
                return Err(Error::Codec("noncanonical graph batch command".into()));
            }
            Ok(commands)
        }
        version => Err(Error::InvalidCommand(format!(
            "unsupported graph command version {version}"
        ))),
    }
}

pub fn restore_snapshot_file(
    path: impl AsRef<Path>,
    snapshot: &LadybugSnapshot,
    target_node_id: &str,
) -> Result<()> {
    if target_node_id.is_empty() || target_node_id.len() > MAX_RHGS_ID_BYTES {
        return Err(Error::InvalidSnapshot(
            "target node id must contain 1..=256 bytes".into(),
        ));
    }
    validate_snapshot_envelope(snapshot)?;
    control::validate_replicated_snapshot_source(
        &snapshot.replicated_control,
        &snapshot.created_by,
    )?;
    let path = path.as_ref();
    ensure_parent(path)?;
    let intent_path = restore_intent_path(path);
    let expected_preparing = RestoreIntent {
        phase: RestorePhase::Preparing,
        db_digest: LogHash::digest(&[&snapshot.db_bytes]),
        control_digest: LogHash::ZERO,
        snapshot_digest: snapshot.digest,
        target_node_digest: LogHash::digest(&[target_node_id.as_bytes()]),
    };
    let control_path = control_sidecar_path(path);
    let staging_db = restore_staging_db_path(path);
    let staging_control = restore_staging_control_path(path);
    let parent = parent_dir(path);
    if path_present(&intent_path)? {
        let intent = read_restore_intent(&intent_path)?;
        if intent.snapshot_digest != snapshot.digest
            || intent.target_node_digest != LogHash::digest(&[target_node_id.as_bytes()])
            || intent.db_digest != expected_preparing.db_digest
        {
            return Err(Error::InvalidSnapshot(
                "an interrupted restore belongs to a different snapshot or target node".into(),
            ));
        }
        match intent.phase {
            RestorePhase::Staged => {
                recover_interrupted_snapshot_publish(path)?;
                return Ok(());
            }
            RestorePhase::Preparing => {
                cleanup_owned_restore_staging(&staging_db, &staging_control, parent)?;
            }
        }
    } else {
        if path_present(path)?
            || path_present(&control_path)?
            || path_present(&staging_db)?
            || path_present(&staging_control)?
        {
            return Err(Error::InvalidSnapshot(
                "RHGS v2 restore target, staging, and graph control must not exist".into(),
            ));
        }
        for sidecar in ladybug_sidecars(path) {
            if path_present(&sidecar)? {
                return Err(Error::InvalidSnapshot(
                    "RHGS v2 restore Ladybug sidecars must not exist".into(),
                ));
            }
        }
        write_restore_intent(&intent_path, &expected_preparing)?;
    }
    let mut temporary = NamedTempFile::new_in(parent).map_err(io_error)?;
    temporary.write_all(&snapshot.db_bytes).map_err(io_error)?;
    temporary.as_file().sync_all().map_err(io_error)?;
    let temporary_path = temporary.path().to_path_buf();
    let database = match open_database(&temporary_path) {
        Ok(database) => database,
        Err(error) => {
            remove_sidecars(&temporary_path);
            return Err(invalid_snapshot_error(error));
        }
    };
    let validation = (|| {
        let connection = Connection::new(&database).map_err(invalid_snapshot_ladybug_error)?;
        connection
            .query("RETURN 1")
            .map_err(invalid_snapshot_ladybug_error)?;
        Ok(())
    })();
    drop(database);
    if validation.is_err() {
        remove_sidecars(&temporary_path);
    }
    validation?;
    for sidecar in ladybug_sidecars(&temporary_path) {
        if path_present(&sidecar)? {
            remove_sidecars(&temporary_path);
            return Err(Error::InvalidSnapshot(
                "snapshot validation left a Ladybug sidecar".into(),
            ));
        }
    }
    temporary.as_file().sync_all().map_err(io_error)?;
    temporary.persist_noclobber(&staging_db).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidSnapshot("restore database staging already exists".into())
        } else {
            io_error(error.error)
        }
    })?;
    let staged = (|| {
        let control = ControlStore::create(
            &staging_control,
            &ControlIdentity::new(
                &snapshot.cluster_id,
                target_node_id,
                snapshot.epoch,
                ConfigurationState::active(snapshot.config_id, LogHash::ZERO),
                1,
                snapshot.materializer_fingerprint,
                LogHash::digest(&[&snapshot.db_bytes]),
            ),
        )
        .map_err(invalid_snapshot_error)?;
        control
            .import_replicated_snapshot(&snapshot.replicated_control, &snapshot.created_by)
            .map_err(invalid_snapshot_error)?;
        drop(control);
        require_regular_file(&staging_control, "staged graph control")?;
        File::open(&staging_control)
            .and_then(|file| file.sync_all())
            .map_err(io_error)?;
        Ok(())
    })();
    staged?;
    if LogHash::digest(&[&snapshot.db_bytes])
        != ControlStore::open_existing(&staging_control)?.user_db_digest()?
    {
        return Err(Error::InvalidSnapshot(
            "RHGS v2 database digest does not match replicated graph control".into(),
        ));
    }
    let control = ControlStore::open_existing(&staging_control)?;
    let control_identity = control.identity()?;
    let tip = control.applied_tip()?;
    if control_identity.cluster_id() != snapshot.cluster_id
        || control_identity.epoch() != snapshot.epoch
        || control_identity.node_id() != target_node_id
        || control_identity.configuration_state().config_id() != snapshot.config_id
        || tip != LogAnchor::new(snapshot.applied_index, snapshot.applied_hash)
        || control_identity.materializer_fingerprint() != snapshot.materializer_fingerprint
    {
        drop(control);
        return Err(Error::InvalidSnapshot(
            "RHGS v2 envelope does not match replicated graph control".into(),
        ));
    }
    drop(control);
    let intent = RestoreIntent {
        phase: RestorePhase::Staged,
        db_digest: lgfx::file_digest(&staging_db)?,
        control_digest: lgfx::file_digest(&staging_control)?,
        snapshot_digest: snapshot.digest,
        target_node_digest: LogHash::digest(&[target_node_id.as_bytes()]),
    };
    require_regular_file(&intent_path, "graph restore intent")?;
    replace_restore_intent(&intent_path, &intent)?;
    recover_interrupted_snapshot_publish(path)
}

fn cleanup_owned_restore_staging(database: &Path, control: &Path, parent: &Path) -> Result<()> {
    for path in [
        database.to_path_buf(),
        control.to_path_buf(),
        append_path_suffix(control, "-journal"),
        append_path_suffix(control, "-wal"),
        append_path_suffix(control, "-shm"),
    ] {
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                return Err(Error::InvalidSnapshot(format!(
                    "owned restore staging path is unexpectedly a directory: {}",
                    path.display()
                )));
            }
            Ok(_) => fs::remove_file(&path).map_err(io_error)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    sync_parent(parent)
}

fn open_database(path: &Path) -> Result<Database> {
    let database = Database::new(path, ladybug_system_config().auto_checkpoint(false))
        .map_err(ladybug_error)?;
    let connection = Connection::new(&database).map_err(ladybug_error)?;
    connection
        .query("CALL force_checkpoint_on_close=false")
        .map_err(ladybug_error)?;
    drop(connection);
    Ok(database)
}

#[cfg(feature = "vfs")]
fn open_tracking_database(path: &Path) -> Result<(Database, DirtyTracker)> {
    let (file_system, tracker) = TrackingFileSystem::new(path);
    let database = Database::new_with_file_system(
        path,
        ladybug_system_config().auto_checkpoint(false),
        Box::new(file_system),
    )
    .map_err(ladybug_error)?;
    let connection = Connection::new(&database).map_err(ladybug_error)?;
    connection
        .query("CALL force_checkpoint_on_close=false")
        .map_err(ladybug_error)?;
    drop(connection);
    Ok((database, tracker))
}

fn control_sidecar_path(path: &Path) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(".control");
    PathBuf::from(sidecar)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestoreIntent {
    phase: RestorePhase,
    db_digest: LogHash,
    control_digest: LogHash,
    snapshot_digest: LogHash,
    target_node_digest: LogHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestorePhase {
    Preparing,
    Staged,
}

fn restore_staging_db_path(path: &Path) -> PathBuf {
    append_path_suffix(path, ".restore.db")
}

fn restore_staging_control_path(path: &Path) -> PathBuf {
    append_path_suffix(path, ".restore.control")
}

fn restore_intent_path(path: &Path) -> PathBuf {
    append_path_suffix(path, ".restore.intent")
}

fn append_path_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn write_restore_intent(path: &Path, intent: &RestoreIntent) -> Result<()> {
    persist_restore_intent(path, intent, true)
}

fn replace_restore_intent(path: &Path, intent: &RestoreIntent) -> Result<()> {
    persist_restore_intent(path, intent, false)
}

fn persist_restore_intent(path: &Path, intent: &RestoreIntent, create_new: bool) -> Result<()> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(RESTORE_INTENT_BYTES)
        .map_err(|_| Error::ResourceExhausted("restore intent allocation failed".into()))?;
    encoded.extend_from_slice(RESTORE_INTENT_MAGIC);
    encoded.push(match intent.phase {
        RestorePhase::Preparing => 0,
        RestorePhase::Staged => 1,
    });
    encoded.extend_from_slice(intent.db_digest.as_bytes());
    encoded.extend_from_slice(intent.control_digest.as_bytes());
    encoded.extend_from_slice(intent.snapshot_digest.as_bytes());
    encoded.extend_from_slice(intent.target_node_digest.as_bytes());
    let digest = LogHash::digest(&[&encoded]);
    encoded.extend_from_slice(digest.as_bytes());
    debug_assert_eq!(encoded.len(), RESTORE_INTENT_BYTES);
    let parent = parent_dir(path);
    let mut temporary = NamedTempFile::new_in(parent).map_err(io_error)?;
    temporary.write_all(&encoded).map_err(io_error)?;
    temporary.as_file().sync_all().map_err(io_error)?;
    if create_new {
        temporary.persist_noclobber(path).map_err(|error| {
            if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::InvalidSnapshot("restore intent already exists".into())
            } else {
                io_error(error.error)
            }
        })?;
    } else {
        temporary
            .persist(path)
            .map_err(|error| io_error(error.error))?;
    }
    sync_parent(parent)
}

fn read_restore_intent(path: &Path) -> Result<RestoreIntent> {
    require_regular_file(path, "graph restore intent")?;
    let encoded = read_bounded_file(path, RESTORE_INTENT_BYTES, "graph restore intent")?;
    if encoded.len() != RESTORE_INTENT_BYTES || !encoded.starts_with(RESTORE_INTENT_MAGIC) {
        return Err(Error::InvalidSnapshot(
            "invalid graph restore intent magic or length".into(),
        ));
    }
    let payload_end = encoded.len() - 32;
    if LogHash::digest(&[&encoded[..payload_end]])
        != LogHash::from_bytes(encoded[payload_end..].try_into().expect("length checked"))
    {
        return Err(Error::InvalidSnapshot(
            "graph restore intent digest mismatch".into(),
        ));
    }
    let phase = match encoded[RESTORE_INTENT_MAGIC.len()] {
        0 => RestorePhase::Preparing,
        1 => RestorePhase::Staged,
        value => {
            return Err(Error::InvalidSnapshot(format!(
                "invalid graph restore intent phase {value}"
            )))
        }
    };
    let mut offset = RESTORE_INTENT_MAGIC.len() + 1;
    let mut hash = || {
        let value = LogHash::from_bytes(
            encoded[offset..offset + 32]
                .try_into()
                .expect("fixed intent length"),
        );
        offset += 32;
        value
    };
    let intent = RestoreIntent {
        phase,
        db_digest: hash(),
        control_digest: hash(),
        snapshot_digest: hash(),
        target_node_digest: hash(),
    };
    if intent.phase == RestorePhase::Preparing && intent.control_digest != LogHash::ZERO {
        return Err(Error::InvalidSnapshot(
            "preparing restore intent must not claim a control digest".into(),
        ));
    }
    Ok(intent)
}

fn recover_interrupted_snapshot_publish(path: &Path) -> Result<()> {
    let intent_path = restore_intent_path(path);
    if !path_present(&intent_path)? {
        if path_present(&restore_staging_db_path(path))?
            || path_present(&restore_staging_control_path(path))?
        {
            return Err(Error::InvalidSnapshot(
                "orphan graph restore staging exists without a durable ownership intent".into(),
            ));
        }
        return Ok(());
    }
    let intent = read_restore_intent(&intent_path)?;
    if intent.phase == RestorePhase::Preparing {
        return Err(Error::InvalidSnapshot(
            "graph restore preparation is incomplete; retry restore_snapshot_file with the same snapshot and target node"
                .into(),
        ));
    }
    let control_path = control_sidecar_path(path);
    let staging_db = restore_staging_db_path(path);
    let staging_control = restore_staging_control_path(path);
    validate_restore_copy(path, &staging_db, intent.db_digest, "Ladybug database")?;
    validate_restore_copy(
        &control_path,
        &staging_control,
        intent.control_digest,
        "graph control",
    )?;
    let parent = parent_dir(path);
    publish_restore_file(
        &staging_control,
        &control_path,
        intent.control_digest,
        "graph control",
    )?;
    sync_parent(parent)?;
    publish_restore_file(&staging_db, path, intent.db_digest, "Ladybug database")?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)?;
    File::open(&control_path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)?;
    sync_parent(parent)?;
    require_file_digest(path, intent.db_digest, "published Ladybug database")?;
    require_file_digest(
        &control_path,
        intent.control_digest,
        "published graph control",
    )?;
    if path_present(&staging_db)? {
        require_regular_file(&staging_db, "staged Ladybug database")?;
        fs::remove_file(&staging_db).map_err(io_error)?;
    }
    if path_present(&staging_control)? {
        require_regular_file(&staging_control, "staged graph control")?;
        fs::remove_file(&staging_control).map_err(io_error)?;
    }
    sync_parent(parent)?;
    fs::remove_file(&intent_path).map_err(io_error)?;
    sync_parent(parent)
}

fn validate_restore_copy(
    canonical: &Path,
    staging: &Path,
    expected: LogHash,
    label: &str,
) -> Result<()> {
    if path_present(canonical)? {
        require_file_digest(canonical, expected, &format!("published {label}"))
    } else if path_present(staging)? {
        require_file_digest(staging, expected, &format!("staged {label}"))
    } else {
        Err(Error::InvalidSnapshot(format!(
            "restore intent is missing both published and staged {label}"
        )))
    }
}

fn publish_restore_file(
    staging: &Path,
    canonical: &Path,
    expected: LogHash,
    label: &str,
) -> Result<()> {
    if path_present(canonical)? {
        return require_file_digest(canonical, expected, &format!("published {label}"));
    }
    require_file_digest(staging, expected, &format!("staged {label}"))?;
    fs::hard_link(staging, canonical).map_err(io_error)?;
    require_file_digest(canonical, expected, &format!("published {label}"))
}

fn require_file_digest(path: &Path, expected: LogHash, label: &str) -> Result<()> {
    require_regular_file(path, label)?;
    if lgfx::file_digest(path)? != expected {
        return Err(Error::InvalidSnapshot(format!(
            "{label} digest does not match restore intent"
        )));
    }
    Ok(())
}

fn path_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::IdentityMismatch(format!(
            "{label} is not a regular file"
        )));
    }
    Ok(())
}

fn read_bounded_file(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    require_regular_file(path, label)?;
    let length = usize::try_from(fs::symlink_metadata(path).map_err(io_error)?.len())
        .map_err(|_| Error::ResourceExhausted(format!("{label} length exceeds platform")))?;
    if length > maximum {
        return Err(Error::ResourceExhausted(format!(
            "{label} exceeds {maximum} bytes"
        )));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::ResourceExhausted(format!("{label} allocation failed")))?;
    bytes.resize(length, 0);
    let mut file = File::open(path).map_err(io_error)?;
    file.read_exact(&mut bytes).map_err(io_error)?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing).map_err(io_error)? != 0 {
        return Err(Error::InvalidSnapshot(format!(
            "{label} grew while it was being read"
        )));
    }
    Ok(bytes)
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sync_parent(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)
}

fn ensure_clean_database(path: &Path) -> Result<()> {
    for sidecar in ladybug_sidecars(path) {
        if path_present(&sidecar)? {
            return Err(Error::InvalidEntry(format!(
                "Ladybug database is not closed cleanly: {}",
                sidecar.display()
            )));
        }
    }
    Ok(())
}

fn reject_legacy_database(path: &Path) -> Result<()> {
    ensure_clean_database(path)?;
    let database = open_database(path)?;
    let connection = Connection::new(&database).map_err(ladybug_error)?;
    for table in ["__RhizaMeta", "__RhizaRequest"] {
        if connection
            .query(&format!("MATCH (n:{table}) RETURN n LIMIT 1"))
            .is_ok()
        {
            return Err(Error::IdentityMismatch(format!(
                "legacy table {table} requires RHGS v2 snapshot bootstrap"
            )));
        }
    }
    Ok(())
}

fn validate_control_database_pair(path: &Path, control: &ControlStore) -> Result<()> {
    let digest = lgfx::file_digest(path)?;
    if let Some(pending) = control.pending()? {
        let tip = control.applied_tip()?;
        if pending.base() != tip
            || pending.base_db_digest() != control.user_db_digest()?
            || pending.entry().index()
                != tip
                    .index()
                    .checked_add(1)
                    .ok_or_else(|| Error::InvalidEntry("pending LGFX index is exhausted".into()))?
        {
            return Err(Error::InvalidEntry(
                "pending LGFX intent does not extend committed graph control".into(),
            ));
        }
        if digest == pending.base_db_digest() {
            return Ok(());
        }
        if digest == pending.target_db_digest()
            && fs::metadata(path).map_err(io_error)?.len() == pending.target_file_bytes()
        {
            return Ok(());
        }
        return Err(Error::InvalidEntry(
            "pending LGFX database digest matches neither base nor target".into(),
        ));
    }
    if digest != control.user_db_digest()? {
        return Err(Error::InvalidEntry(
            "canonical Ladybug digest does not match graph control".into(),
        ));
    }
    Ok(())
}

fn decode_lgfx_command(payload: &[u8]) -> Result<LadybugFileEffectV1> {
    if !payload.starts_with(LGFX_V1_MAGIC) {
        return Err(Error::InvalidCommand(
            "LGFX-only graph apply requires an LGFX v1 command payload".into(),
        ));
    }
    LadybugFileEffectV1::decode(payload)
}

fn validate_lgfx_identity(
    effect: &LadybugFileEffectV1,
    identity: &ControlIdentity,
    configuration: &ConfigurationState,
) -> Result<()> {
    if effect.cluster_id != identity.cluster_id()
        || effect.epoch != identity.epoch()
        || effect.configuration_id != configuration.config_id()
        || effect.recovery_generation != identity.recovery_generation()
        || effect.storage_version != lbug::get_storage_version()
        || effect.materializer_fingerprint != identity.materializer_fingerprint()
    {
        return Err(Error::InvalidEntry(
            "LGFX identity, storage version, or materializer fingerprint mismatch".into(),
        ));
    }
    Ok(())
}

fn ladybug_system_config() -> SystemConfig {
    ladybug_system_config_with_limits(LADYBUG_BUFFER_POOL_BYTES, LADYBUG_MAX_NUM_THREADS)
}

fn ladybug_system_config_with_limits(buffer_pool_size: u64, max_num_threads: u64) -> SystemConfig {
    SystemConfig::default()
        .buffer_pool_size(buffer_pool_size)
        .max_num_threads(max_num_threads)
        .enable_multi_writes(false)
        .throw_on_wal_replay_failure(true)
        .enable_checksums(true)
}

fn transaction<T>(connection: &Connection<'_>, operation: impl FnOnce() -> Result<T>) -> Result<T> {
    connection
        .query("BEGIN TRANSACTION")
        .map_err(ladybug_error)?;
    match operation() {
        Ok(value) => match connection.query("COMMIT") {
            Ok(_) => Ok(value),
            Err(error) => {
                let _ = connection.query("ROLLBACK");
                Err(ladybug_error(error))
            }
        },
        Err(error) => {
            let _ = connection.query("ROLLBACK");
            Err(error)
        }
    }
}

fn read_transaction<T>(
    connection: &Connection<'_>,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    connection
        .query("BEGIN TRANSACTION READ ONLY")
        .map_err(ladybug_error)?;
    match operation() {
        Ok(value) => match connection.query("COMMIT") {
            Ok(_) => Ok(value),
            Err(error) => {
                let _ = connection.query("ROLLBACK");
                Err(ladybug_error(error))
            }
        },
        Err(error) => {
            let _ = connection.query("ROLLBACK");
            Err(error)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueryToken {
    kind: QueryTokenKind,
    start: usize,
    end: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueryTokenKind {
    Identifier { value: String, escaped: bool },
    Parameter(String),
    Integer(String),
    StringLiteral,
    Symbol(char),
    Semicolon,
}

struct AdmittedQuery {
    statement: String,
    referenced_parameters: BTreeSet<String>,
}

fn admit_read_only_query(
    statement: &str,
    parameters: &BTreeMap<String, GraphParameterValue>,
    max_rows: usize,
    max_bytes: usize,
) -> Result<AdmittedQuery> {
    if statement.trim().is_empty() || statement.len() > MAX_GRAPH_QUERY_BYTES {
        return Err(Error::InvalidCommand(format!(
            "graph query must contain 1..={MAX_GRAPH_QUERY_BYTES} bytes"
        )));
    }
    if statement.contains('\0') {
        return Err(Error::InvalidCommand(
            "graph query must not contain NUL".into(),
        ));
    }
    let mut tokens = lex_query(statement)?;
    let body_end = if tokens
        .last()
        .is_some_and(|token| token.kind == QueryTokenKind::Semicolon)
    {
        tokens.pop().expect("last token checked").start
    } else {
        statement.len()
    };
    if tokens.is_empty()
        || tokens
            .iter()
            .any(|token| token.kind == QueryTokenKind::Semicolon)
    {
        return Err(Error::InvalidCommand(
            "graph query must contain exactly one statement".into(),
        ));
    }
    for token in &tokens {
        if let QueryTokenKind::Parameter(name) = &token.kind {
            validate_parameter_name(name)?;
        }
        match &token.kind {
            QueryTokenKind::Identifier { value, .. } | QueryTokenKind::Parameter(value)
                if value.to_ascii_lowercase().starts_with("__rhiza") =>
            {
                return Err(Error::InvalidCommand(
                    "graph query cannot access the reserved __Rhiza namespace".into(),
                ));
            }
            _ => {}
        }
    }
    reject_admin_external_or_transaction_queries(&tokens)?;
    reject_unlabeled_node_patterns(&tokens)?;
    let execution_row_bound = static_execution_row_bound(&tokens, parameters, max_rows)?;
    reject_unbounded_result_containers(
        statement,
        &tokens,
        parameters,
        max_bytes,
        execution_row_bound,
    )?;
    let body = statement[..body_end].trim_end();
    let referenced_parameters = tokens
        .iter()
        .filter_map(|token| match &token.kind {
            QueryTokenKind::Parameter(name) => Some(name.clone()),
            _ => None,
        })
        .collect();
    Ok(AdmittedQuery {
        statement: bounded_read_statement(body, &tokens, parameters, max_rows)?,
        referenced_parameters,
    })
}

fn reject_unbounded_result_containers(
    statement: &str,
    tokens: &[QueryToken],
    parameters: &BTreeMap<String, GraphParameterValue>,
    max_bytes: usize,
    execution_row_bound: usize,
) -> Result<()> {
    let mut remaining_expansion_bytes = max_bytes;
    for (index, token) in tokens.iter().enumerate() {
        if matches!(result_clause_at(tokens, index), Some("RETURN" | "WITH")) {
            let literal_bytes = match token.kind {
                QueryTokenKind::StringLiteral => token.end.saturating_sub(token.start),
                _ => 0,
            };
            let projected_bytes = literal_bytes
                .checked_add(8)
                .and_then(|bytes| bytes.checked_mul(execution_row_bound.max(1)))
                .ok_or_else(|| {
                    Error::InvalidCommand("graph projected expression size overflow".into())
                })?;
            reserve_static_expansion(&mut remaining_expansion_bytes, projected_bytes, max_bytes)?;
        }
        if let QueryTokenKind::Parameter(name) = &token.kind {
            let value = parameters.get(name).ok_or_else(|| {
                Error::InvalidCommand(format!("graph query parameter is missing: {name}"))
            })?;
            let multiplier = result_expansion_multiplier(tokens, index, execution_row_bound);
            reserve_static_expansion(
                &mut remaining_expansion_bytes,
                graph_parameter_expansion_bytes(value)?
                    .checked_mul(multiplier)
                    .ok_or_else(|| {
                        Error::InvalidCommand("graph parameter expansion size overflow".into())
                    })?,
                max_bytes,
            )?;
        }
        let QueryTokenKind::Identifier { value, .. } = &token.kind else {
            continue;
        };
        if !token_is_symbol(tokens.get(index.saturating_add(1)), '(') {
            continue;
        }
        let function = value.to_ascii_uppercase();
        if function == "RANGE" {
            let (arguments, _) = function_arguments(tokens, index.saturating_add(1))?;
            let cardinality = static_range_cardinality(&arguments, parameters)?;
            let bytes = cardinality
                .checked_mul(16)
                .and_then(|bytes| bytes.checked_add(16))
                .and_then(|bytes| {
                    bytes.checked_mul(result_expansion_multiplier(
                        tokens,
                        index,
                        execution_row_bound,
                    ))
                })
                .ok_or_else(|| {
                    Error::InvalidCommand("graph RANGE result cardinality overflow".into())
                })?;
            reserve_static_expansion(&mut remaining_expansion_bytes, bytes, max_bytes)?;
            continue;
        }
        if function == "REPEAT" {
            let bytes = repeat_expansion_bytes(statement, tokens, index, parameters)?
                .checked_mul(result_expansion_multiplier(
                    tokens,
                    index,
                    execution_row_bound,
                ))
                .ok_or_else(|| Error::InvalidCommand("graph REPEAT result size overflow".into()))?;
            reserve_static_expansion(&mut remaining_expansion_bytes, bytes, max_bytes)?;
            continue;
        }
        if matches!(function.as_str(), "LPAD" | "RPAD") {
            let bytes = pad_expansion_bytes(tokens, index, parameters)?
                .checked_mul(result_expansion_multiplier(
                    tokens,
                    index,
                    execution_row_bound,
                ))
                .ok_or_else(|| {
                    Error::InvalidCommand("graph LPAD/RPAD result size overflow".into())
                })?;
            reserve_static_expansion(&mut remaining_expansion_bytes, bytes, max_bytes)?;
            continue;
        }
        if matches!(function.as_str(), "REPLACE" | "REGEXP_REPLACE") {
            return Err(Error::InvalidCommand(format!(
                "graph expansion function {value} has no statically bounded result size"
            )));
        }
        if unbounded_container_function(&function) {
            return Err(Error::InvalidCommand(format!(
                "graph container function {value} has no statically bounded result cardinality"
            )));
        }
    }

    reject_list_comprehensions(tokens)
}

fn result_expansion_multiplier(tokens: &[QueryToken], index: usize, row_bound: usize) -> usize {
    if matches!(result_clause_at(tokens, index), Some("RETURN" | "WITH")) {
        row_bound.max(1)
    } else {
        1
    }
}

fn result_clause_at(tokens: &[QueryToken], end: usize) -> Option<&'static str> {
    let mut clause = None;
    let mut round = 0usize;
    let mut square = 0usize;
    let mut curly = 0usize;
    for token in tokens.iter().take(end) {
        if round == 0 && square == 0 && curly == 0 {
            for keyword in [
                "MATCH", "WHERE", "WITH", "UNWIND", "RETURN", "ORDER", "SKIP", "LIMIT", "UNION",
            ] {
                if token_is_keyword(Some(token), keyword) {
                    clause = Some(keyword);
                    break;
                }
            }
        }
        match token.kind {
            QueryTokenKind::Symbol('(') => round = round.saturating_add(1),
            QueryTokenKind::Symbol(')') => round = round.saturating_sub(1),
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            _ => {}
        }
    }
    clause
}

fn static_execution_row_bound(
    tokens: &[QueryToken],
    parameters: &BTreeMap<String, GraphParameterValue>,
    max_rows: usize,
) -> Result<usize> {
    let overflow_probe = max_rows
        .checked_add(1)
        .ok_or_else(|| Error::InvalidCommand("graph query row limit overflow".into()))?;
    if tokens
        .iter()
        .enumerate()
        .any(|(index, _)| union_clause_starts_at(tokens, index))
    {
        return Ok(max_rows);
    }
    let Some(limit_index) = trailing_limit_index(tokens, 0, tokens.len()) else {
        return Ok(overflow_probe);
    };
    let (_, requested) = requested_limit(tokens, limit_index, tokens.len(), parameters)?;
    Ok(if requested <= max_rows {
        requested
    } else {
        overflow_probe
    })
}

fn reserve_static_expansion(remaining: &mut usize, bytes: usize, max_bytes: usize) -> Result<()> {
    *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
        Error::InvalidCommand(format!(
            "graph statically expanded values exceed {max_bytes} result bytes"
        ))
    })?;
    Ok(())
}

fn graph_parameter_expansion_bytes(value: &GraphParameterValue) -> Result<usize> {
    match value {
        GraphParameterValue::Null | GraphParameterValue::Bool(_) => Ok(1),
        GraphParameterValue::I64(_) | GraphParameterValue::U64(_) | GraphParameterValue::F64(_) => {
            Ok(16)
        }
        GraphParameterValue::String(value) => value
            .len()
            .checked_add(16)
            .ok_or_else(|| Error::InvalidCommand("graph parameter expansion size overflow".into())),
        GraphParameterValue::Bytes(value) => value
            .len()
            .checked_add(16)
            .ok_or_else(|| Error::InvalidCommand("graph parameter expansion size overflow".into())),
        GraphParameterValue::List(values) => values.iter().try_fold(16usize, |size, value| {
            size.checked_add(graph_parameter_expansion_bytes(value)?)
                .ok_or_else(|| {
                    Error::InvalidCommand("graph parameter expansion size overflow".into())
                })
        }),
        GraphParameterValue::Struct(fields) => {
            fields.iter().try_fold(16usize, |size, (name, value)| {
                let value_size = graph_parameter_expansion_bytes(value)?;
                size.checked_add(name.len())
                    .and_then(|size| size.checked_add(value_size))
                    .ok_or_else(|| {
                        Error::InvalidCommand("graph parameter expansion size overflow".into())
                    })
            })
        }
    }
}

fn repeat_expansion_bytes(
    statement: &str,
    tokens: &[QueryToken],
    function: usize,
    parameters: &BTreeMap<String, GraphParameterValue>,
) -> Result<usize> {
    let (arguments, _) = function_arguments(tokens, function.saturating_add(1))?;
    let [string, count] = arguments.as_slice() else {
        return Err(Error::InvalidCommand(
            "graph REPEAT must have statically bounded string and count arguments".into(),
        ));
    };
    let string_bytes = static_string_bytes(statement, string, parameters, "REPEAT")?;
    let count = static_integer(count, parameters)?;
    let count = usize::try_from(count).map_err(|_| {
        Error::InvalidCommand("graph REPEAT count must be a nonnegative integer".into())
    })?;
    let bytes = string_bytes
        .checked_mul(count)
        .ok_or_else(|| Error::InvalidCommand("graph REPEAT result size overflow".into()))?;
    Ok(bytes)
}

fn pad_expansion_bytes(
    tokens: &[QueryToken],
    function: usize,
    parameters: &BTreeMap<String, GraphParameterValue>,
) -> Result<usize> {
    let (arguments, _) = function_arguments(tokens, function.saturating_add(1))?;
    let [_, count, _] = arguments.as_slice() else {
        return Err(Error::InvalidCommand(
            "graph LPAD/RPAD must have string, count, and padding arguments".into(),
        ));
    };
    let count = static_integer(count, parameters)?;
    let count = usize::try_from(count).map_err(|_| {
        Error::InvalidCommand("graph LPAD/RPAD count must be a nonnegative integer".into())
    })?;
    count
        .checked_mul(4)
        .ok_or_else(|| Error::InvalidCommand("graph LPAD/RPAD result size overflow".into()))
}

fn static_string_bytes(
    statement: &str,
    tokens: &[QueryToken],
    parameters: &BTreeMap<String, GraphParameterValue>,
    function: &str,
) -> Result<usize> {
    match tokens {
        [QueryToken {
            kind: QueryTokenKind::StringLiteral,
            start,
            end,
        }] => statement
            .get(start.saturating_add(1)..end.saturating_sub(1))
            .map(str::len)
            .ok_or_else(|| Error::InvalidCommand(format!("graph {function} string is invalid"))),
        [QueryToken {
            kind: QueryTokenKind::Parameter(name),
            ..
        }] => match parameters.get(name) {
            Some(GraphParameterValue::String(value)) => Ok(value.len()),
            Some(_) => Err(Error::InvalidCommand(format!(
                "graph {function} string parameter must be a string"
            ))),
            None => Err(Error::InvalidCommand(format!(
                "graph {function} parameter is missing: {name}"
            ))),
        },
        _ => Err(Error::InvalidCommand(format!(
            "graph {function} result bytes must be statically bounded by a string literal or parameter"
        ))),
    }
}

fn function_arguments(tokens: &[QueryToken], open: usize) -> Result<(Vec<&[QueryToken]>, usize)> {
    let mut arguments = Vec::new();
    let mut start = open.saturating_add(1);
    let mut round = 1usize;
    let mut square = 0usize;
    let mut curly = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token.kind {
            QueryTokenKind::Symbol('(') => round = round.saturating_add(1),
            QueryTokenKind::Symbol(')') => {
                round = round.saturating_sub(1);
                if round == 0 {
                    if index > start || !arguments.is_empty() {
                        arguments.push(&tokens[start..index]);
                    }
                    return Ok((arguments, index));
                }
            }
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            QueryTokenKind::Symbol(',') if round == 1 && square == 0 && curly == 0 => {
                arguments.push(&tokens[start..index]);
                start = index.saturating_add(1);
            }
            _ => {}
        }
    }
    Err(Error::InvalidCommand(
        "graph query contains an unterminated function call".into(),
    ))
}

fn static_range_cardinality(
    arguments: &[&[QueryToken]],
    parameters: &BTreeMap<String, GraphParameterValue>,
) -> Result<usize> {
    if !(2..=3).contains(&arguments.len()) {
        return Err(Error::InvalidCommand(
            "graph RANGE must have static start, end, and optional step arguments".into(),
        ));
    }
    let start = static_integer(arguments[0], parameters)?;
    let end = static_integer(arguments[1], parameters)?;
    let step = if arguments.len() == 3 {
        static_integer(arguments[2], parameters)?
    } else {
        1
    };
    if step == 0 {
        return Err(Error::InvalidCommand(
            "graph RANGE step must not be zero".into(),
        ));
    }
    let distance = if step > 0 {
        if start > end {
            return Ok(0);
        }
        end.checked_sub(start)
    } else {
        if start < end {
            return Ok(0);
        }
        start.checked_sub(end)
    }
    .ok_or_else(|| Error::InvalidCommand("graph RANGE distance overflow".into()))?;
    let step = step.unsigned_abs();
    let cardinality = distance
        .unsigned_abs()
        .checked_div(step)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| Error::InvalidCommand("graph RANGE cardinality overflow".into()))?;
    usize::try_from(cardinality)
        .map_err(|_| Error::InvalidCommand("graph RANGE cardinality is too large".into()))
}

fn static_integer(
    tokens: &[QueryToken],
    parameters: &BTreeMap<String, GraphParameterValue>,
) -> Result<i128> {
    match tokens {
        [QueryToken {
            kind: QueryTokenKind::Integer(value),
            ..
        }] => value
            .parse::<i128>()
            .map_err(|_| Error::InvalidCommand("graph RANGE integer is too large".into())),
        [QueryToken {
            kind: QueryTokenKind::Symbol('-'),
            ..
        }, QueryToken {
            kind: QueryTokenKind::Integer(value),
            ..
        }] => value
            .parse::<i128>()
            .ok()
            .and_then(i128::checked_neg)
            .ok_or_else(|| Error::InvalidCommand("graph RANGE integer is too large".into())),
        [QueryToken {
            kind: QueryTokenKind::Parameter(name),
            ..
        }] => match parameters.get(name) {
            Some(GraphParameterValue::I64(value)) => Ok(i128::from(*value)),
            Some(GraphParameterValue::U64(value)) => Ok(i128::from(*value)),
            Some(_) => Err(Error::InvalidCommand(
                "graph RANGE parameters must be integers".into(),
            )),
            None => Err(Error::InvalidCommand(format!(
                "graph RANGE parameter is missing: {name}"
            ))),
        },
        _ => Err(Error::InvalidCommand(
            "graph RANGE cardinality must be statically bounded by integer literals or parameters"
                .into(),
        )),
    }
}

fn unbounded_container_function(function: &str) -> bool {
    matches!(
        function,
        "COLLECT"
            | "NODES"
            | "RELS"
            | "RELATIONSHIPS"
            | "PROPERTIES"
            | "LABELS"
            | "KEYS"
            | "MAP"
            | "MAP_KEYS"
            | "MAP_VALUES"
            | "LIST_CONCAT"
            | "LIST_CAT"
            | "LIST_APPEND"
            | "LIST_PREPEND"
            | "LIST_SLICE"
            | "LIST_SORT"
            | "LIST_REVERSE_SORT"
            | "LIST_DISTINCT"
            | "LIST_REVERSE"
            | "LIST_TRANSFORM"
            | "LIST_FILTER"
            | "ARRAY_VALUE"
            | "ARRAY_CONCAT"
            | "ARRAY_CAT"
            | "ARRAY_APPEND"
            | "ARRAY_PUSH_BACK"
            | "ARRAY_PREPEND"
            | "ARRAY_PUSH_FRONT"
            | "ARRAY_SLICE"
            | "REGEXP_EXTRACT_ALL"
            | "REGEXP_SPLIT_TO_ARRAY"
            | "STRING_SPLIT"
            | "STR_SPLIT"
            | "STRING_TO_ARRAY"
    )
}

fn reject_list_comprehensions(tokens: &[QueryToken]) -> Result<()> {
    let mut stack = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        match token.kind {
            QueryTokenKind::Symbol('[') => stack.push(index),
            QueryTokenKind::Symbol(']') => {
                let Some(open) = stack.pop() else { continue };
                let body = &tokens[open.saturating_add(1)..index];
                if body.iter().any(|token| token_is_keyword(Some(token), "IN"))
                    && body.iter().any(|token| token_is_symbol(Some(token), '|'))
                {
                    return Err(Error::InvalidCommand(
                        "graph list comprehensions have no statically bounded result cardinality"
                            .into(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn token_is_keyword(token: Option<&QueryToken>, keyword: &str) -> bool {
    matches!(
        token.map(|token| &token.kind),
        Some(QueryTokenKind::Identifier { value, escaped: false })
            if value.eq_ignore_ascii_case(keyword)
    )
}

fn token_is_symbol(token: Option<&QueryToken>, symbol: char) -> bool {
    matches!(token.map(|token| &token.kind), Some(QueryTokenKind::Symbol(value)) if *value == symbol)
}

fn call_clause_starts_at(tokens: &[QueryToken], index: usize) -> bool {
    let mut cursor = index.saturating_add(1);
    if token_is_symbol(tokens.get(cursor), '{') {
        return false;
    }
    if !matches!(
        tokens.get(cursor).map(|token| &token.kind),
        Some(QueryTokenKind::Identifier { .. })
    ) {
        return false;
    }
    cursor = cursor.saturating_add(1);
    while token_is_symbol(tokens.get(cursor), '.')
        && matches!(
            tokens
                .get(cursor.saturating_add(1))
                .map(|token| &token.kind),
            Some(QueryTokenKind::Identifier { .. })
        )
    {
        cursor = cursor.saturating_add(2);
    }
    token_is_symbol(tokens.get(cursor), '(')
}

fn load_from_clause_starts_at(tokens: &[QueryToken], index: usize) -> bool {
    let mut cursor = index.saturating_add(1);
    if token_is_keyword(tokens.get(cursor), "FROM") {
        return true;
    }
    if !token_is_keyword(tokens.get(cursor), "WITH")
        || !token_is_keyword(tokens.get(cursor.saturating_add(1)), "HEADERS")
        || !token_is_symbol(tokens.get(cursor.saturating_add(2)), '(')
    {
        return false;
    }
    cursor = cursor.saturating_add(2);
    let mut depth = 0usize;
    while let Some(token) = tokens.get(cursor) {
        match token.kind {
            QueryTokenKind::Symbol('(') => depth = depth.saturating_add(1),
            QueryTokenKind::Symbol(')') => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return token_is_keyword(tokens.get(cursor.saturating_add(1)), "FROM");
                }
            }
            _ => {}
        }
        cursor = cursor.saturating_add(1);
    }
    false
}

fn reject_admin_external_or_transaction_queries(tokens: &[QueryToken]) -> Result<()> {
    for (index, token) in tokens.iter().enumerate() {
        let QueryTokenKind::Identifier {
            value,
            escaped: false,
        } = &token.kind
        else {
            continue;
        };
        let keyword = value.to_ascii_uppercase();
        let at_statement_start = index == 0;
        let forbidden = match keyword.as_str() {
            "CALL" => call_clause_starts_at(tokens, index),
            "BEGIN" | "COMMIT" | "ROLLBACK" | "CHECKPOINT" | "COPY" | "ATTACH" | "DETACH"
            | "INSTALL" => at_statement_start,
            "TRANSACTION" => index
                .checked_sub(1)
                .is_some_and(|index| token_is_keyword(tokens.get(index), "BEGIN")),
            "LOAD" => load_from_clause_starts_at(tokens, index) || at_statement_start,
            "IMPORT" | "EXPORT" => {
                at_statement_start && token_is_keyword(tokens.get(index + 1), "DATABASE")
            }
            _ => false,
        };
        if forbidden {
            return Err(Error::InvalidCommand(format!(
                "graph query cannot execute admin, external I/O, or transaction clause: {value}"
            )));
        }
    }
    Ok(())
}

fn reject_unlabeled_node_patterns(tokens: &[QueryToken]) -> Result<()> {
    let mut matches = Vec::new();
    let mut stack = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        match token.kind {
            QueryTokenKind::Symbol('(') => stack.push(index),
            QueryTokenKind::Symbol(')') => {
                if let Some(open) = stack.pop() {
                    matches.push((open, index));
                }
            }
            _ => {}
        }
    }

    let mut match_pattern_depths = Vec::new();
    let mut round = 0usize;
    let mut square = 0usize;
    let mut curly = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        if token_is_keyword(Some(token), "MATCH") {
            match_pattern_depths.push((index, round, square, curly));
        }
        match token.kind {
            QueryTokenKind::Symbol('(') => round = round.saturating_add(1),
            QueryTokenKind::Symbol(')') => round = round.saturating_sub(1),
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            _ => {}
        }
    }

    for (open, close) in matches {
        let follows_path = path_connector_after(tokens, close);
        let precedes_path = path_connector_before(tokens, open);
        let function_wrapper = open.checked_sub(1).is_some_and(|previous| {
            matches!(
                tokens.get(previous),
                Some(QueryToken {
                    kind: QueryTokenKind::Identifier { value, .. },
                    ..
                }) if !value.eq_ignore_ascii_case("MATCH")
            )
        });
        let in_match_pattern = !function_wrapper
            && match_pattern_depths.iter().any(
                |&(match_index, base_round, base_square, base_curly)| {
                    open > match_index
                        && node_pattern_is_in_match_clause(
                            tokens,
                            match_index,
                            open,
                            base_round,
                            base_square,
                            base_curly,
                        )
                },
            );
        if (follows_path || precedes_path || in_match_pattern)
            && !node_pattern_has_static_label(tokens, open, close)
        {
            return Err(Error::InvalidCommand(
                "graph node patterns must use an explicit non-reserved label".into(),
            ));
        }
    }
    Ok(())
}

fn node_pattern_is_in_match_clause(
    tokens: &[QueryToken],
    match_index: usize,
    open: usize,
    base_round: usize,
    base_square: usize,
    base_curly: usize,
) -> bool {
    let mut round = base_round;
    let mut square = base_square;
    let mut curly = base_curly;
    for (index, token) in tokens
        .iter()
        .enumerate()
        .take(open.saturating_add(1))
        .skip(match_index.saturating_add(1))
    {
        let at_base = round == base_round && square == base_square && curly == base_curly;
        if at_base
            && index != open
            && matches!(
                token,
                QueryToken {
                    kind: QueryTokenKind::Identifier {
                        value,
                        escaped: false
                    },
                    ..
                } if matches!(
                    value.to_ascii_uppercase().as_str(),
                    "WHERE"
                        | "RETURN"
                        | "WITH"
                        | "UNWIND"
                        | "ORDER"
                        | "SKIP"
                        | "LIMIT"
                        | "UNION"
                        | "MATCH"
                        | "OPTIONAL"
                        | "CALL"
                )
            )
        {
            return false;
        }
        match token.kind {
            QueryTokenKind::Symbol('(') => {
                if index == open {
                    return at_base;
                }
                round = round.saturating_add(1);
            }
            QueryTokenKind::Symbol(')') => round = round.saturating_sub(1),
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            _ => {}
        }
    }
    false
}

fn node_pattern_has_static_label(tokens: &[QueryToken], open: usize, close: usize) -> bool {
    let mut round = 0usize;
    let mut square = 0usize;
    let mut curly = 0usize;
    for (offset, token) in tokens[open.saturating_add(1)..close].iter().enumerate() {
        match token.kind {
            QueryTokenKind::Symbol('(') => round = round.saturating_add(1),
            QueryTokenKind::Symbol(')') => round = round.saturating_sub(1),
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            QueryTokenKind::Symbol(':') if round == 0 && square == 0 && curly == 0 => {
                return matches!(
                    tokens.get(open.saturating_add(2).saturating_add(offset)),
                    Some(QueryToken {
                        kind: QueryTokenKind::Identifier { .. },
                        ..
                    })
                );
            }
            _ => {}
        }
    }
    false
}

fn path_connector_after(tokens: &[QueryToken], close: usize) -> bool {
    (token_is_symbol(tokens.get(close.saturating_add(1)), '-')
        && matches!(
            tokens.get(close.saturating_add(2)).map(|token| &token.kind),
            Some(QueryTokenKind::Symbol('-' | '[' | '>'))
        ))
        || (token_is_symbol(tokens.get(close.saturating_add(1)), '<')
            && token_is_symbol(tokens.get(close.saturating_add(2)), '-'))
}

fn path_connector_before(tokens: &[QueryToken], open: usize) -> bool {
    let Some(previous) = open.checked_sub(1) else {
        return false;
    };
    let before_previous = previous.checked_sub(1);
    (token_is_symbol(tokens.get(previous), '-')
        && matches!(
            before_previous
                .and_then(|index| tokens.get(index))
                .map(|token| &token.kind),
            Some(QueryTokenKind::Symbol('-' | ']' | '<'))
        ))
        || (token_is_symbol(tokens.get(previous), '>')
            && before_previous.is_some_and(|index| token_is_symbol(tokens.get(index), '-')))
}

fn bounded_read_statement(
    statement: &str,
    tokens: &[QueryToken],
    parameters: &BTreeMap<String, GraphParameterValue>,
    max_rows: usize,
) -> Result<String> {
    let execution_limit = max_rows
        .checked_add(1)
        .ok_or_else(|| Error::InvalidCommand("graph query row limit overflow".into()))?;
    let mut round = 0usize;
    let mut square = 0usize;
    let mut curly = 0usize;
    let mut union_indices = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let top_level = round == 0 && square == 0 && curly == 0;
        if top_level && union_clause_starts_at(tokens, index) {
            union_indices.push(index);
        }
        match token.kind {
            QueryTokenKind::Symbol('(') => round = round.saturating_add(1),
            QueryTokenKind::Symbol(')') => round = round.saturating_sub(1),
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            _ => {}
        }
    }
    if !union_indices.is_empty() {
        let mut total_limit = 0usize;
        let mut branch_start = 0usize;
        for branch_end in union_indices.iter().copied().chain([tokens.len()]) {
            let Some(limit_index) = trailing_limit_index(tokens, branch_start, branch_end) else {
                return Err(Error::InvalidCommand(
                    "UNION queries require exactly one explicit bounded LIMIT in every branch"
                        .into(),
                ));
            };
            let (_, requested) = requested_limit(tokens, limit_index, branch_end, parameters)?;
            total_limit = total_limit
                .checked_add(requested)
                .ok_or_else(|| Error::InvalidCommand("graph UNION LIMIT sum overflow".into()))?;
            branch_start = branch_end.saturating_add(1);
            if token_is_keyword(tokens.get(branch_start), "ALL") {
                branch_start = branch_start.saturating_add(1);
            }
        }
        if total_limit > max_rows {
            return Err(Error::InvalidCommand(format!(
                "graph UNION branch LIMIT sum {total_limit} exceeds max_rows {max_rows}"
            )));
        }
        return Ok(statement.to_owned());
    }
    let Some(limit_index) = trailing_limit_index(tokens, 0, tokens.len()) else {
        // Ladybug 0.18.1 does not support a CALL-subquery wrapper. Appending a
        // top-level LIMIT is semantics-preserving for one non-UNION query part.
        // UNION requires explicit per-branch limits above because the backend
        // exposes no execution-time maximum-result-row setting.
        return Ok(format!("{statement}\nLIMIT {execution_limit}"));
    };
    let (limit_value, requested) = requested_limit(tokens, limit_index, tokens.len(), parameters)?;
    if requested <= max_rows {
        return Ok(statement.to_owned());
    }
    if matches!(limit_value.kind, QueryTokenKind::Parameter(_)) {
        return Err(Error::InvalidCommand(format!(
            "graph LIMIT parameter exceeds max_rows {max_rows}"
        )));
    }
    let relative_start = limit_value.start;
    let relative_end = limit_value.end;
    Ok(format!(
        "{}{}{}",
        &statement[..relative_start],
        execution_limit,
        &statement[relative_end..]
    ))
}

fn union_clause_starts_at(tokens: &[QueryToken], index: usize) -> bool {
    if !token_is_keyword(tokens.get(index), "UNION")
        || index
            .checked_sub(1)
            .is_some_and(|previous| token_is_keyword(tokens.get(previous), "AS"))
    {
        return false;
    }
    let mut next = index.saturating_add(1);
    if token_is_keyword(tokens.get(next), "ALL") {
        next = next.saturating_add(1);
    }
    let Some(QueryToken {
        kind: QueryTokenKind::Identifier {
            value,
            escaped: false,
        },
        ..
    }) = tokens.get(next)
    else {
        return false;
    };

    // A branch starts with an unescaped clause keyword, but Ladybug may add
    // valid clause starters over time. Reject only tokens that can continue a
    // preceding expression instead of maintaining an incomplete allow-list.
    !matches!(
        value.to_ascii_uppercase().as_str(),
        "ALL"
            | "AS"
            | "LIMIT"
            | "SKIP"
            | "ORDER"
            | "BY"
            | "WHERE"
            | "ASC"
            | "DESC"
            | "AND"
            | "OR"
            | "XOR"
            | "IN"
            | "IS"
            | "NULL"
    )
}

fn trailing_limit_index(tokens: &[QueryToken], start: usize, end: usize) -> Option<usize> {
    let limit_index = end.checked_sub(2)?;
    if limit_index < start || !token_is_keyword(tokens.get(limit_index), "LIMIT") {
        return None;
    }
    let mut round = 0usize;
    let mut square = 0usize;
    let mut curly = 0usize;
    for token in tokens.get(start..limit_index)? {
        match token.kind {
            QueryTokenKind::Symbol('(') => round = round.saturating_add(1),
            QueryTokenKind::Symbol(')') => round = round.saturating_sub(1),
            QueryTokenKind::Symbol('[') => square = square.saturating_add(1),
            QueryTokenKind::Symbol(']') => square = square.saturating_sub(1),
            QueryTokenKind::Symbol('{') => curly = curly.saturating_add(1),
            QueryTokenKind::Symbol('}') => curly = curly.saturating_sub(1),
            _ => {}
        }
    }
    (round == 0 && square == 0 && curly == 0).then_some(limit_index)
}

fn requested_limit<'a>(
    tokens: &'a [QueryToken],
    limit_index: usize,
    branch_end: usize,
    parameters: &BTreeMap<String, GraphParameterValue>,
) -> Result<(&'a QueryToken, usize)> {
    let [limit_value] = tokens.get(limit_index + 1..branch_end).unwrap_or_default() else {
        return Err(Error::InvalidCommand(
            "graph LIMIT must be one nonnegative integer or parameter".into(),
        ));
    };
    let requested = match &limit_value.kind {
        QueryTokenKind::Integer(value) => value
            .parse::<usize>()
            .map_err(|_| Error::InvalidCommand("graph LIMIT is too large".into()))?,
        QueryTokenKind::Parameter(name) => match parameters.get(name) {
            Some(GraphParameterValue::U64(value)) => usize::try_from(*value)
                .map_err(|_| Error::InvalidCommand("graph LIMIT is too large".into()))?,
            Some(GraphParameterValue::I64(value)) if *value >= 0 => *value as usize,
            Some(_) => {
                return Err(Error::InvalidCommand(
                    "graph LIMIT parameter must be a nonnegative integer".into(),
                ))
            }
            None => {
                return Err(Error::InvalidCommand(format!(
                    "graph LIMIT parameter is missing: {name}"
                )))
            }
        },
        _ => {
            return Err(Error::InvalidCommand(
                "graph LIMIT must be one nonnegative integer or parameter".into(),
            ))
        }
    };
    Ok((limit_value, requested))
}

fn lex_query(statement: &str) -> Result<Vec<QueryToken>> {
    let bytes = statement.as_bytes();
    let mut tokens = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        match bytes[offset] {
            byte if byte.is_ascii_whitespace() => offset += 1,
            b'/' if bytes.get(offset + 1) == Some(&b'/') => {
                offset += 2;
                while offset < bytes.len() && bytes[offset] != b'\n' {
                    offset += 1;
                }
            }
            b'/' if bytes.get(offset + 1) == Some(&b'*') => {
                offset += 2;
                let mut closed = false;
                while offset + 1 < bytes.len() {
                    if bytes[offset] == b'*' && bytes[offset + 1] == b'/' {
                        offset += 2;
                        closed = true;
                        break;
                    }
                    offset += 1;
                }
                if !closed {
                    return Err(Error::InvalidCommand(
                        "graph query contains an unterminated block comment".into(),
                    ));
                }
            }
            quote @ (b'\'' | b'"') => {
                let start = offset;
                skip_quoted_string(bytes, &mut offset, quote)?;
                tokens.push(QueryToken {
                    kind: QueryTokenKind::StringLiteral,
                    start,
                    end: offset,
                });
            }
            b'`' => {
                let start = offset;
                let value = read_escaped_identifier(statement, &mut offset)?;
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Identifier {
                        value,
                        escaped: true,
                    },
                    start,
                    end: offset,
                });
            }
            b'$' => {
                let start = offset;
                offset += 1;
                let name_start = offset;
                while offset < bytes.len()
                    && (bytes[offset].is_ascii_alphanumeric() || bytes[offset] == b'_')
                {
                    offset += 1;
                }
                if name_start == offset {
                    return Err(Error::InvalidCommand(
                        "graph query contains an invalid parameter reference".into(),
                    ));
                }
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Parameter(statement[name_start..offset].into()),
                    start,
                    end: offset,
                });
            }
            b';' => {
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Semicolon,
                    start: offset,
                    end: offset + 1,
                });
                offset += 1;
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = offset;
                offset += 1;
                while offset < bytes.len()
                    && (bytes[offset].is_ascii_alphanumeric() || bytes[offset] == b'_')
                {
                    offset += 1;
                }
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Identifier {
                        value: statement[start..offset].into(),
                        escaped: false,
                    },
                    start,
                    end: offset,
                });
            }
            byte if byte.is_ascii_digit() => {
                let start = offset;
                offset += 1;
                while offset < bytes.len() && bytes[offset].is_ascii_digit() {
                    offset += 1;
                }
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Integer(statement[start..offset].into()),
                    start,
                    end: offset,
                });
            }
            byte if byte.is_ascii() => {
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Symbol(char::from(byte)),
                    start: offset,
                    end: offset + 1,
                });
                offset += 1;
            }
            _ => {
                let start = offset;
                let first = statement[offset..].chars().next().ok_or_else(|| {
                    Error::InvalidCommand("graph query contains invalid UTF-8".into())
                })?;
                if !first.is_alphanumeric() {
                    return Err(Error::InvalidCommand(
                        "graph query contains an unsupported non-ASCII token".into(),
                    ));
                }
                offset += first.len_utf8();
                while offset < bytes.len() {
                    let Some(character) = statement[offset..].chars().next() else {
                        break;
                    };
                    if !character.is_alphanumeric() && character != '_' {
                        break;
                    }
                    offset += character.len_utf8();
                }
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Identifier {
                        value: statement[start..offset].into(),
                        escaped: false,
                    },
                    start,
                    end: offset,
                });
            }
        }
    }
    Ok(tokens)
}

fn skip_quoted_string(bytes: &[u8], offset: &mut usize, quote: u8) -> Result<()> {
    *offset += 1;
    while *offset < bytes.len() {
        if bytes[*offset] == b'\\' {
            *offset += 1;
            if *offset == bytes.len() {
                break;
            }
            *offset += 1;
        } else if bytes[*offset] == quote {
            if bytes.get(*offset + 1) == Some(&quote) {
                *offset += 2;
            } else {
                *offset += 1;
                return Ok(());
            }
        } else {
            *offset += 1;
        }
    }
    Err(Error::InvalidCommand(
        "graph query contains an unterminated string".into(),
    ))
}

fn read_escaped_identifier(statement: &str, offset: &mut usize) -> Result<String> {
    let bytes = statement.as_bytes();
    *offset += 1;
    let mut identifier = String::new();
    while *offset < bytes.len() {
        if bytes[*offset] == b'`' {
            if bytes.get(*offset + 1) == Some(&b'`') {
                identifier.push('`');
                *offset += 2;
                continue;
            }
            *offset += 1;
            if identifier.is_empty() {
                return Err(Error::InvalidCommand(
                    "graph query contains an empty escaped identifier".into(),
                ));
            }
            return Ok(identifier);
        }
        if bytes[*offset] == b'\\' {
            *offset += 1;
            match bytes.get(*offset) {
                Some(b'`') => {
                    identifier.push('`');
                    *offset += 1;
                }
                Some(b'u') => {
                    *offset += 1;
                    identifier.push(read_unicode_escape(bytes, offset, 4)?);
                }
                Some(b'U') => {
                    *offset += 1;
                    identifier.push(read_unicode_escape(bytes, offset, 8)?);
                }
                _ => {
                    return Err(Error::InvalidCommand(
                        "graph query contains an invalid escaped identifier".into(),
                    ))
                }
            }
            continue;
        }
        let character = statement[*offset..]
            .chars()
            .next()
            .ok_or_else(|| Error::InvalidCommand("graph query contains invalid UTF-8".into()))?;
        identifier.push(character);
        *offset += character.len_utf8();
    }
    Err(Error::InvalidCommand(
        "graph query contains an unterminated escaped identifier".into(),
    ))
}

fn read_unicode_escape(bytes: &[u8], offset: &mut usize, digits: usize) -> Result<char> {
    let end = offset
        .checked_add(digits)
        .ok_or_else(|| Error::InvalidCommand("graph escaped identifier length overflow".into()))?;
    let encoded = bytes.get(*offset..end).ok_or_else(|| {
        Error::InvalidCommand("graph query contains a truncated unicode escape".into())
    })?;
    let encoded = std::str::from_utf8(encoded)
        .map_err(|_| Error::InvalidCommand("graph unicode escape is not ASCII".into()))?;
    let value = u32::from_str_radix(encoded, 16)
        .map_err(|_| Error::InvalidCommand("graph unicode escape is invalid".into()))?;
    *offset = end;
    char::from_u32(value)
        .ok_or_else(|| Error::InvalidCommand("graph unicode escape is not a scalar".into()))
}

fn query_parameters(
    parameters: &BTreeMap<String, GraphParameterValue>,
) -> Result<Vec<(&str, Value)>> {
    if parameters.len() > MAX_GRAPH_PARAMETERS {
        return Err(Error::InvalidCommand(format!(
            "graph query exceeds {MAX_GRAPH_PARAMETERS} parameters"
        )));
    }
    let mut remaining = MAX_GRAPH_PARAMETER_VALUES;
    parameters
        .iter()
        .map(|(name, value)| {
            validate_parameter_name(name)?;
            Ok((
                name.as_str(),
                query_parameter_value(value, 0, &mut remaining)?,
            ))
        })
        .collect()
}

fn validate_query_parameter_contract(
    parameters: &BTreeMap<String, GraphParameterValue>,
    referenced: &BTreeSet<String>,
) -> Result<()> {
    let supplied = parameters.keys().cloned().collect::<BTreeSet<_>>();
    if supplied != *referenced {
        return Err(Error::InvalidCommand(
            "graph query parameters must exactly match referenced parameters".into(),
        ));
    }
    Ok(())
}

fn validate_parameter_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_GRAPH_PARAMETER_NAME_BYTES {
        return Err(Error::InvalidCommand(format!(
            "graph parameter name must contain 1..={MAX_GRAPH_PARAMETER_NAME_BYTES} bytes"
        )));
    }
    let mut bytes = name.bytes();
    let first = bytes.next().expect("empty checked");
    if !(first.is_ascii_alphabetic() || first == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(Error::InvalidCommand(
            "graph parameter names must be ASCII identifiers".into(),
        ));
    }
    if name.to_ascii_lowercase().starts_with("__rhiza") {
        return Err(Error::InvalidCommand(
            "graph parameter names cannot use the reserved __Rhiza namespace".into(),
        ));
    }
    Ok(())
}

fn query_parameter_value(
    value: &GraphParameterValue,
    depth: usize,
    remaining: &mut usize,
) -> Result<Value> {
    if depth > MAX_GRAPH_PARAMETER_DEPTH {
        return Err(Error::InvalidCommand(format!(
            "graph parameter nesting exceeds {MAX_GRAPH_PARAMETER_DEPTH}"
        )));
    }
    *remaining = remaining.checked_sub(1).ok_or_else(|| {
        Error::InvalidCommand(format!(
            "graph parameters exceed {MAX_GRAPH_PARAMETER_VALUES} values"
        ))
    })?;
    Ok(match value {
        GraphParameterValue::Null => Value::Null(LogicalType::Any),
        GraphParameterValue::Bool(value) => Value::Bool(*value),
        GraphParameterValue::I64(value) => Value::Int64(*value),
        GraphParameterValue::U64(value) => Value::UInt64(*value),
        GraphParameterValue::F64(value) => Value::Double(value.get()),
        GraphParameterValue::String(value) => {
            if value.len() > MAX_STRING_BYTES {
                return Err(Error::InvalidCommand(format!(
                    "graph parameter strings cannot exceed {MAX_STRING_BYTES} bytes"
                )));
            }
            Value::String(value.clone())
        }
        GraphParameterValue::Bytes(value) => {
            if value.len() > MAX_BLOB_BYTES {
                return Err(Error::InvalidCommand(format!(
                    "graph parameter bytes cannot exceed {MAX_BLOB_BYTES} bytes"
                )));
            }
            Value::Blob(value.clone())
        }
        GraphParameterValue::List(values) => {
            if values.len() > MAX_GRAPH_PARAMETER_CONTAINER_VALUES {
                return Err(Error::InvalidCommand(format!(
                    "graph parameter lists cannot exceed {MAX_GRAPH_PARAMETER_CONTAINER_VALUES} values"
                )));
            }
            let converted = values
                .iter()
                .map(|value| query_parameter_value(value, depth + 1, remaining))
                .collect::<Result<Vec<_>>>()?;
            let element_type = converted
                .first()
                .map_or(LogicalType::String, LogicalType::from);
            if converted
                .iter()
                .any(|value| LogicalType::from(value) != element_type)
            {
                return Err(Error::InvalidCommand(
                    "graph parameter lists must contain one value type".into(),
                ));
            }
            Value::List(element_type, converted)
        }
        GraphParameterValue::Struct(fields) => {
            if fields.len() > MAX_GRAPH_PARAMETER_CONTAINER_VALUES {
                return Err(Error::InvalidCommand(format!(
                    "graph parameter structs cannot exceed {MAX_GRAPH_PARAMETER_CONTAINER_VALUES} fields"
                )));
            }
            let fields = fields
                .iter()
                .map(|(name, value)| {
                    validate_parameter_name(name)?;
                    Ok((
                        name.clone(),
                        query_parameter_value(value, depth + 1, remaining)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Value::Struct(fields)
        }
    })
}

fn graph_result_value(value: Value) -> Result<GraphResultValue> {
    graph_result_value_at(value, 0)
}

const GRAPH_RESULT_VALUE_OVERHEAD: usize = 8;

struct GraphResultBudget {
    limit: usize,
    remaining_bytes: usize,
    remaining_elements: usize,
}

impl GraphResultBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining_bytes: limit,
            remaining_elements: limit / GRAPH_RESULT_VALUE_OVERHEAD,
        }
    }

    fn exceeded(&self) -> Error {
        Error::InvalidCommand(format!("graph query exceeds {} result bytes", self.limit))
    }

    fn consume_bytes(&mut self, bytes: usize) -> Result<()> {
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(bytes)
            .ok_or_else(|| self.exceeded())?;
        Ok(())
    }

    fn ensure_elements(&self, elements: usize) -> Result<()> {
        if elements > self.remaining_elements {
            Err(self.exceeded())
        } else {
            Ok(())
        }
    }

    fn consume_element(&mut self) -> Result<()> {
        self.remaining_elements = self
            .remaining_elements
            .checked_sub(1)
            .ok_or_else(|| self.exceeded())?;
        Ok(())
    }

    fn reserve_column(&mut self, name: &str, logical_type: &LogicalType) -> Result<()> {
        self.consume_element()?;
        self.consume_bytes(GRAPH_RESULT_VALUE_OVERHEAD.saturating_add(name.len()))?;
        self.reserve_logical_type(logical_type, 0)
    }

    fn reserve_row(&mut self, row: &[Value]) -> Result<()> {
        self.ensure_elements(row.len())?;
        for value in row {
            self.reserve_value(value, 0)?;
        }
        Ok(())
    }

    fn reserve_logical_type(&mut self, logical_type: &LogicalType, depth: usize) -> Result<()> {
        if depth > MAX_GRAPH_PARAMETER_DEPTH {
            return Err(Error::InvalidCommand(format!(
                "graph result nesting exceeds {MAX_GRAPH_PARAMETER_DEPTH}"
            )));
        }
        self.consume_element()?;
        self.consume_bytes(GRAPH_RESULT_VALUE_OVERHEAD)?;
        match logical_type {
            LogicalType::List { child_type } => {
                self.reserve_logical_type(child_type, depth + 1)?;
            }
            LogicalType::Array {
                child_type,
                num_elements: _,
            } => {
                self.consume_bytes(8)?;
                self.reserve_logical_type(child_type, depth + 1)?;
            }
            LogicalType::Struct { fields } | LogicalType::Union { types: fields } => {
                self.ensure_elements(fields.len())?;
                for (name, field_type) in fields {
                    self.consume_bytes(name.len())?;
                    self.reserve_logical_type(field_type, depth + 1)?;
                }
            }
            LogicalType::Map {
                key_type,
                value_type,
            } => {
                self.reserve_logical_type(key_type, depth + 1)?;
                self.reserve_logical_type(value_type, depth + 1)?;
            }
            LogicalType::Decimal { .. } => self.consume_bytes(8)?,
            _ => {}
        }
        Ok(())
    }

    fn reserve_value(&mut self, value: &Value, depth: usize) -> Result<()> {
        if depth > MAX_GRAPH_PARAMETER_DEPTH {
            return Err(Error::InvalidCommand(format!(
                "graph result nesting exceeds {MAX_GRAPH_PARAMETER_DEPTH}"
            )));
        }
        self.consume_element()?;
        self.consume_bytes(GRAPH_RESULT_VALUE_OVERHEAD)?;
        match value {
            Value::Null(logical_type) => self.reserve_logical_type(logical_type, depth + 1)?,
            Value::Bool(_) | Value::Int8(_) | Value::UInt8(_) => self.consume_bytes(1)?,
            Value::Int16(_) | Value::UInt16(_) => self.consume_bytes(2)?,
            Value::Int32(_) | Value::UInt32(_) => self.consume_bytes(4)?,
            Value::Int64(_) | Value::UInt64(_) | Value::Double(_) => self.consume_bytes(8)?,
            Value::InternalID(_) => self.consume_bytes(16)?,
            Value::Int128(value) => {
                self.consume_bytes(value.to_string().len().saturating_add(2))?
            }
            Value::Float(value) => self.consume_bytes(value.to_string().len().saturating_add(2))?,
            Value::Date(value) => self.consume_bytes(value.to_string().len().saturating_add(2))?,
            Value::Interval(value) => {
                self.consume_bytes(value.to_string().len().saturating_add(2))?
            }
            Value::Timestamp(value)
            | Value::TimestampTz(value)
            | Value::TimestampNs(value)
            | Value::TimestampMs(value)
            | Value::TimestampSec(value) => {
                self.consume_bytes(value.to_string().len().saturating_add(2))?
            }
            Value::String(value) => self.consume_bytes(value.len().saturating_add(2))?,
            Value::Json(value) => self.consume_bytes(value.to_string().len().saturating_add(2))?,
            Value::Blob(value) => self.consume_bytes(value.len())?,
            Value::List(element_type, values) | Value::Array(element_type, values) => {
                self.reserve_logical_type(element_type, depth + 1)?;
                self.ensure_elements(values.len())?;
                for value in values {
                    self.reserve_value(value, depth + 1)?;
                }
            }
            Value::Struct(fields) => {
                self.ensure_elements(fields.len())?;
                for (name, value) in fields {
                    self.consume_bytes(name.len())?;
                    self.reserve_value(value, depth + 1)?;
                }
            }
            Value::Node(node) => self.reserve_node(node, depth + 1)?,
            Value::Rel(rel) => self.reserve_rel(rel, depth + 1)?,
            Value::RecursiveRel { nodes, rels } => {
                self.ensure_elements(nodes.len().saturating_add(rels.len()))?;
                for node in nodes {
                    self.consume_element()?;
                    self.reserve_node(node, depth + 1)?;
                }
                for rel in rels {
                    self.consume_element()?;
                    self.reserve_rel(rel, depth + 1)?;
                }
            }
            Value::Map((key_type, value_type), entries) => {
                self.reserve_logical_type(key_type, depth + 1)?;
                self.reserve_logical_type(value_type, depth + 1)?;
                self.ensure_elements(entries.len().saturating_mul(2))?;
                for (key, value) in entries {
                    self.reserve_value(key, depth + 1)?;
                    self.reserve_value(value, depth + 1)?;
                }
            }
            Value::Union { types, value } => {
                self.ensure_elements(types.len())?;
                for (name, logical_type) in types {
                    self.consume_bytes(name.len())?;
                    self.reserve_logical_type(logical_type, depth + 1)?;
                }
                self.reserve_value(value, depth + 1)?;
            }
            Value::UUID(value) => self.consume_bytes(value.to_string().len().saturating_add(2))?,
            Value::Decimal(value) => {
                self.consume_bytes(value.to_string().len().saturating_add(2))?
            }
        }
        Ok(())
    }

    fn reserve_node(&mut self, node: &lbug::NodeVal, depth: usize) -> Result<()> {
        if node
            .get_label_name()
            .to_ascii_lowercase()
            .starts_with("__rhiza")
        {
            return Err(Error::InvalidCommand(
                "graph query cannot return reserved __Rhiza nodes".into(),
            ));
        }
        let properties = node.get_properties();
        self.ensure_elements(properties.len())?;
        self.consume_bytes(16usize.saturating_add(node.get_label_name().len()))?;
        for (name, value) in properties {
            self.consume_bytes(name.len())?;
            self.reserve_value(value, depth)?;
        }
        Ok(())
    }

    fn reserve_rel(&mut self, rel: &lbug::RelVal, depth: usize) -> Result<()> {
        let properties = rel.get_properties();
        self.ensure_elements(properties.len())?;
        self.consume_bytes(32usize.saturating_add(rel.get_label_name().len()))?;
        for (name, value) in properties {
            self.consume_bytes(name.len())?;
            self.reserve_value(value, depth)?;
        }
        Ok(())
    }
}

fn graph_logical_type(value: LogicalType) -> Result<GraphLogicalType> {
    Ok(match value {
        LogicalType::Any => GraphLogicalType::Any,
        LogicalType::Bool => GraphLogicalType::Bool,
        LogicalType::Serial => GraphLogicalType::Serial,
        LogicalType::Int64 => GraphLogicalType::I64,
        LogicalType::Int32 => GraphLogicalType::I32,
        LogicalType::Int16 => GraphLogicalType::I16,
        LogicalType::Int8 => GraphLogicalType::I8,
        LogicalType::UInt64 => GraphLogicalType::U64,
        LogicalType::UInt32 => GraphLogicalType::U32,
        LogicalType::UInt16 => GraphLogicalType::U16,
        LogicalType::UInt8 => GraphLogicalType::U8,
        LogicalType::Int128 => GraphLogicalType::I128,
        LogicalType::Double => GraphLogicalType::F64,
        LogicalType::Float => GraphLogicalType::F32,
        LogicalType::Date => GraphLogicalType::Date,
        LogicalType::Interval => GraphLogicalType::Interval,
        LogicalType::Timestamp => GraphLogicalType::Timestamp,
        LogicalType::TimestampTz => GraphLogicalType::TimestampTz,
        LogicalType::TimestampNs => GraphLogicalType::TimestampNs,
        LogicalType::TimestampMs => GraphLogicalType::TimestampMs,
        LogicalType::TimestampSec => GraphLogicalType::TimestampSec,
        LogicalType::InternalID => GraphLogicalType::InternalId,
        LogicalType::String => GraphLogicalType::String,
        LogicalType::Json => GraphLogicalType::Json,
        LogicalType::Blob => GraphLogicalType::Bytes,
        LogicalType::List { child_type } => {
            GraphLogicalType::List(Box::new(graph_logical_type(*child_type)?))
        }
        LogicalType::Array {
            child_type,
            num_elements,
        } => GraphLogicalType::Array {
            element_type: Box::new(graph_logical_type(*child_type)?),
            length: num_elements,
        },
        LogicalType::Struct { fields } => GraphLogicalType::Struct(
            fields
                .into_iter()
                .map(|(name, logical_type)| Ok((name, graph_logical_type(logical_type)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
        LogicalType::Node => GraphLogicalType::Node,
        LogicalType::Rel => GraphLogicalType::Rel,
        LogicalType::RecursiveRel => GraphLogicalType::RecursiveRel,
        LogicalType::Map {
            key_type,
            value_type,
        } => GraphLogicalType::Map {
            key_type: Box::new(graph_logical_type(*key_type)?),
            value_type: Box::new(graph_logical_type(*value_type)?),
        },
        LogicalType::Union { types } => GraphLogicalType::Union(
            types
                .into_iter()
                .map(|(name, logical_type)| Ok((name, graph_logical_type(logical_type)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
        LogicalType::UUID => GraphLogicalType::Uuid,
        LogicalType::Decimal { precision, scale } => GraphLogicalType::Decimal { precision, scale },
    })
}

fn graph_result_value_at(value: Value, depth: usize) -> Result<GraphResultValue> {
    if depth > MAX_GRAPH_PARAMETER_DEPTH {
        return Err(Error::InvalidCommand(format!(
            "graph result nesting exceeds {MAX_GRAPH_PARAMETER_DEPTH}"
        )));
    }
    Ok(match value {
        Value::Null(logical_type) => GraphResultValue::Null(graph_logical_type(logical_type)?),
        Value::Bool(value) => GraphResultValue::Bool(value),
        Value::Int64(value) => GraphResultValue::I64(value),
        Value::Int32(value) => GraphResultValue::I32(value),
        Value::Int16(value) => GraphResultValue::I16(value),
        Value::Int8(value) => GraphResultValue::I8(value),
        Value::UInt64(value) => GraphResultValue::U64(value),
        Value::UInt32(value) => GraphResultValue::U32(value),
        Value::UInt16(value) => GraphResultValue::U16(value),
        Value::UInt8(value) => GraphResultValue::U8(value),
        Value::Int128(value) => GraphResultValue::I128(value.to_string()),
        Value::Double(value) => GraphResultValue::F64(CanonicalF64::new(value)?),
        Value::Float(value) => GraphResultValue::F32(value.to_string()),
        Value::Date(value) => GraphResultValue::Date(value.to_string()),
        Value::Interval(value) => GraphResultValue::Interval(value.to_string()),
        Value::Timestamp(value) => GraphResultValue::Timestamp(value.to_string()),
        Value::TimestampTz(value) => GraphResultValue::TimestampTz(value.to_string()),
        Value::TimestampNs(value) => GraphResultValue::TimestampNs(value.to_string()),
        Value::TimestampMs(value) => GraphResultValue::TimestampMs(value.to_string()),
        Value::TimestampSec(value) => GraphResultValue::TimestampSec(value.to_string()),
        Value::InternalID(value) => GraphResultValue::InternalId(graph_internal_id(&value)),
        Value::String(value) => GraphResultValue::String(value),
        Value::Json(value) => GraphResultValue::Json(value.to_string()),
        Value::Blob(value) => GraphResultValue::Bytes(value),
        Value::List(element_type, values) => GraphResultValue::List {
            element_type: graph_logical_type(element_type)?,
            values: graph_result_values(values, depth + 1)?,
        },
        Value::Array(element_type, values) => GraphResultValue::Array {
            element_type: graph_logical_type(element_type)?,
            values: graph_result_values(values, depth + 1)?,
        },
        Value::Struct(fields) => GraphResultValue::Struct(
            fields
                .into_iter()
                .map(|(name, value)| Ok((name, graph_result_value_at(value, depth + 1)?)))
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Node(node) => GraphResultValue::Node(graph_node(&node, depth + 1)?),
        Value::Rel(rel) => GraphResultValue::Rel(graph_rel(&rel, depth + 1)?),
        Value::RecursiveRel { nodes, rels } => GraphResultValue::RecursiveRel {
            nodes: nodes
                .iter()
                .map(|node| graph_node(node, depth + 1))
                .collect::<Result<Vec<_>>>()?,
            rels: rels
                .iter()
                .map(|rel| graph_rel(rel, depth + 1))
                .collect::<Result<Vec<_>>>()?,
        },
        Value::Map((key_type, value_type), values) => GraphResultValue::Map {
            key_type: graph_logical_type(key_type)?,
            value_type: graph_logical_type(value_type)?,
            entries: values
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        graph_result_value_at(key, depth + 1)?,
                        graph_result_value_at(value, depth + 1)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        },
        Value::Union { types, value } => GraphResultValue::Union {
            variants: types
                .into_iter()
                .map(|(name, logical_type)| Ok((name, graph_logical_type(logical_type)?)))
                .collect::<Result<Vec<_>>>()?,
            value: Box::new(graph_result_value_at(*value, depth + 1)?),
        },
        Value::UUID(value) => GraphResultValue::Uuid(value.to_string()),
        Value::Decimal(value) => GraphResultValue::Decimal(value.to_string()),
    })
}

fn graph_result_values(values: Vec<Value>, depth: usize) -> Result<Vec<GraphResultValue>> {
    values
        .into_iter()
        .map(|value| graph_result_value_at(value, depth))
        .collect()
}

fn graph_internal_id(value: &lbug::InternalID) -> GraphInternalId {
    GraphInternalId {
        offset: value.offset,
        table_id: value.table_id,
    }
}

fn graph_node(value: &lbug::NodeVal, depth: usize) -> Result<GraphNode> {
    Ok(GraphNode {
        id: graph_internal_id(value.get_node_id()),
        label: value.get_label_name().clone(),
        properties: value
            .get_properties()
            .iter()
            .map(|(name, value)| Ok((name.clone(), graph_result_value_at(value.clone(), depth)?)))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn graph_rel(value: &lbug::RelVal, depth: usize) -> Result<GraphRel> {
    Ok(GraphRel {
        src: graph_internal_id(value.get_src_node()),
        dst: graph_internal_id(value.get_dst_node()),
        label: value.get_label_name().clone(),
        properties: value
            .get_properties()
            .iter()
            .map(|(name, value)| Ok((name.clone(), graph_result_value_at(value.clone(), depth)?)))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn apply_command(
    connection: &Connection<'_>,
    command: &GraphCommandV1,
) -> Result<GraphCommandResultV1> {
    match &command.operation {
        GraphOperationV1::PutDocument { id, value } => {
            let created = document(connection, id)?.is_none();
            if created {
                create_document(connection, id, value)?;
            } else {
                update_document(connection, id, value)?;
            }
            Ok(GraphCommandResultV1::PutDocument { created })
        }
        GraphOperationV1::DeleteDocument { id } => {
            let existed = document(connection, id)?.is_some();
            if existed {
                execute(
                    connection,
                    "MATCH (d:RhizaDocument) WHERE d.id = $id DELETE d",
                    vec![("id", Value::String(id.clone()))],
                )?;
            }
            Ok(GraphCommandResultV1::DeleteDocument { existed })
        }
    }
}

fn create_document(connection: &Connection<'_>, id: &str, value: &GraphValueV1) -> Result<()> {
    execute(
        connection,
        "CREATE (d:RhizaDocument {id: $id, kind: $kind, bool_value: $bool_value, i64_value: $i64_value, u64_value: $u64_value, f64_value: $f64_value, string_value: $string_value, bytes_value: $bytes_value})",
        document_parameters(id, value),
    )?;
    Ok(())
}

fn update_document(connection: &Connection<'_>, id: &str, value: &GraphValueV1) -> Result<()> {
    execute(
        connection,
        "MATCH (d:RhizaDocument) WHERE d.id = $id SET d.kind = $kind, d.bool_value = $bool_value, d.i64_value = $i64_value, d.u64_value = $u64_value, d.f64_value = $f64_value, d.string_value = $string_value, d.bytes_value = $bytes_value",
        document_parameters(id, value),
    )?;
    Ok(())
}

fn document_parameters(id: &str, value: &GraphValueV1) -> Vec<(&'static str, Value)> {
    let mut parameters = vec![
        ("id", Value::String(id.into())),
        ("kind", Value::UInt8(value_tag(value))),
        ("bool_value", Value::Null(LogicalType::Bool)),
        ("i64_value", Value::Null(LogicalType::Int64)),
        ("u64_value", Value::Null(LogicalType::UInt64)),
        ("f64_value", Value::Null(LogicalType::Double)),
        ("string_value", Value::Null(LogicalType::String)),
        ("bytes_value", Value::Null(LogicalType::Blob)),
    ];
    match value {
        GraphValueV1::Null => {}
        GraphValueV1::Bool(value) => parameters[2].1 = Value::Bool(*value),
        GraphValueV1::I64(value) => parameters[3].1 = Value::Int64(*value),
        GraphValueV1::U64(value) => parameters[4].1 = Value::UInt64(*value),
        GraphValueV1::F64(value) => parameters[5].1 = Value::Double(value.get()),
        GraphValueV1::String(value) => parameters[6].1 = Value::String(value.clone()),
        GraphValueV1::Bytes(value) => parameters[7].1 = Value::Blob(value.clone()),
    }
    parameters
}

fn value_tag(value: &GraphValueV1) -> u8 {
    match value {
        GraphValueV1::Null => 0,
        GraphValueV1::Bool(_) => 1,
        GraphValueV1::I64(_) => 2,
        GraphValueV1::U64(_) => 3,
        GraphValueV1::F64(_) => 4,
        GraphValueV1::String(_) => 5,
        GraphValueV1::Bytes(_) => 6,
    }
}

fn document(connection: &Connection<'_>, id: &str) -> Result<Option<GraphValueV1>> {
    let rows = execute(
        connection,
        "MATCH (d:RhizaDocument) WHERE d.id = $id RETURN d.kind, d.bool_value, d.i64_value, d.u64_value, d.f64_value, d.string_value, d.bytes_value",
        vec![("id", Value::String(id.into()))],
    )?;
    let Some(row) = one_or_none(rows, "document lookup")? else {
        return Ok(None);
    };
    Ok(Some(decode_document(&row)?))
}

fn decode_document(row: &[Value]) -> Result<GraphValueV1> {
    if row.len() != 7 {
        return Err(Error::Ladybug(
            "document lookup returned wrong shape".into(),
        ));
    }
    let tag = match &row[0] {
        Value::UInt8(value) => *value,
        value => return Err(unexpected_value("document kind", value)),
    };
    let value = match tag {
        0 => GraphValueV1::Null,
        1 => GraphValueV1::Bool(expect_bool(&row[1], "bool_value")?),
        2 => GraphValueV1::I64(expect_i64(&row[2], "i64_value")?),
        3 => GraphValueV1::U64(expect_u64(&row[3], "u64_value")?),
        4 => GraphValueV1::from_f64(expect_f64(&row[4], "f64_value")?)?,
        5 => GraphValueV1::String(expect_string(&row[5], "string_value")?),
        6 => GraphValueV1::Bytes(expect_blob(&row[6], "bytes_value")?),
        value => {
            return Err(Error::Ladybug(format!(
                "unknown stored document kind {value}"
            )))
        }
    };
    Ok(value)
}

fn execute(
    connection: &Connection<'_>,
    query: &str,
    parameters: Vec<(&str, Value)>,
) -> Result<Vec<Vec<Value>>> {
    if parameters.is_empty() {
        return connection
            .query(query)
            .map(|result| result.collect())
            .map_err(ladybug_error);
    }
    let mut statement = connection.prepare(query).map_err(ladybug_error)?;
    connection
        .execute(&mut statement, parameters)
        .map(|result| result.collect())
        .map_err(ladybug_error)
}

fn one_or_none(mut rows: Vec<Vec<Value>>, context: &str) -> Result<Option<Vec<Value>>> {
    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows.pop()),
        _ => Err(Error::Ladybug(format!(
            "{context} returned more than one row"
        ))),
    }
}

fn expect_bool(value: &Value, field: &str) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        value => Err(unexpected_value(field, value)),
    }
}

fn expect_i64(value: &Value, field: &str) -> Result<i64> {
    match value {
        Value::Int64(value) => Ok(*value),
        value => Err(unexpected_value(field, value)),
    }
}

fn expect_u64(value: &Value, field: &str) -> Result<u64> {
    match value {
        Value::UInt64(value) => Ok(*value),
        value => Err(unexpected_value(field, value)),
    }
}

fn expect_f64(value: &Value, field: &str) -> Result<f64> {
    match value {
        Value::Double(value) => Ok(*value),
        value => Err(unexpected_value(field, value)),
    }
}

fn expect_string(value: &Value, field: &str) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        value => Err(unexpected_value(field, value)),
    }
}

fn expect_blob(value: &Value, field: &str) -> Result<Vec<u8>> {
    match value {
        Value::Blob(value) => Ok(value.clone()),
        value => Err(unexpected_value(field, value)),
    }
}

fn unexpected_value(field: &str, value: &Value) -> Error {
    Error::Ladybug(format!("unexpected value for {field}: {value:?}"))
}

fn validate_nonempty_bytes(field: &str, value: &str, maximum: usize) -> Result<()> {
    if value.is_empty() || value.len() > maximum {
        Err(Error::InvalidCommand(format!(
            "{field} must contain 1..={maximum} bytes"
        )))
    } else {
        Ok(())
    }
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .expect("validated graph values fit in u32")
            .to_be_bytes(),
    );
    output.extend_from_slice(value);
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::Codec("length overflow".into()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::Codec("truncated graph command".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| Error::Codec("invalid fixed-width value".into()))
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn bytes(&mut self, maximum: usize) -> Result<&'a [u8]> {
        let length = u32::from_be_bytes(self.array()?) as usize;
        if length > maximum {
            return Err(Error::Codec(format!(
                "length {length} exceeds maximum {maximum}"
            )));
        }
        self.take(length)
    }

    fn string(&mut self, maximum: usize) -> Result<String> {
        String::from_utf8(self.bytes(maximum)?.to_vec())
            .map_err(|_| Error::Codec("graph strings must be UTF-8".into()))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    Ok(())
}

fn ladybug_sidecars(path: &Path) -> [PathBuf; 4] {
    [".wal", ".wal.checkpoint", ".shadow", ".tmp"].map(|suffix| ladybug_sidecar(path, suffix))
}

fn ladybug_sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn remove_sidecars(path: &Path) {
    for sidecar in ladybug_sidecars(path) {
        let _ = fs::remove_file(sidecar);
    }
}

fn ladybug_error(error: lbug::Error) -> Error {
    Error::Ladybug(error.to_string())
}

fn ladybug_prepare_error(error: lbug::Error) -> Error {
    match &error {
        lbug::Error::FailedPreparedStatement(_) => Error::InvalidCommand(error.to_string()),
        _ => Error::Ladybug(error.to_string()),
    }
}

fn ladybug_execution_error(error: lbug::Error) -> Error {
    match error {
        lbug::Error::FailedQuery(message)
            if message.starts_with(LADYBUG_CONVERSION_ERROR_PREFIX) =>
        {
            Error::InvalidCommand(format!("Query execution failed: {message}"))
        }
        lbug::Error::FailedQuery(message) if message == LADYBUG_BUFFER_POOL_EXHAUSTED => {
            Error::ResourceExhausted(message)
        }
        lbug::Error::FailedQuery(message) if is_ladybug_interruption(&message) => {
            Error::ResourceExhausted(format!(
                "graph query timed out or was interrupted: {message}"
            ))
        }
        error => Error::Ladybug(error.to_string()),
    }
}

fn is_ladybug_interruption(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("interrupt") || message.contains("timed out") || message.contains("timeout")
}

fn io_error(error: std::io::Error) -> Error {
    Error::Io(error.to_string())
}

fn invalid_snapshot_error(error: impl std::fmt::Display) -> Error {
    Error::InvalidSnapshot(error.to_string())
}

fn invalid_snapshot_ladybug_error(error: lbug::Error) -> Error {
    invalid_snapshot_error(error)
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn snapshot_fixture() -> (tempfile::TempDir, LadybugSnapshot) {
        let dir = tempfile::tempdir().unwrap();
        let source =
            LadybugStateMachine::open(dir.path().join("source.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        let snapshot = source.create_snapshot(0).unwrap();
        (dir, snapshot)
    }

    #[test]
    fn snapshot_codec_round_trips_one_canonical_envelope() {
        let (_dir, snapshot) = snapshot_fixture();

        let encoded = encode_snapshot(&snapshot).unwrap();
        let decoded = decode_snapshot(&encoded).unwrap();

        assert_eq!(decoded, snapshot);
        assert_eq!(encode_snapshot(&decoded).unwrap(), encoded);
    }

    #[test]
    fn snapshot_codec_rejects_unknown_version_and_tamper() {
        let (_dir, snapshot) = snapshot_fixture();
        let encoded = encode_snapshot(&snapshot).unwrap();

        let mut unknown_version = encoded.clone();
        unknown_version[4..6].copy_from_slice(&3_u16.to_be_bytes());
        assert!(matches!(
            decode_snapshot(&unknown_version),
            Err(Error::InvalidSnapshot(message)) if message.contains("version")
        ));

        let mut tampered = encoded;
        *tampered.last_mut().unwrap() ^= 0xff;
        assert!(matches!(
            decode_snapshot(&tampered),
            Err(Error::InvalidSnapshot(_))
        ));
    }

    #[test]
    fn restore_rejects_tampered_bytes_and_identity() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.db_bytes[0] ^= 0xff;
        let target = dir.path().join("bytes.lbug");
        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());

        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.cluster_id.push_str("-other");
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("identity.lbug");
        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn restore_rejects_a_tampered_materializer_fingerprint() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.materializer_fingerprint = LogHash::ZERO;
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("fingerprint.lbug");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn snapshot_decoder_rejects_lengths_before_allocating_fields() {
        let (_dir, snapshot) = snapshot_fixture();
        let encoded = encode_snapshot(&snapshot).unwrap();

        let mut oversized_identity = encoded.clone();
        oversized_identity[6..14].copy_from_slice(&((MAX_RHGS_ID_BYTES + 1) as u64).to_be_bytes());
        assert!(matches!(
            decode_snapshot(&oversized_identity),
            Err(Error::ResourceExhausted(_))
        ));

        let mut decoder = SnapshotDecoder::new(&encoded);
        decoder.take(SNAPSHOT_WIRE_MAGIC.len()).unwrap();
        decoder.u16().unwrap();
        decoder.bytes(MAX_RHGS_ID_BYTES, "cluster id").unwrap();
        decoder.bytes(MAX_RHGS_ID_BYTES, "source node id").unwrap();
        decoder.u64().unwrap();
        decoder.u64().unwrap();
        decoder.u64().unwrap();
        decoder.take(32).unwrap();
        decoder.u64().unwrap();
        decoder.take(32).unwrap();
        decoder.take(32).unwrap();
        let db_length_offset = decoder.offset;
        let mut oversized_db = encoded.clone();
        oversized_db[db_length_offset..db_length_offset + 8]
            .copy_from_slice(&((MAX_RHGS_DB_BYTES as u64) + 1).to_be_bytes());
        assert!(matches!(
            decode_snapshot(&oversized_db),
            Err(Error::ResourceExhausted(_))
        ));

        let db_length = u64::from_be_bytes(
            encoded[db_length_offset..db_length_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let control_length_offset = db_length_offset + 8 + db_length;
        let mut oversized_control = encoded.clone();
        oversized_control[control_length_offset..control_length_offset + 8]
            .copy_from_slice(&((MAX_RHGS_CONTROL_BYTES as u64) + 1).to_be_bytes());
        assert!(matches!(
            decode_snapshot(&oversized_control),
            Err(Error::ResourceExhausted(_))
        ));

        let mut truncated_db = encoded;
        let declared = u64::from_be_bytes(
            truncated_db[db_length_offset..db_length_offset + 8]
                .try_into()
                .unwrap(),
        );
        truncated_db[db_length_offset..db_length_offset + 8]
            .copy_from_slice(&(declared + 1).to_be_bytes());
        assert!(matches!(
            decode_snapshot(&truncated_db),
            Err(Error::InvalidSnapshot(message)) if message.contains("truncated")
        ));

        assert!(matches!(
            ensure_rhgs_total_bound(MAX_RHGS_V2_BYTES + 1),
            Err(Error::ResourceExhausted(_))
        ));
    }

    #[test]
    fn restore_rejects_outer_source_node_that_differs_from_replicated_control() {
        let (dir, mut snapshot) = snapshot_fixture();
        snapshot.created_by = "forged-source".into();
        snapshot.digest = snapshot.recompute_digest();
        let target = dir.path().join("source-mismatch.lbug");

        assert!(matches!(
            restore_snapshot_file(&target, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(message)) if message.contains("source node")
        ));
        assert!(!target.exists());
        assert!(!control_sidecar_path(&target).exists());
        assert!(!restore_intent_path(&target).exists());
    }

    fn recreate_interrupted_restore(
        target: &Path,
        snapshot: &LadybugSnapshot,
        keep_db: bool,
        keep_control: bool,
    ) {
        let db_stage = restore_staging_db_path(target);
        let control = control_sidecar_path(target);
        let control_stage = restore_staging_control_path(target);
        fs::copy(target, &db_stage).unwrap();
        fs::copy(&control, &control_stage).unwrap();
        let intent = RestoreIntent {
            phase: RestorePhase::Staged,
            db_digest: lgfx::file_digest(target).unwrap(),
            control_digest: lgfx::file_digest(&control).unwrap(),
            snapshot_digest: snapshot.digest,
            target_node_digest: LogHash::digest(&[b"node-2"]),
        };
        write_restore_intent(&restore_intent_path(target), &intent).unwrap();
        if !keep_db {
            fs::remove_file(target).unwrap();
        }
        if !keep_control {
            fs::remove_file(control).unwrap();
        }
    }

    fn write_preparing_intent(target: &Path, snapshot: &LadybugSnapshot) {
        write_restore_intent(
            &restore_intent_path(target),
            &RestoreIntent {
                phase: RestorePhase::Preparing,
                db_digest: LogHash::digest(&[snapshot.db_bytes()]),
                control_digest: LogHash::ZERO,
                snapshot_digest: snapshot.digest(),
                target_node_digest: LogHash::digest(&[b"node-2"]),
            },
        )
        .unwrap();
    }

    #[test]
    fn preparing_restore_states_fail_closed_on_startup_and_same_retry_rebuilds_staging() {
        for phase in ["intent-only", "database-only", "staging-pair"] {
            let (dir, snapshot) = snapshot_fixture();
            let target = dir.path().join(format!("{phase}.lbug"));

            if phase == "staging-pair" {
                restore_snapshot_file(&target, &snapshot, "node-2").unwrap();
                fs::rename(&target, restore_staging_db_path(&target)).unwrap();
                fs::rename(
                    control_sidecar_path(&target),
                    restore_staging_control_path(&target),
                )
                .unwrap();
            }
            write_preparing_intent(&target, &snapshot);
            if phase == "database-only" {
                fs::write(restore_staging_db_path(&target), snapshot.db_bytes()).unwrap();
            }

            assert!(matches!(
                LadybugStateMachine::open(&target, "cluster-1", "node-2", 7, 3),
                Err(Error::InvalidSnapshot(message)) if message.contains("preparation is incomplete")
            ));
            assert!(!target.exists());
            assert!(!control_sidecar_path(&target).exists());

            restore_snapshot_file(&target, &snapshot, "node-2").unwrap();
            LadybugStateMachine::open(&target, "cluster-1", "node-2", 7, 3).unwrap();
            assert!(!restore_intent_path(&target).exists());
        }
    }

    #[test]
    fn orphan_restore_staging_without_intent_never_becomes_a_fresh_database() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("orphan.lbug");
        fs::write(restore_staging_db_path(&target), b"orphan").unwrap();

        assert!(matches!(
            LadybugStateMachine::open(&target, "cluster-1", "node-2", 7, 3),
            Err(Error::InvalidSnapshot(message)) if message.contains("orphan")
        ));
        assert!(!target.exists());
        assert!(!control_sidecar_path(&target).exists());
    }

    #[test]
    fn startup_recovers_every_durable_restore_publish_crash_state() {
        for (keep_db, keep_control) in [(false, false), (false, true), (true, false), (true, true)]
        {
            let (dir, snapshot) = snapshot_fixture();
            let target = dir.path().join("restored.lbug");
            restore_snapshot_file(&target, &snapshot, "node-2").unwrap();
            recreate_interrupted_restore(&target, &snapshot, keep_db, keep_control);

            let restored = LadybugStateMachine::open(&target, "cluster-1", "node-2", 7, 3).unwrap();
            assert_eq!(restored.materialized_tip().unwrap().index(), 0);
            assert!(!restore_intent_path(&target).exists());
            assert!(!restore_staging_db_path(&target).exists());
            assert!(!restore_staging_control_path(&target).exists());
        }
    }

    #[test]
    fn retrying_the_same_interrupted_restore_completes_without_clobbering() {
        let (dir, snapshot) = snapshot_fixture();
        let target = dir.path().join("restored.lbug");
        restore_snapshot_file(&target, &snapshot, "node-2").unwrap();
        recreate_interrupted_restore(&target, &snapshot, false, true);

        restore_snapshot_file(&target, &snapshot, "node-2").unwrap();
        LadybugStateMachine::open(&target, "cluster-1", "node-2", 7, 3).unwrap();
    }

    #[test]
    fn fresh_create_reservation_has_one_owner_and_loser_does_not_cleanup_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = Arc::new(dir.path().join("race.lbug"));
        let barrier = Arc::new(Barrier::new(2));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                LadybugStateMachine::create_new(
                    &path,
                    "cluster-1",
                    "node-1",
                    7,
                    ConfigurationState::active(3, LogHash::ZERO),
                    1,
                )
                .is_ok()
            }));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|success| *success)
            .count();
        assert_eq!(successes, 1);
        LadybugStateMachine::open(&*path, "cluster-1", "node-1", 7, 3).unwrap();
    }

    #[test]
    fn fresh_create_failure_preserves_a_control_file_it_did_not_create() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("owned-cleanup.lbug");
        let control_path = control_sidecar_path(&path);
        ControlStore::create(
            &control_path,
            &ControlIdentity::new(
                "other-cluster",
                "other-node",
                9,
                ConfigurationState::active(1, LogHash::ZERO),
                1,
                graph_materializer_fingerprint(),
                LogHash::ZERO,
            ),
        )
        .unwrap();
        let before = fs::read(&control_path).unwrap();

        assert!(LadybugStateMachine::create_new(
            &path,
            "cluster-1",
            "node-1",
            7,
            ConfigurationState::active(3, LogHash::ZERO),
            1,
        )
        .is_err());
        assert_eq!(fs::read(&control_path).unwrap(), before);
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn restore_and_open_reject_symlink_targets_and_sidecars_without_following_them() {
        use std::os::unix::fs::symlink;

        let (dir, snapshot) = snapshot_fixture();
        let referent = dir.path().join("referent");
        fs::write(&referent, b"do-not-touch").unwrap();
        let target = dir.path().join("symlink.lbug");
        symlink(&referent, &target).unwrap();
        assert!(restore_snapshot_file(&target, &snapshot, "node-2").is_err());
        assert_eq!(fs::read(&referent).unwrap(), b"do-not-touch");

        fs::remove_file(&target).unwrap();
        let wal = ladybug_sidecar(&target, ".wal");
        symlink(&referent, &wal).unwrap();
        assert!(restore_snapshot_file(&target, &snapshot, "node-2").is_err());
        assert_eq!(fs::read(&referent).unwrap(), b"do-not-touch");
    }

    #[test]
    fn lgfx_target_digest_fast_path_still_requires_target_file_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.lbug");
        let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        let command =
            GraphCommandV1::put_document("request-1", "doc", GraphValueV1::U64(1)).unwrap();
        let request = encode_replicated_graph_command(&command).unwrap();
        let mut effect = LadybugFileEffectV1::decode(
            &state
                .prepare_graph_effect(&request, 0, LogHash::ZERO)
                .unwrap(),
        )
        .unwrap();
        state.close_database_cleanly().unwrap();
        let installed = dir.path().join("installed.lbug");
        apply_lgfx_to_exact_base(&path, &installed, &effect).unwrap();
        fs::rename(installed, &path).unwrap();
        state.reopen_database().unwrap();
        effect.target_file_bytes += LGFX_CHUNK_BYTES as u64;

        assert!(matches!(
            state.install_lgfx_effect(&effect, false),
            Err(Error::InvalidEntry(message)) if message.contains("target size")
        ));
    }

    #[cfg(feature = "vfs")]
    #[test]
    fn steady_state_lgfx_diff_reads_only_tracked_candidate_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.lbug");
        let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        let first = GraphCommandV1::put_document("request-1", "doc", GraphValueV1::U64(1)).unwrap();
        let first_request = encode_replicated_graph_command(&first).unwrap();
        let first_effect = state
            .prepare_graph_effect(&first_request, 0, LogHash::ZERO)
            .unwrap();
        let first_hash = LogEntry::calculate_hash(
            "cluster-1",
            1,
            7,
            3,
            EntryType::Command,
            LogHash::ZERO,
            &first_effect,
        );
        state
            .apply_entry(&LogEntry {
                cluster_id: "cluster-1".into(),
                epoch: 7,
                config_id: 3,
                index: 1,
                entry_type: EntryType::Command,
                payload: first_effect,
                prev_hash: LogHash::ZERO,
                hash: first_hash,
            })
            .unwrap();

        let update =
            GraphCommandV1::put_document("request-2", "doc", GraphValueV1::U64(2)).unwrap();
        let update_request = encode_replicated_graph_command(&update).unwrap();
        lgfx::reset_diff_scan_metrics();
        let encoded_effect = state
            .prepare_graph_effect(&update_request, 1, first_hash)
            .unwrap();
        let effect = LadybugFileEffectV1::decode(&encoded_effect).unwrap();
        let metrics = lgfx::diff_scan_metrics();

        assert_eq!(metrics.full_target_bytes_read, 0);
        assert_eq!(metrics.full_base_bytes_read, 0);
        assert!(metrics.sparse_candidate_chunks > 0);
        assert_eq!(
            metrics.sparse_target_bytes_read,
            metrics.sparse_candidate_chunks * LGFX_CHUNK_BYTES as u64
        );
        assert!(metrics.sparse_base_bytes_read <= metrics.sparse_target_bytes_read);
        assert!(metrics.sparse_target_bytes_read < effect.target_file_bytes);
        assert!(
            metrics.sparse_base_bytes_read + metrics.sparse_target_bytes_read
                < effect.target_file_bytes * 2
        );
        assert!(effect.chunks.len() as u64 <= metrics.sparse_candidate_chunks);
    }

    #[cfg(feature = "vfs")]
    #[test]
    fn shrink_then_regrow_falls_back_and_applies_the_zeroed_suffix() {
        use lbug::{FileSystem, LockMode, OpenOptions};

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("base.lbug");
        let target = dir.path().join("target.lbug");
        let installed = dir.path().join("installed.lbug");
        let file_bytes = LGFX_CHUNK_BYTES * 3;
        fs::write(&base, vec![0x5a; file_bytes]).unwrap();
        fs::copy(&base, &target).unwrap();

        let (file_system, tracker) = TrackingFileSystem::new(&target);
        tracker.reset();
        let handle = file_system
            .open(
                target.to_str().unwrap(),
                OpenOptions {
                    read: true,
                    write: true,
                    create: false,
                    create_or_truncate: false,
                    temporary: false,
                    lock: LockMode::None,
                },
            )
            .unwrap();
        handle.truncate(LGFX_CHUNK_BYTES as u64).unwrap();
        handle.truncate(file_bytes as u64).unwrap();
        handle.sync().unwrap();
        drop(handle);

        let dirty = tracker.dirty_snapshot();
        let target_changed =
            lgfx::file_digest(&base).unwrap() != lgfx::file_digest(&target).unwrap();
        let chunks = if dirty.needs_full_diff(target_changed) {
            diff_closed_ladybug_files(&base, &target).unwrap()
        } else {
            lgfx::diff_closed_ladybug_file_chunks(&base, &target, &dirty.chunk_indices).unwrap()
        };
        let effect = LadybugFileEffectV1 {
            cluster_id: "cluster-1".into(),
            epoch: 7,
            configuration_id: 3,
            recovery_generation: 1,
            base_log_index: 0,
            base_log_hash: LogHash::ZERO,
            base_db_digest: lgfx::file_digest(&base).unwrap(),
            base_file_bytes: file_bytes as u64,
            target_db_digest: lgfx::file_digest(&target).unwrap(),
            target_file_bytes: file_bytes as u64,
            storage_version: lbug::get_storage_version(),
            materializer_fingerprint: graph_materializer_fingerprint(),
            request_id: "shrink-regrow".into(),
            request_digest: LogHash::digest(&[b"shrink-regrow"]),
            result_encoding_version: 1,
            bounded_result: b"result".to_vec(),
            chunks_digest: lgfx_chunks_digest(&chunks),
            chunks,
        };

        apply_lgfx_to_exact_base(&base, &installed, &effect).unwrap();
        assert_eq!(fs::read(&installed).unwrap(), fs::read(&target).unwrap());
        assert!(fs::read(&installed).unwrap()[LGFX_CHUNK_BYTES..]
            .iter()
            .all(|byte| *byte == 0));
    }
}

#[cfg(test)]
mod query_tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn document_read_returns_the_tip_when_the_document_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        assert_eq!(
            state.get_document_with_tip("missing").unwrap(),
            (None, 0, LogHash::ZERO)
        );
    }

    #[test]
    fn execute_returns_rows_with_and_without_parameters() {
        let database = Database::in_memory(SystemConfig::default()).unwrap();
        let connection = Connection::new(&database).unwrap();

        assert_eq!(
            execute(&connection, "RETURN 1", vec![]).unwrap(),
            vec![vec![Value::Int64(1)]]
        );
        assert_eq!(
            execute(
                &connection,
                "RETURN $value",
                vec![("value", Value::String("rhiza".into()))],
            )
            .unwrap(),
            vec![vec![Value::String("rhiza".into())]]
        );
    }

    #[test]
    fn database_lifecycle_lock_allows_concurrent_readers() {
        let dir = tempfile::tempdir().unwrap();
        let state = std::sync::Arc::new(
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap(),
        );
        let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();

        let entered = std::thread::scope(|scope| {
            for _ in 0..2 {
                let state = std::sync::Arc::clone(&state);
                let release = std::sync::Arc::clone(&release);
                let entered_tx = entered_tx.clone();
                scope.spawn(move || {
                    let _guard = state.read_database().unwrap();
                    entered_tx.send(()).unwrap();
                    while !release.load(std::sync::atomic::Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                });
            }
            drop(entered_tx);
            let first = entered_rx
                .recv_timeout(std::time::Duration::from_secs(3))
                .is_ok();
            let second = entered_rx
                .recv_timeout(std::time::Duration::from_secs(3))
                .is_ok();
            release.store(true, std::sync::atomic::Ordering::Release);
            first && second
        });

        assert!(
            entered,
            "both readers must hold the lifecycle lock together"
        );
    }

    #[test]
    fn physical_lgfx_install_waits_for_a_lifecycle_reader_before_replacing_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = std::sync::Arc::new(
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap(),
        );
        let command = GraphCommandV1::put_document(
            "request-1",
            "document-1",
            GraphValueV1::String("value".into()),
        )
        .unwrap();
        let request = encode_replicated_graph_command(&command).unwrap();
        let payload = state
            .prepare_graph_effect(&request, 0, LogHash::ZERO)
            .unwrap();
        let hash = LogEntry::calculate_hash(
            "cluster-1",
            1,
            7,
            3,
            EntryType::Command,
            LogHash::ZERO,
            &payload,
        );
        let entry = LogEntry {
            cluster_id: "cluster-1".into(),
            epoch: 7,
            config_id: 3,
            index: 1,
            entry_type: EntryType::Command,
            payload,
            prev_hash: LogHash::ZERO,
            hash,
        };
        let lifecycle_reader = state.read_database().unwrap();
        let (applied_tx, applied_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let state = std::sync::Arc::clone(&state);
            scope.spawn(move || applied_tx.send(state.apply_entry(&entry)).unwrap());
            assert!(applied_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err());
            drop(lifecycle_reader);
            applied_rx
                .recv_timeout(std::time::Duration::from_secs(3))
                .expect("LGFX install must continue after the lifecycle reader exits")
                .unwrap();
        });

        assert_eq!(
            state.get_document("document-1").unwrap(),
            Some(GraphValueV1::String("value".into()))
        );
    }

    #[test]
    fn direct_query_converts_nodes_and_relationships_without_display_coercion() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        let rows = {
            let guard = state.read_database().unwrap();
            let database = guard.as_ref().unwrap();
            let connection = Connection::new(database).unwrap();
            transaction(&connection, || {
                execute(
                    &connection,
                    "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))",
                    vec![],
                )?;
                execute(
                    &connection,
                    "CREATE REL TABLE Knows(FROM Person TO Person, since INT64)",
                    vec![],
                )?;
                execute(
                    &connection,
                    "CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})",
                    vec![],
                )?;
                execute(
                    &connection,
                    "MATCH (a:Person), (b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' CREATE (a)-[:Knows {since: 2020}]->(b)",
                    vec![],
                )?;
                Ok(())
            })
            .unwrap();
            execute(
                &connection,
                "MATCH (a:Person)-[r:Knows]->(b:Person) RETURN a, r, b",
                vec![],
            )
            .unwrap()
        };

        assert_eq!(rows.len(), 1);
        let row = rows
            .into_iter()
            .next()
            .unwrap()
            .into_iter()
            .map(graph_result_value)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(matches!(&row[0], GraphResultValue::Node(node) if node.label == "Person"));
        assert!(matches!(&row[1], GraphResultValue::Rel(rel) if rel.label == "Knows"));
        assert!(matches!(&row[2], GraphResultValue::Node(node) if node.label == "Person"));
        assert_eq!(
            vec![
                graph_logical_type(LogicalType::Node).unwrap(),
                graph_logical_type(LogicalType::Rel).unwrap(),
                graph_logical_type(LogicalType::Node).unwrap(),
            ],
            vec![
                GraphLogicalType::Node,
                GraphLogicalType::Rel,
                GraphLogicalType::Node,
            ]
        );
    }

    #[test]
    fn read_only_query_supports_general_cypher_and_collection_parameters() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        {
            let guard = state.read_database().unwrap();
            let database = guard.as_ref().unwrap();
            let connection = Connection::new(database).unwrap();
            transaction(&connection, || {
                execute(
                    &connection,
                    "CREATE (:RhizaDocument {id: 'document-1', kind: 6, string_value: 'alpha'}), (:RhizaDocument {id: 'document-2', kind: 6, string_value: 'beta'}), (:RhizaDocument {id: 'document-3', kind: 6, string_value: 'gamma'})",
                    vec![],
                )?;
                Ok(())
            })
            .unwrap();
        }
        let parameters = BTreeMap::from([(
            "ids".into(),
            GraphParameterValue::List(vec![
                GraphParameterValue::String("document-1".into()),
                GraphParameterValue::String("document-2".into()),
                GraphParameterValue::String("document-3".into()),
            ]),
        )]);

        let result = state
            .query_read_only(
                "MATCH (v:RhizaDocument) WHERE v.id IN $ids RETURN v.id AS id, upper(v.string_value) AS value ORDER BY v.id",
                &parameters,
                10,
                16 * 1024,
                1_000,
            )
            .unwrap();

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "value"]
        );
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].len(), 2);
        assert!(matches!(&result.rows[0][1], GraphResultValue::String(value) if value == "ALPHA"));
    }

    #[test]
    fn read_only_query_requires_bounded_limits_in_every_union_branch() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        assert!(matches!(
            state.query_read_only(
                "RETURN 1 AS value UNION RETURN 2 AS value",
                &BTreeMap::new(),
                2,
                4096,
                1_000,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("explicit bounded LIMIT")
        ));

        assert!(matches!(
            state.query_read_only(
                "RETURN 1 AS return UNION RETURN 2 AS value LIMIT 1",
                &BTreeMap::new(),
                2,
                4096,
                1_000,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("explicit bounded LIMIT")
        ));

        assert!(matches!(
            state.query_read_only(
                "RETURN 1 AS value UNION WITH 2 AS value RETURN value LIMIT 1",
                &BTreeMap::new(),
                2,
                4096,
                1_000,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("explicit bounded LIMIT")
        ));

        let keyword_alias =
            admit_read_only_query("RETURN 1 AS union LIMIT 1", &BTreeMap::new(), 1, 4096).unwrap();
        assert_eq!(keyword_alias.statement, "RETURN 1 AS union LIMIT 1");

        let result = state
            .query_read_only(
                "RETURN 1 AS value LIMIT 1 UNION RETURN 2 AS value LIMIT 1",
                &BTreeMap::new(),
                2,
                4096,
                1_000,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        let result = state
            .query_read_only(
                "RETURN 1 AS value LIMIT 1 UNION WITH 2 AS value RETURN value LIMIT 1",
                &BTreeMap::new(),
                2,
                4096,
                1_000,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        assert!(matches!(
            state.query_read_only(
                "RETURN 1 AS value LIMIT 1 UNION RETURN 2 AS value LIMIT 2",
                &BTreeMap::new(),
                2,
                4096,
                1_000,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("LIMIT sum")
        ));
    }

    #[test]
    fn read_only_query_bounds_large_collection_results_by_bytes_not_element_count() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        let result = state
            .query_read_only(
                "RETURN range(1, 1025) AS values LIMIT 1",
                &BTreeMap::new(),
                1,
                64 * 1024,
                1_000,
            )
            .unwrap();

        assert!(matches!(
            &result.rows[0][0],
            GraphResultValue::List { values, .. } if values.len() == 1025
        ));
    }

    #[test]
    fn admission_rejects_unbounded_containers_before_ladybug_execution() {
        let huge = admit_read_only_query(
            "RETURN range(1, 70000) AS values LIMIT 1",
            &BTreeMap::new(),
            1,
            1024 * 1024,
        );
        assert!(matches!(
            huge,
            Err(Error::InvalidCommand(message)) if message.contains("statically expanded values")
        ));
        assert!(matches!(
            admit_read_only_query(
                "RETURN repeat('a', 2000000) AS value LIMIT 1",
                &BTreeMap::new(),
                1,
                1024 * 1024,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("statically expanded values")
        ));

        for query in [
            "UNWIND range(1, 1000000000) AS value RETURN value LIMIT 1",
            "RETURN repeat('a', 800000), repeat('b', 800000) LIMIT 1",
            "RETURN lpad('a', 100000000, 'x') AS value LIMIT 1",
            "RETURN rpad('a', 100000000, 'x') AS value LIMIT 1",
        ] {
            assert!(
                matches!(
                    admit_read_only_query(query, &BTreeMap::new(), 1, 1024 * 1024),
                    Err(Error::InvalidCommand(message))
                        if message.contains("statically expanded values")
                ),
                "allocation-amplifying query must fail admission: {query}"
            );
        }

        assert!(matches!(
            admit_read_only_query(
                "UNWIND range(1, 10000) AS x RETURN repeat('a', 4096) AS value LIMIT 10000",
                &BTreeMap::new(),
                10_000,
                1024 * 1024,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("statically expanded values")
        ));
        let repeated_literal = format!(
            "UNWIND range(1, 10000) AS x RETURN concat('{}') AS value LIMIT 10000",
            "x".repeat(1024)
        );
        assert!(matches!(
            admit_read_only_query(
                &repeated_literal,
                &BTreeMap::new(),
                10_000,
                1024 * 1024,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("statically expanded values")
        ));

        let repeated_parameter = BTreeMap::from([(
            "value".into(),
            GraphParameterValue::String("x".repeat(MAX_STRING_BYTES)),
        )]);
        assert!(matches!(
            admit_read_only_query(
                "RETURN [$value, $value, $value, $value] AS values LIMIT 1",
                &repeated_parameter,
                1,
                1024 * 1024,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("statically expanded values")
        ));
        assert!(matches!(
            admit_read_only_query(
                "RETURN replace($value, 'x', $value) AS value LIMIT 1",
                &repeated_parameter,
                1,
                1024 * 1024,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("expansion function")
        ));

        for query in [
            "MATCH (v:RhizaDocument) RETURN collect(v.id) LIMIT 1",
            "RETURN [value IN [1, 2] | value] AS values LIMIT 1",
            "MATCH p = (a:RhizaDocument)-[:Related]->(b:RhizaDocument) RETURN nodes(p) LIMIT 1",
            "RETURN string_split('a,b', ',') AS values LIMIT 1",
        ] {
            assert!(
                matches!(
                    admit_read_only_query(query, &BTreeMap::new(), 1, 1024 * 1024),
                    Err(Error::InvalidCommand(message))
                        if message.contains("statically bounded result cardinality")
                            || message.contains("list comprehensions")
                ),
                "container-producing query must fail admission: {query}"
            );
        }
    }

    #[test]
    fn admission_allows_statically_bounded_range_parameters() {
        let parameters = BTreeMap::from([
            ("start".into(), GraphParameterValue::I64(-10)),
            ("end".into(), GraphParameterValue::U64(10)),
            ("step".into(), GraphParameterValue::U64(2)),
        ]);
        let admitted = admit_read_only_query(
            "RETURN range($start, $end, $step) AS values LIMIT 1",
            &parameters,
            1,
            4096,
        )
        .unwrap();
        assert_eq!(
            admitted.statement,
            "RETURN range($start, $end, $step) AS values LIMIT 1"
        );

        assert!(matches!(
            admit_read_only_query(
                "RETURN range(1, 1 + 10) AS values LIMIT 1",
                &BTreeMap::new(),
                1,
                4096,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("statically bounded")
        ));
    }

    #[test]
    fn read_only_query_rejects_huge_nested_container_before_result_conversion() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        let error = state
            .query_read_only(
                "RETURN [range(1, 70000), range(1, 70000)] AS values LIMIT 1",
                &BTreeMap::new(),
                1,
                1024 * 1024,
                5_000,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidCommand(message) if message.contains("1048576 result bytes")
        ));
    }

    #[test]
    fn read_only_query_requires_static_labels_without_restricting_labeled_joins() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        for query in [
            "MATCH (n) RETURN n LIMIT 1",
            "MATCH (n) RETURN count(n) LIMIT 1",
            "MATCH (n:RhizaDocument)-->(m) RETURN m LIMIT 1",
        ] {
            assert!(matches!(
                state.query_read_only(query, &BTreeMap::new(), 1, 4096, 1_000),
                Err(Error::InvalidCommand(message))
                    if message.contains("explicit non-reserved label")
            ));
        }

        let labeled_join = state
            .query_read_only(
                "MATCH (a:RhizaDocument), (b:RhizaDocument) RETURN a.id, b.id LIMIT 1",
                &BTreeMap::new(),
                1,
                4096,
                1_000,
            )
            .unwrap();
        assert!(labeled_join.rows.is_empty());
    }

    #[test]
    fn read_only_query_preserves_typed_whole_node_results() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        {
            let guard = state.read_database().unwrap();
            let database = guard.as_ref().unwrap();
            let connection = Connection::new(database).unwrap();
            execute(
                &connection,
                "CREATE (:RhizaDocument {id: 'document-1', kind: 3, i64_value: 7})",
                vec![],
            )
            .unwrap();
        }

        let result = state
            .query_read_only(
                "MATCH (v:RhizaDocument) RETURN v",
                &BTreeMap::new(),
                1,
                16 * 1024,
                1_000,
            )
            .unwrap();

        assert!(
            matches!(&result.rows[0][0], GraphResultValue::Node(node) if node.label == "RhizaDocument")
        );
    }

    #[test]
    fn read_only_query_keeps_safety_gates_and_classifies_user_errors() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        for query in [
            "CREATE (:RhizaDocument {id: 'forbidden'})",
            "CALL show_tables() RETURN *",
            "LOAD httpfs",
            "MATCH (m:__RhizaMeta) RETURN m",
            "RETURN (",
        ] {
            assert!(
                matches!(
                    state.query_read_only(query, &BTreeMap::new(), 10, 4096, 1_000),
                    Err(Error::InvalidCommand(_))
                ),
                "user query must be rejected without becoming an internal Ladybug error: {query}"
            );
        }

        let result = state
            .query_read_only("RETURN 1 AS transaction", &BTreeMap::new(), 1, 4096, 1_000)
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn admission_rejects_load_from_in_every_reading_clause_position() {
        for query in [
            "LOAD FROM '/tmp/rhiza.csv' RETURN *",
            "LOAD WITH HEADERS (id STRING) FROM '/tmp/rhiza.csv' RETURN id",
            "MATCH (v:RhizaDocument) LOAD FROM '/tmp/rhiza.csv' RETURN v.id",
            "WITH 1 AS seed LOAD WITH HEADERS (id STRING) FROM '/tmp/rhiza.csv' RETURN seed",
        ] {
            assert!(
                matches!(
                    admit_read_only_query(query, &BTreeMap::new(), 10, 4096),
                    Err(Error::InvalidCommand(message)) if message.contains("external I/O")
                ),
                "LOAD FROM must be rejected by admission before Ladybug executes it: {query}"
            );
        }
    }

    #[test]
    fn read_only_query_allows_nonreserved_keywords_as_names() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        {
            let guard = state.read_database().unwrap();
            let database = guard.as_ref().unwrap();
            let connection = Connection::new(database).unwrap();
            execute(
                &connection,
                "CREATE NODE TABLE KeywordNode(id STRING, call INT64, limit INT64, load INT64, PRIMARY KEY(id))",
                vec![],
            )
            .unwrap();
            execute(
                &connection,
                "CREATE (:KeywordNode {id: 'one', call: 1, limit: 2, load: 3})",
                vec![],
            )
            .unwrap();
        }

        let result = state
            .query_read_only(
                "MATCH (call:KeywordNode) RETURN call.call AS call, call.limit AS limit, call.load AS load LIMIT 1",
                &BTreeMap::new(),
                1,
                4096,
                1_000,
            )
            .unwrap();

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["call", "limit", "load"]
        );
        assert_eq!(result.rows.len(), 1);

        let result = state
            .query_read_only(
                "MATCH (limit:KeywordNode) WITH limit AS load RETURN load.call AS call, load.limit AS limit, load.load AS load LIMIT 1",
                &BTreeMap::new(),
                1,
                4096,
                1_000,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn read_only_query_supports_bounded_parameterized_limit() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();
        let admitted = BTreeMap::from([("limit".into(), GraphParameterValue::U64(1))]);
        let result = state
            .query_read_only(
                "UNWIND [1, 2] AS n RETURN n LIMIT $limit",
                &admitted,
                2,
                4096,
                1_000,
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        let excessive = BTreeMap::from([("limit".into(), GraphParameterValue::U64(3))]);
        assert!(matches!(
            state.query_read_only(
                "UNWIND [1, 2] AS n RETURN n LIMIT $limit",
                &excessive,
                2,
                4096,
                1_000,
            ),
            Err(Error::InvalidCommand(message)) if message.contains("max_rows")
        ));
    }

    #[test]
    fn typed_empty_collections_and_union_descriptors_remain_distinct() {
        let empty_strings = graph_result_value(Value::List(LogicalType::String, vec![])).unwrap();
        let empty_integers = graph_result_value(Value::List(LogicalType::Int64, vec![])).unwrap();
        assert_eq!(
            empty_strings,
            GraphResultValue::List {
                element_type: GraphLogicalType::String,
                values: vec![],
            }
        );
        assert_eq!(
            empty_integers,
            GraphResultValue::List {
                element_type: GraphLogicalType::I64,
                values: vec![],
            }
        );
        assert_ne!(empty_strings, empty_integers);

        let map = graph_result_value(Value::Map(
            (LogicalType::String, LogicalType::Int64),
            vec![],
        ))
        .unwrap();
        assert_eq!(
            map,
            GraphResultValue::Map {
                key_type: GraphLogicalType::String,
                value_type: GraphLogicalType::I64,
                entries: vec![],
            }
        );

        let union = graph_result_value(Value::Union {
            types: vec![
                ("name".into(), LogicalType::String),
                ("count".into(), LogicalType::Int64),
            ],
            value: Box::new(Value::String("rhiza".into())),
        })
        .unwrap();
        assert_eq!(
            union,
            GraphResultValue::Union {
                variants: vec![
                    ("name".into(), GraphLogicalType::String),
                    ("count".into(), GraphLogicalType::I64),
                ],
                value: Box::new(GraphResultValue::String("rhiza".into())),
            }
        );
    }

    #[test]
    fn admission_bounds_queries_without_overriding_a_smaller_explicit_limit() {
        let admitted = admit_read_only_query(
            "MATCH (v:RhizaDocument) RETURN v.id",
            &BTreeMap::new(),
            10,
            4096,
        )
        .unwrap();
        assert!(admitted.statement.ends_with("LIMIT 11"));
        assert!(admitted
            .statement
            .contains("MATCH (v:RhizaDocument) RETURN v.id"));

        let admitted = admit_read_only_query(
            "MATCH (v:RhizaDocument) RETURN v.id LIMIT 3",
            &BTreeMap::new(),
            10,
            4096,
        )
        .unwrap();
        assert!(admitted.statement.ends_with("LIMIT 3"));
        assert!(admitted
            .statement
            .contains("MATCH (v:RhizaDocument) RETURN v.id LIMIT 3"));

        let admitted =
            admit_read_only_query("RETURN 1 AS limit", &BTreeMap::new(), 1, 4096).unwrap();
        assert_eq!(admitted.statement, "RETURN 1 AS limit\nLIMIT 2");

        let admitted = admit_read_only_query(
            "RETURN 1 AS call, 2 AS load, 3 AS limit LIMIT 9",
            &BTreeMap::new(),
            1,
            4096,
        )
        .unwrap();
        assert_eq!(
            admitted.statement,
            "RETURN 1 AS call, 2 AS load, 3 AS limit LIMIT 2"
        );

        assert!(matches!(
            admit_read_only_query("RETURN 1", &BTreeMap::new(), usize::MAX, 4096),
            Err(Error::InvalidCommand(message)) if message.contains("overflow")
        ));
    }

    #[test]
    fn query_error_mapping_keeps_prepare_and_execution_failures_separate() {
        assert!(matches!(
            ladybug_prepare_error(lbug::Error::FailedPreparedStatement("syntax".into())),
            Error::InvalidCommand(_)
        ));
        assert!(matches!(
            ladybug_execution_error(lbug::Error::FailedQuery("storage".into())),
            Error::Ladybug(_)
        ));
        assert!(matches!(
            ladybug_execution_error(lbug::Error::FailedQuery(
                "Conversion exception: Cast failed".into()
            )),
            Error::InvalidCommand(_)
        ));
        assert!(matches!(
            ladybug_execution_error(lbug::Error::FailedQuery(
                "Buffer manager exception: Unable to allocate memory! The buffer pool is full and no memory could be freed!".into()
            )),
            Error::ResourceExhausted(_)
        ));
        assert!(matches!(
            ladybug_execution_error(lbug::Error::FailedQuery(
                "Interrupted while executing query".into()
            )),
            Error::ResourceExhausted(_)
        ));
    }

    #[test]
    fn query_timeout_is_typed_as_resource_exhaustion() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        let error = state
            .query_read_only(
                "UNWIND range(1, 10000) AS x UNWIND range(1, 10000) AS y RETURN sum(x * y) AS total LIMIT 1",
                &BTreeMap::new(),
                1,
                1024 * 1024,
                1,
            )
            .unwrap_err();

        assert!(matches!(error, Error::ResourceExhausted(_)));
    }

    #[test]
    fn admission_stops_amplified_results_before_native_materialization() {
        let dir = tempfile::tempdir().unwrap();
        let state =
            LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
                .unwrap();

        let error = state
            .query_read_only(
                "UNWIND range(1, 10000) AS x RETURN repeat('a', 4096) AS value LIMIT 10000",
                &BTreeMap::new(),
                10_000,
                1024 * 1024,
                5_000,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidCommand(message) if message.contains("statically expanded values")
        ));
    }

    proptest! {
        #[test]
        fn lexer_ignores_arbitrary_keyword_like_text_inside_strings_and_comments(
            payload in "[A-Za-z0-9_ ;]{0,64}"
        ) {
            let comment = payload.replace("*/", "* /");
            let query = format!(
                "/* {comment} */ MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.id LIMIT 1"
            );
            prop_assert!(admit_read_only_query(&query, &BTreeMap::from([(
                "id".into(),
                GraphParameterValue::String("document".into()),
            )]), 10, 4096).is_ok());
        }
    }
}
