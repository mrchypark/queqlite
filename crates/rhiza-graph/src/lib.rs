//! LadybugDB materialization for deterministic QuePaxa log entries.
//!
//! The write surface is deliberately semantic: callers encode [`GraphCommandV1`]
//! values instead of submitting arbitrary Cypher. This keeps generated values,
//! external I/O, transaction control, and other non-replayable behavior outside
//! the authoritative state machine.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use lbug::{Connection, Database, LogicalType, SystemConfig, Value};
use rhiza_core::{
    EntryType, ExecutionProfile, LogEntry, LogHash, LogIndex, ReplicatedCommandEnvelope,
};
use tempfile::NamedTempFile;

const COMMAND_MAGIC: &[u8; 6] = b"RHGC\0\x01";
const RESULT_MAGIC: &[u8; 6] = b"RHGR\0\x01";
const SNAPSHOT_DOMAIN: &[u8] = b"rhiza-ladybug-snapshot-v1\0";
const SNAPSHOT_WIRE_MAGIC: &[u8; 4] = b"RHGS";
const SNAPSHOT_WIRE_VERSION: u16 = 1;
const MATERIALIZER_DOMAIN: &[u8] = b"rhiza-graph-materializer-v1\0";
const SCHEMA_VERSION: &str = "1";
const MAX_REQUEST_ID_BYTES: usize = 256;
const MAX_DOCUMENT_ID_BYTES: usize = 1024;
const MAX_STRING_BYTES: usize = 256 * 1024;
const MAX_BLOB_BYTES: usize = 4096;
pub const MAX_GRAPH_QUERY_BYTES: usize = 64 * 1024;
pub const MAX_GRAPH_PARAMETERS: usize = 999;
pub const MAX_GRAPH_PARAMETER_DEPTH: usize = 16;
// V1 can materialize at most one sentinel row beyond four admitted cells. With
// four 256 KiB string cells per row, Ladybug's native result stays below about
// 2 MiB before rhiza observes the sentinel and returns an explicit limit error.
pub const MAX_GRAPH_RETURN_PROJECTIONS: usize = 4;
pub const MAX_GRAPH_RESULT_CELLS: usize = 4;
const MAX_GRAPH_PARAMETER_VALUES: usize = 4096;
const MAX_GRAPH_CONTAINER_VALUES: usize = 1024;
const MAX_GRAPH_PARAMETER_NAME_BYTES: usize = 256;

const CREATE_META_TABLE: &str = r#"
CREATE NODE TABLE IF NOT EXISTS __RhizaMeta(
    key STRING PRIMARY KEY,
    value STRING
)
"#;

const CREATE_REQUEST_TABLE: &str = r#"
CREATE NODE TABLE IF NOT EXISTS __RhizaRequest(
    request_id STRING PRIMARY KEY,
    command_hash STRING,
    original_log_index UINT64,
    original_log_hash STRING,
    result BLOB
)
"#;

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

    fn recompute_digest(&self) -> LogHash {
        let cluster_id = length_prefixed(self.cluster_id.as_bytes());
        let created_by = length_prefixed(self.created_by.as_bytes());
        let database_length = u64::try_from(self.db_bytes.len()).expect("usize fits in u64");
        LogHash::digest(&[
            SNAPSHOT_DOMAIN,
            &cluster_id,
            &created_by,
            &self.epoch.to_be_bytes(),
            &self.config_id.to_be_bytes(),
            &self.storage_version.to_be_bytes(),
            &self.applied_index.to_be_bytes(),
            self.applied_hash.as_bytes(),
            self.materializer_fingerprint.as_bytes(),
            &database_length.to_be_bytes(),
            &self.db_bytes,
        ])
    }
}

/// Encodes a complete Ladybug snapshot as one canonical, versioned archive object.
pub fn encode_snapshot(snapshot: &LadybugSnapshot) -> Result<Vec<u8>> {
    validate_snapshot_envelope(snapshot)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(SNAPSHOT_WIRE_MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_WIRE_VERSION.to_be_bytes());
    encode_snapshot_bytes(&mut encoded, snapshot.cluster_id.as_bytes());
    encode_snapshot_bytes(&mut encoded, snapshot.created_by.as_bytes());
    encoded.extend_from_slice(&snapshot.epoch.to_be_bytes());
    encoded.extend_from_slice(&snapshot.config_id.to_be_bytes());
    encoded.extend_from_slice(&snapshot.applied_index.to_be_bytes());
    encoded.extend_from_slice(snapshot.applied_hash.as_bytes());
    encoded.extend_from_slice(&snapshot.storage_version.to_be_bytes());
    encoded.extend_from_slice(snapshot.materializer_fingerprint.as_bytes());
    encoded.extend_from_slice(snapshot.digest.as_bytes());
    encode_snapshot_bytes(&mut encoded, &snapshot.db_bytes);
    Ok(encoded)
}

/// Decodes and verifies a canonical Ladybug snapshot archive object.
pub fn decode_snapshot(encoded: &[u8]) -> Result<LadybugSnapshot> {
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
        cluster_id: decoder.string()?,
        created_by: decoder.string()?,
        epoch: decoder.u64()?,
        config_id: decoder.u64()?,
        applied_index: decoder.u64()?,
        applied_hash: LogHash::from_bytes(decoder.array()?),
        storage_version: decoder.u64()?,
        materializer_fingerprint: LogHash::from_bytes(decoder.array()?),
        digest: LogHash::from_bytes(decoder.array()?),
        db_bytes: decoder.bytes()?.to_vec(),
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
    if snapshot.cluster_id.is_empty() || snapshot.created_by.is_empty() {
        return Err(Error::InvalidSnapshot(
            "snapshot identity contains an empty cluster or source node".into(),
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

fn encode_snapshot_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    encoded.extend_from_slice(
        &u64::try_from(value.len())
            .expect("usize fits in u64")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(value);
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

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = usize::try_from(self.u64()?).map_err(|_| {
            Error::InvalidSnapshot("snapshot envelope length exceeds this platform".into())
        })?;
        self.take(length)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| Error::InvalidSnapshot("snapshot identity is not valid UTF-8".into()))
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Clone, Debug)]
struct Identity {
    cluster_id: String,
    node_id: String,
    epoch: u64,
    config_id: u64,
}

/// Authoritative LadybugDB materialized state guarded by a single local writer.
pub struct LadybugStateMachine {
    path: PathBuf,
    identity: Identity,
    database: RwLock<Option<Database>>,
    writer: Mutex<()>,
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
        let identity = Identity {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            epoch,
            config_id,
        };
        let database = open_database(&path)?;
        initialize_or_validate(&database, &identity)?;
        Ok(Self {
            path,
            identity,
            database: RwLock::new(Some(database)),
            writer: Mutex::new(()),
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
        let guard = self.write_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        transaction(&connection, || {
            self.apply_in_transaction(&connection, entry)
        })
    }

    pub fn applied_index(&self) -> Result<LogIndex> {
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        meta_u64(&connection, "applied_index")
    }

    pub fn applied_hash(&self) -> Result<LogHash> {
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        meta_hash(&connection, "applied_hash")
    }

    /// Safe read boundary for the fixed document projection. No raw Cypher is accepted.
    pub fn get_document(&self, id: &str) -> Result<Option<GraphValueV1>> {
        validate_nonempty_bytes("document id", id, MAX_DOCUMENT_ID_BYTES)?;
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        document(&connection, id)
    }

    /// Reads one fixed document projection and the materialized log tip while
    /// holding one shared database boundary that excludes materializer writes.
    pub fn get_document_with_tip(
        &self,
        id: &str,
    ) -> Result<(Option<GraphValueV1>, LogIndex, LogHash)> {
        validate_nonempty_bytes("document id", id, MAX_DOCUMENT_ID_BYTES)?;
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        let value = document(&connection, id)?;
        let applied_index = meta_u64(&connection, "applied_index")?;
        let hash = meta_hash(&connection, "applied_hash")?;
        Ok((value, applied_index, hash))
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
        let admitted = admit_read_only_query(statement, max_rows)?;
        validate_query_parameter_contract(
            parameters,
            &admitted.referenced_parameters,
            admitted.id_parameter.as_deref(),
        )?;
        let parameters = query_parameters(parameters)?;
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        connection.set_query_timeout(timeout_ms);
        {
            let mut prepared = connection
                .prepare(&admitted.statement)
                .map_err(ladybug_error)?;
            if !prepared.is_read_only() {
                return Err(Error::InvalidCommand(
                    "graph query must be read-only".into(),
                ));
            }
            let mut result = connection
                .execute(&mut prepared, parameters)
                .map_err(ladybug_error)?;
            let column_names = result.get_column_names();
            let column_types = result.get_column_data_types();
            if column_names.len() != admitted.projection_count
                || column_types.len() != admitted.projection_count
            {
                return Err(Error::InvalidCommand(
                    "graph query projection shape does not match its admitted RETURN list".into(),
                ));
            }
            let columns = column_names
                .into_iter()
                .zip(column_types)
                .map(|(name, logical_type)| {
                    Ok(GraphColumn {
                        name,
                        logical_type: graph_logical_type(logical_type)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let tuple_count = usize::try_from(result.get_num_tuples()).map_err(|_| {
                Error::InvalidCommand("graph query row count exceeds this platform".into())
            })?;
            if tuple_count > admitted.allowed_rows {
                let message = if max_rows <= MAX_GRAPH_RESULT_CELLS / admitted.projection_count {
                    format!("graph query exceeds {max_rows} rows")
                } else {
                    format!("graph query exceeds {MAX_GRAPH_RESULT_CELLS} result cells")
                };
                return Err(Error::InvalidCommand(message));
            }
            let mut result_bytes = columns.iter().map(graph_column_size).sum::<usize>();
            if result_bytes > max_bytes {
                return Err(Error::InvalidCommand(format!(
                    "graph query exceeds {max_bytes} result bytes"
                )));
            }
            let mut rows = Vec::with_capacity(tuple_count);
            loop {
                let next =
                    panic::catch_unwind(AssertUnwindSafe(|| result.next())).map_err(|_| {
                        Error::Ladybug("Ladybug result value conversion panicked".into())
                    })?;
                let Some(row) = next else { break };
                let next_cells = (rows.len() + 1)
                    .checked_mul(admitted.projection_count)
                    .ok_or_else(|| Error::InvalidCommand("graph result cell overflow".into()))?;
                if next_cells > MAX_GRAPH_RESULT_CELLS {
                    return Err(Error::InvalidCommand(format!(
                        "graph query exceeds {MAX_GRAPH_RESULT_CELLS} result cells"
                    )));
                }
                let row = row
                    .into_iter()
                    .map(graph_result_value)
                    .collect::<Result<Vec<_>>>()?;
                for value in &row {
                    result_bytes = result_bytes
                        .checked_add(graph_result_value_size(value))
                        .ok_or_else(|| {
                            Error::InvalidCommand("graph query result size overflow".into())
                        })?;
                    if result_bytes > max_bytes {
                        return Err(Error::InvalidCommand(format!(
                            "graph query exceeds {max_bytes} result bytes"
                        )));
                    }
                }
                rows.push(row);
            }
            let applied_index = meta_u64(&connection, "applied_index")?;
            let hash = meta_hash(&connection, "applied_hash")?;
            Ok(GraphQueryResult {
                columns,
                rows,
                applied_index,
                hash,
            })
        }
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
        let guard = self.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        matching_request(&connection, request_id, command_payload)
    }

    /// Drains crate-owned operations, checkpoints, closes Ladybug, copies the
    /// single database file, and reopens it before returning.
    pub fn create_snapshot(&self, target: LogIndex) -> Result<LadybugSnapshot> {
        let mut guard = self.write_database()?;
        let checkpoint_wal = ladybug_sidecar(&self.path, ".wal.checkpoint");
        if checkpoint_wal.exists() {
            return Err(Error::InvalidSnapshot(format!(
                "checkpoint found stale sidecar file {}",
                checkpoint_wal.display()
            )));
        }
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        let applied_index = meta_u64(&connection, "applied_index")?;
        if applied_index != target {
            return Err(Error::InvalidSnapshot(format!(
                "snapshot target {target} does not match applied index {applied_index}"
            )));
        }
        let applied_hash = meta_hash(&connection, "applied_hash")?;
        connection.query("CHECKPOINT").map_err(ladybug_error)?;
        drop(connection);

        let database = guard.take().ok_or(Error::Closed)?;
        drop(database);
        for sidecar in ladybug_sidecars(&self.path) {
            if sidecar.exists() {
                let reopened = open_database(&self.path)?;
                *guard = Some(reopened);
                return Err(Error::InvalidSnapshot(format!(
                    "checkpoint left sidecar file {}",
                    sidecar.display()
                )));
            }
        }
        let read_result = fs::read(&self.path).map_err(io_error);
        *guard = Some(open_database(&self.path)?);
        let db_bytes = read_result?;
        let storage_version = lbug::get_storage_version();
        let mut snapshot = LadybugSnapshot {
            cluster_id: self.identity.cluster_id.clone(),
            created_by: self.identity.node_id.clone(),
            epoch: self.identity.epoch,
            config_id: self.identity.config_id,
            applied_index,
            applied_hash,
            storage_version,
            materializer_fingerprint: graph_materializer_fingerprint(),
            digest: LogHash::ZERO,
            db_bytes,
        };
        snapshot.digest = snapshot.recompute_digest();
        Ok(snapshot)
    }

    fn apply_in_transaction(
        &self,
        connection: &Connection<'_>,
        entry: &LogEntry,
    ) -> Result<ApplyOutcome> {
        validate_identity(connection, &self.identity)?;
        if entry.cluster_id != self.identity.cluster_id {
            return Err(Error::IdentityMismatch("cluster_id".into()));
        }
        if entry.epoch != self.identity.epoch {
            return Err(Error::IdentityMismatch("epoch".into()));
        }
        if entry.config_id != self.identity.config_id {
            return Err(Error::IdentityMismatch("config_id".into()));
        }

        let current_index = meta_u64(connection, "applied_index")?;
        let current_hash = meta_hash(connection, "applied_hash")?;
        if entry.index == current_index {
            if entry.hash != current_hash {
                return Err(Error::InvalidEntry(
                    "current index was reapplied with a different hash".into(),
                ));
            }
            let result = if entry.entry_type == EntryType::Command {
                let command = decode_replicated_graph_command(&entry.payload)?;
                Some(
                    matching_request(connection, command.request_id(), &entry.payload)?
                        .ok_or_else(|| {
                            Error::InvalidEntry(
                                "applied graph command is missing its request record".into(),
                            )
                        })?
                        .result,
                )
            } else {
                None
            };
            return Ok(ApplyOutcome {
                applied_index: current_index,
                applied_hash: current_hash,
                result,
            });
        }
        let expected = current_index
            .checked_add(1)
            .ok_or_else(|| Error::InvalidEntry("applied index is exhausted".into()))?;
        if entry.index != expected {
            return Err(Error::InvalidEntry(format!(
                "expected index {expected}, got {}",
                entry.index
            )));
        }
        if entry.prev_hash != current_hash {
            return Err(Error::InvalidEntry(
                "prev_hash does not match the materialized graph tip".into(),
            ));
        }

        let result = match entry.entry_type {
            EntryType::Command => {
                let command = decode_replicated_graph_command(&entry.payload)?;
                if let Some(record) =
                    matching_request(connection, command.request_id(), &entry.payload)?
                {
                    Some(record.result)
                } else {
                    let result = apply_command(connection, &command)?;
                    record_request(connection, &command, entry, &result)?;
                    Some(result)
                }
            }
            EntryType::ConfigChange
            | EntryType::SnapshotBarrier
            | EntryType::SnapshotPublished
            | EntryType::Noop => None,
        };

        set_meta(connection, "applied_index", &entry.index.to_string())?;
        set_meta(connection, "applied_hash", &entry.hash.to_hex())?;
        Ok(ApplyOutcome {
            applied_index: entry.index,
            applied_hash: entry.hash,
            result,
        })
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

pub fn restore_snapshot_file(
    path: impl AsRef<Path>,
    snapshot: &LadybugSnapshot,
    target_node_id: &str,
) -> Result<()> {
    if target_node_id.is_empty() {
        return Err(Error::InvalidSnapshot("target node id is empty".into()));
    }
    if snapshot.recompute_digest() != snapshot.digest {
        return Err(Error::InvalidSnapshot(
            "snapshot digest does not match its contents".into(),
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
    let path = path.as_ref();
    ensure_parent(path)?;
    if path.exists()
        || ladybug_sidecars(path)
            .iter()
            .any(|sidecar| sidecar.exists())
    {
        return Err(Error::InvalidSnapshot(
            "restore target or a Ladybug sidecar already exists".into(),
        ));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
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
        validate_snapshot_database(&database, snapshot, &snapshot.created_by)?;
        rebind_snapshot_node(&database, target_node_id)?;
        validate_snapshot_database(&database, snapshot, target_node_id)?;
        let connection = Connection::new(&database).map_err(invalid_snapshot_ladybug_error)?;
        connection
            .query("CHECKPOINT")
            .map_err(invalid_snapshot_ladybug_error)?;
        Ok(())
    })();
    drop(database);
    if validation.is_err() {
        remove_sidecars(&temporary_path);
    }
    validation?;
    for sidecar in ladybug_sidecars(&temporary_path) {
        if sidecar.exists() {
            remove_sidecars(&temporary_path);
            return Err(Error::InvalidSnapshot(
                "snapshot validation left a Ladybug sidecar".into(),
            ));
        }
    }
    temporary.as_file().sync_all().map_err(io_error)?;
    temporary.persist_noclobber(path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidSnapshot("restore target already exists".into())
        } else {
            io_error(error.error)
        }
    })?;
    if let Err(error) = File::open(path).and_then(|file| file.sync_all()) {
        remove_failed_install(path, parent);
        return Err(io_error(error));
    }
    if let Err(error) = File::open(parent).and_then(|directory| directory.sync_all()) {
        remove_failed_install(path, parent);
        return Err(io_error(error));
    }
    Ok(())
}

fn validate_snapshot_database(
    database: &Database,
    snapshot: &LadybugSnapshot,
    expected_node_id: &str,
) -> Result<()> {
    let connection = Connection::new(database).map_err(invalid_snapshot_ladybug_error)?;
    for (key, expected) in [
        ("cluster_id", snapshot.cluster_id.as_str()),
        ("node_id", expected_node_id),
        ("schema_version", SCHEMA_VERSION),
    ] {
        let actual = get_meta(&connection, key)
            .map_err(invalid_snapshot_error)?
            .ok_or_else(|| Error::InvalidSnapshot(format!("missing metadata {key}")))?;
        if actual != expected {
            return Err(Error::InvalidSnapshot(format!(
                "metadata {key} does not match the snapshot identity"
            )));
        }
    }
    for (key, expected) in [
        ("epoch", snapshot.epoch),
        ("config_id", snapshot.config_id),
        ("applied_index", snapshot.applied_index),
    ] {
        let actual = meta_u64(&connection, key).map_err(invalid_snapshot_error)?;
        if actual != expected {
            return Err(Error::InvalidSnapshot(format!(
                "metadata {key} does not match the snapshot identity"
            )));
        }
    }
    if meta_hash(&connection, "applied_hash").map_err(invalid_snapshot_error)?
        != snapshot.applied_hash
    {
        return Err(Error::InvalidSnapshot(
            "metadata applied_hash does not match the snapshot identity".into(),
        ));
    }
    let fingerprint = get_meta(&connection, "materializer_fingerprint")
        .map_err(invalid_snapshot_error)?
        .ok_or_else(|| Error::InvalidSnapshot("missing materializer fingerprint".into()))?;
    if fingerprint != snapshot.materializer_fingerprint.to_hex() {
        return Err(Error::InvalidSnapshot(
            "inner materializer fingerprint does not match the snapshot envelope".into(),
        ));
    }
    Ok(())
}

fn rebind_snapshot_node(database: &Database, target_node_id: &str) -> Result<()> {
    let connection = Connection::new(database).map_err(invalid_snapshot_ladybug_error)?;
    transaction(&connection, || {
        set_meta(&connection, "node_id", target_node_id)
    })
    .map_err(invalid_snapshot_error)
}

fn open_database(path: &Path) -> Result<Database> {
    Database::new(
        path,
        SystemConfig::default()
            .enable_multi_writes(false)
            .throw_on_wal_replay_failure(true)
            .enable_checksums(true),
    )
    .map_err(ladybug_error)
}

fn initialize_or_validate(database: &Database, identity: &Identity) -> Result<()> {
    let connection = Connection::new(database).map_err(ladybug_error)?;
    transaction(&connection, || {
        connection.query(CREATE_META_TABLE).map_err(ladybug_error)?;
        connection
            .query(CREATE_REQUEST_TABLE)
            .map_err(ladybug_error)?;
        connection
            .query(CREATE_DOCUMENT_TABLE)
            .map_err(ladybug_error)?;
        if get_meta(&connection, "cluster_id")?.is_none() {
            for key in [
                "node_id",
                "epoch",
                "config_id",
                "applied_index",
                "applied_hash",
                "schema_version",
                "materializer_fingerprint",
            ] {
                if get_meta(&connection, key)?.is_some() {
                    return Err(Error::IdentityMismatch(
                        "partially initialized metadata".into(),
                    ));
                }
            }
            create_meta(&connection, "cluster_id", &identity.cluster_id)?;
            create_meta(&connection, "node_id", &identity.node_id)?;
            create_meta(&connection, "epoch", &identity.epoch.to_string())?;
            create_meta(&connection, "config_id", &identity.config_id.to_string())?;
            create_meta(&connection, "applied_index", "0")?;
            create_meta(&connection, "applied_hash", &LogHash::ZERO.to_hex())?;
            create_meta(&connection, "schema_version", SCHEMA_VERSION)?;
            create_meta(
                &connection,
                "materializer_fingerprint",
                &graph_materializer_fingerprint().to_hex(),
            )?;
        }
        validate_identity(&connection, identity)
    })
}

fn validate_identity(connection: &Connection<'_>, identity: &Identity) -> Result<()> {
    validate_meta(connection, "cluster_id", &identity.cluster_id)?;
    validate_meta(connection, "node_id", &identity.node_id)?;
    validate_meta(connection, "epoch", &identity.epoch.to_string())?;
    validate_meta(connection, "config_id", &identity.config_id.to_string())?;
    validate_meta(connection, "schema_version", SCHEMA_VERSION)?;
    validate_meta(
        connection,
        "materializer_fingerprint",
        &graph_materializer_fingerprint().to_hex(),
    )
}

fn validate_meta(connection: &Connection<'_>, key: &str, expected: &str) -> Result<()> {
    let actual = get_meta(connection, key)?
        .ok_or_else(|| Error::IdentityMismatch(format!("missing {key}")))?;
    if actual == expected {
        Ok(())
    } else {
        Err(Error::IdentityMismatch(key.into()))
    }
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
    projection_count: usize,
    allowed_rows: usize,
    referenced_parameters: BTreeSet<String>,
    id_parameter: Option<String>,
}

fn admit_read_only_query(statement: &str, max_rows: usize) -> Result<AdmittedQuery> {
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
    if tokens
        .last()
        .is_some_and(|token| token.kind == QueryTokenKind::Semicolon)
    {
        tokens.pop();
    }
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
            QueryTokenKind::Symbol('{') | QueryTokenKind::Symbol('}') => {
                return Err(Error::InvalidCommand(
                    "graph query subqueries and map literals are not supported".into(),
                ));
            }
            _ => {}
        }
    }
    reject_function_calls(&tokens)?;

    let pattern_start = if token_is_keyword(tokens.first(), "MATCH") {
        1
    } else {
        return Err(Error::InvalidCommand(
            "graph query must begin with MATCH".into(),
        ));
    };
    let (where_index, return_index) = find_match_clauses(&tokens, pattern_start)?;
    validate_typed_pattern(&tokens[pattern_start..where_index.unwrap_or(return_index)])?;
    let id_parameter = if let Some(where_index) = where_index {
        if where_index + 1 == return_index {
            return Err(Error::InvalidCommand(
                "graph query WHERE clause cannot be empty".into(),
            ));
        }
        validate_where_expression(&tokens[where_index + 1..return_index])?
    } else {
        None
    };
    let return_layout = validate_return_layout(&tokens, return_index + 1)?;
    let projection_count =
        validate_return_projections(&tokens[return_index + 1..return_layout.projection_end])?;
    let cell_rows = MAX_GRAPH_RESULT_CELLS / projection_count;
    let allowed_rows = max_rows.min(cell_rows);
    let requested_limit = return_layout.limit;
    let execution_limit = match requested_limit {
        Some(limit) if limit <= allowed_rows => limit,
        _ => allowed_rows
            .checked_add(1)
            .ok_or_else(|| Error::InvalidCommand("graph query row limit overflow".into()))?,
    };
    let base_end = return_layout
        .limit_start
        .unwrap_or_else(|| tokens.last().expect("nonempty checked").end);
    let base = statement[..base_end]
        .trim_end()
        .trim_end_matches(';')
        .trim_end();
    let referenced_parameters = tokens
        .iter()
        .filter_map(|token| match &token.kind {
            QueryTokenKind::Parameter(name) => Some(name.clone()),
            _ => None,
        })
        .collect();
    Ok(AdmittedQuery {
        statement: format!("{base} LIMIT {execution_limit}"),
        projection_count,
        allowed_rows,
        referenced_parameters,
        id_parameter,
    })
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

fn unsafe_clause_keyword(token: &QueryToken) -> bool {
    let QueryTokenKind::Identifier {
        value,
        escaped: false,
    } = &token.kind
    else {
        return false;
    };
    matches!(
        value.to_ascii_uppercase().as_str(),
        "BEGIN"
            | "COMMIT"
            | "ROLLBACK"
            | "CHECKPOINT"
            | "TRANSACTION"
            | "CALL"
            | "COPY"
            | "LOAD"
            | "IMPORT"
            | "EXPORT"
            | "ATTACH"
            | "DETACH"
            | "INSTALL"
            | "EXTENSION"
            | "CREATE"
            | "DROP"
            | "ALTER"
            | "RENAME"
            | "TRUNCATE"
            | "GRANT"
            | "REVOKE"
            | "MERGE"
            | "SET"
            | "DELETE"
            | "REMOVE"
            | "INSERT"
            | "UPDATE"
    )
}

fn reject_function_calls(tokens: &[QueryToken]) -> Result<()> {
    for (index, pair) in tokens.windows(2).enumerate() {
        let QueryTokenKind::Identifier { value, escaped } = &pair[0].kind else {
            continue;
        };
        if token_is_symbol(pair.get(1), '(')
            && (*escaped
                || !matches!(
                    value.to_ascii_uppercase().as_str(),
                    "MATCH" | "OPTIONAL" | "RETURN" | "WHERE"
                ))
        {
            return Err(Error::InvalidCommand(format!(
                "graph query functions are not supported: token {index} ({value})"
            )));
        }
    }
    Ok(())
}

fn find_match_clauses(
    tokens: &[QueryToken],
    pattern_start: usize,
) -> Result<(Option<usize>, usize)> {
    let mut round = 0usize;
    let mut square = 0usize;
    let mut where_index = None;
    for index in pattern_start..tokens.len() {
        update_depth(&tokens[index], &mut round, &mut square)?;
        if round != 0 || square != 0 {
            continue;
        }
        if token_is_keyword(tokens.get(index), "WHERE") && where_index.is_none() {
            where_index = Some(index);
        } else if token_is_keyword(tokens.get(index), "RETURN") {
            return Ok((where_index, index));
        } else if unsafe_clause_keyword(&tokens[index]) || matches_top_level_clause(&tokens[index])
        {
            return Err(Error::InvalidCommand(
                "graph query contains an unsupported clause before RETURN".into(),
            ));
        }
    }
    Err(Error::InvalidCommand(
        "graph query must contain one top-level RETURN clause".into(),
    ))
}

fn matches_top_level_clause(token: &QueryToken) -> bool {
    [
        "MATCH", "OPTIONAL", "WITH", "UNWIND", "CALL", "UNION", "RETURN",
    ]
    .iter()
    .any(|keyword| token_is_keyword(Some(token), keyword))
}

fn update_depth(token: &QueryToken, round: &mut usize, square: &mut usize) -> Result<()> {
    match token.kind {
        QueryTokenKind::Symbol('(') => *round += 1,
        QueryTokenKind::Symbol(')') => {
            *round = round
                .checked_sub(1)
                .ok_or_else(|| Error::InvalidCommand("graph query has an unmatched ')'".into()))?;
        }
        QueryTokenKind::Symbol('[') => *square += 1,
        QueryTokenKind::Symbol(']') => {
            *square = square
                .checked_sub(1)
                .ok_or_else(|| Error::InvalidCommand("graph query has an unmatched ']'".into()))?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_typed_pattern(tokens: &[QueryToken]) -> Result<()> {
    if tokens.len() != 5
        || !token_is_symbol(tokens.first(), '(')
        || !token_is_identifier(tokens.get(1), "v")
        || !token_is_symbol(tokens.get(2), ':')
        || !token_is_identifier(tokens.get(3), "RhizaDocument")
        || !token_is_symbol(tokens.get(4), ')')
    {
        return Err(Error::InvalidCommand(
            "graph query V1 requires exactly MATCH (v:RhizaDocument)".into(),
        ));
    }
    Ok(())
}

fn token_is_identifier(token: Option<&QueryToken>, expected: &str) -> bool {
    matches!(
        token.map(|token| &token.kind),
        Some(QueryTokenKind::Identifier { value, .. }) if value == expected
    )
}

fn graph_document_property(token: Option<&QueryToken>) -> Option<&str> {
    let Some(QueryToken {
        kind: QueryTokenKind::Identifier { value, .. },
        ..
    }) = token
    else {
        return None;
    };
    [
        "id",
        "kind",
        "bool_value",
        "i64_value",
        "u64_value",
        "f64_value",
        "string_value",
        "bytes_value",
    ]
    .into_iter()
    .find(|property| value == property)
}

fn validate_where_expression(tokens: &[QueryToken]) -> Result<Option<String>> {
    let [variable, dot, property, equals, value] = tokens else {
        return Err(Error::InvalidCommand(
            "graph query V1 WHERE must be exactly v.id = $parameter".into(),
        ));
    };
    if !token_is_identifier(Some(variable), "v")
        || !token_is_symbol(Some(dot), '.')
        || !token_is_identifier(Some(property), "id")
        || !token_is_symbol(Some(equals), '=')
    {
        return Err(Error::InvalidCommand(
            "graph query V1 WHERE must be exactly v.id = $parameter".into(),
        ));
    }
    if let QueryTokenKind::Parameter(name) = &value.kind {
        Ok(Some(name.clone()))
    } else {
        Err(Error::InvalidCommand(
            "graph query V1 WHERE id must compare to a string parameter".into(),
        ))
    }
}

struct ReturnLayout {
    projection_end: usize,
    limit: Option<usize>,
    limit_start: Option<usize>,
}

fn validate_return_layout(tokens: &[QueryToken], start: usize) -> Result<ReturnLayout> {
    if start >= tokens.len() {
        return Err(Error::InvalidCommand(
            "graph RETURN clause cannot be empty".into(),
        ));
    }
    let mut limit = None;
    for index in start..tokens.len() {
        if token_is_keyword(tokens.get(index), "ORDER")
            && token_is_keyword(tokens.get(index + 1), "BY")
        {
            return Err(Error::InvalidCommand(
                "graph query V1 does not support ORDER BY".into(),
            ));
        }
        if clause_keyword_here(tokens, index, "SKIP") {
            return Err(Error::InvalidCommand(
                "graph query V1 does not support SKIP".into(),
            ));
        }
        if clause_keyword_here(tokens, index, "LIMIT") && limit.replace(index).is_some() {
            return Err(Error::InvalidCommand(
                "graph query has multiple LIMIT clauses".into(),
            ));
        }
    }
    let projection_end = limit.unwrap_or(tokens.len());
    let (limit_value, limit_start) = if let Some(limit) = limit {
        (
            Some(parse_single_integer(
                tokens,
                limit + 1,
                tokens.len(),
                "LIMIT",
            )?),
            Some(tokens[limit].start),
        )
    } else {
        (None, None)
    };
    Ok(ReturnLayout {
        projection_end,
        limit: limit_value,
        limit_start,
    })
}

fn clause_keyword_here(tokens: &[QueryToken], index: usize, keyword: &str) -> bool {
    token_is_keyword(tokens.get(index), keyword)
        && !token_is_keyword(
            index.checked_sub(1).and_then(|index| tokens.get(index)),
            "AS",
        )
        && !token_is_symbol(
            index.checked_sub(1).and_then(|index| tokens.get(index)),
            '.',
        )
}

fn parse_single_integer(
    tokens: &[QueryToken],
    start: usize,
    end: usize,
    clause: &str,
) -> Result<usize> {
    let [token] = tokens.get(start..end).unwrap_or_default() else {
        return Err(Error::InvalidCommand(format!(
            "graph {clause} must be one literal nonnegative integer"
        )));
    };
    let QueryTokenKind::Integer(value) = &token.kind else {
        return Err(Error::InvalidCommand(format!(
            "graph {clause} must be one literal nonnegative integer"
        )));
    };
    value
        .parse()
        .map_err(|_| Error::InvalidCommand(format!("graph {clause} integer is too large")))
}

fn validate_return_projections(tokens: &[QueryToken]) -> Result<usize> {
    if tokens.is_empty() {
        return Err(Error::InvalidCommand(
            "graph RETURN clause cannot be empty".into(),
        ));
    }
    let mut projections = Vec::new();
    let mut start = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        if token_is_symbol(Some(token), ',') {
            projections.push(&tokens[start..index]);
            start = index + 1;
        }
    }
    projections.push(&tokens[start..]);
    if projections.len() > MAX_GRAPH_RETURN_PROJECTIONS {
        return Err(Error::InvalidCommand(format!(
            "graph RETURN exceeds {MAX_GRAPH_RETURN_PROJECTIONS} projections"
        )));
    }
    for projection in &projections {
        validate_scalar_projection(projection)?;
    }
    Ok(projections.len())
}

fn validate_scalar_projection(tokens: &[QueryToken]) -> Result<()> {
    if tokens.is_empty() {
        return Err(Error::InvalidCommand(
            "graph RETURN contains an empty projection".into(),
        ));
    }
    let property = property_width(tokens).is_some_and(|width| width == tokens.len());
    let parameter = matches!(
        tokens,
        [QueryToken {
            kind: QueryTokenKind::Parameter(_),
            ..
        }]
    );
    if property || parameter {
        Ok(())
    } else {
        Err(Error::InvalidCommand(
            "graph RETURN projections must be one whitelisted property or parameter".into(),
        ))
    }
}

fn property_width(tokens: &[QueryToken]) -> Option<usize> {
    if token_is_identifier(tokens.first(), "v")
        && token_is_symbol(tokens.get(1), '.')
        && graph_document_property(tokens.get(2)).is_some()
    {
        Some(3)
    } else {
        None
    }
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
    id_parameter: Option<&str>,
) -> Result<()> {
    let supplied = parameters.keys().cloned().collect::<BTreeSet<_>>();
    if supplied != *referenced {
        return Err(Error::InvalidCommand(
            "graph query parameters must exactly match referenced parameters".into(),
        ));
    }
    for value in parameters.values() {
        if matches!(
            value,
            GraphParameterValue::List(_) | GraphParameterValue::Struct(_)
        ) {
            return Err(Error::InvalidCommand(
                "graph query V1 parameters must be scalar".into(),
            ));
        }
    }
    if let Some(name) = id_parameter {
        if !matches!(parameters.get(name), Some(GraphParameterValue::String(_))) {
            return Err(Error::InvalidCommand(
                "graph query V1 id parameter must be a string".into(),
            ));
        }
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
            if values.len() > MAX_GRAPH_CONTAINER_VALUES {
                return Err(Error::InvalidCommand(format!(
                    "graph parameter lists cannot exceed {MAX_GRAPH_CONTAINER_VALUES} values"
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
            if fields.len() > MAX_GRAPH_CONTAINER_VALUES {
                return Err(Error::InvalidCommand(format!(
                    "graph parameter structs cannot exceed {MAX_GRAPH_CONTAINER_VALUES} fields"
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
    if values.len() > MAX_GRAPH_CONTAINER_VALUES {
        return Err(Error::InvalidCommand(format!(
            "graph result containers cannot exceed {MAX_GRAPH_CONTAINER_VALUES} values"
        )));
    }
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

fn graph_result_value_size(value: &GraphResultValue) -> usize {
    const OVERHEAD: usize = 8;
    match value {
        GraphResultValue::Null(logical_type) => {
            OVERHEAD.saturating_add(graph_logical_type_size(logical_type))
        }
        GraphResultValue::I128(value)
        | GraphResultValue::F32(value)
        | GraphResultValue::Date(value)
        | GraphResultValue::Interval(value)
        | GraphResultValue::Timestamp(value)
        | GraphResultValue::TimestampTz(value)
        | GraphResultValue::TimestampNs(value)
        | GraphResultValue::TimestampMs(value)
        | GraphResultValue::TimestampSec(value)
        | GraphResultValue::String(value)
        | GraphResultValue::Json(value)
        | GraphResultValue::Uuid(value)
        | GraphResultValue::Decimal(value) => {
            OVERHEAD.saturating_add(value.len()).saturating_add(2)
        }
        GraphResultValue::Bool(_) | GraphResultValue::I8(_) | GraphResultValue::U8(_) => {
            OVERHEAD + 1
        }
        GraphResultValue::I16(_) | GraphResultValue::U16(_) => OVERHEAD + 2,
        GraphResultValue::I32(_) | GraphResultValue::U32(_) => OVERHEAD + 4,
        GraphResultValue::I64(_) | GraphResultValue::U64(_) | GraphResultValue::F64(_) => {
            OVERHEAD + 8
        }
        GraphResultValue::InternalId(_) => OVERHEAD + 16,
        GraphResultValue::Bytes(value) => OVERHEAD.saturating_add(value.len()),
        GraphResultValue::List {
            element_type,
            values,
        }
        | GraphResultValue::Array {
            element_type,
            values,
        } => values.iter().fold(
            OVERHEAD.saturating_add(graph_logical_type_size(element_type)),
            |size, value| size.saturating_add(graph_result_value_size(value)),
        ),
        GraphResultValue::Struct(fields) => fields.iter().fold(OVERHEAD, |size, (name, value)| {
            size.saturating_add(name.len())
                .saturating_add(graph_result_value_size(value))
        }),
        GraphResultValue::Node(node) => OVERHEAD.saturating_add(graph_node_size(node)),
        GraphResultValue::Rel(rel) => OVERHEAD.saturating_add(graph_rel_size(rel)),
        GraphResultValue::RecursiveRel { nodes, rels } => nodes
            .iter()
            .map(graph_node_size)
            .chain(rels.iter().map(graph_rel_size))
            .fold(OVERHEAD, usize::saturating_add),
        GraphResultValue::Map {
            key_type,
            value_type,
            entries,
        } => entries.iter().fold(
            OVERHEAD
                .saturating_add(graph_logical_type_size(key_type))
                .saturating_add(graph_logical_type_size(value_type)),
            |size, (key, value)| {
                size.saturating_add(graph_result_value_size(key))
                    .saturating_add(graph_result_value_size(value))
            },
        ),
        GraphResultValue::Union { variants, value } => variants
            .iter()
            .fold(OVERHEAD, |size, (name, logical_type)| {
                size.saturating_add(name.len())
                    .saturating_add(graph_logical_type_size(logical_type))
            })
            .saturating_add(graph_result_value_size(value)),
    }
}

fn graph_column_size(column: &GraphColumn) -> usize {
    8usize
        .saturating_add(column.name.len())
        .saturating_add(graph_logical_type_size(&column.logical_type))
}

fn graph_logical_type_size(logical_type: &GraphLogicalType) -> usize {
    const OVERHEAD: usize = 8;
    match logical_type {
        GraphLogicalType::List(element_type) => {
            OVERHEAD.saturating_add(graph_logical_type_size(element_type))
        }
        GraphLogicalType::Array {
            element_type,
            length: _,
        } => OVERHEAD
            .saturating_add(graph_logical_type_size(element_type))
            .saturating_add(8),
        GraphLogicalType::Struct(fields) | GraphLogicalType::Union(fields) => {
            fields.iter().fold(OVERHEAD, |size, (name, logical_type)| {
                size.saturating_add(name.len())
                    .saturating_add(graph_logical_type_size(logical_type))
            })
        }
        GraphLogicalType::Map {
            key_type,
            value_type,
        } => OVERHEAD
            .saturating_add(graph_logical_type_size(key_type))
            .saturating_add(graph_logical_type_size(value_type)),
        GraphLogicalType::Decimal { .. } => OVERHEAD + 8,
        _ => OVERHEAD,
    }
}

fn graph_node_size(node: &GraphNode) -> usize {
    16 + node.label.len()
        + node
            .properties
            .iter()
            .map(|(name, value)| name.len() + graph_result_value_size(value))
            .sum::<usize>()
}

fn graph_rel_size(rel: &GraphRel) -> usize {
    32 + rel.label.len()
        + rel
            .properties
            .iter()
            .map(|(name, value)| name.len() + graph_result_value_size(value))
            .sum::<usize>()
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
    Ok(Some(value))
}

fn record_request(
    connection: &Connection<'_>,
    command: &GraphCommandV1,
    entry: &LogEntry,
    result: &GraphCommandResultV1,
) -> Result<()> {
    execute(
        connection,
        "CREATE (r:__RhizaRequest {request_id: $request_id, command_hash: $command_hash, original_log_index: $original_log_index, original_log_hash: $original_log_hash, result: $result})",
        vec![
            ("request_id", Value::String(command.request_id.clone())),
            (
                "command_hash",
                Value::String(command_digest(&entry.payload).to_hex()),
            ),
            ("original_log_index", Value::UInt64(entry.index)),
            ("original_log_hash", Value::String(entry.hash.to_hex())),
            ("result", Value::Blob(result.encode())),
        ],
    )?;
    Ok(())
}

fn matching_request(
    connection: &Connection<'_>,
    request_id: &str,
    command_payload: &[u8],
) -> Result<Option<RequestRecord>> {
    let rows = execute(
        connection,
        "MATCH (r:__RhizaRequest) WHERE r.request_id = $request_id RETURN r.command_hash, r.original_log_index, r.original_log_hash, r.result",
        vec![("request_id", Value::String(request_id.into()))],
    )?;
    let Some(row) = one_or_none(rows, "request lookup")? else {
        return Ok(None);
    };
    if row.len() != 4 {
        return Err(Error::Ladybug("request lookup returned wrong shape".into()));
    }
    let stored_digest = expect_string(&row[0], "command_hash")?;
    let original_log_index = expect_u64(&row[1], "original_log_index")?;
    let original_log_hash = parse_hash(&expect_string(&row[2], "original_log_hash")?)?;
    if stored_digest != command_digest(command_payload).to_hex() {
        return Err(Error::RequestConflict {
            request_id: request_id.into(),
            original_log_index,
            original_log_hash,
        });
    }
    let result = GraphCommandResultV1::decode(&expect_blob(&row[3], "result")?)?;
    Ok(Some(RequestRecord {
        original_log_index,
        original_log_hash,
        result,
    }))
}

fn command_digest(payload: &[u8]) -> LogHash {
    LogHash::digest(&[b"rhiza-graph-command-digest-v1\0", payload])
}

fn get_meta(connection: &Connection<'_>, key: &str) -> Result<Option<String>> {
    let rows = execute(
        connection,
        "MATCH (m:__RhizaMeta) WHERE m.key = $key RETURN m.value",
        vec![("key", Value::String(key.into()))],
    )?;
    one_or_none(rows, "metadata lookup")?
        .map(|row| {
            row.first()
                .ok_or_else(|| Error::Ladybug("metadata lookup returned an empty row".into()))
                .and_then(|value| expect_string(value, "metadata value"))
        })
        .transpose()
}

fn create_meta(connection: &Connection<'_>, key: &str, value: &str) -> Result<()> {
    execute(
        connection,
        "CREATE (m:__RhizaMeta {key: $key, value: $value})",
        vec![
            ("key", Value::String(key.into())),
            ("value", Value::String(value.into())),
        ],
    )?;
    Ok(())
}

fn set_meta(connection: &Connection<'_>, key: &str, value: &str) -> Result<()> {
    if get_meta(connection, key)?.is_none() {
        return Err(Error::IdentityMismatch(format!("missing {key}")));
    }
    execute(
        connection,
        "MATCH (m:__RhizaMeta) WHERE m.key = $key SET m.value = $value",
        vec![
            ("key", Value::String(key.into())),
            ("value", Value::String(value.into())),
        ],
    )?;
    Ok(())
}

fn meta_u64(connection: &Connection<'_>, key: &str) -> Result<u64> {
    get_meta(connection, key)?
        .ok_or_else(|| Error::IdentityMismatch(format!("missing {key}")))?
        .parse()
        .map_err(|_| Error::IdentityMismatch(key.into()))
}

fn meta_hash(connection: &Connection<'_>, key: &str) -> Result<LogHash> {
    parse_hash(
        &get_meta(connection, key)?
            .ok_or_else(|| Error::IdentityMismatch(format!("missing {key}")))?,
    )
}

fn parse_hash(value: &str) -> Result<LogHash> {
    LogHash::from_hex(value).ok_or_else(|| Error::Ladybug("stored log hash is invalid".into()))
}

fn execute(
    connection: &Connection<'_>,
    query: &str,
    parameters: Vec<(&str, Value)>,
) -> Result<Vec<Vec<Value>>> {
    let mut statement = connection.prepare(query).map_err(ladybug_error)?;
    if parameters.is_empty() {
        return connection
            .query(query)
            .map(|result| result.collect())
            .map_err(ladybug_error);
    }
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

fn length_prefixed(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(8 + value.len());
    encoded.extend_from_slice(
        &u64::try_from(value.len())
            .expect("usize fits in u64")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(value);
    encoded
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

fn remove_failed_install(path: &Path, parent: &Path) {
    let _ = fs::remove_file(path);
    remove_sidecars(path);
    let _ = File::open(parent).and_then(|directory| directory.sync_all());
}

fn ladybug_error(error: lbug::Error) -> Error {
    Error::Ladybug(error.to_string())
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
        unknown_version[4..6].copy_from_slice(&2_u16.to_be_bytes());
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
}

#[cfg(test)]
mod query_tests {
    use super::*;
    use proptest::prelude::*;

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
    fn admission_injects_one_extra_row_for_explicit_limit_errors() {
        let admitted = admit_read_only_query("MATCH (v:RhizaDocument) RETURN v.id", 10).unwrap();
        assert!(admitted.statement.ends_with("LIMIT 5"));
        assert_eq!(admitted.allowed_rows, 4);

        let admitted =
            admit_read_only_query("MATCH (v:RhizaDocument) RETURN v.id LIMIT 3", 10).unwrap();
        assert!(admitted.statement.ends_with("LIMIT 3"));

        let projections = std::iter::repeat_n("v.id", 4)
            .collect::<Vec<_>>()
            .join(", ");
        let admitted = admit_read_only_query(
            &format!("MATCH (v:RhizaDocument) RETURN {projections}"),
            usize::MAX,
        )
        .unwrap();
        assert_eq!(admitted.allowed_rows, 1);
        assert!(admitted.statement.ends_with("LIMIT 2"));
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
            prop_assert!(admit_read_only_query(&query, 10).is_ok());
        }
    }
}
