#![cfg_attr(
    not(any(feature = "sql", feature = "graph", feature = "kv")),
    allow(dead_code, unreachable_code, unused_imports, unused_variables)
)]

use std::{
    collections::{HashMap, HashSet},
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

#[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
compile_error!("rhiza-node requires at least one execution profile feature: sql, graph, or kv");

#[cfg(feature = "graph")]
use std::collections::BTreeMap;
#[cfg(feature = "sql")]
use std::sync::atomic::AtomicUsize;

use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Extension, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
#[cfg(feature = "sql")]
use rhiza_archive::SnapshotRecord;
use rhiza_core::{
    Command, CommandKind, ConfigChange, ConfigurationState, EntryType, ExecutionProfile, LogAnchor,
    LogEntry, LogHash, LogIndex, RecoveryAnchor, StoredCommand,
};
#[cfg(feature = "graph")]
use rhiza_graph::{
    encode_replicated_graph_command, CanonicalF64, GraphColumn, GraphInternalId, GraphLogicalType,
    GraphNode, GraphParameterValue, GraphQueryResult, GraphRel, GraphResultValue,
    LadybugFileEffectV1, LadybugStateMachine, RequestRecord as GraphRequestRecord, LGFX_V1_MAGIC,
};
#[cfg(feature = "kv")]
use rhiza_kv::{
    encode_replicated_kv_batch, encode_replicated_kv_command, KvRequestRecord, RedbStateMachine,
};
#[cfg(feature = "kv")]
pub use rhiza_kv::{KvScanResult, KvScanRow, MAX_KV_SCAN_RESULT_BYTES, MAX_KV_SCAN_ROWS};
#[cfg(feature = "sql")]
use rhiza_log::{decode_segment_for_cluster, write_segment_file};
use rhiza_log::{FileLogStore, IndexRange, LogStore};
#[cfg(feature = "sql")]
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
#[cfg(feature = "sql")]
use rhiza_quepaxa::Consensus;
use rhiza_quepaxa::{
    CertifiedDecisionInspection, DecisionInspection, DecisionProof, Membership, RecordRequest,
    RecordSummary, RecorderFileStore, RecorderRpc, RejectReason, ThreeNodeConsensus,
};
#[cfg(feature = "sql")]
use rhiza_sql::{
    decode_qwal_v1, encode_put_request, encode_sql_command, restore_snapshot_file,
    RecoverySnapshot, RequestConflict, RequestOutcome, SqlCommand, SqlCommandResult,
    SqlEffectPreparation, SqlQueryResult, SqlStatement, SqlValue, SqliteStateMachine,
    MAX_SQL_STATEMENTS, QWAL_V1_MAGIC,
};
#[cfg(not(feature = "sql"))]
type SqlCommandResult = ();

mod admin;
pub mod durability;
#[cfg(feature = "graph")]
mod graph;
#[cfg(feature = "kv")]
mod kv;
mod recorder_tcp;
pub use admin::*;
pub use durability::{
    restore_checkpoint_to_fresh_data_dir, restore_checkpoint_to_fresh_data_dir_for_node,
    restore_successor_checkpoint_to_fresh_data_dir, CheckpointCoordinator, DurabilityError,
    DurabilityHealth, DurabilityMode, SuccessorRestorePreparation,
};
#[cfg(feature = "graph")]
pub use graph::*;
#[cfg(feature = "kv")]
pub use kv::*;
#[cfg(feature = "recorder-postcard-rpc")]
pub use recorder_tcp::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls,
    RecorderPostcardRpcTlsClientConfig, RecorderPostcardRpcTlsServerConfig,
    TcpPostcardRpcRecorderClient,
};
pub use recorder_tcp::{
    serve_recorder_tcp, serve_recorder_tcp_tls, validate_recorder_tcp_endpoint,
    RecorderTlsClientConfig, RecorderTlsServerConfig, TcpPostcardRecorderClient,
};

pub const MAX_FETCH_ENTRIES: u32 = 1_024;
pub const MAX_COMMAND_BYTES: usize = 256 * 1024;
pub const MAX_REQUEST_ID_BYTES: usize = 256;
pub const MAX_KEY_BYTES: usize = 4 * 1024;
pub const MAX_VALUE_BYTES: usize = 240 * 1024;
pub const MAX_HTTP_BODY_BYTES: usize = MAX_COMMAND_BYTES * 6 + 16 * 1024;
pub const DEFAULT_CLIENT_CONCURRENCY: usize = 16;
pub const DEFAULT_PEER_CONCURRENCY: usize = 32;
pub const DEFAULT_WRITER_BATCH_MAX: usize = 8;
const MAX_WRITE_BATCH_MEMBERS: usize = 64;
pub const DEFAULT_WRITER_BATCH_WINDOW: Duration = Duration::from_micros(500);
pub const PROTOCOL_VERSION: &str = "1";
pub const RECORDER_PROTOCOL_VERSION: &str = "2";
const RECORDER_WIRE_VERSION: u16 = 2;
pub const VERSION_HEADER: &str = "x-rhiza-version";
pub const NODE_ID_HEADER: &str = "x-rhiza-node-id";
pub const RECOVERY_GENERATION_HEADER: &str = "x-rhiza-recovery-generation";
pub const RECORDER_PATH: &str = "/v1/quepaxa/recorder";
pub const RECORDER_IDENTITY_PATH: &str = "/v2/quepaxa/recorder/identity";
pub const RECORDER_STORE_COMMAND_PATH: &str = "/v2/quepaxa/recorder/store-command";
pub const RECORDER_FETCH_COMMAND_PATH: &str = "/v2/quepaxa/recorder/fetch-command";
pub const RECORDER_INSPECT_PROOF_PATH: &str = "/v2/quepaxa/recorder/inspect-proof";
pub const RECORDER_INSPECT_RECORD_PATH: &str = "/v2/quepaxa/recorder/inspect-record";
pub const RECORDER_RECORD_PATH: &str = "/v2/quepaxa/recorder/record";
pub const RECORDER_INSTALL_PROOF_PATH: &str = "/v2/quepaxa/recorder/install-decision-proof";
pub const LOG_FETCH_PATH: &str = "/v1/log/fetch";
#[cfg(feature = "sql")]
pub const WRITE_PATH: &str = "/v1/write";
#[cfg(feature = "sql")]
pub const READ_PATH: &str = "/v1/read";
#[cfg(feature = "sql")]
pub const SQL_EXECUTE_PATH: &str = "/v1/sql/execute";
#[cfg(feature = "sql")]
pub const SQL_QUERY_PATH: &str = "/v1/sql/query";
#[cfg(feature = "graph")]
pub const GRAPH_PUT_DOCUMENT_PATH: &str = "/v1/graph/documents/put";
#[cfg(feature = "graph")]
pub const GRAPH_DELETE_DOCUMENT_PATH: &str = "/v1/graph/documents/delete";
#[cfg(feature = "graph")]
pub const GRAPH_GET_DOCUMENT_PATH: &str = "/v1/graph/documents/get";
#[cfg(feature = "graph")]
pub const GRAPH_QUERY_PATH: &str = "/v1/graph/query";
#[cfg(feature = "kv")]
pub const KV_PUT_PATH: &str = "/v1/kv/put";
#[cfg(feature = "kv")]
pub const KV_DELETE_PATH: &str = "/v1/kv/delete";
#[cfg(feature = "kv")]
pub const KV_GET_PATH: &str = "/v1/kv/get";
#[cfg(feature = "kv")]
pub const KV_SCAN_PATH: &str = "/v1/kv/scan";
#[cfg(feature = "sql")]
pub const SQL_EXECUTE_RESPONSE_VERSION: u16 = 1;
pub const LIVEZ_PATH: &str = "/livez";
pub const READYZ_PATH: &str = "/readyz";
const MAX_STARTUP_RECOVERY_ENTRIES: usize = 100_000;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT_WRITE_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const SYNC_FLUSH_RETRY_INITIAL: Duration = Duration::from_millis(50);
const SYNC_FLUSH_RETRY_MAX: Duration = Duration::from_secs(1);
#[cfg(feature = "sql")]
pub const DEFAULT_SQL_MAX_ROWS: u32 = 1_000;
#[cfg(feature = "sql")]
pub const MAX_SQL_MAX_ROWS: u32 = 10_000;
#[cfg(feature = "sql")]
pub const MAX_SQL_RESULT_BYTES: usize = 1024 * 1024;
#[cfg(feature = "sql")]
pub const MAX_SQL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
#[cfg(feature = "kv")]
pub const DEFAULT_KV_SCAN_LIMIT: u32 = 100;
#[cfg(feature = "kv")]
pub const MAX_KV_SCAN_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
#[cfg(feature = "graph")]
pub const DEFAULT_GRAPH_MAX_ROWS: u32 = 1_000;
#[cfg(feature = "graph")]
pub const MAX_GRAPH_MAX_ROWS: u32 = 10_000;
#[cfg(feature = "graph")]
pub const MAX_GRAPH_RESULT_BYTES: usize = 1024 * 1024;
#[cfg(feature = "graph")]
pub const MAX_GRAPH_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
#[cfg(feature = "graph")]
const GRAPH_QUERY_TIMEOUT_MS: u64 = 5_000;

pub fn effective_cluster_id(
    profile: ExecutionProfile,
    logical_cluster_id: &str,
) -> Result<String, ConfigError> {
    if let Some(actual) = canonical_cluster_profile(logical_cluster_id) {
        if actual != profile {
            return Err(ConfigError::ClusterIdProfileMismatch {
                expected: profile,
                actual,
            });
        }
        return Ok(logical_cluster_id.to_owned());
    }
    Ok(format!("rhiza:{}:{logical_cluster_id}", profile.as_str()))
}

pub const fn execution_profile_compiled(profile: ExecutionProfile) -> bool {
    match profile {
        ExecutionProfile::Sqlite => cfg!(feature = "sql"),
        ExecutionProfile::Graph => cfg!(feature = "graph"),
        ExecutionProfile::Kv => cfg!(feature = "kv"),
    }
}

fn canonical_cluster_profile(cluster_id: &str) -> Option<ExecutionProfile> {
    [
        ("rhiza:sql:", ExecutionProfile::Sqlite),
        ("rhiza:graph:", ExecutionProfile::Graph),
        ("rhiza:kv:", ExecutionProfile::Kv),
    ]
    .into_iter()
    .find_map(|(prefix, profile)| cluster_id.starts_with(prefix).then_some(profile))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    EmptyClusterId,
    ClusterIdProfileMismatch {
        expected: ExecutionProfile,
        actual: ExecutionProfile,
    },
    EmptyNodeId,
    EmptyDataDir,
    InvalidEpoch,
    InvalidConfigId,
    InvalidRecoveryGeneration,
    InvalidWriterBatchMax(usize),
    InvalidWriterBatchWindow,
    EmptyPeerNodeId,
    EmptyPeerBaseUrl,
    InvalidPeerBaseUrl(String),
    EmptyPeerToken,
    DuplicatePeerToken,
    InvalidPeerCount(usize),
    DuplicatePeerNodeId(String),
    LocalNodeMissing,
    PeerMembershipMismatch,
    EmptyClientToken,
    ClientTokenConflictsWithPeer,
    EmptyAdminToken,
    AdminTokenConflictsWithRuntime,
    HttpClient(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyClusterId => write!(f, "cluster_id must not be empty"),
            Self::ClusterIdProfileMismatch { expected, actual } => write!(
                f,
                "cluster_id is canonical for the {} profile, not {}",
                actual.as_str(),
                expected.as_str()
            ),
            Self::EmptyNodeId => write!(f, "node_id must not be empty"),
            Self::EmptyDataDir => write!(f, "data_dir must not be empty"),
            Self::InvalidEpoch => write!(f, "epoch must be positive"),
            Self::InvalidConfigId => write!(f, "config_id must be positive"),
            Self::InvalidRecoveryGeneration => {
                write!(f, "recovery_generation must be positive")
            }
            Self::InvalidWriterBatchMax(max) => write!(
                f,
                "writer batch max must be within 1..={MAX_WRITE_BATCH_MEMBERS}, got {max}"
            ),
            Self::InvalidWriterBatchWindow => write!(
                f,
                "writer batch window must be positive and shorter than the client deadline"
            ),
            Self::EmptyPeerNodeId => write!(f, "peer node_id must not be empty"),
            Self::EmptyPeerBaseUrl => write!(f, "peer base_url must not be empty"),
            Self::InvalidPeerBaseUrl(url) => write!(f, "invalid peer base_url: {url}"),
            Self::EmptyPeerToken => write!(f, "peer token must not be empty"),
            Self::DuplicatePeerToken => write!(f, "peer tokens must be unique"),
            Self::InvalidPeerCount(count) => {
                write!(
                    f,
                    "peer membership requires between three and seven nodes, got {count}"
                )
            }
            Self::DuplicatePeerNodeId(node_id) => {
                write!(f, "peer node_id must be unique: {node_id}")
            }
            Self::LocalNodeMissing => write!(f, "peer set must include the local node_id"),
            Self::PeerMembershipMismatch => {
                write!(
                    f,
                    "peer identities must exactly match the canonical membership"
                )
            }
            Self::EmptyClientToken => write!(f, "client token must not be empty"),
            Self::ClientTokenConflictsWithPeer => {
                write!(f, "client token must differ from every peer token")
            }
            Self::EmptyAdminToken => write!(f, "admin token must not be empty"),
            Self::AdminTokenConflictsWithRuntime => {
                write!(f, "admin token must differ from client and peer tokens")
            }
            Self::HttpClient(message) => write!(f, "HTTP client configuration failed: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {}

fn validate_recovery_generation(recovery_generation: u64) -> Result<(), ConfigError> {
    if recovery_generation == 0 {
        Err(ConfigError::InvalidRecoveryGeneration)
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckMode {
    HaFirst,
    DrStrong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    Local,
    ReadBarrier,
    AppliedIndex(LogIndex),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FetchLogRequest {
    pub from_index: LogIndex,
    pub max_entries: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct FetchLogResponse {
    pub entries: Vec<LogEntry>,
    pub last_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum FetchLogError {
    SnapshotRequired {
        anchor: Box<RecoveryAnchor>,
    },
    Gap {
        expected: LogIndex,
        actual: Option<LogIndex>,
    },
    Decode {
        message: String,
    },
    Transport {
        message: String,
    },
    InvalidAnchor {
        expected: LogHash,
        actual: LogHash,
    },
    InvalidEntry {
        index: LogIndex,
        message: String,
    },
    ForeignIdentity {
        index: LogIndex,
    },
    InvalidRequest {
        message: String,
    },
}

impl fmt::Display for FetchLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotRequired { anchor } => {
                write!(
                    f,
                    "snapshot restore required at qlog anchor {}",
                    anchor.compacted().index()
                )
            }
            Self::Gap { expected, actual } => {
                write!(f, "qlog gap: expected {expected}, got {actual:?}")
            }
            Self::Decode { message } => write!(f, "qlog response decode failed: {message}"),
            Self::Transport { message } => write!(f, "qlog transport failed: {message}"),
            Self::InvalidAnchor { .. } => write!(f, "qlog response has an invalid anchor"),
            Self::InvalidEntry { index, message } => {
                write!(f, "qlog entry {index} is invalid: {message}")
            }
            Self::ForeignIdentity { index } => {
                write!(f, "qlog entry {index} has a foreign identity")
            }
            Self::InvalidRequest { message } => write!(f, "invalid qlog request: {message}"),
        }
    }
}

impl std::error::Error for FetchLogError {}

pub trait LogPeer: Send + Sync {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InMemoryLogPeer {
    entries: Vec<LogEntry>,
    anchor: Option<RecoveryAnchor>,
}

impl InMemoryLogPeer {
    pub fn new(mut entries: Vec<LogEntry>) -> Self {
        entries.sort_by_key(|entry| entry.index);
        Self {
            entries,
            anchor: None,
        }
    }

    pub fn with_anchor(mut entries: Vec<LogEntry>, anchor: RecoveryAnchor) -> Self {
        entries.sort_by_key(|entry| entry.index);
        Self {
            entries,
            anchor: Some(anchor),
        }
    }
}

impl LogPeer for InMemoryLogPeer {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        if let Some(anchor) = &self.anchor {
            if request.from_index <= anchor.compacted().index() {
                return Err(FetchLogError::SnapshotRequired {
                    anchor: Box::new(anchor.clone()),
                });
            }
        }
        let entries = self
            .entries
            .iter()
            .filter(|entry| entry.index >= request.from_index)
            .take(request.max_entries as usize)
            .cloned()
            .collect();
        let last_index = self
            .entries
            .last()
            .map(|entry| entry.index)
            .or_else(|| {
                self.anchor
                    .as_ref()
                    .map(|anchor| anchor.compacted().index())
            })
            .unwrap_or(0);
        Ok(FetchLogResponse {
            entries,
            last_index,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct RecorderWire<T> {
    version: u16,
    body: T,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", content = "body")]
enum RecorderV2Result<T> {
    Ok(T),
    Rejected(RejectReason),
    Error(String),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct StoreCommandV2 {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    config_digest: LogHash,
    command_hash: LogHash,
    command: StoredCommand,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct FetchCommandV2 {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    config_digest: LogHash,
    command_hash: LogHash,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct InspectProofV2 {
    slot: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct InstallProofV2 {
    proof: DecisionProof,
    members: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct HttpRecorderClient {
    base_url: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    client: Arc<std::sync::OnceLock<reqwest::blocking::Client>>,
}

impl HttpRecorderClient {
    pub fn new(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        Self::new_with_recovery_generation(base_url, local_node_id, peer_token, 1)
    }

    pub fn new_with_recovery_generation(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        let peer = PeerConfig::new(local_node_id, base_url, peer_token)?;
        Ok(Self {
            base_url: peer.base_url,
            local_node_id: peer.node_id,
            peer_token: peer.token,
            recovery_generation,
            client: Arc::new(std::sync::OnceLock::new()),
        })
    }

    pub fn with_recovery_generation(
        mut self,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        self.recovery_generation = recovery_generation;
        Ok(self)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn client(&self) -> rhiza_quepaxa::Result<&reqwest::blocking::Client> {
        if self.client.get().is_none() {
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(HTTP_CONNECT_TIMEOUT)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build()
                .map_err(|error| rhiza_quepaxa::Error::Io(error.to_string()))?;
            let _ = self.client.set(client);
        }
        self.client
            .get()
            .ok_or_else(|| rhiza_quepaxa::Error::Io("HTTP client initialization failed".into()))
    }

    fn post_v2<T, U>(&self, path: &str, body: T) -> rhiza_quepaxa::Result<U>
    where
        T: serde::Serialize,
        U: serde::de::DeserializeOwned,
    {
        let response = self
            .client()?
            .post(self.url(path))
            .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, &self.local_node_id)
            .header(
                RECOVERY_GENERATION_HEADER,
                self.recovery_generation.to_string(),
            )
            .bearer_auth(&self.peer_token)
            .json(&RecorderWire {
                version: RECORDER_WIRE_VERSION,
                body,
            })
            .send()
            .map_err(|error| rhiza_quepaxa::Error::Io(error.to_string()))?;
        let status = response.status();
        let wire = response
            .json::<RecorderWire<RecorderV2Result<U>>>()
            .map_err(|error| rhiza_quepaxa::Error::Decode(error.to_string()))?;
        if wire.version != RECORDER_WIRE_VERSION {
            return Err(rhiza_quepaxa::Error::Decode(
                "recorder wire version mismatch".into(),
            ));
        }
        match wire.body {
            RecorderV2Result::Ok(value) if status.is_success() => Ok(value),
            RecorderV2Result::Ok(_) => Err(rhiza_quepaxa::Error::Io(format!(
                "recorder rpc returned HTTP {status}"
            ))),
            RecorderV2Result::Rejected(reason) => Err(rhiza_quepaxa::Error::Rejected(reason)),
            RecorderV2Result::Error(message) => Err(rhiza_quepaxa::Error::Io(message)),
        }
    }
}

impl RecorderRpc for HttpRecorderClient {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.post_v2(RECORDER_IDENTITY_PATH, ())
    }

    fn store_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.post_v2(
            RECORDER_STORE_COMMAND_PATH,
            StoreCommandV2 {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            },
        )
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        self.post_v2(
            RECORDER_FETCH_COMMAND_PATH,
            FetchCommandV2 {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            },
        )
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.post_v2(RECORDER_RECORD_PATH, request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.post_v2(
            RECORDER_INSTALL_PROOF_PATH,
            InstallProofV2 {
                proof,
                members: membership.members().to_vec(),
            },
        )
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.post_v2(RECORDER_INSPECT_PROOF_PATH, InspectProofV2 { slot })
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.post_v2(RECORDER_INSPECT_RECORD_PATH, InspectProofV2 { slot })
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug)]
pub struct HttpLogPeer {
    base_url: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    client: Arc<std::sync::OnceLock<reqwest::blocking::Client>>,
}

impl HttpLogPeer {
    pub fn new(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        Self::new_with_recovery_generation(base_url, local_node_id, peer_token, 1)
    }

    pub fn new_with_recovery_generation(
        base_url: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        let peer = PeerConfig::new(local_node_id, base_url, peer_token)?;
        Ok(Self {
            base_url: peer.base_url,
            local_node_id: peer.node_id,
            peer_token: peer.token,
            recovery_generation,
            client: Arc::new(std::sync::OnceLock::new()),
        })
    }

    pub fn with_recovery_generation(
        mut self,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        self.recovery_generation = recovery_generation;
        Ok(self)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn client(&self) -> Result<&reqwest::blocking::Client, FetchLogError> {
        if self.client.get().is_none() {
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(HTTP_CONNECT_TIMEOUT)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build()
                .map_err(|error| FetchLogError::Transport {
                    message: error.to_string(),
                })?;
            let _ = self.client.set(client);
        }
        self.client.get().ok_or_else(|| FetchLogError::Transport {
            message: "HTTP client initialization failed".into(),
        })
    }
}

impl LogPeer for HttpLogPeer {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        let response = self
            .client()?
            .post(self.url(LOG_FETCH_PATH))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, &self.local_node_id)
            .header(
                RECOVERY_GENERATION_HEADER,
                self.recovery_generation.to_string(),
            )
            .bearer_auth(&self.peer_token)
            .json(&request)
            .send()
            .map_err(|err| FetchLogError::Transport {
                message: err.to_string(),
            })?;
        let status = response.status();
        match response
            .json::<FetchLogHttpResponse>()
            .map_err(|err| FetchLogError::Decode {
                message: err.to_string(),
            })? {
            FetchLogHttpResponse::Fetched(response) if status.is_success() => Ok(response),
            FetchLogHttpResponse::Fetched(_) => Err(FetchLogError::Transport {
                message: format!("log rpc returned HTTP {status}"),
            }),
            FetchLogHttpResponse::Failed(error) => Err(error),
        }
    }
}

#[derive(Clone)]
struct RecorderRouteState<R> {
    recorder: R,
    peers: Vec<PeerConfig>,
}

#[derive(Clone)]
struct AuthenticatedPeer(String);

#[derive(Clone)]
struct LogRouteState<P> {
    peer: P,
}

#[derive(Clone)]
struct NodeRouteState {
    runtime: Arc<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    write_operations: Arc<tokio::sync::Mutex<HashMap<String, WriteOperation>>>,
    writer: tokio::sync::mpsc::Sender<QueuedWrite>,
}

#[derive(Clone)]
struct WriteOperation {
    payload: Vec<u8>,
    result: tokio::sync::watch::Receiver<Option<WriteOperationResult>>,
}

#[derive(Clone)]
enum WriteOperationResult {
    Runtime(Result<ClientWriteResponse, NodeError>),
    DurabilityUnavailable,
}

#[derive(Clone)]
enum ClientWriteResponse {
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    Unavailable,
    #[cfg(feature = "sql")]
    KeyValue(WriteResponse),
    #[cfg(feature = "sql")]
    Sql(SqlExecuteResponse),
    #[cfg(feature = "graph")]
    Graph(GraphMutationOutcome),
    #[cfg(feature = "kv")]
    Kv(KvMutationOutcome),
}

struct QueuedWrite {
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    sender: tokio::sync::watch::Sender<Option<WriteOperationResult>>,
}

enum QueuedOperation {
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    Unavailable,
    #[cfg(feature = "sql")]
    KeyValue { key: String, value: String },
    #[cfg(feature = "sql")]
    Sql(SqlCommand),
    #[cfg(feature = "graph")]
    Graph(GraphCommandV1),
    #[cfg(feature = "kv")]
    Kv(KvCommandV1),
}

struct RuntimeBatchMember {
    #[cfg(feature = "sql")]
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
}

#[cfg(feature = "kv")]
fn classify_pending_request(
    canonical_by_request: &mut HashMap<String, usize>,
    members: &[RuntimeBatchMember],
    index: usize,
    request_id: &str,
) -> Result<Option<usize>, NodeError> {
    let Some(&canonical) = canonical_by_request.get(request_id) else {
        canonical_by_request.insert(request_id.to_owned(), index);
        return Ok(None);
    };
    if members[canonical].payload == members[index].payload {
        Ok(Some(canonical))
    } else {
        Err(NodeError::InvalidRequest(format!(
            "request id {request_id:?} was reused with another command in the same writer batch"
        )))
    }
}

impl ClientWriteResponse {
    fn applied_index(&self) -> LogIndex {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::KeyValue(response) => response.applied_index,
            #[cfg(feature = "sql")]
            Self::Sql(response) => response.applied_index,
            #[cfg(feature = "graph")]
            Self::Graph(response) => response.applied_index(),
            #[cfg(feature = "kv")]
            Self::Kv(response) => response.applied_index(),
        }
    }
}

#[cfg(feature = "sql")]
#[derive(Clone)]
pub struct NodeService {
    runtime: Arc<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    #[cfg(feature = "sql")]
    sql_reads_in_flight: Arc<AtomicUsize>,
}

#[cfg(feature = "sql")]
struct SqlReadActivity {
    active: Arc<AtomicUsize>,
}

#[cfg(feature = "sql")]
impl SqlReadActivity {
    fn enter(active: &Arc<AtomicUsize>) -> (Self, bool) {
        let previous = active.fetch_add(1, Ordering::Relaxed);
        debug_assert_ne!(previous, usize::MAX);
        (
            Self {
                active: active.clone(),
            },
            previous == 0,
        )
    }
}

#[cfg(feature = "sql")]
impl Drop for SqlReadActivity {
    fn drop(&mut self) {
        let previous = self.active.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0);
    }
}

#[cfg(feature = "sql")]
impl NodeService {
    pub fn new(runtime: Arc<NodeRuntime>, coordinator: Option<Arc<CheckpointCoordinator>>) -> Self {
        Self {
            runtime,
            coordinator,
            #[cfg(feature = "sql")]
            sql_reads_in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[cfg(feature = "sql")]
    pub async fn put(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, NodeError> {
        self.write(WriteRequest {
            request_id: request_id.into(),
            key: key.into(),
            value: value.into(),
        })
        .await
    }

    #[cfg(feature = "sql")]
    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, NodeError> {
        self.write_allowed()?;
        let runtime = self.runtime.clone();
        let response = tokio::task::spawn_blocking(move || {
            runtime.write(&request.request_id, &request.key, &request.value)
        })
        .await
        .map_err(node_service_task_error)??;
        self.confirm_committed(response.applied_index).await?;
        Ok(response)
    }

    #[cfg(feature = "sql")]
    pub async fn execute_sql(&self, command: SqlCommand) -> Result<SqlExecuteResponse, NodeError> {
        self.write_allowed()?;
        let runtime = self.runtime.clone();
        let response =
            tokio::task::spawn_blocking(move || runtime.execute_sql_with_results(command))
                .await
                .map_err(node_service_task_error)??;
        self.confirm_committed(response.applied_index).await?;
        Ok(response)
    }

    #[cfg(feature = "sql")]
    pub async fn read(
        &self,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ReadResponse, NodeError> {
        let runtime = self.runtime.clone();
        let key = key.to_owned();
        self.run_sql_read_operation(consistency, move || runtime.read(&key, consistency))
            .await
            .map_err(node_service_task_error)?
    }

    #[cfg(feature = "sql")]
    pub async fn query(
        &self,
        statement: SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        let runtime = self.runtime.clone();
        self.run_sql_read_operation(consistency, move || {
            runtime.query_sql(&statement, consistency, max_rows)
        })
        .await
        .map_err(node_service_task_error)?
    }

    #[cfg(feature = "sql")]
    async fn run_sql_read_operation<F, T>(
        &self,
        consistency: ReadConsistency,
        operation: F,
    ) -> Result<T, tokio::task::JoinError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if consistency == ReadConsistency::ReadBarrier {
            return run_read_operation(consistency, operation).await;
        }
        let (activity, sole_read) = SqlReadActivity::enter(&self.sql_reads_in_flight);
        let operation = move || {
            let _activity = activity;
            operation()
        };
        if sole_read {
            run_read_operation(consistency, operation).await
        } else {
            tokio::task::spawn_blocking(operation).await
        }
    }

    #[cfg(feature = "sql")]
    fn write_allowed(&self) -> Result<(), NodeError> {
        self.coordinator
            .as_ref()
            .map_or(Ok(()), |coordinator| coordinator.write_allowed())
            .map_err(|error| NodeError::Unavailable(error.to_string()))
    }

    #[cfg(feature = "sql")]
    async fn confirm_committed(&self, index: LogIndex) -> Result<(), NodeError> {
        confirm_write_durability(self.runtime.as_ref(), self.coordinator.as_deref(), index)
            .await
            .map_err(|error| NodeError::Unavailable(error.to_string()))
    }
}

#[cfg(feature = "sql")]
fn node_service_task_error(error: tokio::task::JoinError) -> NodeError {
    NodeError::Fatal(format!("node service task failed: {error}"))
}

#[doc(hidden)]
pub async fn run_read_operation<F, T>(
    consistency: ReadConsistency,
    operation: F,
) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    if consistency != ReadConsistency::ReadBarrier
        && matches!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        )
    {
        Ok(tokio::task::block_in_place(operation))
    } else {
        tokio::task::spawn_blocking(operation).await
    }
}

#[derive(Clone)]
struct PeerGateState {
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    protocol_version: &'static str,
    slots: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone)]
struct ClientGateState {
    runtime: Arc<NodeRuntime>,
    slots: Arc<tokio::sync::Semaphore>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
}

#[derive(Clone)]
struct RuntimeLogPeer {
    runtime: Arc<NodeRuntime>,
}

impl LogPeer for RuntimeLogPeer {
    fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        self.runtime.fetch_log(request)
    }
}

fn fetch_runtime_log(
    runtime: &NodeRuntime,
    request: FetchLogRequest,
) -> Result<FetchLogResponse, FetchLogError> {
    if request.from_index == 0 || request.max_entries > MAX_FETCH_ENTRIES {
        return Err(FetchLogError::InvalidRequest {
            message: "invalid fetch bounds".into(),
        });
    }
    let state = runtime
        .log_store
        .logical_state()
        .map_err(|error| FetchLogError::Transport {
            message: error.to_string(),
        })?;
    if let Some(anchor) = state.anchor {
        if request.from_index <= anchor.compacted().index() {
            return Err(FetchLogError::SnapshotRequired {
                anchor: Box::new(anchor),
            });
        }
    }
    let last_index = state.tip.map_or(0, |tip| tip.index());
    if request.max_entries == 0 || request.from_index > last_index {
        return Ok(FetchLogResponse {
            entries: Vec::new(),
            last_index,
        });
    }
    let end = request
        .from_index
        .saturating_add(u64::from(request.max_entries) - 1)
        .min(last_index);
    let range = IndexRange::new(request.from_index, end).map_err(|error| {
        FetchLogError::InvalidRequest {
            message: error.to_string(),
        }
    })?;
    let entries =
        runtime
            .log_store
            .read_range(range)
            .map_err(|error| FetchLogError::Transport {
                message: error.to_string(),
            })?;
    Ok(FetchLogResponse {
        entries,
        last_index,
    })
}

pub fn recorder_router<R, P>(recorder: R, peers: P) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: Into<Vec<PeerConfig>>,
{
    recorder_router_for_generation(recorder, peers, 1)
}

pub fn recorder_router_for_generation<R, P>(
    recorder: R,
    peers: P,
    recovery_generation: u64,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: Into<Vec<PeerConfig>>,
{
    recorder_routes(
        recorder,
        peers.into(),
        recovery_generation,
        Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY)),
    )
    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

pub fn log_peer_router<P, C>(peer: P, peers: C) -> Router
where
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    log_peer_router_for_generation(peer, peers, 1)
}

pub fn log_peer_router_for_generation<P, C>(peer: P, peers: C, recovery_generation: u64) -> Router
where
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    log_routes(
        peer,
        peers.into(),
        recovery_generation,
        Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY)),
    )
    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

pub fn node_rpc_router<R, P, C>(recorder: R, peer: P, peers: C) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    node_rpc_router_for_generation(recorder, peer, peers, 1)
}

pub fn node_rpc_router_for_generation<R, P, C>(
    recorder: R,
    peer: P,
    peers: C,
    recovery_generation: u64,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    node_rpc_router_with_limits_for_generation(
        recorder,
        peer,
        peers,
        DEFAULT_PEER_CONCURRENCY,
        DEFAULT_PEER_CONCURRENCY,
        recovery_generation,
    )
}

pub fn node_rpc_router_with_limits<R, P, C>(
    recorder: R,
    peer: P,
    peers: C,
    recorder_concurrency: usize,
    log_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    node_rpc_router_with_limits_for_generation(
        recorder,
        peer,
        peers,
        recorder_concurrency,
        log_concurrency,
        1,
    )
}

pub fn node_rpc_router_with_limits_for_generation<R, P, C>(
    recorder: R,
    peer: P,
    peers: C,
    recorder_concurrency: usize,
    log_concurrency: usize,
    recovery_generation: u64,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    P: LogPeer + Clone + Send + Sync + 'static,
    C: Into<Vec<PeerConfig>>,
{
    let peers = peers.into();
    let recorder_slots = Arc::new(tokio::sync::Semaphore::new(recorder_concurrency));
    let log_slots = Arc::new(tokio::sync::Semaphore::new(log_concurrency));
    recorder_routes(recorder, peers.clone(), recovery_generation, recorder_slots)
        .merge(log_routes(peer, peers, recovery_generation, log_slots))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

pub fn node_router<R>(runtime: Arc<NodeRuntime>, recorder: R) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_limits(
        runtime,
        recorder,
        DEFAULT_CLIENT_CONCURRENCY,
        DEFAULT_PEER_CONCURRENCY,
    )
}

pub fn node_router_with_limits<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    client_concurrency: usize,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_optional_checkpoint(
        runtime,
        recorder,
        None,
        client_concurrency,
        peer_concurrency,
    )
}

pub fn node_router_with_checkpoint<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    coordinator: Arc<CheckpointCoordinator>,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_checkpoint_and_limits(
        runtime,
        recorder,
        coordinator,
        DEFAULT_CLIENT_CONCURRENCY,
        DEFAULT_PEER_CONCURRENCY,
    )
}

pub fn node_router_with_checkpoint_and_limits<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    coordinator: Arc<CheckpointCoordinator>,
    client_concurrency: usize,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    node_router_with_optional_checkpoint(
        runtime,
        recorder,
        Some(coordinator),
        client_concurrency,
        peer_concurrency,
    )
}

fn node_router_with_optional_checkpoint<R>(
    runtime: Arc<NodeRuntime>,
    recorder: R,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    client_concurrency: usize,
    peer_concurrency: usize,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let peers = runtime.config.peers.clone();
    let recovery_generation = runtime.config.recovery_generation();
    let client_slots = Arc::new(tokio::sync::Semaphore::new(client_concurrency));
    let recorder_slots = Arc::new(tokio::sync::Semaphore::new(peer_concurrency));
    let log_slots = Arc::new(tokio::sync::Semaphore::new(peer_concurrency));
    let write_operations = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let (writer, writer_receiver) = tokio::sync::mpsc::channel(client_concurrency.max(1));
    tokio::spawn(writer_loop(
        Arc::downgrade(&runtime),
        coordinator.clone(),
        write_operations.clone(),
        writer_receiver,
        runtime.config.writer_batch_max,
        runtime.config.writer_batch_window,
    ));
    #[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
    let client_routes: Router = match runtime.config().execution_profile() {
        ExecutionProfile::Sqlite => {
            #[cfg(feature = "sql")]
            {
                Router::new()
                    .route(WRITE_PATH, post(handle_write))
                    .route(READ_PATH, post(handle_read))
                    .route(SQL_EXECUTE_PATH, post(handle_sql_execute))
                    .route(SQL_QUERY_PATH, post(handle_sql_query))
            }
            #[cfg(not(feature = "sql"))]
            unreachable!("SQL runtime cannot open without the sql feature")
        }
        ExecutionProfile::Graph => {
            #[cfg(feature = "graph")]
            {
                Router::new()
                    .route(GRAPH_PUT_DOCUMENT_PATH, post(handle_graph_put_document))
                    .route(
                        GRAPH_DELETE_DOCUMENT_PATH,
                        post(handle_graph_delete_document),
                    )
                    .route(GRAPH_GET_DOCUMENT_PATH, post(handle_graph_get_document))
                    .route(GRAPH_QUERY_PATH, post(handle_graph_query))
            }
            #[cfg(not(feature = "graph"))]
            unreachable!("graph runtime cannot open without the graph feature")
        }
        ExecutionProfile::Kv => {
            #[cfg(feature = "kv")]
            {
                Router::new()
                    .route(KV_PUT_PATH, post(handle_kv_put))
                    .route(KV_DELETE_PATH, post(handle_kv_delete))
                    .route(KV_GET_PATH, post(handle_kv_get))
                    .route(KV_SCAN_PATH, post(handle_kv_scan))
            }
            #[cfg(not(feature = "kv"))]
            unreachable!("KV runtime cannot open without the kv feature")
        }
    }
    .route_layer(middleware::from_fn_with_state(
        ClientGateState {
            runtime: runtime.clone(),
            slots: client_slots,
            coordinator: coordinator.clone(),
        },
        client_gate,
    ))
    .with_state(NodeRouteState {
        runtime: runtime.clone(),
        coordinator: coordinator.clone(),
        write_operations: write_operations.clone(),
        writer: writer.clone(),
    });
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    let client_routes: Router = Router::new();
    let health_routes = Router::new()
        .route(LIVEZ_PATH, get(handle_livez))
        .route(READYZ_PATH, get(handle_readyz))
        .with_state(NodeRouteState {
            runtime: runtime.clone(),
            coordinator,
            write_operations,
            writer,
        });
    recorder_routes(recorder, peers.clone(), recovery_generation, recorder_slots)
        .merge(log_routes(
            RuntimeLogPeer { runtime },
            peers,
            recovery_generation,
            log_slots,
        ))
        .merge(client_routes)
        .merge(health_routes)
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
}

fn recorder_routes<R>(
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Router
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let recorder_peers = peers.clone();
    Router::new()
        .route(RECORDER_IDENTITY_PATH, post(handle_recorder_identity::<R>))
        .route(
            RECORDER_STORE_COMMAND_PATH,
            post(handle_recorder_store_command::<R>),
        )
        .route(
            RECORDER_FETCH_COMMAND_PATH,
            post(handle_recorder_fetch_command::<R>),
        )
        .route(
            RECORDER_INSPECT_PROOF_PATH,
            post(handle_recorder_inspect_proof::<R>),
        )
        .route(
            RECORDER_INSPECT_RECORD_PATH,
            post(handle_recorder_inspect_record::<R>),
        )
        .route(RECORDER_RECORD_PATH, post(handle_recorder_record::<R>))
        .route(
            RECORDER_INSTALL_PROOF_PATH,
            post(handle_recorder_install_proof::<R>),
        )
        .route_layer(middleware::from_fn_with_state(
            PeerGateState {
                peers,
                recovery_generation,
                protocol_version: RECORDER_PROTOCOL_VERSION,
                slots,
            },
            peer_gate,
        ))
        .with_state(RecorderRouteState {
            recorder,
            peers: recorder_peers,
        })
}

fn log_routes<P>(
    peer: P,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Router
where
    P: LogPeer + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(LOG_FETCH_PATH, post(handle_fetch_log::<P>))
        .route_layer(middleware::from_fn_with_state(
            PeerGateState {
                peers,
                recovery_generation,
                protocol_version: PROTOCOL_VERSION,
                slots,
            },
            peer_gate,
        ))
        .with_state(LogRouteState { peer })
}

async fn handle_recorder_identity<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<RecorderWire<()>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.recorder_id()
        })
        .await,
    )
}

async fn handle_recorder_store_command<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<RecorderWire<StoreCommandV2>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION || !valid_recorder_command(&request.body.command) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.store_command_for(
                body.cluster_id,
                body.epoch,
                body.config_id,
                body.config_digest,
                body.command_hash,
                body.command,
            )
        })
        .await,
    )
}

async fn handle_recorder_fetch_command<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<RecorderWire<FetchCommandV2>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let body = request.body;
            recorder.fetch_command_for(
                body.cluster_id,
                body.epoch,
                body.config_id,
                body.config_digest,
                body.command_hash,
            )
        })
        .await,
    )
}

async fn handle_recorder_inspect_proof<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<RecorderWire<InspectProofV2>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.inspect_decision_proof(request.body.slot)
        })
        .await,
    )
}

async fn handle_recorder_inspect_record<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<RecorderWire<InspectProofV2>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.inspect_record_summary(request.body.slot)
        })
        .await,
    )
}

async fn handle_recorder_record<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Extension(authenticated_peer): Extension<AuthenticatedPeer>,
    Json(request): Json<RecorderWire<RecordRequest>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION || !valid_recorder_record(&request.body) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !authenticated_proposer_admitted(
        &authenticated_peer.0,
        &request.body.proposal.proposer_id,
        &state.peers,
    ) {
        return recorder_v2_response::<RecordSummary>(Ok(Err(rhiza_quepaxa::Error::Rejected(
            RejectReason::InvalidRequest,
        ))));
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            recorder.record(request.body)
        })
        .await,
    )
}

fn valid_recorder_command(command: &StoredCommand) -> bool {
    command.payload.len() <= MAX_COMMAND_BYTES
}

fn valid_recorder_record(request: &RecordRequest) -> bool {
    !request.cluster_id.is_empty()
        && request.cluster_id.len() <= MAX_REQUEST_ID_BYTES
        && request.command.as_ref().is_none_or(valid_recorder_command)
}

fn authenticated_proposer_admitted(
    authenticated_peer_id: &str,
    proposer_id: &str,
    peers: &[PeerConfig],
) -> bool {
    // Record requests carry config identity but not its membership. Configured peers are therefore
    // the transport identity authority for records and proofs until rebuilt after a transition.
    peers
        .iter()
        .any(|peer| peer.node_id == authenticated_peer_id)
        && peers.iter().any(|peer| peer.node_id == proposer_id)
}

async fn handle_recorder_install_proof<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Extension(authenticated_peer): Extension<AuthenticatedPeer>,
    Json(request): Json<RecorderWire<InstallProofV2>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !authenticated_proposer_admitted(
        &authenticated_peer.0,
        &request.body.proof.proposal().proposer_id,
        &state.peers,
    ) {
        return recorder_v2_response::<()>(Ok(Err(rhiza_quepaxa::Error::Rejected(
            RejectReason::InvalidRequest,
        ))));
    }
    let recorder = state.recorder;
    recorder_v2_response(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let membership = Membership::from_voters(request.body.members)?;
            recorder.install_decision_proof(request.body.proof, &membership)
        })
        .await,
    )
}

fn recorder_v2_response<T: serde::Serialize>(
    result: Result<rhiza_quepaxa::Result<T>, tokio::task::JoinError>,
) -> Response {
    let (status, body) = match result {
        Ok(Ok(value)) => (StatusCode::OK, RecorderV2Result::Ok(value)),
        Ok(Err(rhiza_quepaxa::Error::Rejected(reason))) => {
            (StatusCode::CONFLICT, RecorderV2Result::Rejected(reason))
        }
        Ok(Err(error)) => (
            recorder_error_status(&error),
            RecorderV2Result::Error(error.to_string()),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            RecorderV2Result::Error(error.to_string()),
        ),
    };
    (
        status,
        Json(RecorderWire {
            version: RECORDER_WIRE_VERSION,
            body,
        }),
    )
        .into_response()
}

async fn handle_fetch_log<P>(
    State(state): State<LogRouteState<P>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<FetchLogRequest>,
) -> Response
where
    P: LogPeer + Clone + Send + Sync + 'static,
{
    if request.from_index == 0 || request.max_entries > MAX_FETCH_ENTRIES {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let peer = state.peer;
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        peer.fetch_log(request)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(FetchLogHttpResponse::Fetched(response)).into_response(),
        Ok(Err(error)) => (
            fetch_log_error_status(&error),
            Json(FetchLogHttpResponse::Failed(error)),
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(feature = "sql")]
async fn handle_write(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<WriteRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let payload = match canonical_put(&request.request_id, &request.key, &request.value) {
        Ok(payload) => payload,
        Err(error) => return node_error_response(error),
    };
    let request_id = request.request_id.clone();
    let operation = QueuedOperation::KeyValue {
        key: request.key,
        value: request.value,
    };
    coordinate_write(state, permit, request_id, payload, operation).await
}

#[cfg(feature = "sql")]
async fn handle_sql_execute(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<SqlExecuteRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    if let Err(error) = validate_field(
        "request_id",
        &request.request_id,
        MAX_REQUEST_ID_BYTES,
        false,
    ) {
        return node_error_response(error);
    }
    let command = SqlCommand {
        request_id: request.request_id.clone(),
        statements: request.statements,
    };
    let payload = match encode_sql_command_with_index(&command) {
        Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
        Ok(_) => {
            return node_error_response(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )))
        }
        Err(error) => return node_error_response(error),
    };
    let request_id = command.request_id.clone();
    coordinate_write(
        state,
        permit,
        request_id,
        payload,
        QueuedOperation::Sql(command),
    )
    .await
}

async fn coordinate_write(
    state: NodeRouteState,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
) -> Response {
    let deadline = tokio::time::Instant::now() + CLIENT_WRITE_WAIT_TIMEOUT;
    let (mut receiver, queued) = {
        let mut operations = state.write_operations.lock().await;
        if let Some(operation) = operations.get(&request_id) {
            if operation.payload != payload {
                return client_error_response(
                    StatusCode::CONFLICT,
                    "request_conflict",
                    false,
                    "request id is already in flight with a different payload",
                    None,
                );
            }
            (operation.result.clone(), None)
        } else {
            let (sender, receiver) = tokio::sync::watch::channel(None);
            operations.insert(
                request_id.clone(),
                WriteOperation {
                    payload: payload.clone(),
                    result: receiver.clone(),
                },
            );
            (
                receiver,
                Some(QueuedWrite {
                    request_id: request_id.clone(),
                    payload,
                    operation,
                    permit,
                    sender,
                }),
            )
        }
    };
    if let Some(queued) = queued {
        match tokio::time::timeout_at(deadline, state.writer.send(queued)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                state.write_operations.lock().await.remove(&request_id);
                return client_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "durability_unavailable",
                    true,
                    "writer queue is unavailable",
                    None,
                );
            }
            Err(_) => {
                state.write_operations.lock().await.remove(&request_id);
                return client_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "write_timeout",
                    true,
                    "write did not enter the queue before the response deadline",
                    None,
                );
            }
        }
    }
    let wait = async {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                return WriteOperationResult::DurabilityUnavailable;
            }
        }
    };
    match tokio::time::timeout_at(deadline, wait).await {
        Ok(WriteOperationResult::Runtime(Ok(response))) => match response {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            ClientWriteResponse::Unavailable => {
                unreachable!("no execution profiles are compiled in")
            }
            #[cfg(feature = "sql")]
            ClientWriteResponse::KeyValue(response) => Json(response).into_response(),
            #[cfg(feature = "sql")]
            ClientWriteResponse::Sql(response) => Json(response).into_response(),
            #[cfg(feature = "graph")]
            ClientWriteResponse::Graph(outcome) => {
                Json(graph_mutation_response(outcome)).into_response()
            }
            #[cfg(feature = "kv")]
            ClientWriteResponse::Kv(outcome) => Json(kv_mutation_response(outcome)).into_response(),
        },
        Ok(WriteOperationResult::Runtime(Err(error))) => node_error_response(error),
        Ok(WriteOperationResult::DurabilityUnavailable) => client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "durability_unavailable",
            true,
            "durability confirmation is unavailable",
            None,
        ),
        Err(_) => client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "write_timeout",
            true,
            "write did not complete before the response deadline",
            None,
        ),
    }
}

async fn writer_loop(
    runtime: std::sync::Weak<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    write_operations: Arc<tokio::sync::Mutex<HashMap<String, WriteOperation>>>,
    mut receiver: tokio::sync::mpsc::Receiver<QueuedWrite>,
    batch_max: usize,
    batch_window: Duration,
) {
    while let Some(first) = receiver.recv().await {
        let mut queued = Vec::with_capacity(batch_max);
        queued.push(first);
        let deadline = tokio::time::Instant::now() + batch_window;
        while queued.len() < batch_max {
            match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(next)) => queued.push(next),
                Ok(None) | Err(_) => break,
            }
        }

        let mut dispatch = Vec::with_capacity(queued.len());
        let mut members = Vec::with_capacity(queued.len());
        for queued in queued {
            members.push(RuntimeBatchMember {
                #[cfg(feature = "sql")]
                request_id: queued.request_id.clone(),
                payload: queued.payload,
                operation: queued.operation,
            });
            dispatch.push((queued.request_id, queued.sender, queued.permit));
        }

        let Some(runtime) = runtime.upgrade() else {
            for (request_id, sender, _permit) in dispatch {
                sender.send_replace(Some(WriteOperationResult::DurabilityUnavailable));
                write_operations.lock().await.remove(&request_id);
            }
            break;
        };

        let blocking_runtime = runtime.clone();
        let results =
            tokio::task::spawn_blocking(move || blocking_runtime.execute_client_batch(members))
                .await
                .unwrap_or_else(|error| {
                    (0..dispatch.len())
                        .map(|_| {
                            Err(NodeError::Fatal(format!(
                                "writer batch task failed: {error}"
                            )))
                        })
                        .collect()
                });
        let dispatch = dispatch
            .into_iter()
            .map(|(request_id, sender, permit)| {
                drop(permit);
                (request_id, sender)
            })
            .collect::<Vec<_>>();

        let committed_index = results
            .iter()
            .filter_map(|result| result.as_ref().ok().map(ClientWriteResponse::applied_index))
            .max();
        let durability_available = if let Some(index) = committed_index {
            confirm_write_durability(runtime.as_ref(), coordinator.as_deref(), index)
                .await
                .is_ok()
        } else {
            true
        };

        for ((request_id, sender), result) in dispatch.into_iter().zip(results) {
            let delivered = if !durability_available && result.is_ok() {
                WriteOperationResult::DurabilityUnavailable
            } else {
                WriteOperationResult::Runtime(result)
            };
            sender.send_replace(Some(delivered));
            write_operations.lock().await.remove(&request_id);
        }
    }
}

#[cfg(feature = "sql")]
async fn handle_sql_query(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<SqlQueryRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let max_rows = request.max_rows.unwrap_or(DEFAULT_SQL_MAX_ROWS);
    if max_rows == 0 || max_rows > MAX_SQL_MAX_ROWS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "max_rows must be between 1 and {MAX_SQL_MAX_ROWS}"
        )));
    }
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.query_sql(&request.statement, consistency, max_rows)
    })
    .await;
    match result {
        Ok(Ok(response)) => sql_query_http_response(response),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "sql")]
fn sql_query_http_response(response: SqlQueryResponse) -> Response {
    match serde_json::to_vec(&response) {
        Ok(encoded) if encoded.len() <= MAX_SQL_RESPONSE_BYTES => (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            encoded,
        )
            .into_response(),
        Ok(_) => node_error_response(NodeError::InvalidRequest(format!(
            "SQL response exceeds {MAX_SQL_RESPONSE_BYTES} bytes"
        ))),
        Err(error) => node_error_response(NodeError::InvalidRequest(error.to_string())),
    }
}

#[cfg(feature = "graph")]
async fn handle_graph_put_document(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphPutDocumentRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let value = match GraphValueV1::try_from(request.value) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let command = match GraphCommandV1::put_document(request.request_id, request.id, value) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_graph_mutation(state, permit, command).await
}

#[cfg(feature = "graph")]
async fn handle_graph_delete_document(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphDeleteDocumentRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let command = match GraphCommandV1::delete_document(request.request_id, request.id) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_graph_mutation(state, permit, command).await
}

#[cfg(feature = "graph")]
async fn execute_graph_mutation(
    state: NodeRouteState,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    command: GraphCommandV1,
) -> Response {
    let payload = match encode_replicated_graph_command(&command) {
        Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
        Ok(_) => {
            return node_error_response(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )))
        }
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    coordinate_write(
        state,
        permit,
        command.request_id().to_owned(),
        payload,
        QueuedOperation::Graph(command),
    )
    .await
}

#[cfg(feature = "graph")]
async fn handle_graph_get_document(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphGetDocumentRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.get_graph_document(&request.id, consistency)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(GraphGetDocumentResponse {
            value: response.value.map(GraphValueDto::from),
            applied_index: response.applied_index,
            hash: response.hash,
        })
        .into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "graph")]
fn with_graph_client_permit<T>(
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    response_work: impl FnOnce() -> T,
) -> T {
    let result = response_work();
    drop(permit);
    result
}

#[cfg(feature = "graph")]
async fn handle_graph_query(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<GraphQueryRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let parameters = match request
        .statement
        .parameters
        .into_iter()
        .map(|(name, value)| GraphParameterValue::try_from(value).map(|value| (name, value)))
        .collect::<Result<BTreeMap<_, _>, _>>()
    {
        Ok(parameters) => parameters,
        Err(error) => return node_error_response(error),
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let max_rows = request.max_rows.unwrap_or(DEFAULT_GRAPH_MAX_ROWS);
    if max_rows == 0 || max_rows > MAX_GRAPH_MAX_ROWS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "max_rows must be between 1 and {MAX_GRAPH_MAX_ROWS}"
        )));
    }
    let result = tokio::task::spawn_blocking(move || {
        runtime.query_graph(
            &request.statement.cypher,
            &parameters,
            consistency,
            max_rows,
        )
    })
    .await;
    with_graph_client_permit(permit, || match result {
        Ok(Ok(result)) => {
            let response = GraphQueryResponse::from(result);
            match serde_json::to_vec(&response) {
                Ok(encoded) if encoded.len() <= MAX_GRAPH_RESPONSE_BYTES => {
                    Json(response).into_response()
                }
                Ok(_) => node_error_response(NodeError::InvalidRequest(format!(
                    "graph response exceeds {MAX_GRAPH_RESPONSE_BYTES} bytes"
                ))),
                Err(error) => node_error_response(NodeError::InvalidRequest(error.to_string())),
            }
        }
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    })
}

#[cfg(feature = "kv")]
async fn handle_kv_put(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvPutRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let key = match decode_base64("key", &request.key) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let value = match decode_base64("value", &request.value) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let command = match KvCommandV1::put(request.request_id, key, value) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_kv_mutation(state, permit, command).await
}

#[cfg(feature = "kv")]
async fn handle_kv_delete(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvDeleteRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let key = match decode_base64("key", &request.key) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let command = match KvCommandV1::delete(request.request_id, key) {
        Ok(command) => command,
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    execute_kv_mutation(state, permit, command).await
}

#[cfg(feature = "kv")]
async fn execute_kv_mutation(
    state: NodeRouteState,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    command: KvCommandV1,
) -> Response {
    let payload = match encode_replicated_kv_command(&command) {
        Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
        Ok(_) => {
            return node_error_response(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )))
        }
        Err(error) => return node_error_response(NodeError::InvalidRequest(error.to_string())),
    };
    coordinate_write(
        state,
        permit,
        command.request_id().to_owned(),
        payload,
        QueuedOperation::Kv(command),
    )
    .await
}

#[cfg(feature = "kv")]
async fn handle_kv_get(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvGetRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let key = match decode_base64("key", &request.key) {
        Ok(value) => value,
        Err(error) => return node_error_response(error),
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.get_kv(&key, consistency)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(KvGetResponse {
            value: response.value.as_deref().map(encode_base64),
            applied_index: response.applied_index,
            hash: response.hash,
        })
        .into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "kv")]
enum DecodedKvScan {
    Range {
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    },
    Prefix(Vec<u8>),
}

#[cfg(feature = "kv")]
async fn handle_kv_scan(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<KvScanRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let limit = request
        .limit
        .unwrap_or(usize::try_from(DEFAULT_KV_SCAN_LIMIT).expect("u32 fits usize"));
    if limit == 0 || limit > MAX_KV_SCAN_ROWS {
        return node_error_response(NodeError::InvalidRequest(format!(
            "limit must be between 1 and {MAX_KV_SCAN_ROWS}"
        )));
    }
    let cursor = match request.cursor {
        Some(cursor) => match decode_base64("cursor", &cursor) {
            Ok(cursor) => Some(cursor),
            Err(error) => return node_error_response(error),
        },
        None => None,
    };
    let scan = match (request.prefix, request.start, request.end) {
        (Some(prefix), None, None) => match decode_base64("prefix", &prefix) {
            Ok(prefix) => DecodedKvScan::Prefix(prefix),
            Err(error) => return node_error_response(error),
        },
        (None, Some(start), end) => {
            let start = match decode_base64("start", &start) {
                Ok(start) => start,
                Err(error) => return node_error_response(error),
            };
            let end = match end {
                Some(end) => match decode_base64("end", &end) {
                    Ok(end) => Some(end),
                    Err(error) => return node_error_response(error),
                },
                None => None,
            };
            DecodedKvScan::Range { start, end }
        }
        _ => {
            return node_error_response(NodeError::InvalidRequest(
                "provide either prefix alone or start with optional end".into(),
            ))
        }
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        match scan {
            DecodedKvScan::Range { start, end } => runtime.scan_kv_range(
                &start,
                end.as_deref(),
                limit,
                cursor.as_deref(),
                consistency,
            ),
            DecodedKvScan::Prefix(prefix) => {
                runtime.scan_kv_prefix(&prefix, limit, cursor.as_deref(), consistency)
            }
        }
    })
    .await;
    match result {
        Ok(Ok(result)) => {
            let response = KvScanResponse {
                entries: result
                    .rows()
                    .iter()
                    .map(|row| KvScanEntryDto {
                        key: encode_base64(row.key()),
                        value: encode_base64(row.value()),
                    })
                    .collect(),
                next_cursor: result.next_cursor().map(encode_base64),
                applied_index: result.tip().applied_index(),
                hash: result.tip().applied_hash(),
            };
            match serde_json::to_vec(&response) {
                Ok(encoded) if encoded.len() <= MAX_KV_SCAN_RESPONSE_BYTES => {
                    Json(response).into_response()
                }
                Ok(_) => node_error_response(NodeError::ResourceExhausted(format!(
                    "KV scan response exceeds {MAX_KV_SCAN_RESPONSE_BYTES} bytes"
                ))),
                Err(error) => node_error_response(NodeError::InvalidRequest(error.to_string())),
            }
        }
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

#[cfg(feature = "sql")]
async fn handle_read(
    State(state): State<NodeRouteState>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    request: Result<Json<ReadRequest>, JsonRejection>,
) -> Response {
    let request = match client_json(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let runtime = state.runtime;
    let consistency = request
        .consistency
        .unwrap_or(runtime.config.read_consistency());
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        runtime.read(&request.key, consistency)
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(response).into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

async fn peer_gate(
    State(state): State<PeerGateState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !recovery_generation_matches(request.headers(), state.recovery_generation) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(authenticated_peer) =
        authenticated_peer(request.headers(), &state.peers, state.protocol_version)
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let permit = match state.slots.try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    request.extensions_mut().insert(permit);
    request
        .extensions_mut()
        .insert(AuthenticatedPeer(authenticated_peer));
    next.run(request).await
}

async fn client_gate(
    State(state): State<ClientGateState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !client_authenticated(request.headers(), state.runtime.config.client_token()) {
        return client_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            false,
            "client authentication failed",
            None,
        );
    }
    if let Some(response) = runtime_readiness_response(state.runtime.as_ref()) {
        return response;
    }
    if client_write_path(request.uri().path())
        && state
            .coordinator
            .as_ref()
            .is_some_and(|coordinator| coordinator.write_allowed().is_err())
    {
        return client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "writes_unavailable",
            true,
            "writes are temporarily unavailable",
            None,
        );
    }
    let permit = match state.slots.try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => {
            return client_error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                true,
                "client request capacity is exhausted",
                None,
            )
        }
    };
    request.extensions_mut().insert(permit);
    next.run(request).await
}

fn client_write_path(path: &str) -> bool {
    #[cfg(feature = "sql")]
    if matches!(path, WRITE_PATH | SQL_EXECUTE_PATH) {
        return true;
    }
    #[cfg(feature = "graph")]
    if matches!(path, GRAPH_PUT_DOCUMENT_PATH | GRAPH_DELETE_DOCUMENT_PATH) {
        return true;
    }
    #[cfg(feature = "kv")]
    if matches!(path, KV_PUT_PATH | KV_DELETE_PATH) {
        return true;
    }
    false
}

async fn handle_livez() -> StatusCode {
    StatusCode::OK
}

async fn handle_readyz(State(state): State<NodeRouteState>) -> StatusCode {
    if state.runtime.is_ready()
        && state
            .coordinator
            .as_ref()
            .is_none_or(|coordinator| coordinator.health() == DurabilityHealth::Available)
    {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn next_sync_flush_retry(current: Duration) -> Duration {
    current.saturating_mul(2).min(SYNC_FLUSH_RETRY_MAX)
}

/// Confirms that a committed write has reached the configured durability boundary.
///
/// Synchronous archive I/O failures are retried with bounded backoff until the
/// archive recovers or the runtime begins shutdown.
pub async fn confirm_write_durability(
    runtime: &NodeRuntime,
    coordinator: Option<&CheckpointCoordinator>,
    index: LogIndex,
) -> Result<(), DurabilityError> {
    let Some(coordinator) = coordinator else {
        return Ok(());
    };
    coordinator.note_committed(index);
    if !matches!(coordinator.mode(), DurabilityMode::Sync) {
        return Ok(());
    }

    let mut retry_delay = SYNC_FLUSH_RETRY_INITIAL;
    loop {
        if runtime.operation_cancelled.load(Ordering::Acquire) {
            return Err(DurabilityError::Unavailable);
        }
        match coordinator.flush_runtime(runtime, index).await {
            Ok(tip) if tip.index() >= index => return Ok(()),
            Ok(tip) => {
                return Err(DurabilityError::LocalLogGap {
                    expected: index,
                    actual: Some(tip.index()),
                })
            }
            Err(DurabilityError::Archive(_) | DurabilityError::Io(_)) => {
                let cancelled = runtime.operation_cancelled_notify.notified();
                tokio::pin!(cancelled);
                cancelled.as_mut().enable();
                if runtime.operation_cancelled.load(Ordering::Acquire) {
                    return Err(DurabilityError::Unavailable);
                }
                tokio::select! {
                    () = tokio::time::sleep(retry_delay) => {}
                    () = &mut cancelled => return Err(DurabilityError::Unavailable),
                }
                retry_delay = next_sync_flush_retry(retry_delay);
            }
            Err(error) => return Err(error),
        }
    }
}

fn authenticated_peer(
    headers: &HeaderMap,
    peers: &[PeerConfig],
    protocol_version: &str,
) -> Option<String> {
    if header_text(headers, VERSION_HEADER) != Some(protocol_version) {
        return None;
    }
    let node_id = header_text(headers, NODE_ID_HEADER)?;
    let token = bearer_token(headers)?;
    peer_credentials_authenticated(node_id, token, peers).then(|| node_id.to_owned())
}

fn peer_credentials_authenticated(node_id: &str, token: &str, peers: &[PeerConfig]) -> bool {
    peers
        .iter()
        .find(|peer| peer.node_id == node_id)
        .is_some_and(|peer| secrets_equal(peer.token.as_bytes(), token.as_bytes()))
}

fn recovery_generation_matches(headers: &HeaderMap, expected: u64) -> bool {
    let expected = expected.to_string();
    header_text(headers, RECOVERY_GENERATION_HEADER) == Some(expected.as_str())
}

fn client_authenticated(headers: &HeaderMap, expected_token: &str) -> bool {
    !expected_token.is_empty()
        && version_matches(headers)
        && bearer_token(headers)
            .is_some_and(|token| secrets_equal(expected_token.as_bytes(), token.as_bytes()))
}

fn version_matches(headers: &HeaderMap) -> bool {
    header_text(headers, VERSION_HEADER) == Some(PROTOCOL_VERSION)
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    header_text(headers, "authorization")?.strip_prefix("Bearer ")
}

fn secrets_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn node_error_response(error: NodeError) -> Response {
    let (status, code, retryable, statement_index) = match &error {
        NodeError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request", false, None),
        #[cfg(feature = "sql")]
        NodeError::InvalidSqlStatement {
            statement_index, ..
        } => (
            StatusCode::BAD_REQUEST,
            "invalid_request",
            false,
            Some(*statement_index),
        ),
        #[cfg(feature = "sql")]
        NodeError::RequestConflict(_) => (StatusCode::CONFLICT, "request_conflict", false, None),
        NodeError::PreconditionFailed(_) => {
            (StatusCode::CONFLICT, "precondition_failed", false, None)
        }
        NodeError::SnapshotRequired(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "snapshot_required",
            false,
            None,
        ),
        NodeError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", true, None),
        NodeError::ResourceExhausted(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "resource_exhausted",
            true,
            None,
        ),
        NodeError::ConfigurationTransition { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "configuration_transition",
            true,
            None,
        ),
        NodeError::Contention(_) => (StatusCode::SERVICE_UNAVAILABLE, "contention", true, None),
        NodeError::WinnerLimitExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "winner_limit_exceeded",
            true,
            None,
        ),
        NodeError::DataRootLocked(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "data_root_locked",
            false,
            None,
        ),
        NodeError::UnsupportedAckMode(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "unsupported_ack_mode",
            false,
            None,
        ),
        NodeError::ExecutionProfileMismatch { .. } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "execution_profile_mismatch",
            false,
            None,
        ),
        NodeError::Storage(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_error",
            false,
            None,
        ),
        NodeError::Reconciliation(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "reconciliation_error",
            false,
            None,
        ),
        NodeError::Invariant(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "invariant_violation",
            false,
            None,
        ),
        NodeError::Fatal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "fatal", false, None),
    };
    client_error_response(status, code, retryable, error.to_string(), statement_index)
}

#[allow(clippy::result_large_err)]
fn client_json<T>(request: Result<Json<T>, JsonRejection>) -> Result<T, Response> {
    request.map(|Json(request)| request).map_err(|rejection| {
        let status = rejection.status();
        let code = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "payload_too_large"
        } else {
            "invalid_json"
        };
        client_error_response(status, code, false, rejection.body_text(), None)
    })
}

fn client_task_error(error: tokio::task::JoinError) -> Response {
    client_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "task_failed",
        false,
        format!("request task failed: {error}"),
        None,
    )
}

fn client_error_response(
    status: StatusCode,
    code: impl Into<String>,
    retryable: bool,
    message: impl Into<String>,
    statement_index: Option<usize>,
) -> Response {
    (
        status,
        Json(ClientErrorResponse {
            code: code.into(),
            retryable,
            message: message.into(),
            statement_index,
        }),
    )
        .into_response()
}

pub fn install_successor_recorder(
    recorder: &RecorderFileStore,
    next_config_id: u64,
    membership: Membership,
    stop: &StopInformation,
) -> Result<rhiza_quepaxa::ConfigurationState, NodeError> {
    if stop.version != 2 || stop.entry.config_id.checked_add(1) != Some(next_config_id) {
        return Err(NodeError::PreconditionFailed(
            "successor identity does not match the Stop proof".into(),
        ));
    }
    recorder
        .install_successor_from_proof(membership, &stop.proof)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

pub fn recover_successor_recorder_after_checkpoint(
    recorder: &RecorderFileStore,
    config: &NodeConfig,
    next_config_id: u64,
    membership: Membership,
    stop: &StopInformation,
) -> Result<rhiza_quepaxa::ConfigurationState, NodeError> {
    let installed = install_successor_recorder(recorder, next_config_id, membership.clone(), stop)?;
    let log = FileLogStore::open_with_configuration(
        config.data_dir.join("consensus/log"),
        &config.cluster_id,
        config.epoch,
        config.log_initial_configuration.clone(),
    )
    .map_err(|error| NodeError::Storage(error.to_string()))?;
    let recovered_configuration = log
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    if !recovered_configuration.is_active() {
        return Ok(installed);
    }
    if recovered_configuration.config_id() != next_config_id
        || recovered_configuration.digest() != membership.digest()
    {
        return Err(NodeError::Reconciliation(
            "recovered successor qlog configuration does not match the target bundle".into(),
        ));
    }
    let tip = log
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?
        .tip
        .ok_or_else(|| NodeError::Reconciliation("recovered successor qlog is empty".into()))?;
    recorder
        .recover_successor_activation_from_checkpoint(
            stop.entry.index,
            stop.entry.hash,
            tip.index(),
            tip.hash(),
        )
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

fn recorder_error_status(error: &rhiza_quepaxa::Error) -> StatusCode {
    match error {
        rhiza_quepaxa::Error::NoQuorum
        | rhiza_quepaxa::Error::CommandUnavailable
        | rhiza_quepaxa::Error::Io(_)
        | rhiza_quepaxa::Error::RecorderRootLocked(_) => StatusCode::SERVICE_UNAVAILABLE,
        rhiza_quepaxa::Error::Rejected(_) => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn fetch_log_error_status(error: &FetchLogError) -> StatusCode {
    match error {
        FetchLogError::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
        FetchLogError::SnapshotRequired { .. } | FetchLogError::Gap { .. } => StatusCode::CONFLICT,
        FetchLogError::Decode { .. } | FetchLogError::Transport { .. } => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        FetchLogError::InvalidAnchor { .. }
        | FetchLogError::InvalidEntry { .. }
        | FetchLogError::ForeignIdentity { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn runtime_readiness_response(runtime: &NodeRuntime) -> Option<Response> {
    if runtime.is_fatal() {
        Some(client_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "fatal",
            false,
            runtime
                .fatal_reason()
                .unwrap_or_else(|| "node is fatally unavailable".into()),
            None,
        ))
    } else if !runtime.is_ready() {
        Some(client_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            true,
            "node is not ready",
            None,
        ))
    } else {
        None
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", content = "body")]
enum FetchLogHttpResponse {
    Fetched(FetchLogResponse),
    Failed(FetchLogError),
}

pub fn catch_up_missing_entries<P: LogPeer + ?Sized>(
    local_last_index: LogIndex,
    local_last_hash: LogHash,
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    peer: &P,
    max_entries: u32,
) -> Result<Vec<LogEntry>, FetchLogError> {
    if max_entries == 0 {
        return Ok(Vec::new());
    }
    if max_entries > MAX_FETCH_ENTRIES {
        return Err(FetchLogError::InvalidRequest {
            message: format!("max_entries exceeds {MAX_FETCH_ENTRIES}"),
        });
    }
    if cluster_id.is_empty() {
        return Err(FetchLogError::InvalidRequest {
            message: "cluster_id must not be empty".into(),
        });
    }
    let from_index =
        local_last_index
            .checked_add(1)
            .ok_or_else(|| FetchLogError::InvalidRequest {
                message: "local qlog index is exhausted".into(),
            })?;
    let response = peer.fetch_log(FetchLogRequest {
        from_index,
        max_entries,
    })?;
    if response.entries.len() > max_entries as usize {
        return Err(FetchLogError::InvalidRequest {
            message: "peer returned more entries than requested".into(),
        });
    }
    if response.last_index < local_last_index {
        return Err(FetchLogError::Gap {
            expected: local_last_index,
            actual: Some(response.last_index),
        });
    }
    if response.entries.is_empty() && response.last_index >= from_index {
        return Err(FetchLogError::Gap {
            expected: from_index,
            actual: None,
        });
    }
    validate_fetched_entries(
        from_index,
        local_last_hash,
        cluster_id,
        epoch,
        config_id,
        response.entries,
    )
}

fn validate_fetched_entries(
    from_index: LogIndex,
    local_last_hash: LogHash,
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    entries: Vec<LogEntry>,
) -> Result<Vec<LogEntry>, FetchLogError> {
    validate_fetched_entries_with_configuration(
        from_index,
        local_last_hash,
        cluster_id,
        epoch,
        ConfigurationState::active(config_id, LogHash::ZERO),
        entries,
    )
}

fn validate_fetched_entries_with_configuration(
    from_index: LogIndex,
    local_last_hash: LogHash,
    cluster_id: &str,
    epoch: u64,
    mut configuration_state: ConfigurationState,
    entries: Vec<LogEntry>,
) -> Result<Vec<LogEntry>, FetchLogError> {
    let mut expected = from_index;
    let mut previous_hash = local_last_hash;
    for entry in &entries {
        if entry.index != expected {
            return Err(FetchLogError::Gap {
                expected,
                actual: Some(entry.index),
            });
        }
        if entry.cluster_id != cluster_id || entry.epoch != epoch {
            return Err(FetchLogError::ForeignIdentity { index: entry.index });
        }
        if entry.prev_hash != previous_hash {
            return Err(FetchLogError::InvalidAnchor {
                expected: previous_hash,
                actual: entry.prev_hash,
            });
        }
        if entry.recompute_hash() != entry.hash {
            return Err(FetchLogError::InvalidEntry {
                index: entry.index,
                message: "hash does not match entry contents".into(),
            });
        }
        validate_entry_shape(entry).map_err(|message| FetchLogError::InvalidEntry {
            index: entry.index,
            message,
        })?;
        configuration_state = configuration_state.validate_entry(entry).map_err(|error| {
            FetchLogError::InvalidEntry {
                index: entry.index,
                message: error.to_string(),
            }
        })?;
        expected = expected
            .checked_add(1)
            .ok_or_else(|| FetchLogError::InvalidEntry {
                index: entry.index,
                message: "qlog index is exhausted".into(),
            })?;
        previous_hash = entry.hash;
    }
    Ok(entries)
}

#[derive(Clone, Eq, PartialEq)]
pub struct PeerConfig {
    node_id: String,
    base_url: String,
    log_base_url: String,
    token: String,
}

impl fmt::Debug for PeerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerConfig")
            .field("node_id", &self.node_id)
            .field("base_url", &self.base_url)
            .field("log_base_url", &self.log_base_url)
            .field("token", &"[redacted]")
            .finish()
    }
}

impl PeerConfig {
    pub fn new(
        node_id: impl Into<String>,
        base_url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let base_url = base_url.into();
        Self::new_with_log_url(node_id, base_url.clone(), base_url, token)
    }

    pub fn new_with_log_url(
        node_id: impl Into<String>,
        base_url: impl Into<String>,
        log_base_url: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let node_id = node_id.into();
        if !valid_nonblank_header_value(&node_id) {
            return Err(ConfigError::EmptyPeerNodeId);
        }
        let base_url = validate_peer_base_url(base_url.into())?;
        let log_base_url = validate_peer_base_url(log_base_url.into())?;
        let token = token.into();
        if !valid_auth_token(&token) {
            return Err(ConfigError::EmptyPeerToken);
        }
        Ok(Self {
            node_id,
            base_url,
            log_base_url,
            token,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn log_base_url(&self) -> &str {
        &self.log_base_url
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

fn validate_peer_base_url(url: String) -> Result<String, ConfigError> {
    let url = url.trim_end_matches('/').to_string();
    if url.trim().is_empty() {
        return Err(ConfigError::EmptyPeerBaseUrl);
    }
    let parsed =
        reqwest::Url::parse(&url).map_err(|_| ConfigError::InvalidPeerBaseUrl(url.clone()))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ConfigError::InvalidPeerBaseUrl(url));
    }
    Ok(url)
}

pub(crate) fn valid_nonblank_header_value(value: &str) -> bool {
    !value.trim().is_empty()
        && axum::http::HeaderValue::try_from(value).is_ok_and(|value| value.to_str().is_ok())
}

pub(crate) fn valid_auth_token(value: &str) -> bool {
    valid_nonblank_header_value(value) && !value.chars().any(char::is_whitespace)
}

#[derive(Clone, Eq, PartialEq)]
pub struct NodeConfig {
    cluster_id_source: String,
    logical_cluster_id: String,
    cluster_id: String,
    node_id: String,
    data_dir: PathBuf,
    epoch: u64,
    membership: Membership,
    log_initial_configuration: ConfigurationState,
    configuration_state: ConfigurationState,
    predecessor_stop_entry: Option<LogEntry>,
    recovery_generation: u64,
    peers: Vec<PeerConfig>,
    client_token: String,
    read_consistency: ReadConsistency,
    ack_mode: AckMode,
    writer_batch_max: usize,
    writer_batch_window: Duration,
    execution_profile: ExecutionProfile,
}

impl fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeConfig")
            .field("cluster_id_source", &self.cluster_id_source)
            .field("logical_cluster_id", &self.logical_cluster_id)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("data_dir", &self.data_dir)
            .field("epoch", &self.epoch)
            .field("membership", &self.membership.members())
            .field("log_initial_configuration", &self.log_initial_configuration)
            .field("configuration_state", &self.configuration_state)
            .field(
                "predecessor_stop_entry",
                &self
                    .predecessor_stop_entry
                    .as_ref()
                    .map(|entry| (entry.index, entry.hash)),
            )
            .field("recovery_generation", &self.recovery_generation)
            .field("peers", &self.peers)
            .field("client_token", &"[redacted]")
            .field("read_consistency", &self.read_consistency)
            .field("ack_mode", &self.ack_mode)
            .field("writer_batch_max", &self.writer_batch_max)
            .field("writer_batch_window", &self.writer_batch_window)
            .field("execution_profile", &self.execution_profile)
            .finish()
    }
}

impl NodeConfig {
    pub fn new<P>(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        data_dir: PathBuf,
        epoch: u64,
        config_id: u64,
        peers: P,
        client_token: impl Into<String>,
    ) -> Result<Self, ConfigError>
    where
        P: Into<Vec<PeerConfig>>,
    {
        let peers = peers.into();
        let membership = membership_from_peers(&peers)?;
        let configuration_state = ConfigurationState::active(config_id, membership.digest());
        Self::new_with_configuration(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            peers,
            client_token,
        )
    }

    pub fn new_embedded<I, S>(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        data_dir: PathBuf,
        epoch: u64,
        config_id: u64,
        members: I,
    ) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let cluster_id = cluster_id.into();
        let node_id = node_id.into();
        validate_node_identity(&cluster_id, &node_id, &data_dir, epoch, config_id)?;
        let membership = membership_from_node_ids(members.into_iter().map(Into::into).collect())?;
        if !membership.contains(&node_id) {
            return Err(ConfigError::LocalNodeMissing);
        }
        let configuration_state = ConfigurationState::active(config_id, membership.digest());
        Self::from_validated_parts(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            Vec::new(),
            String::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_configuration<P>(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        data_dir: PathBuf,
        epoch: u64,
        membership: Membership,
        configuration_state: ConfigurationState,
        peers: P,
        client_token: impl Into<String>,
    ) -> Result<Self, ConfigError>
    where
        P: Into<Vec<PeerConfig>>,
    {
        let cluster_id = cluster_id.into();
        let node_id = node_id.into();
        let client_token = client_token.into();
        let peers = peers.into();

        validate_node_identity(
            &cluster_id,
            &node_id,
            &data_dir,
            epoch,
            configuration_state.config_id(),
        )?;
        if !(3..=7).contains(&peers.len()) {
            return Err(ConfigError::InvalidPeerCount(peers.len()));
        }
        let mut peer_ids = HashSet::with_capacity(peers.len());
        let mut peer_tokens = HashSet::with_capacity(peers.len());
        for peer in &peers {
            if !peer_ids.insert(peer.node_id.clone()) {
                return Err(ConfigError::DuplicatePeerNodeId(peer.node_id.clone()));
            }
            if !peer_tokens.insert(peer.token.as_str()) {
                return Err(ConfigError::DuplicatePeerToken);
            }
        }
        if !peer_ids.contains(&node_id) {
            return Err(ConfigError::LocalNodeMissing);
        }
        if peer_ids.len() != membership.members().len()
            || membership
                .members()
                .iter()
                .any(|member| !peer_ids.contains(member))
        {
            return Err(ConfigError::PeerMembershipMismatch);
        }
        if configuration_state.is_active() && configuration_state.digest() != membership.digest() {
            return Err(ConfigError::PeerMembershipMismatch);
        }
        if !valid_auth_token(&client_token) {
            return Err(ConfigError::EmptyClientToken);
        }
        if peer_tokens.contains(client_token.as_str()) {
            return Err(ConfigError::ClientTokenConflictsWithPeer);
        }

        Self::from_validated_parts(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            peers,
            client_token,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_validated_parts(
        cluster_id: String,
        node_id: String,
        data_dir: PathBuf,
        epoch: u64,
        membership: Membership,
        configuration_state: ConfigurationState,
        peers: Vec<PeerConfig>,
        client_token: String,
    ) -> Result<Self, ConfigError> {
        let log_initial_configuration = ConfigurationState::active(
            configuration_state.config_id(),
            configuration_state.digest(),
        );
        let logical_cluster_id = cluster_id
            .strip_prefix("rhiza:sql:")
            .unwrap_or(&cluster_id)
            .to_owned();
        let effective_cluster_id = effective_cluster_id(ExecutionProfile::Sqlite, &cluster_id)?;
        Ok(Self {
            cluster_id_source: cluster_id,
            cluster_id: effective_cluster_id,
            logical_cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            log_initial_configuration,
            configuration_state,
            predecessor_stop_entry: None,
            recovery_generation: 1,
            peers,
            client_token,
            read_consistency: ReadConsistency::ReadBarrier,
            ack_mode: AckMode::HaFirst,
            writer_batch_max: DEFAULT_WRITER_BATCH_MAX,
            writer_batch_window: DEFAULT_WRITER_BATCH_WINDOW,
            execution_profile: ExecutionProfile::Sqlite,
        })
    }

    pub fn with_execution_profile(
        mut self,
        execution_profile: ExecutionProfile,
    ) -> Result<Self, ConfigError> {
        self.cluster_id = effective_cluster_id(execution_profile, &self.cluster_id_source)?;
        self.execution_profile = execution_profile;
        Ok(self)
    }

    pub fn with_read_consistency(mut self, read_consistency: ReadConsistency) -> Self {
        self.read_consistency = read_consistency;
        self
    }

    pub fn with_ack_mode(mut self, ack_mode: AckMode) -> Self {
        self.ack_mode = ack_mode;
        self
    }

    pub fn with_writer_batching(
        mut self,
        max: usize,
        window: Duration,
    ) -> Result<Self, ConfigError> {
        if max == 0 || max > MAX_WRITE_BATCH_MEMBERS {
            return Err(ConfigError::InvalidWriterBatchMax(max));
        }
        if window.is_zero() || window >= CLIENT_WRITE_WAIT_TIMEOUT {
            return Err(ConfigError::InvalidWriterBatchWindow);
        }
        self.writer_batch_max = max;
        self.writer_batch_window = window;
        Ok(self)
    }

    pub fn with_log_initial_configuration(mut self, configuration: ConfigurationState) -> Self {
        self.log_initial_configuration = configuration;
        self
    }

    pub fn with_predecessor_stop_entry(mut self, entry: LogEntry) -> Self {
        self.predecessor_stop_entry = Some(entry);
        self
    }

    pub fn with_recovery_generation(
        mut self,
        recovery_generation: u64,
    ) -> Result<Self, ConfigError> {
        validate_recovery_generation(recovery_generation)?;
        self.recovery_generation = recovery_generation;
        Ok(self)
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn logical_cluster_id(&self) -> &str {
        &self.logical_cluster_id
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn config_id(&self) -> u64 {
        self.configuration_state.config_id()
    }

    pub const fn recovery_generation(&self) -> u64 {
        self.recovery_generation
    }

    pub fn peers(&self) -> &[PeerConfig] {
        &self.peers
    }

    pub const fn membership(&self) -> &Membership {
        &self.membership
    }

    pub const fn configuration_state(&self) -> &ConfigurationState {
        &self.configuration_state
    }

    pub const fn log_initial_configuration(&self) -> &ConfigurationState {
        &self.log_initial_configuration
    }

    pub fn client_token(&self) -> &str {
        &self.client_token
    }

    pub const fn read_consistency(&self) -> ReadConsistency {
        self.read_consistency
    }

    pub const fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }

    pub const fn writer_batch_max(&self) -> usize {
        self.writer_batch_max
    }

    pub const fn writer_batch_window(&self) -> Duration {
        self.writer_batch_window
    }

    pub const fn execution_profile(&self) -> ExecutionProfile {
        self.execution_profile
    }
}

fn validate_node_identity(
    cluster_id: &str,
    node_id: &str,
    data_dir: &Path,
    epoch: u64,
    config_id: u64,
) -> Result<(), ConfigError> {
    if cluster_id.trim().is_empty() {
        return Err(ConfigError::EmptyClusterId);
    }
    if node_id.trim().is_empty() {
        return Err(ConfigError::EmptyNodeId);
    }
    if data_dir.as_os_str().is_empty() {
        return Err(ConfigError::EmptyDataDir);
    }
    if epoch == 0 {
        return Err(ConfigError::InvalidEpoch);
    }
    if config_id == 0 {
        return Err(ConfigError::InvalidConfigId);
    }
    Ok(())
}

fn membership_from_node_ids(members: Vec<String>) -> Result<Membership, ConfigError> {
    if !(3..=7).contains(&members.len()) {
        return Err(ConfigError::InvalidPeerCount(members.len()));
    }
    Membership::from_voters(members.clone()).map_err(|error| match error {
        rhiza_quepaxa::Error::DuplicateRecorderIdentity => {
            let duplicate = members
                .iter()
                .find(|candidate| {
                    members
                        .iter()
                        .filter(|member| *member == *candidate)
                        .count()
                        > 1
                })
                .cloned()
                .unwrap_or_default();
            ConfigError::DuplicatePeerNodeId(duplicate)
        }
        rhiza_quepaxa::Error::EmptyRecorderIdentity => ConfigError::EmptyPeerNodeId,
        _ => ConfigError::InvalidPeerCount(members.len()),
    })
}

fn membership_from_peers(peers: &[PeerConfig]) -> Result<Membership, ConfigError> {
    membership_from_node_ids(peers.iter().map(|peer| peer.node_id.clone()).collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    UnsupportedAckMode(AckMode),
    ExecutionProfileMismatch {
        expected: ExecutionProfile,
        actual: ExecutionProfile,
    },
    DataRootLocked(PathBuf),
    SnapshotRequired(Box<RecoveryAnchor>),
    Storage(String),
    Reconciliation(String),
    Invariant(String),
    Unavailable(String),
    ResourceExhausted(String),
    ConfigurationTransition {
        state: Box<ConfigurationState>,
    },
    Contention(String),
    WinnerLimitExceeded,
    #[cfg(feature = "sql")]
    RequestConflict(RequestConflict),
    InvalidRequest(String),
    #[cfg(feature = "sql")]
    InvalidSqlStatement {
        statement_index: usize,
        message: String,
    },
    PreconditionFailed(String),
    Fatal(String),
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedAckMode(mode) => {
                write!(
                    f,
                    "ack mode {mode:?} is unsupported without synchronous archive"
                )
            }
            Self::ExecutionProfileMismatch { expected, actual } => write!(
                f,
                "execution profile mismatch: expected {expected}, got {actual}"
            ),
            Self::DataRootLocked(path) => {
                write!(f, "node data root is already owned: {}", path.display())
            }
            Self::SnapshotRequired(anchor) => write!(
                f,
                "snapshot restore required at qlog anchor {}",
                anchor.compacted().index()
            ),
            Self::Storage(message) => write!(f, "node storage failed: {message}"),
            Self::Reconciliation(message) => write!(f, "node reconciliation failed: {message}"),
            Self::Invariant(message) => write!(f, "node invariant failed: {message}"),
            Self::Unavailable(message) => write!(f, "node unavailable: {message}"),
            Self::ResourceExhausted(message) => {
                write!(f, "node query resources exhausted: {message}")
            }
            Self::ConfigurationTransition { state } => write!(
                f,
                "node unavailable during configuration transition: {state:?}"
            ),
            Self::Contention(message) => write!(f, "node contention: {message}"),
            Self::WinnerLimitExceeded => write!(f, "foreign winner retry limit exceeded"),
            #[cfg(feature = "sql")]
            Self::RequestConflict(conflict) => conflict.fmt(f),
            Self::InvalidRequest(message) => write!(f, "invalid request: {message}"),
            #[cfg(feature = "sql")]
            Self::InvalidSqlStatement {
                statement_index,
                message,
            } => write!(
                f,
                "invalid SQL statement at index {statement_index}: {message}"
            ),
            Self::PreconditionFailed(message) => write!(f, "precondition failed: {message}"),
            Self::Fatal(message) => write!(f, "node is fatally unavailable: {message}"),
        }
    }
}

impl std::error::Error for NodeError {}

pub type RuntimeError = NodeError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeConfigurationStatus {
    Active,
    Stopped,
    AwaitingActivation,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct NodeStatus {
    pub ready: bool,
    pub configuration_status: RuntimeConfigurationStatus,
    pub configuration_state: ConfigurationState,
    pub stop_anchor: Option<rhiza_core::LogAnchor>,
    pub active_config_id: u64,
    pub active_membership_digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StopInformation {
    pub version: u16,
    pub entry: LogEntry,
    pub proof: DecisionProof,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub request_id: String,
    pub key: String,
    pub value: String,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WriteResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ClientErrorResponse {
    pub code: String,
    pub retryable: bool,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_index: Option<usize>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub key: String,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadResponse {
    pub value: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlExecuteRequest {
    pub request_id: String,
    pub statements: Vec<SqlStatement>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlExecuteResponse {
    pub version: u16,
    pub applied_index: LogIndex,
    pub hash: LogHash,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<SqlStatementResult>,
}

#[cfg(feature = "sql")]
impl From<WriteResponse> for SqlExecuteResponse {
    fn from(response: WriteResponse) -> Self {
        sql_execute_response(response, None)
    }
}

#[cfg(feature = "sql")]
fn sql_execute_response(
    response: WriteResponse,
    result: Option<SqlCommandResult>,
) -> SqlExecuteResponse {
    let results = result
        .map(|result| {
            result
                .statement_results
                .into_iter()
                .enumerate()
                .map(|(statement_index, result)| SqlStatementResult {
                    statement_index,
                    rows_affected: result.rows_affected,
                    returning: result.returning,
                })
                .collect()
        })
        .unwrap_or_default();
    SqlExecuteResponse {
        version: SQL_EXECUTE_RESPONSE_VERSION,
        applied_index: response.applied_index,
        hash: response.hash,
        results,
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlStatementResult {
    pub statement_index: usize,
    pub rows_affected: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returning: Option<SqlQueryResult>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryRequest {
    pub statement: SqlStatement,
    pub consistency: Option<ReadConsistency>,
    pub max_rows: Option<u32>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphValueDto {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(String),
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphQueryParameterDto {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(String),
    List(Vec<Self>),
    Struct(BTreeMap<String, Self>),
}

#[cfg(feature = "graph")]
impl TryFrom<GraphQueryParameterDto> for GraphParameterValue {
    type Error = NodeError;

    fn try_from(value: GraphQueryParameterDto) -> Result<Self, Self::Error> {
        Ok(match value {
            GraphQueryParameterDto::Null => Self::Null,
            GraphQueryParameterDto::Bool(value) => Self::Bool(value),
            GraphQueryParameterDto::I64(value) => Self::I64(value),
            GraphQueryParameterDto::U64(value) => Self::U64(value),
            GraphQueryParameterDto::F64(value) => Self::F64(
                CanonicalF64::new(value)
                    .map_err(|error| NodeError::InvalidRequest(error.to_string()))?,
            ),
            GraphQueryParameterDto::String(value) => Self::String(value),
            GraphQueryParameterDto::Bytes(value) => {
                Self::Bytes(decode_base64("graph parameter bytes", &value)?)
            }
            GraphQueryParameterDto::List(values) => Self::List(
                values
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            GraphQueryParameterDto::Struct(values) => Self::Struct(
                values
                    .into_iter()
                    .map(|(name, value)| Self::try_from(value).map(|value| (name, value)))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphQueryStatementDto {
    pub cypher: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, GraphQueryParameterDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphQueryRequest {
    pub statement: GraphQueryStatementDto,
    pub consistency: Option<ReadConsistency>,
    pub max_rows: Option<u32>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphInternalIdDto {
    pub offset: u64,
    pub table_id: u64,
}

#[cfg(feature = "graph")]
impl From<GraphInternalId> for GraphInternalIdDto {
    fn from(value: GraphInternalId) -> Self {
        Self {
            offset: value.offset,
            table_id: value.table_id,
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphNamedValueDto {
    pub name: String,
    pub value: GraphResultValueDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphNodeDto {
    pub id: GraphInternalIdDto,
    pub label: String,
    pub properties: Vec<GraphNamedValueDto>,
}

#[cfg(feature = "graph")]
impl From<GraphNode> for GraphNodeDto {
    fn from(value: GraphNode) -> Self {
        Self {
            id: value.id.into(),
            label: value.label,
            properties: named_graph_values(value.properties),
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphRelDto {
    pub src: GraphInternalIdDto,
    pub dst: GraphInternalIdDto,
    pub label: String,
    pub properties: Vec<GraphNamedValueDto>,
}

#[cfg(feature = "graph")]
impl From<GraphRel> for GraphRelDto {
    fn from(value: GraphRel) -> Self {
        Self {
            src: value.src.into(),
            dst: value.dst.into(),
            label: value.label,
            properties: named_graph_values(value.properties),
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphRecursiveRelDto {
    pub nodes: Vec<GraphNodeDto>,
    pub rels: Vec<GraphRelDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMapEntryDto {
    pub key: GraphResultValueDto,
    pub value: GraphResultValueDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphNamedLogicalTypeDto {
    pub name: String,
    pub logical_type: GraphLogicalTypeDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphArrayTypeDto {
    pub element_type: Box<GraphLogicalTypeDto>,
    pub length: u64,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMapTypeDto {
    pub key_type: Box<GraphLogicalTypeDto>,
    pub value_type: Box<GraphLogicalTypeDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphDecimalTypeDto {
    pub precision: u32,
    pub scale: u32,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphLogicalTypeDto {
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
    List(Box<Self>),
    Array(GraphArrayTypeDto),
    Struct(Vec<GraphNamedLogicalTypeDto>),
    Node,
    Rel,
    RecursiveRel,
    Map(GraphMapTypeDto),
    Union(Vec<GraphNamedLogicalTypeDto>),
    Uuid,
    Decimal(GraphDecimalTypeDto),
}

#[cfg(feature = "graph")]
impl From<GraphLogicalType> for GraphLogicalTypeDto {
    fn from(value: GraphLogicalType) -> Self {
        match value {
            GraphLogicalType::Any => Self::Any,
            GraphLogicalType::Bool => Self::Bool,
            GraphLogicalType::Serial => Self::Serial,
            GraphLogicalType::I64 => Self::I64,
            GraphLogicalType::I32 => Self::I32,
            GraphLogicalType::I16 => Self::I16,
            GraphLogicalType::I8 => Self::I8,
            GraphLogicalType::U64 => Self::U64,
            GraphLogicalType::U32 => Self::U32,
            GraphLogicalType::U16 => Self::U16,
            GraphLogicalType::U8 => Self::U8,
            GraphLogicalType::I128 => Self::I128,
            GraphLogicalType::F64 => Self::F64,
            GraphLogicalType::F32 => Self::F32,
            GraphLogicalType::Date => Self::Date,
            GraphLogicalType::Interval => Self::Interval,
            GraphLogicalType::Timestamp => Self::Timestamp,
            GraphLogicalType::TimestampTz => Self::TimestampTz,
            GraphLogicalType::TimestampNs => Self::TimestampNs,
            GraphLogicalType::TimestampMs => Self::TimestampMs,
            GraphLogicalType::TimestampSec => Self::TimestampSec,
            GraphLogicalType::InternalId => Self::InternalId,
            GraphLogicalType::String => Self::String,
            GraphLogicalType::Json => Self::Json,
            GraphLogicalType::Bytes => Self::Bytes,
            GraphLogicalType::List(element_type) => Self::List(Box::new((*element_type).into())),
            GraphLogicalType::Array {
                element_type,
                length,
            } => Self::Array(GraphArrayTypeDto {
                element_type: Box::new((*element_type).into()),
                length,
            }),
            GraphLogicalType::Struct(fields) => Self::Struct(named_graph_logical_types(fields)),
            GraphLogicalType::Node => Self::Node,
            GraphLogicalType::Rel => Self::Rel,
            GraphLogicalType::RecursiveRel => Self::RecursiveRel,
            GraphLogicalType::Map {
                key_type,
                value_type,
            } => Self::Map(GraphMapTypeDto {
                key_type: Box::new((*key_type).into()),
                value_type: Box::new((*value_type).into()),
            }),
            GraphLogicalType::Union(types) => Self::Union(named_graph_logical_types(types)),
            GraphLogicalType::Uuid => Self::Uuid,
            GraphLogicalType::Decimal { precision, scale } => {
                Self::Decimal(GraphDecimalTypeDto { precision, scale })
            }
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphCollectionValueDto {
    pub element_type: GraphLogicalTypeDto,
    pub values: Vec<GraphResultValueDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMapValueDto {
    pub key_type: GraphLogicalTypeDto,
    pub value_type: GraphLogicalTypeDto,
    pub entries: Vec<GraphMapEntryDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphUnionValueDto {
    pub variants: Vec<GraphNamedLogicalTypeDto>,
    pub value: Box<GraphResultValueDto>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphResultValueDto {
    Null(GraphLogicalTypeDto),
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
    F64(f64),
    F32(String),
    Date(String),
    Interval(String),
    Timestamp(String),
    TimestampTz(String),
    TimestampNs(String),
    TimestampMs(String),
    TimestampSec(String),
    InternalId(GraphInternalIdDto),
    String(String),
    Json(String),
    Bytes(String),
    List(GraphCollectionValueDto),
    Array(GraphCollectionValueDto),
    Struct(Vec<GraphNamedValueDto>),
    Node(GraphNodeDto),
    Rel(GraphRelDto),
    RecursiveRel(GraphRecursiveRelDto),
    Map(GraphMapValueDto),
    Union(GraphUnionValueDto),
    Uuid(String),
    Decimal(String),
}

#[cfg(feature = "graph")]
impl From<GraphResultValue> for GraphResultValueDto {
    fn from(value: GraphResultValue) -> Self {
        match value {
            GraphResultValue::Null(value) => Self::Null(value.into()),
            GraphResultValue::Bool(value) => Self::Bool(value),
            GraphResultValue::I64(value) => Self::I64(value),
            GraphResultValue::I32(value) => Self::I32(value),
            GraphResultValue::I16(value) => Self::I16(value),
            GraphResultValue::I8(value) => Self::I8(value),
            GraphResultValue::U64(value) => Self::U64(value),
            GraphResultValue::U32(value) => Self::U32(value),
            GraphResultValue::U16(value) => Self::U16(value),
            GraphResultValue::U8(value) => Self::U8(value),
            GraphResultValue::I128(value) => Self::I128(value),
            GraphResultValue::F64(value) => Self::F64(value.get()),
            GraphResultValue::F32(value) => Self::F32(value),
            GraphResultValue::Date(value) => Self::Date(value),
            GraphResultValue::Interval(value) => Self::Interval(value),
            GraphResultValue::Timestamp(value) => Self::Timestamp(value),
            GraphResultValue::TimestampTz(value) => Self::TimestampTz(value),
            GraphResultValue::TimestampNs(value) => Self::TimestampNs(value),
            GraphResultValue::TimestampMs(value) => Self::TimestampMs(value),
            GraphResultValue::TimestampSec(value) => Self::TimestampSec(value),
            GraphResultValue::InternalId(value) => Self::InternalId(value.into()),
            GraphResultValue::String(value) => Self::String(value),
            GraphResultValue::Json(value) => Self::Json(value),
            GraphResultValue::Bytes(value) => Self::Bytes(encode_base64(&value)),
            GraphResultValue::List {
                element_type,
                values,
            } => Self::List(GraphCollectionValueDto {
                element_type: element_type.into(),
                values: values.into_iter().map(Self::from).collect(),
            }),
            GraphResultValue::Array {
                element_type,
                values,
            } => Self::Array(GraphCollectionValueDto {
                element_type: element_type.into(),
                values: values.into_iter().map(Self::from).collect(),
            }),
            GraphResultValue::Struct(values) => Self::Struct(named_graph_values(values)),
            GraphResultValue::Node(value) => Self::Node(value.into()),
            GraphResultValue::Rel(value) => Self::Rel(value.into()),
            GraphResultValue::RecursiveRel { nodes, rels } => {
                Self::RecursiveRel(GraphRecursiveRelDto {
                    nodes: nodes.into_iter().map(Into::into).collect(),
                    rels: rels.into_iter().map(Into::into).collect(),
                })
            }
            GraphResultValue::Map {
                key_type,
                value_type,
                entries,
            } => Self::Map(GraphMapValueDto {
                key_type: key_type.into(),
                value_type: value_type.into(),
                entries: entries
                    .into_iter()
                    .map(|(key, value)| GraphMapEntryDto {
                        key: key.into(),
                        value: value.into(),
                    })
                    .collect(),
            }),
            GraphResultValue::Union { variants, value } => Self::Union(GraphUnionValueDto {
                variants: named_graph_logical_types(variants),
                value: Box::new(Self::from(*value)),
            }),
            GraphResultValue::Uuid(value) => Self::Uuid(value),
            GraphResultValue::Decimal(value) => Self::Decimal(value),
        }
    }
}

#[cfg(feature = "graph")]
fn named_graph_values(values: Vec<(String, GraphResultValue)>) -> Vec<GraphNamedValueDto> {
    values
        .into_iter()
        .map(|(name, value)| GraphNamedValueDto {
            name,
            value: value.into(),
        })
        .collect()
}

#[cfg(feature = "graph")]
fn named_graph_logical_types(
    values: Vec<(String, GraphLogicalType)>,
) -> Vec<GraphNamedLogicalTypeDto> {
    values
        .into_iter()
        .map(|(name, logical_type)| GraphNamedLogicalTypeDto {
            name,
            logical_type: logical_type.into(),
        })
        .collect()
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphColumnDto {
    pub name: String,
    pub logical_type: GraphLogicalTypeDto,
}

#[cfg(feature = "graph")]
impl From<GraphColumn> for GraphColumnDto {
    fn from(value: GraphColumn) -> Self {
        Self {
            name: value.name,
            logical_type: value.logical_type.into(),
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphQueryResponse {
    pub columns: Vec<GraphColumnDto>,
    pub rows: Vec<Vec<GraphResultValueDto>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "graph")]
impl From<GraphQueryResult> for GraphQueryResponse {
    fn from(value: GraphQueryResult) -> Self {
        Self {
            columns: value.columns.into_iter().map(Into::into).collect(),
            rows: value
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(Into::into).collect())
                .collect(),
            applied_index: value.applied_index,
            hash: value.hash,
        }
    }
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPutDocumentRequest {
    pub request_id: String,
    pub id: String,
    pub value: GraphValueDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDeleteDocumentRequest {
    pub request_id: String,
    pub id: String,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphGetDocumentRequest {
    pub id: String,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum GraphMutationResultDto {
    PutDocument { created: bool },
    DeleteDocument { existed: bool },
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphMutationResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
    pub result: GraphMutationResultDto,
}

#[cfg(feature = "graph")]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GraphGetDocumentResponse {
    pub value: Option<GraphValueDto>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvPutRequest {
    pub request_id: String,
    pub key: String,
    pub value: String,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvDeleteRequest {
    pub request_id: String,
    pub key: String,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvGetRequest {
    pub key: String,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct KvScanRequest {
    pub start: Option<String>,
    pub end: Option<String>,
    pub prefix: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub consistency: Option<ReadConsistency>,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum KvMutationResultDto {
    Put { replaced: bool },
    Delete { existed: bool },
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvMutationResponse {
    pub applied_index: LogIndex,
    pub hash: LogHash,
    pub result: KvMutationResultDto,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvGetResponse {
    pub value: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvScanEntryDto {
    pub key: String,
    pub value: String,
}

#[cfg(feature = "kv")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct KvScanResponse {
    pub entries: Vec<KvScanEntryDto>,
    pub next_cursor: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[cfg(feature = "graph")]
fn graph_mutation_response(outcome: GraphMutationOutcome) -> GraphMutationResponse {
    GraphMutationResponse {
        applied_index: outcome.applied_index(),
        hash: outcome.hash(),
        result: match outcome.result() {
            GraphCommandResultV1::PutDocument { created } => {
                GraphMutationResultDto::PutDocument { created: *created }
            }
            GraphCommandResultV1::DeleteDocument { existed } => {
                GraphMutationResultDto::DeleteDocument { existed: *existed }
            }
        },
    }
}

#[cfg(feature = "kv")]
fn kv_mutation_response(outcome: KvMutationOutcome) -> KvMutationResponse {
    KvMutationResponse {
        applied_index: outcome.applied_index(),
        hash: outcome.hash(),
        result: match outcome.result() {
            KvCommandResultV1::Put { replaced } => KvMutationResultDto::Put {
                replaced: *replaced,
            },
            KvCommandResultV1::Delete { existed } => {
                KvMutationResultDto::Delete { existed: *existed }
            }
        },
    }
}

#[cfg(feature = "kv")]
fn validate_kv_scan_required_index(
    result: &KvScanResult,
    required_index: Option<LogIndex>,
) -> Result<(), NodeError> {
    let applied_index = result.tip().applied_index();
    if required_index.is_some_and(|required| applied_index < required) {
        return Err(NodeError::Unavailable(format!(
            "local applied index {applied_index} has not reached {}",
            required_index.expect("checked above")
        )));
    }
    Ok(())
}

#[cfg(feature = "graph")]
impl TryFrom<GraphValueDto> for GraphValueV1 {
    type Error = NodeError;

    fn try_from(value: GraphValueDto) -> Result<Self, Self::Error> {
        match value {
            GraphValueDto::Null => Ok(Self::Null),
            GraphValueDto::Bool(value) => Ok(Self::Bool(value)),
            GraphValueDto::I64(value) => Ok(Self::I64(value)),
            GraphValueDto::U64(value) => Ok(Self::U64(value)),
            GraphValueDto::F64(value) => {
                Self::from_f64(value).map_err(|error| NodeError::InvalidRequest(error.to_string()))
            }
            GraphValueDto::String(value) => Ok(Self::String(value)),
            GraphValueDto::Bytes(value) => decode_base64("value", &value).map(Self::Bytes),
        }
    }
}

#[cfg(feature = "graph")]
impl From<GraphValueV1> for GraphValueDto {
    fn from(value: GraphValueV1) -> Self {
        match value {
            GraphValueV1::Null => Self::Null,
            GraphValueV1::Bool(value) => Self::Bool(value),
            GraphValueV1::I64(value) => Self::I64(value),
            GraphValueV1::U64(value) => Self::U64(value),
            GraphValueV1::F64(value) => Self::F64(value.get()),
            GraphValueV1::String(value) => Self::String(value),
            GraphValueV1::Bytes(value) => Self::Bytes(encode_base64(&value)),
        }
    }
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[usize::from(first >> 2)] as char);
        encoded.push(ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[usize::from(third & 0x3f)] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn decode_base64(field: &str, encoded: &str) -> Result<Vec<u8>, NodeError> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = encoded.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(NodeError::InvalidRequest(format!(
            "{field} must be canonical padded base64"
        )));
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    for (chunk_index, chunk) in bytes.chunks_exact(4).enumerate() {
        let last = chunk_index + 1 == bytes.len() / 4;
        let first = sextet(chunk[0]);
        let second = sextet(chunk[1]);
        let third = (chunk[2] != b'=').then(|| sextet(chunk[2])).flatten();
        let fourth = (chunk[3] != b'=').then(|| sextet(chunk[3])).flatten();
        let has_padding = chunk[2] == b'=' || chunk[3] == b'=';
        if first.is_none()
            || second.is_none()
            || (chunk[2] != b'=' && third.is_none())
            || (chunk[3] != b'=' && fourth.is_none())
            || (!last && has_padding)
            || (chunk[2] == b'=' && chunk[3] != b'=')
        {
            return Err(NodeError::InvalidRequest(format!(
                "{field} must be canonical padded base64"
            )));
        }
        let first = first.unwrap();
        let second = second.unwrap();
        decoded.push((first << 2) | (second >> 4));
        if let Some(third) = third {
            decoded.push((second << 4) | (third >> 2));
            if let Some(fourth) = fourth {
                decoded.push((third << 6) | fourth);
            } else if third & 0x03 != 0 {
                return Err(NodeError::InvalidRequest(format!(
                    "{field} must be canonical padded base64"
                )));
            }
        } else if second & 0x0f != 0 {
            return Err(NodeError::InvalidRequest(format!(
                "{field} must be canonical padded base64"
            )));
        }
    }
    Ok(decoded)
}

enum Materializer {
    #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
    Unavailable,
    #[cfg(feature = "sql")]
    Sql(Box<SqliteStateMachine>),
    #[cfg(feature = "graph")]
    Graph(Arc<LadybugStateMachine>),
    #[cfg(feature = "kv")]
    Kv(Arc<RedbStateMachine>),
}

#[cfg(feature = "sql")]
struct SqlMaterializerGuard<'a>(MutexGuard<'a, Materializer>);

#[cfg(feature = "sql")]
impl std::ops::Deref for SqlMaterializerGuard<'_> {
    type Target = SqliteStateMachine;

    fn deref(&self) -> &Self::Target {
        match &*self.0 {
            Materializer::Sql(state) => state,
            #[cfg(feature = "graph")]
            Materializer::Graph(_) => unreachable!("SQL guard validated the materializer profile"),
            #[cfg(feature = "kv")]
            Materializer::Kv(_) => unreachable!("SQL guard validated the materializer profile"),
        }
    }
}

impl Materializer {
    fn ensure_profile_available(profile: ExecutionProfile) -> Result<(), NodeError> {
        if execution_profile_compiled(profile) {
            Ok(())
        } else {
            Err(NodeError::Unavailable(format!(
                "{} execution profile is not compiled in",
                profile.as_str()
            )))
        }
    }

    fn open(
        config: &NodeConfig,
        _configuration_state: &ConfigurationState,
    ) -> Result<Self, NodeError> {
        match config.execution_profile() {
            ExecutionProfile::Sqlite => {
                #[cfg(feature = "sql")]
                {
                    let path = config.data_dir().join("sqlite/db.sqlite");
                    let state = SqliteStateMachine::open_with_configuration(
                        path,
                        config.cluster_id(),
                        config.node_id(),
                        config.epoch(),
                        _configuration_state.clone(),
                    )
                    .map_err(|error| NodeError::Storage(error.to_string()))?;
                    Ok(Self::Sql(Box::new(state)))
                }
                #[cfg(not(feature = "sql"))]
                Err(NodeError::Unavailable(
                    "sql execution profile is not compiled in".into(),
                ))
            }
            ExecutionProfile::Graph => {
                #[cfg(feature = "graph")]
                {
                    LadybugStateMachine::open_with_configuration(
                        config.data_dir().join("ladybug/graph.lbug"),
                        config.cluster_id(),
                        config.node_id(),
                        config.epoch(),
                        // New DB/control pairs must replay uncompacted qlog history from genesis.
                        // Existing pairs ignore this seed and retain their replicated control.
                        config.log_initial_configuration().clone(),
                        config.recovery_generation(),
                    )
                    .map(Arc::new)
                    .map(Self::Graph)
                    .map_err(|error| NodeError::Storage(error.to_string()))
                }
                #[cfg(not(feature = "graph"))]
                Err(NodeError::Unavailable(
                    "graph execution profile is not compiled in".into(),
                ))
            }
            ExecutionProfile::Kv => {
                #[cfg(feature = "kv")]
                {
                    RedbStateMachine::open(
                        config.data_dir().join("kv/data.redb"),
                        config.cluster_id(),
                        config.node_id(),
                        config.epoch(),
                        _configuration_state.config_id(),
                    )
                    .map(Arc::new)
                    .map(Self::Kv)
                    .map_err(|error| NodeError::Storage(error.to_string()))
                }
                #[cfg(not(feature = "kv"))]
                Err(NodeError::Unavailable(
                    "kv execution profile is not compiled in".into(),
                ))
            }
        }
    }

    fn profile(&self) -> ExecutionProfile {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(_) => ExecutionProfile::Sqlite,
            #[cfg(feature = "graph")]
            Self::Graph(_) => ExecutionProfile::Graph,
            #[cfg(feature = "kv")]
            Self::Kv(_) => ExecutionProfile::Kv,
        }
    }

    fn applied_index(&self) -> Result<LogIndex, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .applied_index_value()
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state.applied_index().map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state.applied_index().map_err(|error| error.to_string()),
        }
    }

    fn applied_hash(&self) -> Result<LogHash, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .applied_hash_value()
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state.applied_hash().map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state.applied_hash().map_err(|error| error.to_string()),
        }
    }

    fn configuration_state(&self) -> Result<Option<ConfigurationState>, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .configuration_state_value()
                .map(Some)
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state
                .configuration_state_value()
                .map(Some)
                .map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(_) => Ok(None),
        }
    }

    /// Returns the committed base used only to seed startup qlog reconciliation.
    ///
    /// Graph may have a durable LGFX pending intent while its public read state
    /// remains unavailable. In that case this returns the committed control
    /// base so the matching next qlog entry can be redelivered.
    fn startup_reconciliation_base_state(
        &self,
    ) -> Result<(LogAnchor, Option<ConfigurationState>), String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => Ok((
                LogAnchor::new(
                    state
                        .applied_index_value()
                        .map_err(|error| error.to_string())?,
                    state
                        .applied_hash_value()
                        .map_err(|error| error.to_string())?,
                ),
                Some(
                    state
                        .configuration_state_value()
                        .map_err(|error| error.to_string())?,
                ),
            )),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state
                .reconciliation_base_state()
                .map(|(tip, configuration)| (tip, Some(configuration)))
                .map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => Ok((
                LogAnchor::new(
                    state.applied_index().map_err(|error| error.to_string())?,
                    state.applied_hash().map_err(|error| error.to_string())?,
                ),
                None,
            )),
        }
    }

    fn apply_entry(&self, entry: &LogEntry) -> Result<Option<SqlCommandResult>, String> {
        match self {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            Self::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "sql")]
            Self::Sql(state) => state
                .apply_entry_with_result(entry)
                .map(|outcome| outcome.sql_result().cloned())
                .map_err(|error| error.to_string()),
            #[cfg(feature = "graph")]
            Self::Graph(state) => state
                .apply_entry(entry)
                .map(|_| None)
                .map_err(|error| error.to_string()),
            #[cfg(feature = "kv")]
            Self::Kv(state) => state
                .apply_entry(entry)
                .map(|_| None)
                .map_err(|error| error.to_string()),
        }
    }
}

pub struct NodeRuntime {
    config: NodeConfig,
    consensus: Arc<ThreeNodeConsensus>,
    log_store: FileLogStore,
    materializer: Mutex<Materializer>,
    commit: Mutex<()>,
    checkpointing: AtomicBool,
    operation_cancelled: AtomicBool,
    operation_cancelled_notify: tokio::sync::Notify,
    ready: AtomicBool,
    fatal: AtomicBool,
    fatal_reason: Mutex<Option<String>>,
    _data_root_lock: fs::File,
}

#[cfg(feature = "sql")]
struct ExecutedPayload {
    response: WriteResponse,
    sql_result: Option<SqlCommandResult>,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedSnapshotPublication {
    anchor: RecoveryAnchor,
}

impl fmt::Debug for NodeRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeRuntime")
            .field("config", &self.config)
            .field("ready", &self.ready.load(Ordering::Acquire))
            .field("fatal", &self.fatal.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl NodeRuntime {
    pub fn open(
        config: NodeConfig,
        consensus: Arc<ThreeNodeConsensus>,
        peer_candidates: &[&dyn LogPeer],
    ) -> Result<Self, NodeError> {
        if config.ack_mode == AckMode::DrStrong {
            return Err(NodeError::UnsupportedAckMode(AckMode::DrStrong));
        }
        Materializer::ensure_profile_available(config.execution_profile())?;
        fs::create_dir_all(&config.data_dir)
            .map_err(|error| NodeError::Storage(error.to_string()))?;
        let lock_path = config.data_dir.join(".node.lock");
        let data_root_lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| NodeError::Storage(error.to_string()))?;
        match data_root_lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(NodeError::DataRootLocked(config.data_dir.clone()));
            }
            Err(fs::TryLockError::Error(error)) => {
                return Err(NodeError::Storage(error.to_string()));
            }
        }

        let log_store = FileLogStore::open_with_configuration(
            config.data_dir.join("consensus/log"),
            &config.cluster_id,
            config.epoch,
            config.log_initial_configuration.clone(),
        )
        .map_err(|error| NodeError::Storage(error.to_string()))?;
        let persisted_configuration = log_store
            .configuration_state()
            .map_err(|error| NodeError::Storage(error.to_string()))?;
        let materializer = Materializer::open(&config, &persisted_configuration)?;
        reconcile_local_storage(&config, &log_store, &materializer)?;
        recover_peer_candidates(
            &config,
            consensus.as_ref(),
            &log_store,
            &materializer,
            peer_candidates,
        )?;
        recover_startup_decisions(&config, consensus.as_ref(), &log_store, &materializer)?;

        Ok(Self {
            config,
            consensus,
            log_store,
            materializer: Mutex::new(materializer),
            commit: Mutex::new(()),
            checkpointing: AtomicBool::new(false),
            operation_cancelled: AtomicBool::new(false),
            operation_cancelled_notify: tokio::sync::Notify::new(),
            ready: AtomicBool::new(true),
            fatal: AtomicBool::new(false),
            fatal_reason: Mutex::new(None),
            _data_root_lock: data_root_lock,
        })
    }

    #[cfg(feature = "sql")]
    pub fn write(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, NodeError> {
        let payload = canonical_put(request_id, key, value)?;
        let _commit = self.lock_commit()?;
        self.execute_put_payload_locked(request_id, key, value, payload)
            .map(|outcome| outcome.response)
    }

    #[cfg(feature = "graph")]
    pub fn mutate_graph(&self, command: GraphCommandV1) -> Result<GraphMutationOutcome, NodeError> {
        let payload = encode_replicated_graph_command(&command)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
        if payload.len() > MAX_COMMAND_BYTES {
            return Err(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        let _commit = self.lock_commit()?;
        self.mutate_graph_payload_locked(&command, payload)
    }

    /// Executes an ordered, non-atomic batch of graph mutations.
    ///
    /// Each newly applied command is prepared and committed as its own exact-base LGFX entry.
    /// Per-command conflicts remain isolated in the returned vector, whose order and length match
    /// `commands`. The whole vector is validated before the first write attempt, so an outer `Err`
    /// guarantees that nothing was attempted.
    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(unreachable_patterns)
    )]
    pub fn mutate_graph_batch(
        &self,
        commands: Vec<GraphCommandV1>,
    ) -> Result<Vec<Result<GraphMutationOutcome, NodeError>>, NodeError> {
        self.require_execution_profile(ExecutionProfile::Graph)?;
        validate_typed_batch_len(commands.len())?;
        let mut members = Vec::with_capacity(commands.len());
        for command in commands {
            let payload = encode_replicated_graph_command(&command)
                .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
            validate_command_size(&payload)?;
            members.push(RuntimeBatchMember {
                #[cfg(feature = "sql")]
                request_id: command.request_id().to_owned(),
                payload,
                operation: QueuedOperation::Graph(command),
            });
        }
        Ok(self
            .execute_client_batch(members)
            .into_iter()
            .map(|result| {
                result.and_then(|response| match response {
                    ClientWriteResponse::Graph(outcome) => Ok(outcome),
                    _ => Err(NodeError::Invariant(
                        "graph batch returned a response for another profile".into(),
                    )),
                })
            })
            .collect())
    }

    #[cfg(feature = "graph")]
    fn mutate_graph_payload_locked(
        &self,
        command: &GraphCommandV1,
        payload: Vec<u8>,
    ) -> Result<GraphMutationOutcome, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        if let Some(record) = self.check_graph_request(command.request_id(), &payload)? {
            return Ok(GraphMutationOutcome::from_record(record));
        }
        loop {
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let proposal_payload = self
                .graph_materializer()?
                .prepare_graph_effect(&payload, last_index, last_hash)
                .map_err(|error| self.map_graph_mutation_error(error))?;
            if !proposal_payload.starts_with(LGFX_V1_MAGIC) {
                return Err(self.latch(NodeError::Invariant(
                    "graph materializer prepared a non-LGFX proposal".into(),
                )));
            }
            if proposal_payload.len() > MAX_COMMAND_BYTES {
                return Err(NodeError::ResourceExhausted(format!(
                    "graph LGFX effect exceeds {MAX_COMMAND_BYTES} bytes"
                )));
            }
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let entry = self
                .consensus
                .propose_at_cancellable(
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, proposal_payload.clone()),
                    &self.operation_cancelled,
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;
            if let Some(record) = self.check_graph_request(command.request_id(), &payload)? {
                return Ok(GraphMutationOutcome::from_record(record));
            }
            if entry.entry_type == EntryType::Command && entry.payload == proposal_payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed graph request was not recorded by Ladybug".into(),
                )));
            }
        }
    }

    #[cfg(feature = "graph")]
    pub fn get_graph_document(
        &self,
        id: &str,
        consistency: ReadConsistency,
    ) -> Result<GraphReadResponse, NodeError> {
        match consistency {
            ReadConsistency::Local => self.get_graph_document_local(id, None),
            ReadConsistency::AppliedIndex(required) => {
                self.get_graph_document_local(id, Some(required))
            }
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.get_graph_document_local(id, None)
            }
        }
    }

    #[cfg(feature = "graph")]
    pub fn query_graph(
        &self,
        statement: &str,
        parameters: &BTreeMap<String, GraphParameterValue>,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<GraphQueryResult, NodeError> {
        if max_rows == 0 || max_rows > MAX_GRAPH_MAX_ROWS {
            return Err(NodeError::InvalidRequest(format!(
                "max_rows must be between 1 and {MAX_GRAPH_MAX_ROWS}"
            )));
        }
        match consistency {
            ReadConsistency::Local => self.query_graph_local(statement, parameters, None, max_rows),
            ReadConsistency::AppliedIndex(required) => {
                self.query_graph_local(statement, parameters, Some(required), max_rows)
            }
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.query_graph_local(statement, parameters, None, max_rows)
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn mutate_kv(&self, command: KvCommandV1) -> Result<KvMutationOutcome, NodeError> {
        let payload = encode_replicated_kv_command(&command)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
        if payload.len() > MAX_COMMAND_BYTES {
            return Err(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        let _commit = self.lock_commit()?;
        self.mutate_kv_payload_locked(&command, payload)
    }

    /// Executes an ordered, non-atomic batch of KV mutations.
    ///
    /// Valid commands may share one QuePaxa log entry. Per-command conflicts remain isolated in
    /// the returned vector, whose order and length match `commands`. The whole vector is validated
    /// before the first write attempt, so an outer `Err` guarantees that nothing was attempted.
    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(unreachable_patterns)
    )]
    pub fn mutate_kv_batch(
        &self,
        commands: Vec<KvCommandV1>,
    ) -> Result<Vec<Result<KvMutationOutcome, NodeError>>, NodeError> {
        self.require_execution_profile(ExecutionProfile::Kv)?;
        validate_typed_batch_len(commands.len())?;
        let mut members = Vec::with_capacity(commands.len());
        for command in commands {
            let payload = encode_replicated_kv_command(&command)
                .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
            validate_command_size(&payload)?;
            members.push(RuntimeBatchMember {
                #[cfg(feature = "sql")]
                request_id: command.request_id().to_owned(),
                payload,
                operation: QueuedOperation::Kv(command),
            });
        }
        Ok(self
            .execute_client_batch(members)
            .into_iter()
            .map(|result| {
                result.and_then(|response| match response {
                    ClientWriteResponse::Kv(outcome) => Ok(outcome),
                    _ => Err(NodeError::Invariant(
                        "KV batch returned a response for another profile".into(),
                    )),
                })
            })
            .collect())
    }

    #[cfg(feature = "kv")]
    fn mutate_kv_payload_locked(
        &self,
        command: &KvCommandV1,
        payload: Vec<u8>,
    ) -> Result<KvMutationOutcome, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        if let Some(record) = self.check_kv_request(command.request_id(), &payload)? {
            return Ok(KvMutationOutcome::from_record(record));
        }
        loop {
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let entry = self
                .consensus
                .propose_at_cancellable(
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, payload.clone()),
                    &self.operation_cancelled,
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;
            if let Some(record) = self.check_kv_request(command.request_id(), &payload)? {
                return Ok(KvMutationOutcome::from_record(record));
            }
            if entry.entry_type == EntryType::Command && entry.payload == payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed KV request was not recorded by redb".into(),
                )));
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn get_kv(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<KvReadResponse, NodeError> {
        match consistency {
            ReadConsistency::Local => self.get_kv_local(key, None),
            ReadConsistency::AppliedIndex(required) => self.get_kv_local(key, Some(required)),
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.get_kv_local(key, None)
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn scan_kv_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, NodeError> {
        match consistency {
            ReadConsistency::Local => self.scan_kv_range_local(start, end, limit, cursor, None),
            ReadConsistency::AppliedIndex(required) => {
                self.scan_kv_range_local(start, end, limit, cursor, Some(required))
            }
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.scan_kv_range_local(start, end, limit, cursor, None)
            }
        }
    }

    #[cfg(feature = "kv")]
    pub fn scan_kv_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
        cursor: Option<&[u8]>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, NodeError> {
        match consistency {
            ReadConsistency::Local => self.scan_kv_prefix_local(prefix, limit, cursor, None),
            ReadConsistency::AppliedIndex(required) => {
                self.scan_kv_prefix_local(prefix, limit, cursor, Some(required))
            }
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.scan_kv_prefix_local(prefix, limit, cursor, None)
            }
        }
    }

    #[cfg(feature = "sql")]
    pub fn execute_sql(&self, command: SqlCommand) -> Result<WriteResponse, NodeError> {
        self.execute_sql_with_results(command)
            .map(|response| WriteResponse {
                applied_index: response.applied_index,
                hash: response.hash,
            })
    }

    /// Executes an ordered, non-atomic batch of SQL commands.
    ///
    /// Each newly applied command is prepared and committed as its own exact-base QWAL entry.
    /// Per-command conflicts remain isolated in the returned vector, whose order and length match
    /// `commands`. The whole vector is validated before the first write attempt, so an outer `Err`
    /// guarantees that nothing was attempted.
    #[cfg(feature = "sql")]
    pub fn execute_sql_batch(
        &self,
        commands: Vec<SqlCommand>,
    ) -> Result<Vec<Result<SqlExecuteResponse, NodeError>>, NodeError> {
        self.require_execution_profile(ExecutionProfile::Sqlite)?;
        validate_typed_batch_len(commands.len())?;
        let mut members = Vec::with_capacity(commands.len());
        for command in commands {
            validate_field(
                "request_id",
                &command.request_id,
                MAX_REQUEST_ID_BYTES,
                false,
            )?;
            let payload = encode_sql_command_with_index(&command)?;
            validate_command_size(&payload)?;
            members.push(RuntimeBatchMember {
                request_id: command.request_id.clone(),
                payload,
                operation: QueuedOperation::Sql(command),
            });
        }
        Ok(self
            .execute_client_batch(members)
            .into_iter()
            .map(|result| {
                result.and_then(|response| match response {
                    ClientWriteResponse::Sql(response) => Ok(response),
                    _ => Err(NodeError::Invariant(
                        "SQL batch returned a response for another profile".into(),
                    )),
                })
            })
            .collect())
    }

    #[cfg(feature = "sql")]
    fn execute_sql_with_results(
        &self,
        command: SqlCommand,
    ) -> Result<SqlExecuteResponse, NodeError> {
        validate_field(
            "request_id",
            &command.request_id,
            MAX_REQUEST_ID_BYTES,
            false,
        )?;
        let payload = encode_sql_command_with_index(&command)?;
        if payload.len() > MAX_COMMAND_BYTES {
            return Err(NodeError::InvalidRequest(format!(
                "command exceeds {MAX_COMMAND_BYTES} bytes"
            )));
        }
        let _commit = self.lock_commit()?;
        let outcome = self.execute_sql_payload_locked(&command, payload)?;
        Ok(sql_execute_response(outcome.response, outcome.sql_result))
    }

    #[cfg(feature = "sql")]
    fn execute_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        if self.config.execution_profile != ExecutionProfile::Sqlite {
            return self.execute_profile_client_batch(members);
        }
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }

        members
            .iter()
            .map(|member| self.execute_single_member_locked(member))
            .collect()
    }

    #[cfg(not(feature = "sql"))]
    fn execute_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        self.execute_profile_client_batch(members)
    }

    fn execute_profile_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        #[cfg(feature = "graph")]
        if self.config.execution_profile == ExecutionProfile::Graph {
            return self.execute_graph_client_batch(members);
        }
        #[cfg(feature = "kv")]
        if self.config.execution_profile == ExecutionProfile::Kv {
            return self.execute_kv_client_batch(members);
        }
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }
        members
            .iter()
            .map(|member| self.execute_profile_member_locked(member))
            .collect()
    }

    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(irrefutable_let_patterns, unreachable_patterns)
    )]
    fn execute_graph_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }

        members
            .iter()
            .map(|member| self.execute_profile_member_locked(member))
            .collect()
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns, unreachable_patterns)
    )]
    fn execute_kv_client_batch(
        &self,
        members: Vec<RuntimeBatchMember>,
    ) -> Vec<Result<ClientWriteResponse, NodeError>> {
        let _commit = match self.lock_commit() {
            Ok(commit) => commit,
            Err(error) => return members.into_iter().map(|_| Err(error.clone())).collect(),
        };
        if let Err(error) = self
            .ensure_ready()
            .and_then(|_| self.ensure_writes_active())
        {
            return members.into_iter().map(|_| Err(error.clone())).collect();
        }

        let mut results = vec![None; members.len()];
        let mut pending = Vec::new();
        let mut canonical_by_request = HashMap::new();
        let mut aliases = vec![None; members.len()];
        for (index, member) in members.iter().enumerate() {
            let QueuedOperation::Kv(command) = &member.operation else {
                results[index] = Some(Err(NodeError::ExecutionProfileMismatch {
                    expected: ExecutionProfile::Kv,
                    actual: ExecutionProfile::Sqlite,
                }));
                continue;
            };
            match self.check_kv_request(command.request_id(), &member.payload) {
                Ok(Some(record)) => {
                    results[index] = Some(Ok(ClientWriteResponse::Kv(
                        KvMutationOutcome::from_record(record),
                    )));
                }
                Ok(None) => match classify_pending_request(
                    &mut canonical_by_request,
                    &members,
                    index,
                    command.request_id(),
                ) {
                    Ok(None) => pending.push(index),
                    Ok(Some(canonical)) => aliases[index] = Some(canonical),
                    Err(error) => results[index] = Some(Err(error)),
                },
                Err(error) => results[index] = Some(Err(error)),
            }
        }

        while !pending.is_empty() {
            if pending.len() == 1 {
                let index = pending[0];
                results[index] = Some(self.execute_profile_member_locked(&members[index]));
                break;
            }
            let commands = pending
                .iter()
                .map(|index| match &members[*index].operation {
                    QueuedOperation::Kv(command) => command.clone(),
                    _ => unreachable!("KV pending members were validated above"),
                })
                .collect::<Vec<_>>();
            let full_payload = match encode_replicated_kv_batch(&commands) {
                Ok(payload) => payload,
                Err(error) => {
                    let error = NodeError::InvalidRequest(error.to_string());
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let (proposal_count, proposal_payload) = if full_payload.len() <= MAX_COMMAND_BYTES {
                (commands.len(), full_payload)
            } else {
                let mut prefix = None;
                for count in (2..commands.len()).rev() {
                    let payload = encode_replicated_kv_batch(&commands[..count])
                        .expect("the validated KV batch prefix remains valid");
                    if payload.len() <= MAX_COMMAND_BYTES {
                        prefix = Some((count, payload));
                        break;
                    }
                }
                let Some(prefix) = prefix else {
                    let index = pending.remove(0);
                    results[index] = Some(self.execute_profile_member_locked(&members[index]));
                    continue;
                };
                prefix
            };
            let proposed_indices = pending[..proposal_count].to_vec();
            let (last_index, last_hash) = match self.ensure_materialized_tip() {
                Ok(tip) => tip,
                Err(error) => {
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let slot = match last_index.checked_add(1) {
                Some(slot) => slot,
                None => {
                    let error = self.latch(NodeError::Invariant("qlog index is exhausted".into()));
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let entry = match self.consensus.propose_at_cancellable(
                slot,
                last_hash,
                Command::new(CommandKind::Deterministic, proposal_payload.clone()),
                &self.operation_cancelled,
            ) {
                Ok(entry) => entry,
                Err(error) => {
                    let error = self.map_consensus_error(error);
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            if let Err(error) = self.persist_entry(&entry, slot, last_hash) {
                for index in pending.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
                break;
            }

            let mut remaining = Vec::new();
            for index in pending.drain(..) {
                let member = &members[index];
                let QueuedOperation::Kv(command) = &member.operation else {
                    unreachable!("KV pending members were validated above");
                };
                match self.check_kv_request(command.request_id(), &member.payload) {
                    Ok(Some(record)) => {
                        results[index] = Some(Ok(ClientWriteResponse::Kv(
                            KvMutationOutcome::from_record(record),
                        )));
                    }
                    Ok(None) => remaining.push(index),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }
            if entry.entry_type == EntryType::Command
                && entry.payload == proposal_payload
                && remaining
                    .iter()
                    .any(|index| proposed_indices.contains(index))
            {
                let error = self.latch(NodeError::Invariant(
                    "committed KV batch did not record every request".into(),
                ));
                for index in remaining.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
            }
            pending = remaining;
        }

        for (index, canonical) in aliases.into_iter().enumerate() {
            if let Some(canonical) = canonical {
                results[index] = results[canonical].clone();
            }
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(self.latch(NodeError::Invariant(
                        "KV writer batch omitted a request result".into(),
                    )))
                })
            })
            .collect()
    }

    fn execute_profile_member_locked(
        &self,
        member: &RuntimeBatchMember,
    ) -> Result<ClientWriteResponse, NodeError> {
        match &member.operation {
            #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
            QueuedOperation::Unavailable => unreachable!("no execution profiles are compiled in"),
            #[cfg(feature = "graph")]
            QueuedOperation::Graph(command) => self
                .mutate_graph_payload_locked(command, member.payload.clone())
                .map(ClientWriteResponse::Graph),
            #[cfg(feature = "kv")]
            QueuedOperation::Kv(command) => self
                .mutate_kv_payload_locked(command, member.payload.clone())
                .map(ClientWriteResponse::Kv),
            #[cfg(feature = "sql")]
            QueuedOperation::KeyValue { .. } | QueuedOperation::Sql(_) => {
                Err(NodeError::ExecutionProfileMismatch {
                    expected: self.config.execution_profile,
                    actual: ExecutionProfile::Sqlite,
                })
            }
        }
    }

    #[cfg(feature = "sql")]
    fn execute_single_member_locked(
        &self,
        member: &RuntimeBatchMember,
    ) -> Result<ClientWriteResponse, NodeError> {
        if let QueuedOperation::Sql(command) = &member.operation {
            self.execute_sql_payload_locked(command, member.payload.clone())
                .map(|outcome| {
                    ClientWriteResponse::Sql(sql_execute_response(
                        outcome.response,
                        outcome.sql_result,
                    ))
                })
        } else if let QueuedOperation::KeyValue { key, value } = &member.operation {
            self.execute_put_payload_locked(&member.request_id, key, value, member.payload.clone())
                .map(|outcome| ClientWriteResponse::KeyValue(outcome.response))
        } else {
            Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: self.config.execution_profile,
            })
        }
    }

    #[cfg(feature = "sql")]
    fn execute_sql_payload_locked(
        &self,
        command: &SqlCommand,
        request_payload: Vec<u8>,
    ) -> Result<ExecutedPayload, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        if let Some(outcome) = self.check_request(&command.request_id, &request_payload)? {
            return Ok(ExecutedPayload {
                response: write_response(outcome),
                sql_result: self.replay_sql_result(
                    &command.request_id,
                    &request_payload,
                    outcome,
                )?,
            });
        }

        loop {
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let proposal_payload =
                self.prepare_sql_proposal(command, &request_payload, last_index, last_hash)?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let entry = self
                .consensus
                .propose_at_cancellable(
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, proposal_payload.clone()),
                    &self.operation_cancelled,
                )
                .map_err(|error| self.map_consensus_error(error))?;
            let sql_result = self.persist_entry(&entry, slot, last_hash)?;
            if let Some(outcome) = self.check_request(&command.request_id, &request_payload)? {
                return Ok(ExecutedPayload {
                    response: write_response(outcome),
                    sql_result: sql_result.or(self.replay_sql_result(
                        &command.request_id,
                        &request_payload,
                        outcome,
                    )?),
                });
            }
            if entry.entry_type == EntryType::Command && entry.payload == proposal_payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed SQL request was not recorded by SQLite".into(),
                )));
            }
        }
    }

    #[cfg(feature = "sql")]
    fn prepare_sql_proposal(
        &self,
        command: &SqlCommand,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        match sqlite.prepare_sql_effect(command, request_payload, base_index, base_hash) {
            Ok(SqlEffectPreparation::Effect(payload)) if !payload.starts_with(QWAL_V1_MAGIC) => {
                Err(self.latch(NodeError::Invariant(
                    "SQLite materializer prepared a non-QWAL SQL proposal".into(),
                )))
            }
            Ok(SqlEffectPreparation::Effect(payload)) if payload.len() <= MAX_COMMAND_BYTES => {
                Ok(payload)
            }
            Ok(SqlEffectPreparation::Effect(_)) => Err(NodeError::ResourceExhausted(format!(
                "SQL effect exceeds {MAX_COMMAND_BYTES} bytes"
            ))),
            Err(rhiza_sql::Error::ResourceExhausted(message)) => {
                Err(NodeError::ResourceExhausted(message))
            }
            Err(error) => {
                let message = error.to_string();
                let statement_index = first_invalid_sql_statement(command, |prefix| {
                    let Ok(prefix_payload) = encode_sql_command(prefix) else {
                        return true;
                    };
                    sqlite
                        .prepare_sql_effect(prefix, &prefix_payload, base_index, base_hash)
                        .is_err()
                });
                match statement_index {
                    Some(statement_index) => Err(NodeError::InvalidSqlStatement {
                        statement_index,
                        message,
                    }),
                    None => Err(NodeError::InvalidRequest(message)),
                }
            }
        }
    }

    #[cfg(feature = "sql")]
    fn execute_put_payload_locked(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
        payload: Vec<u8>,
    ) -> Result<ExecutedPayload, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;

        if let Some(outcome) = self.check_request(request_id, &payload)? {
            return Ok(ExecutedPayload {
                response: write_response(outcome),
                sql_result: None,
            });
        }

        loop {
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let proposal_payload = self
                .lock_sqlite()?
                .prepare_put_effect(request_id, key, value, &payload, last_index, last_hash)
                .map_err(|error| self.map_sqlite_error(error))?;
            if !proposal_payload.starts_with(QWAL_V1_MAGIC) {
                return Err(self.latch(NodeError::Invariant(
                    "SQLite materializer prepared a non-QWAL legacy put proposal".into(),
                )));
            }
            if proposal_payload.len() > MAX_COMMAND_BYTES {
                return Err(NodeError::ResourceExhausted(format!(
                    "SQLite QWAL effect exceeds {MAX_COMMAND_BYTES} bytes"
                )));
            }
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            let entry = self
                .consensus
                .propose_at_cancellable(
                    slot,
                    last_hash,
                    Command::new(CommandKind::Deterministic, proposal_payload.clone()),
                    &self.operation_cancelled,
                )
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;

            if let Some(outcome) = self.check_request(request_id, &payload)? {
                return Ok(ExecutedPayload {
                    response: write_response(outcome),
                    sql_result: None,
                });
            }
            if entry.entry_type == EntryType::Command && entry.payload == proposal_payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed legacy put request was not recorded by SQLite QWAL".into(),
                )));
            }
        }
    }

    #[cfg(feature = "sql")]
    fn replay_sql_result(
        &self,
        request_id: &str,
        payload: &[u8],
        outcome: RequestOutcome,
    ) -> Result<Option<SqlCommandResult>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        let stored = sqlite
            .check_sql_request(request_id, payload)
            .map_err(|error| self.map_sqlite_error(error))?
            .ok_or_else(|| {
                self.latch(NodeError::Invariant(
                    "committed SQL request result is missing".into(),
                ))
            })?;
        if stored.0 != outcome {
            return Err(self.latch(NodeError::Invariant(
                "stored SQL request outcome changed".into(),
            )));
        }
        Ok(stored.1)
    }

    #[cfg(feature = "sql")]
    pub fn read(&self, key: &str, consistency: ReadConsistency) -> Result<ReadResponse, NodeError> {
        validate_key(key)?;
        match consistency {
            ReadConsistency::Local => self.read_local(key, None),
            ReadConsistency::AppliedIndex(required) => self.read_local(key, Some(required)),
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.read_local(key, None)
            }
        }
    }

    #[cfg(feature = "sql")]
    pub fn query_sql(
        &self,
        statement: &SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        if max_rows == 0 || max_rows > MAX_SQL_MAX_ROWS {
            return Err(NodeError::InvalidRequest(format!(
                "max_rows must be between 1 and {MAX_SQL_MAX_ROWS}"
            )));
        }
        match consistency {
            ReadConsistency::Local => self.query_sql_local(statement, None, max_rows),
            ReadConsistency::AppliedIndex(required) => {
                self.query_sql_local(statement, Some(required), max_rows)
            }
            ReadConsistency::ReadBarrier => {
                let _commit = self.lock_commit()?;
                self.ensure_ready()?;
                self.commit_read_barrier()?;
                self.query_sql_local(statement, None, max_rows)
            }
        }
    }

    pub fn applied_index(&self) -> Result<LogIndex, NodeError> {
        self.ensure_ready()?;
        self.lock_materializer()?
            .applied_index()
            .map_err(|error| self.latch(NodeError::Storage(error)))
    }

    pub fn applied_hash(&self) -> Result<LogHash, NodeError> {
        self.ensure_ready()?;
        self.lock_materializer()?
            .applied_hash()
            .map_err(|error| self.latch(NodeError::Storage(error)))
    }

    pub fn cancel_operations(&self) {
        self.operation_cancelled.store(true, Ordering::Release);
        self.operation_cancelled_notify.notify_waiters();
    }

    pub fn materialize_next_decision(&self) -> Result<bool, NodeError> {
        let _commit = self.lock_commit()?;
        self.ensure_ready()?;
        let (last_index, last_hash) = self.ensure_materialized_tip()?;
        let slot = last_index
            .checked_add(1)
            .ok_or_else(|| self.latch(NodeError::Invariant("qlog index is exhausted".into())))?;
        match self
            .consensus
            .inspect_decision_at(slot, last_hash)
            .map_err(|error| self.map_consensus_error(error))?
        {
            DecisionInspection::Committed(entry) => {
                self.persist_entry(&entry, slot, last_hash)?;
                Ok(true)
            }
            DecisionInspection::Empty | DecisionInspection::Pending => Ok(false),
            DecisionInspection::Unavailable => Err(NodeError::Unavailable(
                "decision proof inspection did not reach quorum".into(),
            )),
        }
    }

    pub async fn run_background_materializer<F>(
        self: Arc<Self>,
        poll_interval: Duration,
        shutdown: F,
    ) -> Result<(), NodeError>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let poll_interval = poll_interval.max(Duration::from_millis(10));
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = tokio::time::sleep(poll_interval) => {
                    loop {
                        let runtime = Arc::clone(&self);
                        let mut operation = tokio::task::spawn_blocking(move || runtime.materialize_next_decision());
                        let (result, shutting_down) = tokio::select! {
                            () = &mut shutdown => {
                                self.cancel_operations();
                                (operation.await, true)
                            }
                            result = &mut operation => (result, false),
                        };
                        if shutting_down {
                            return match result {
                                Ok(Ok(_) | Err(NodeError::Unavailable(_) | NodeError::Contention(_))) => Ok(()),
                                Ok(Err(error)) => Err(error),
                                Err(error) => Err(NodeError::Fatal(format!("materializer task failed: {error}"))),
                            };
                        }
                        match result {
                            Ok(Ok(true)) => continue,
                            Ok(Ok(false) | Err(NodeError::Unavailable(_) | NodeError::Contention(_))) => break,
                            Ok(Err(error)) => return Err(error),
                            Err(error) => return Err(NodeError::Fatal(format!("materializer task failed: {error}"))),
                        }
                    }
                }
            }
        }
    }

    pub const fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub const fn consensus(&self) -> &Arc<ThreeNodeConsensus> {
        &self.consensus
    }

    pub const fn log_store(&self) -> &FileLogStore {
        &self.log_store
    }

    pub fn configuration_state(&self) -> Result<ConfigurationState, NodeError> {
        self.log_store
            .configuration_state()
            .map_err(|error| NodeError::Storage(error.to_string()))
    }

    pub fn status(&self) -> Result<NodeStatus, NodeError> {
        let configuration_state = self.configuration_state()?;
        let (configuration_status, active_config_id) = if configuration_state.is_active() {
            (
                RuntimeConfigurationStatus::Active,
                configuration_state.config_id(),
            )
        } else if configuration_state.config_id() == self.consensus.config_id() {
            (
                RuntimeConfigurationStatus::Stopped,
                configuration_state.config_id(),
            )
        } else {
            (
                RuntimeConfigurationStatus::AwaitingActivation,
                configuration_state
                    .config_id()
                    .checked_add(1)
                    .ok_or_else(|| {
                        NodeError::Invariant("successor configuration id is exhausted".into())
                    })?,
            )
        };
        Ok(NodeStatus {
            ready: self.is_ready(),
            stop_anchor: configuration_state.stop().copied(),
            active_config_id,
            active_membership_digest: self.config.membership.digest(),
            configuration_status,
            configuration_state,
        })
    }

    pub fn stop_current_configuration(&self) -> Result<StopInformation, NodeError> {
        let _commit = self.lock_commit()?;
        self.stop_current_configuration_locked(None)
    }

    pub fn stop_current_configuration_for_successor(
        &self,
        successor: &Membership,
    ) -> Result<StopInformation, NodeError> {
        let _commit = self.lock_commit()?;
        self.stop_current_configuration_locked(Some(successor))
    }

    pub fn stop_current_configuration_if(
        &self,
        expected_config_id: u64,
    ) -> Result<StopInformation, NodeError> {
        let _commit = self.lock_commit()?;
        let state = self.configuration_state()?;
        if !state.is_active() || state.config_id() != expected_config_id {
            return Err(NodeError::PreconditionFailed(
                "active configuration does not match expected_config_id".into(),
            ));
        }
        self.stop_current_configuration_locked(None)
    }

    fn stop_current_configuration_locked(
        &self,
        successor: Option<&Membership>,
    ) -> Result<StopInformation, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;
        let state = self.configuration_state()?;
        let stop_command = match successor {
            Some(successor) => ConfigChange::bound_stop(
                self.config.cluster_id.clone(),
                state.config_id(),
                state.digest(),
                state.config_id().checked_add(1).ok_or_else(|| {
                    NodeError::Invariant("successor config id is exhausted".into())
                })?,
                successor.members().to_vec(),
            )
            .map_err(|error| NodeError::Invariant(error.to_string()))?
            .to_stored_command(),
            None => ConfigChange::stop(state.config_id(), state.digest()).to_stored_command(),
        };
        loop {
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index
                .checked_add(1)
                .ok_or_else(|| NodeError::Invariant("qlog index is exhausted".into()))?;
            let entry = self
                .consensus
                .propose_stored_at(slot, last_hash, stop_command.clone())
                .map_err(|error| self.map_consensus_error(error))?;
            self.persist_entry(&entry, slot, last_hash)?;
            let decided = StoredCommand::new(entry.entry_type, entry.payload.clone());
            if decided != stop_command {
                let current = self.configuration_state()?;
                if current.is_active() {
                    continue;
                }
                return Err(NodeError::ConfigurationTransition {
                    state: Box::new(current),
                });
            }
            let proof = self
                .consensus
                .inspect_decision_proof_at(slot)
                .map_err(|error| self.map_consensus_error(error))?
                .ok_or_else(|| {
                    NodeError::Unavailable("durable Stop proof is unavailable".into())
                })?;
            if proof
                .proposal()
                .value
                .as_ref()
                .map(|value| value.entry_hash)
                != Some(entry.hash)
            {
                return Err(self.latch(NodeError::Reconciliation(
                    "Stop proof differs from committed stop entry".into(),
                )));
            }
            return Ok(StopInformation {
                version: 2,
                entry,
                proof,
            });
        }
    }

    pub fn activate_successor(&self) -> Result<LogEntry, NodeError> {
        let _commit = self.lock_commit()?;
        self.activate_successor_locked(None)
    }

    pub fn activate_successor_if(&self, expected_config_id: u64) -> Result<LogEntry, NodeError> {
        let _commit = self.lock_commit()?;
        self.activate_successor_locked(Some(expected_config_id))
    }

    fn activate_successor_locked(
        &self,
        expected_config_id: Option<u64>,
    ) -> Result<LogEntry, NodeError> {
        self.ensure_ready()?;
        let state = self.configuration_state()?;
        let stop = state
            .stop()
            .copied()
            .ok_or_else(|| NodeError::ConfigurationTransition {
                state: Box::new(state.clone()),
            })?;
        if state.config_id() == self.consensus.config_id() {
            return Err(NodeError::ConfigurationTransition {
                state: Box::new(state),
            });
        }
        let successor_config_id = state.config_id().checked_add(1).ok_or_else(|| {
            NodeError::Invariant("successor configuration id is exhausted".into())
        })?;
        if expected_config_id.is_some_and(|expected| expected != successor_config_id) {
            return Err(NodeError::PreconditionFailed(
                "successor configuration does not match expected_config_id".into(),
            ));
        }
        let stop_entry = self.recover_stop_entry(stop)?;
        let entry = self
            .consensus
            .propose_activation_for_stop_entry(&stop_entry)
            .map_err(|error| self.map_consensus_error(error))?;
        self.persist_entry(&entry, stop.index() + 1, stop.hash())?;
        Ok(entry)
    }

    pub(crate) fn recover_stop_entry(&self, stop: LogAnchor) -> Result<LogEntry, NodeError> {
        if let Some(entry) = self
            .log_store
            .read(stop.index())
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .filter(|entry| entry.hash == stop.hash())
        {
            return Ok(entry);
        }
        if let Some(entry) = self
            .config
            .predecessor_stop_entry
            .as_ref()
            .filter(|entry| entry.index == stop.index() && entry.hash == stop.hash())
        {
            validate_entry_envelope(&self.config, entry, entry.index, entry.prev_hash)?;
            return Ok(entry.clone());
        }
        let proof = self
            .consensus
            .inspect_decision_proof_at(stop.index())
            .map_err(|error| self.map_consensus_error(error))?
            .ok_or_else(|| NodeError::Unavailable("durable Stop proof is unavailable".into()))?;
        let value = proof
            .proposal()
            .value
            .as_ref()
            .filter(|value| value.entry_hash == stop.hash())
            .ok_or_else(|| {
                self.latch(NodeError::Reconciliation(
                    "Stop proof differs from compacted anchor".into(),
                ))
            })?;
        match self
            .consensus
            .inspect_decision_at(stop.index(), value.prev_hash)
            .map_err(|error| self.map_consensus_error(error))?
        {
            DecisionInspection::Committed(entry) if entry.hash == stop.hash() => Ok(entry),
            DecisionInspection::Unavailable => Err(NodeError::Unavailable(
                "durable Stop command is unavailable".into(),
            )),
            DecisionInspection::Committed(_)
            | DecisionInspection::Empty
            | DecisionInspection::Pending => Err(self.latch(NodeError::Reconciliation(
                "Stop decision differs from compacted anchor".into(),
            ))),
        }
    }

    pub fn log_root(&self) -> Result<LogAnchor, NodeError> {
        let _commit = self.lock_commit()?;
        self.log_root_unlocked()
    }

    fn log_root_unlocked(&self) -> Result<LogAnchor, NodeError> {
        let state = self
            .log_store
            .logical_state()
            .map_err(|error| NodeError::Storage(error.to_string()))?;
        Ok(state.tip.map_or_else(
            || {
                state
                    .anchor
                    .map_or(LogAnchor::new(0, LogHash::ZERO), |anchor| {
                        *anchor.compacted()
                    })
            },
            |entry| LogAnchor::new(entry.index(), entry.hash()),
        ))
    }

    pub fn fetch_log(&self, request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        fetch_runtime_log(self, request)
    }

    #[cfg(feature = "sql")]
    pub fn create_recovery_snapshot(&self) -> Result<RecoverySnapshot, NodeError> {
        let _commit = self.lock_commit()?;
        self.ensure_ready()?;
        self.ensure_materialized_tip()?;
        self.lock_sqlite()?
            .create_recovery_snapshot(self.config.recovery_generation)
            .map_err(|error| self.map_sqlite_error(error))
    }

    pub async fn checkpoint_compact(
        &self,
        coordinator: &CheckpointCoordinator,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        coordinator.checkpoint_compact(self).await
    }

    #[cfg(feature = "sql")]
    pub fn verify_snapshot_publication(
        &self,
        snapshot: &RecoverySnapshot,
        publication: &SnapshotRecord,
    ) -> Result<VerifiedSnapshotPublication, NodeError> {
        let anchor = snapshot.anchor();
        let manifest = publication.manifest();
        let publication_digest = LogHash::from_hex(publication.sha256()).ok_or_else(|| {
            NodeError::Reconciliation("published snapshot digest is invalid".into())
        })?;
        if anchor.cluster_id() != self.config.cluster_id
            || anchor.epoch() != self.config.epoch
            || anchor.config_id() != self.config.config_id()
            || anchor.recovery_generation() != self.config.recovery_generation
            || manifest.cluster_id() != anchor.cluster_id()
            || manifest.epoch() != anchor.epoch()
            || manifest.config_id() != anchor.config_id()
            || manifest.index() != anchor.compacted().index()
            || manifest.applied_hash() != anchor.compacted().hash()
            || manifest.snapshot_id() != anchor.snapshot().snapshot_id()
            || publication_digest != anchor.snapshot().digest()
            || publication.size_bytes() != anchor.snapshot().size_bytes()
            || LogHash::digest(&[snapshot.db_bytes()]) != anchor.snapshot().digest()
            || snapshot.db_bytes().len() as u64 != anchor.snapshot().size_bytes()
        {
            return Err(NodeError::Reconciliation(
                "published snapshot does not match the runtime recovery anchor".into(),
            ));
        }
        Ok(VerifiedSnapshotPublication {
            anchor: anchor.clone(),
        })
    }

    #[cfg(feature = "sql")]
    pub fn compact_log(&self, publication: &VerifiedSnapshotPublication) -> Result<(), NodeError> {
        let _commit = self.lock_commit()?;
        self.ensure_ready()?;
        let applied_index = self.applied_index()?;
        let applied_hash = self.applied_hash()?;
        let anchor = &publication.anchor;
        if anchor.cluster_id() != self.config.cluster_id
            || anchor.epoch() != self.config.epoch
            || anchor.config_id() != self.config.config_id()
            || anchor.recovery_generation() != self.config.recovery_generation
            || anchor.compacted().index() != applied_index
            || anchor.compacted().hash() != applied_hash
        {
            return Err(NodeError::Reconciliation(
                "verified snapshot anchor does not match the current applied entry".into(),
            ));
        }
        self.log_store
            .compact_prefix(anchor)
            .map_err(|error| NodeError::Storage(error.to_string()))
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && !self.fatal.load(Ordering::Acquire)
            && !self.checkpointing.load(Ordering::Acquire)
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal.load(Ordering::Acquire)
    }

    pub fn fatal_reason(&self) -> Option<String> {
        self.fatal_reason
            .lock()
            .ok()
            .and_then(|reason| reason.clone())
    }

    #[cfg(feature = "sql")]
    fn read_local(
        &self,
        key: &str,
        required_index: Option<LogIndex>,
    ) -> Result<ReadResponse, NodeError> {
        self.ensure_ready()?;
        let sqlite = self.lock_sqlite()?;
        let (applied_index, hash) = sqlite
            .applied_tip_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        let value = sqlite
            .get_value(key)
            .map_err(|error| self.map_sqlite_error(error))?;
        Ok(ReadResponse {
            value,
            applied_index,
            hash,
        })
    }

    #[cfg(feature = "graph")]
    fn get_graph_document_local(
        &self,
        id: &str,
        required_index: Option<LogIndex>,
    ) -> Result<GraphReadResponse, NodeError> {
        self.ensure_ready()?;
        let graph = self.graph_materializer()?;
        let (value, applied_index, hash) = graph
            .get_document_with_tip(id)
            .map_err(|error| self.map_graph_read_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        Ok(GraphReadResponse {
            value,
            applied_index,
            hash,
        })
    }

    #[cfg(feature = "graph")]
    fn query_graph_local(
        &self,
        statement: &str,
        parameters: &BTreeMap<String, GraphParameterValue>,
        required_index: Option<LogIndex>,
        max_rows: u32,
    ) -> Result<GraphQueryResult, NodeError> {
        self.ensure_ready()?;
        let graph = self.graph_materializer()?;
        let result = graph
            .query_read_only(
                statement,
                parameters,
                usize::try_from(max_rows).expect("u32 fits usize"),
                MAX_GRAPH_RESULT_BYTES,
                GRAPH_QUERY_TIMEOUT_MS,
            )
            .map_err(|error| self.map_graph_read_error(error))?;
        if required_index.is_some_and(|required| result.applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {} has not reached {}",
                result.applied_index,
                required_index.expect("checked above")
            )));
        }
        Ok(result)
    }

    #[cfg(feature = "kv")]
    fn get_kv_local(
        &self,
        key: &[u8],
        required_index: Option<LogIndex>,
    ) -> Result<KvReadResponse, NodeError> {
        self.ensure_ready()?;
        let kv = self.kv_materializer()?;
        let result = kv
            .get_with_tip(key)
            .map_err(|error| self.map_kv_read_error(error))?;
        let (value, tip) = result.into_parts();
        let applied_index = tip.applied_index();
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        Ok(KvReadResponse {
            value,
            applied_index,
            hash: tip.applied_hash(),
        })
    }

    #[cfg(feature = "kv")]
    fn scan_kv_range_local(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
        required_index: Option<LogIndex>,
    ) -> Result<KvScanResult, NodeError> {
        self.ensure_ready()?;
        let kv = self.kv_materializer()?;
        let result = kv
            .scan_range(start, end, limit, cursor)
            .map_err(|error| self.map_kv_read_error(error))?;
        validate_kv_scan_required_index(&result, required_index)?;
        Ok(result)
    }

    #[cfg(feature = "kv")]
    fn scan_kv_prefix_local(
        &self,
        prefix: &[u8],
        limit: usize,
        cursor: Option<&[u8]>,
        required_index: Option<LogIndex>,
    ) -> Result<KvScanResult, NodeError> {
        self.ensure_ready()?;
        let kv = self.kv_materializer()?;
        let result = kv
            .scan_prefix(prefix, limit, cursor)
            .map_err(|error| self.map_kv_read_error(error))?;
        validate_kv_scan_required_index(&result, required_index)?;
        Ok(result)
    }

    #[cfg(feature = "sql")]
    fn query_sql_local(
        &self,
        statement: &SqlStatement,
        required_index: Option<LogIndex>,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        self.ensure_ready()?;
        let sqlite = self.lock_sqlite()?;
        let (applied_index, hash) = sqlite
            .applied_tip_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        let SqlQueryResult { columns, rows } = sqlite
            .query_sql(
                statement,
                usize::try_from(max_rows).expect("u32 fits usize"),
                MAX_SQL_RESULT_BYTES,
            )
            .map_err(|error| match error {
                rhiza_sql::Error::ResourceExhausted(message) => {
                    NodeError::ResourceExhausted(message)
                }
                other => NodeError::InvalidSqlStatement {
                    statement_index: 0,
                    message: other.to_string(),
                },
            })?;
        Ok(SqlQueryResponse {
            columns,
            rows,
            applied_index,
            hash,
        })
    }

    fn commit_read_barrier(&self) -> Result<(), NodeError> {
        self.ensure_writes_active()?;
        loop {
            self.ensure_ready()?;
            let (last_index, last_hash) = self.ensure_materialized_tip()?;
            let slot = last_index.checked_add(1).ok_or_else(|| {
                self.latch(NodeError::Invariant("qlog index is exhausted".into()))
            })?;
            match self
                .consensus
                .inspect_decision_at(slot, last_hash)
                .map_err(|error| self.map_consensus_error(error))?
            {
                DecisionInspection::Committed(entry) => {
                    self.persist_entry(&entry, slot, last_hash)?;
                }
                DecisionInspection::Pending | DecisionInspection::Empty => {
                    let entry = self
                        .consensus
                        .propose_at_cancellable(
                            slot,
                            last_hash,
                            Command::new(CommandKind::ReadBarrier, Vec::new()),
                            &self.operation_cancelled,
                        )
                        .map_err(|error| self.map_consensus_error(error))?;
                    let is_barrier =
                        entry.entry_type == EntryType::Noop && entry.payload.is_empty();
                    self.persist_entry(&entry, slot, last_hash)?;
                    if is_barrier {
                        return Ok(());
                    }
                }
                DecisionInspection::Unavailable => {
                    return Err(NodeError::Unavailable(
                        "decision inspection did not reach quorum".into(),
                    ));
                }
            }
        }
    }

    #[cfg(feature = "sql")]
    fn check_request(
        &self,
        request_id: &str,
        payload: &[u8],
    ) -> Result<Option<RequestOutcome>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        sqlite
            .check_request(request_id, payload)
            .map_err(|error| self.map_sqlite_error(error))
    }

    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(irrefutable_let_patterns)
    )]
    fn check_graph_request(
        &self,
        request_id: &str,
        payload: &[u8],
    ) -> Result<Option<GraphRequestRecord>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Graph(graph) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Graph,
                actual: materializer.profile(),
            });
        };
        graph
            .check_request(request_id, payload)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns)
    )]
    fn check_kv_request(
        &self,
        request_id: &str,
        payload: &[u8],
    ) -> Result<Option<KvRequestRecord>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Kv(kv) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Kv,
                actual: materializer.profile(),
            });
        };
        kv.check_request(request_id, payload)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))
    }

    fn ensure_materialized_tip(&self) -> Result<(LogIndex, LogHash), NodeError> {
        let (last_index, last_hash) = self.durable_tip()?;
        let materializer = self.lock_materializer()?;
        let applied_index = materializer
            .applied_index()
            .map_err(|error| self.latch(NodeError::Storage(error)))?;
        let applied_hash = materializer
            .applied_hash()
            .map_err(|error| self.latch(NodeError::Storage(error)))?;
        if (applied_index, applied_hash) != (last_index, last_hash) {
            return Err(self.latch(NodeError::Invariant(format!(
                "qlog tip {last_index}/{} differs from {} materializer tip {applied_index}/{}",
                last_hash.to_hex(),
                materializer.profile(),
                applied_hash.to_hex()
            ))));
        }
        Ok((last_index, last_hash))
    }

    fn durable_tip(&self) -> Result<(LogIndex, LogHash), NodeError> {
        static_log_tip(&self.log_store).map_err(|error| self.latch(error))
    }

    fn persist_entry(
        &self,
        entry: &LogEntry,
        expected_index: LogIndex,
        expected_prev_hash: LogHash,
    ) -> Result<Option<SqlCommandResult>, NodeError> {
        let configuration_state = self.configuration_state()?;
        validate_runtime_entry(
            &self.config,
            &configuration_state,
            entry,
            expected_index,
            expected_prev_hash,
        )
        .map_err(|error| self.latch(error))?;
        self.log_store
            .append(entry)
            .map_err(|error| self.latch(NodeError::Storage(error.to_string())))?;
        self.lock_materializer()?
            .apply_entry(entry)
            .map_err(|error| self.latch(NodeError::Invariant(error)))
    }

    fn require_execution_profile(&self, expected: ExecutionProfile) -> Result<(), NodeError> {
        if self.config.execution_profile == expected {
            Ok(())
        } else {
            Err(NodeError::ExecutionProfileMismatch {
                expected,
                actual: self.config.execution_profile,
            })
        }
    }

    fn ensure_ready(&self) -> Result<(), NodeError> {
        if self.fatal.load(Ordering::Acquire) {
            return Err(NodeError::Fatal(
                self.fatal_reason()
                    .unwrap_or_else(|| "fatal state is latched".into()),
            ));
        }
        if !self.ready.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable("runtime is not ready".into()));
        }
        if self.checkpointing.load(Ordering::Acquire) {
            return Err(NodeError::Unavailable(
                "runtime checkpoint transition is in progress".into(),
            ));
        }
        Ok(())
    }

    fn ensure_writes_active(&self) -> Result<(), NodeError> {
        let state = self.configuration_state()?;
        if state.is_active() {
            Ok(())
        } else {
            Err(NodeError::ConfigurationTransition {
                state: Box::new(state),
            })
        }
    }

    fn lock_commit(&self) -> Result<MutexGuard<'_, ()>, NodeError> {
        self.commit
            .lock()
            .map_err(|_| self.latch(NodeError::Invariant("commit mutex is poisoned".into())))
    }

    fn lock_materializer(&self) -> Result<MutexGuard<'_, Materializer>, NodeError> {
        self.materializer.lock().map_err(|_| {
            self.latch(NodeError::Invariant(
                "materializer mutex is poisoned".into(),
            ))
        })
    }

    #[cfg(feature = "graph")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "kv")),
        allow(irrefutable_let_patterns)
    )]
    fn graph_materializer(&self) -> Result<Arc<LadybugStateMachine>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Graph(graph) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Graph,
                actual: materializer.profile(),
            });
        };
        Ok(Arc::clone(graph))
    }

    #[cfg(feature = "kv")]
    #[cfg_attr(
        all(not(feature = "sql"), not(feature = "graph")),
        allow(irrefutable_let_patterns)
    )]
    fn kv_materializer(&self) -> Result<Arc<RedbStateMachine>, NodeError> {
        let materializer = self.lock_materializer()?;
        let Materializer::Kv(kv) = &*materializer else {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Kv,
                actual: materializer.profile(),
            });
        };
        Ok(Arc::clone(kv))
    }

    #[cfg(feature = "sql")]
    fn lock_sqlite(&self) -> Result<SqlMaterializerGuard<'_>, NodeError> {
        let guard = self.lock_materializer()?;
        if !matches!(&*guard, Materializer::Sql(_)) {
            return Err(NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: guard.profile(),
            });
        }
        Ok(SqlMaterializerGuard(guard))
    }

    #[cfg(feature = "sql")]
    fn map_sqlite_error(&self, error: rhiza_sql::Error) -> NodeError {
        match error {
            rhiza_sql::Error::RequestConflict(conflict) => NodeError::RequestConflict(conflict),
            rhiza_sql::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_sql::Error::InvalidCommand(message)
            | rhiza_sql::Error::IdentityMismatch(message)
            | rhiza_sql::Error::InvalidEntry(message)
            | rhiza_sql::Error::InvalidSnapshot(message) => {
                self.latch(NodeError::Invariant(message))
            }
            other => self.latch(NodeError::Storage(other.to_string())),
        }
    }

    #[cfg(feature = "graph")]
    fn map_graph_read_error(&self, error: rhiza_graph::Error) -> NodeError {
        match error {
            rhiza_graph::Error::InvalidCommand(_) => NodeError::InvalidRequest(error.to_string()),
            rhiza_graph::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_graph::Error::Ladybug(_) | rhiza_graph::Error::Io(_) => {
                self.latch(NodeError::Storage(error.to_string()))
            }
            rhiza_graph::Error::Closed
            | rhiza_graph::Error::Codec(_)
            | rhiza_graph::Error::InvalidEntry(_)
            | rhiza_graph::Error::IdentityMismatch(_)
            | rhiza_graph::Error::RequestConflict { .. }
            | rhiza_graph::Error::InvalidSnapshot(_) => {
                self.latch(NodeError::Invariant(error.to_string()))
            }
        }
    }

    #[cfg(feature = "graph")]
    fn map_graph_mutation_error(&self, error: rhiza_graph::Error) -> NodeError {
        match error {
            rhiza_graph::Error::InvalidCommand(_) | rhiza_graph::Error::RequestConflict { .. } => {
                NodeError::InvalidRequest(error.to_string())
            }
            rhiza_graph::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_graph::Error::Ladybug(_)
            | rhiza_graph::Error::Io(_)
            | rhiza_graph::Error::Closed => self.latch(NodeError::Storage(error.to_string())),
            rhiza_graph::Error::Codec(_)
            | rhiza_graph::Error::InvalidEntry(_)
            | rhiza_graph::Error::IdentityMismatch(_)
            | rhiza_graph::Error::InvalidSnapshot(_) => {
                self.latch(NodeError::Invariant(error.to_string()))
            }
        }
    }

    #[cfg(feature = "kv")]
    fn map_kv_read_error(&self, error: rhiza_kv::Error) -> NodeError {
        match error {
            rhiza_kv::Error::InvalidCommand(_) | rhiza_kv::Error::InvalidQuery(_) => {
                NodeError::InvalidRequest(error.to_string())
            }
            rhiza_kv::Error::ResourceExhausted(message) => NodeError::ResourceExhausted(message),
            rhiza_kv::Error::Database(_) | rhiza_kv::Error::Io(_) => {
                self.latch(NodeError::Storage(error.to_string()))
            }
            rhiza_kv::Error::Codec(_)
            | rhiza_kv::Error::InvalidEntry(_)
            | rhiza_kv::Error::PartialInitialization
            | rhiza_kv::Error::RequestConflict { .. }
            | rhiza_kv::Error::InvalidSnapshot(_) => {
                self.latch(NodeError::Invariant(error.to_string()))
            }
        }
    }

    fn map_consensus_error(&self, error: rhiza_quepaxa::Error) -> NodeError {
        match error {
            rhiza_quepaxa::Error::NoQuorum
            | rhiza_quepaxa::Error::CommandUnavailable
            | rhiza_quepaxa::Error::Cancelled
            | rhiza_quepaxa::Error::Io(_) => NodeError::Unavailable(error.to_string()),
            rhiza_quepaxa::Error::ConflictingCertificates
            | rhiza_quepaxa::Error::ChainConflict { .. } => {
                self.latch(NodeError::Reconciliation(error.to_string()))
            }
            other => self.latch(NodeError::Invariant(other.to_string())),
        }
    }

    fn latch(&self, error: NodeError) -> NodeError {
        self.ready.store(false, Ordering::Release);
        if !self.fatal.swap(true, Ordering::AcqRel) {
            if let Ok(mut reason) = self.fatal_reason.lock() {
                *reason = Some(error.to_string());
            }
        }
        error
    }
}

pub fn rehydrate_recorder_after_checkpoint(
    runtime: &NodeRuntime,
    recorder: &RecorderFileStore,
    checkpoint_index: LogIndex,
) -> Result<(), NodeError> {
    if let Some(anchor) = runtime
        .log_store()
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?
        .anchor
    {
        if checkpoint_index < anchor.compacted().index() {
            return Err(NodeError::SnapshotRequired(Box::new(anchor)));
        }
    }
    let applied_index = runtime.applied_index()?;
    if checkpoint_index > applied_index {
        return Err(NodeError::Reconciliation(format!(
            "checkpoint tip {checkpoint_index} is ahead of local applied index {applied_index}"
        )));
    }

    for index in checkpoint_index.saturating_add(1)..=applied_index {
        let entry = runtime
            .log_store()
            .read(index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!(
                    "qlog entry {index} is missing during recorder rehydration"
                ))
            })?;
        let certified = match runtime
            .consensus()
            .inspect_certified_decision_at(index, entry.prev_hash)
            .map_err(startup_consensus_error)?
        {
            CertifiedDecisionInspection::Committed(certified) => certified,
            CertifiedDecisionInspection::Empty => {
                return Err(NodeError::Reconciliation(format!(
                    "qlog entry {index} has no recorder decision certificate"
                )))
            }
            CertifiedDecisionInspection::Pending => {
                return Err(NodeError::Reconciliation(format!(
                    "qlog entry {index} has only a pending recorder decision"
                )))
            }
            CertifiedDecisionInspection::Unavailable => {
                return Err(NodeError::Unavailable(format!(
                    "recorder decision certificate is unavailable at qlog index {index}"
                )))
            }
        };
        if certified.entry != entry {
            return Err(NodeError::Reconciliation(format!(
                "recorder decision certificate differs from qlog entry {index}"
            )));
        }
        let command = StoredCommand::new(entry.entry_type, entry.payload.clone());
        recorder
            .store_command(command.hash(), command)
            .map_err(|error| {
                NodeError::Reconciliation(format!(
                    "cannot restore recorder command at qlog index {index}: {error}"
                ))
            })?;
        let proof = certified.proof.clone();
        recorder
            .install_decision_proof_record(proof, runtime.consensus().membership())
            .map_err(|error| {
                NodeError::Reconciliation(format!(
                    "cannot install recorder decision at qlog index {index}: {error}"
                ))
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    #[cfg(feature = "graph")]
    use rhiza_core::LogEntry;
    use rhiza_core::{EntryType, ExecutionProfile, LogHash, StoredCommand};
    #[cfg(feature = "graph")]
    use rhiza_graph::{
        encode_replicated_graph_batch, encode_replicated_graph_command, GraphCommandV1,
        GraphValueV1, LGFX_V1_MAGIC,
    };
    #[cfg(feature = "kv")]
    use rhiza_kv::KvCommandV1;
    use rhiza_log::LogStore as _;
    use rhiza_quepaxa::{
        AcceptedValue, Membership, Proposal, ProposalPriority, RecordRequest, ThreeNodeConsensus,
    };
    use std::sync::Arc;

    #[cfg(any(feature = "graph", feature = "kv"))]
    use super::node_error_response;
    #[cfg(feature = "graph")]
    use super::with_graph_client_permit;
    use super::{
        client_authenticated, next_sync_flush_retry, run_read_operation, sql_query_http_response,
        valid_recorder_record, Duration, HeaderMap, NodeError, ReadConsistency, SqlCommand,
        SqlQueryResponse, SqlStatement, SqlValue, MAX_COMMAND_BYTES, MAX_SQL_RESPONSE_BYTES,
        PROTOCOL_VERSION, SYNC_FLUSH_RETRY_INITIAL, VERSION_HEADER,
    };
    use super::{NodeConfig, NodeRuntime, NodeService};

    #[test]
    fn client_authentication_rejects_empty_expected_token() {
        let mut headers = HeaderMap::new();
        headers.insert(VERSION_HEADER, HeaderValue::from_static(PROTOCOL_VERSION));
        headers.insert("authorization", HeaderValue::from_static("Bearer "));

        assert!(!client_authenticated(&headers, ""));
    }

    #[test]
    fn recorder_record_rejects_oversized_inline_command() {
        let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
        let command = StoredCommand::new(
            EntryType::Command,
            vec![0_u8; MAX_COMMAND_BYTES.saturating_add(1)],
        );
        let request = RecordRequest {
            cluster_id: "rhiza:sql:node-unit-test".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            slot: 1,
            step: 1,
            proposal: Proposal::new(
                ProposalPriority::MAX,
                "n1",
                1,
                AcceptedValue::from_command(
                    "rhiza:sql:node-unit-test",
                    1,
                    1,
                    1,
                    LogHash::ZERO,
                    &command,
                ),
            ),
            command: Some(command),
        };

        assert!(!valid_recorder_record(&request));
    }

    #[test]
    fn sync_flush_retry_doubles_to_a_jitter_free_cap() {
        let mut delay = SYNC_FLUSH_RETRY_INITIAL;
        let mut delays = Vec::new();
        for _ in 0..7 {
            delays.push(delay);
            delay = next_sync_flush_retry(delay);
        }

        assert_eq!(
            delays,
            [50, 100, 200, 400, 800, 1_000, 1_000].map(Duration::from_millis)
        );
    }

    #[test]
    fn blocking_operation_offloads_on_current_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = run_read_operation(ReadConsistency::Local, || std::thread::current().id())
                .await
                .unwrap();

            assert_ne!(worker, caller);
        });
    }

    #[test]
    fn blocking_operation_runs_inline_on_multi_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = run_read_operation(ReadConsistency::AppliedIndex(1), || {
                std::thread::current().id()
            })
            .await
            .unwrap();

            assert_eq!(worker, caller);
        });
    }

    #[test]
    fn read_barrier_offloads_on_multi_thread_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker =
                run_read_operation(ReadConsistency::ReadBarrier, || std::thread::current().id())
                    .await
                    .unwrap();

            assert_ne!(worker, caller);
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_service_adaptive_sql_read_returns_point_and_query_results() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);

        let point = service
            .read("missing", ReadConsistency::Local)
            .await
            .unwrap();
        let query = service
            .query(
                SqlStatement {
                    sql: "SELECT ?1 AS value".into(),
                    parameters: vec![SqlValue::Integer(7)],
                },
                ReadConsistency::AppliedIndex(0),
                1,
            )
            .await
            .unwrap();

        assert_eq!(point.value, None);
        assert_eq!(query.columns, vec!["value"]);
        assert_eq!(query.rows, vec![vec![SqlValue::Integer(7)]]);
    }

    #[test]
    fn node_service_adaptive_sql_read_stays_inline_and_recovers_direct_panic() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = service
                .run_sql_read_operation(ReadConsistency::Local, || std::thread::current().id())
                .await
                .unwrap();

            assert_eq!(worker, caller);
            assert_eq!(
                service
                    .sql_reads_in_flight
                    .load(std::sync::atomic::Ordering::Acquire),
                0
            );
        });

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(
                service.run_sql_read_operation(ReadConsistency::AppliedIndex(0), || -> () {
                    panic!("inline SQL read panic")
                }),
            )
        }));
        assert!(panic.is_err());
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_service_adaptive_sql_read_offloads_overlap_and_recovers_join_error() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_service = service.clone();
        let first = tokio::spawn(async move {
            first_service
                .run_sql_read_operation(ReadConsistency::Local, move || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
                .unwrap();
        });
        entered_rx.await.unwrap();
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );

        let caller = std::thread::current().id();
        let worker = service
            .run_sql_read_operation(ReadConsistency::AppliedIndex(0), || {
                std::thread::current().id()
            })
            .await
            .unwrap();
        assert_ne!(worker, caller);
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );

        let error = service
            .run_sql_read_operation(ReadConsistency::Local, || -> () {
                panic!("contended SQL read panic")
            })
            .await
            .unwrap_err();
        assert!(error.is_panic());
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );

        release_tx.send(()).unwrap();
        first.await.unwrap();
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

    #[test]
    fn node_service_adaptive_sql_read_offloads_on_current_thread() {
        let (_dir, node) = sql_test_runtime();
        let service = NodeService::new(Arc::new(node), None);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let caller = std::thread::current().id();
            let worker = service
                .run_sql_read_operation(ReadConsistency::Local, || std::thread::current().id())
                .await
                .unwrap();

            assert_ne!(worker, caller);
            assert_eq!(
                service
                    .sql_reads_in_flight
                    .load(std::sync::atomic::Ordering::Acquire),
                0
            );
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_service_read_barrier_offloads_without_counting_fast_reads() {
        let (_dir, runtime) = sql_test_runtime();
        let service = NodeService::new(Arc::new(runtime), None);
        let caller = std::thread::current().id();
        let worker = service
            .run_sql_read_operation(ReadConsistency::ReadBarrier, || std::thread::current().id())
            .await
            .unwrap();

        assert_ne!(worker, caller);
        assert_eq!(
            service
                .sql_reads_in_flight
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
    }

    #[test]
    fn embedded_sql_query_keeps_raw_budget_without_http_json_budget() {
        let (_dir, runtime) = sql_test_runtime();
        let response = runtime
            .query_sql(
                &SqlStatement {
                    sql: "SELECT replace(hex(zeroblob(700000)), '00', char(1)) AS value".into(),
                    parameters: Vec::new(),
                },
                ReadConsistency::Local,
                1,
            )
            .unwrap();

        assert!(serde_json::to_vec(&response).unwrap().len() > MAX_SQL_RESPONSE_BYTES);
    }

    #[test]
    fn sql_http_response_rejects_encoded_body_over_limit() {
        let response = SqlQueryResponse {
            columns: vec!["value".into()],
            rows: vec![vec![SqlValue::Text("\u{1}".repeat(700_000))]],
            applied_index: 0,
            hash: LogHash::ZERO,
        };

        assert_eq!(
            sql_query_http_response(response).status(),
            axum::http::StatusCode::BAD_REQUEST
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_response_work_holds_client_capacity_until_completion() {
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = std::sync::Arc::new(slots.clone().try_acquire_owned().unwrap());

        let capacity_exhausted_during_response =
            with_graph_client_permit(permit, || slots.clone().try_acquire_owned().is_err());

        assert!(capacity_exhausted_during_response);
        assert!(slots.try_acquire().is_ok());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_client_query_error_returns_400_without_latching_readiness() {
        let (_dir, runtime) = graph_test_runtime();

        let error = runtime.map_graph_read_error(rhiza_graph::Error::InvalidCommand(
            "unknown property".into(),
        ));
        let response = node_error_response(error);

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_resource_exhaustion_returns_503_without_latching_readiness() {
        let (_dir, runtime) = graph_test_runtime();

        let error = runtime.map_graph_read_error(rhiza_graph::Error::ResourceExhausted(
            "buffer pool is full".into(),
        ));
        let response = node_error_response(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_resource_exhaustion_returns_503_without_latching_readiness() {
        let (_dir, runtime) = kv_test_runtime();

        let error = runtime.map_kv_read_error(rhiza_kv::Error::ResourceExhausted(
            "scan result is too large".into(),
        ));
        let response = node_error_response(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_batch_preserves_order_with_one_lgfx_effect_per_new_request() {
        let (_dir, runtime) = graph_test_runtime();
        let canonical =
            GraphCommandV1::put_document("same", "document", GraphValueV1::String("first".into()))
                .unwrap();
        let conflict = GraphCommandV1::put_document(
            "same",
            "document",
            GraphValueV1::String("conflict".into()),
        )
        .unwrap();
        let unrelated =
            GraphCommandV1::put_document("other", "other", GraphValueV1::U64(2)).unwrap();
        let results = runtime
            .mutate_graph_batch(vec![canonical.clone(), canonical, conflict, unrelated])
            .unwrap();

        let canonical = results[0].as_ref().unwrap().applied_index();
        assert_eq!(results[1].as_ref().unwrap().applied_index(), canonical);
        assert!(matches!(
            results[2],
            Err(super::NodeError::InvalidRequest(_))
        ));
        assert_eq!(results[3].as_ref().unwrap().applied_index(), canonical + 1);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
        for index in 1..=2 {
            assert!(runtime
                .log_store()
                .read(index)
                .unwrap()
                .unwrap()
                .payload
                .starts_with(LGFX_V1_MAGIC));
        }
        assert!(runtime.is_ready());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_profile_shape_rejects_semantic_rhgc_and_rhgb_commands() {
        let first = GraphCommandV1::put_document("first", "first", GraphValueV1::U64(1)).unwrap();
        let second =
            GraphCommandV1::put_document("second", "second", GraphValueV1::U64(2)).unwrap();
        let semantic_payloads = [
            encode_replicated_graph_command(&first).unwrap(),
            encode_replicated_graph_batch(&[first, second]).unwrap(),
        ];

        for payload in semantic_payloads {
            let entry = LogEntry {
                cluster_id: "rhiza:graph:node-unit-test".into(),
                epoch: 1,
                config_id: 1,
                index: 1,
                entry_type: EntryType::Command,
                hash: LogEntry::calculate_hash(
                    "rhiza:graph:node-unit-test",
                    1,
                    1,
                    1,
                    EntryType::Command,
                    LogHash::ZERO,
                    &payload,
                ),
                prev_hash: LogHash::ZERO,
                payload,
            };

            assert!(
                super::validate_profile_entry_shape(ExecutionProfile::Graph, &entry)
                    .unwrap_err()
                    .contains("LGFX")
            );
        }
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_batch_coalesces_exact_retry_and_isolates_conflicting_duplicate() {
        let (_dir, runtime) = kv_test_runtime();
        let canonical = KvCommandV1::put("same", b"key".to_vec(), b"first".to_vec()).unwrap();
        let conflict = KvCommandV1::put("same", b"key".to_vec(), b"conflict".to_vec()).unwrap();
        let unrelated = KvCommandV1::put("other", b"other".to_vec(), b"second".to_vec()).unwrap();
        let results = runtime
            .mutate_kv_batch(vec![canonical.clone(), canonical, conflict, unrelated])
            .unwrap();

        let canonical = results[0].as_ref().unwrap().applied_index();
        assert_eq!(results[1].as_ref().unwrap().applied_index(), canonical);
        assert!(matches!(
            results[2],
            Err(super::NodeError::InvalidRequest(_))
        ));
        assert_eq!(results[3].as_ref().unwrap().applied_index(), canonical);
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
        assert!(runtime.is_ready());
    }

    #[test]
    fn sql_batch_preflight_rejects_entire_vector_without_growing_log() {
        let (_dir, runtime) = sql_test_runtime();
        let valid = SqlCommand {
            request_id: "valid".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        };
        let invalid = SqlCommand {
            request_id: String::new(),
            statements: valid.statements.clone(),
        };

        let error = runtime.execute_sql_batch(vec![valid, invalid]).unwrap_err();

        assert!(matches!(error, NodeError::InvalidRequest(_)));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[test]
    fn legacy_put_endpoint_commits_qwal_instead_of_raw_put_payload() {
        let (_dir, runtime) = sql_test_runtime();

        let response = runtime.write("legacy-put", "key", "value").unwrap();

        let entry = runtime
            .log_store()
            .read(response.applied_index)
            .unwrap()
            .unwrap();
        assert!(entry.payload.starts_with(b"QWAL\0\x01"));
        assert!(!entry.payload.starts_with(b"put\t"));
        assert_eq!(
            runtime.read("key", ReadConsistency::Local).unwrap().value,
            Some("value".into())
        );
    }

    #[test]
    fn sql_batch_preserves_order_commits_individual_qwal_effects_and_retries_exactly() {
        let (_dir, runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let commands = (1..=3)
            .map(|id| SqlCommand {
                request_id: format!("insert-{id}"),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO batch_items(id, value) VALUES (?1, ?2)".into(),
                    parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("value-{id}"))],
                }],
            })
            .collect::<Vec<_>>();

        let first = runtime.execute_sql_batch(commands.clone()).unwrap();
        let first_indices = first
            .iter()
            .map(|result| result.as_ref().unwrap().applied_index)
            .collect::<Vec<_>>();
        let log_index = runtime.log_store().last_index().unwrap();
        let replay = runtime.execute_sql_batch(commands).unwrap();

        assert_eq!(first_indices, vec![2, 3, 4]);
        for index in 1..=4 {
            let entry = runtime.log_store().read(index).unwrap().unwrap();
            assert!(entry.payload.starts_with(b"QWAL\0\x01"));
        }
        assert_eq!(
            replay
                .iter()
                .map(|result| result.as_ref().unwrap().applied_index)
                .collect::<Vec<_>>(),
            first_indices
        );
        assert_eq!(runtime.log_store().last_index().unwrap(), log_index);
    }

    #[test]
    fn sql_effect_over_qlog_limit_is_resource_exhausted() {
        let (_dir, runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE large_effect(value BLOB NOT NULL)".into(),
                    parameters: vec![],
                }],
            })
            .unwrap();

        let error = runtime
            .execute_sql(SqlCommand {
                request_id: "large-effect".into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO large_effect(value) VALUES (randomblob(400000))".into(),
                    parameters: vec![],
                }],
            })
            .unwrap_err();

        assert!(matches!(error, NodeError::ResourceExhausted(_)));
        assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
    }

    #[test]
    fn sql_batch_isolates_request_conflict_from_unrelated_member() {
        let (_dir, runtime) = sql_test_runtime();
        runtime
            .execute_sql(SqlCommand {
                request_id: "schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let insert = |request_id: &str, id: i64| SqlCommand {
            request_id: request_id.into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_items(id, value) VALUES (?1, ?2)".into(),
                parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("value-{id}"))],
            }],
        };

        let results = runtime
            .execute_sql_batch(vec![
                insert("same", 1),
                insert("same", 2),
                insert("other", 3),
            ])
            .unwrap();

        assert!(results[0].is_ok());
        assert!(matches!(results[1], Err(NodeError::RequestConflict(_))));
        assert!(results[2].is_ok());
        assert!(runtime.is_ready());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn typed_batch_wrong_profile_is_rejected_before_log_attempt() {
        let (_dir, runtime) = graph_test_runtime();
        let command = SqlCommand {
            request_id: "wrong-profile".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE should_not_exist(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        };

        let error = runtime.execute_sql_batch(vec![command]).unwrap_err();

        assert!(matches!(
            error,
            NodeError::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: ExecutionProfile::Graph
            }
        ));
        assert_eq!(runtime.log_store().last_index().unwrap(), None);
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_query_timeout_returns_503_without_latching_readiness() {
        let (_dir, runtime) = graph_test_runtime();
        let graph = runtime.graph_materializer().unwrap();
        let graph_error = graph
            .query_read_only(
                "UNWIND range(1, 10000) AS x UNWIND range(1, 10000) AS y RETURN sum(x * y) AS total LIMIT 1",
                &std::collections::BTreeMap::new(),
                1,
                1024 * 1024,
                1,
            )
            .unwrap_err();

        let response = node_error_response(runtime.map_graph_read_error(graph_error));

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(runtime.is_ready());
        assert!(!runtime.is_fatal());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_internal_error_returns_500_and_latches_readiness() {
        let (_dir, runtime) = graph_test_runtime();

        let error =
            runtime.map_graph_read_error(rhiza_graph::Error::Ladybug("connection failed".into()));
        let response = node_error_response(error);

        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(!runtime.is_ready());
        assert!(runtime.is_fatal());
    }

    fn sql_test_runtime() -> (tempfile::TempDir, NodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = "node-unit-test";
        let config = NodeConfig::new_embedded(
            cluster_id,
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Sqlite)
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:sql:node-unit-test",
                "n1",
                1,
                1,
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        (dir, runtime)
    }

    #[cfg(feature = "graph")]
    fn graph_test_runtime() -> (tempfile::TempDir, NodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = "node-unit-test";
        let config = NodeConfig::new_embedded(
            cluster_id,
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Graph)
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:graph:node-unit-test",
                "n1",
                1,
                1,
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        (dir, runtime)
    }

    #[cfg(feature = "kv")]
    fn kv_test_runtime() -> (tempfile::TempDir, NodeRuntime) {
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = "node-unit-test";
        let config = NodeConfig::new_embedded(
            cluster_id,
            "n1",
            dir.path().join("node"),
            1,
            1,
            ["n1", "n2", "n3"],
        )
        .unwrap()
        .with_execution_profile(ExecutionProfile::Kv)
        .unwrap();
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recovered_tip(
                "rhiza:kv:node-unit-test",
                "n1",
                1,
                1,
                [
                    dir.path().join("recorders/n1"),
                    dir.path().join("recorders/n2"),
                    dir.path().join("recorders/n3"),
                ],
                1,
                LogHash::ZERO,
            )
            .unwrap(),
        );
        let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
        (dir, runtime)
    }
}

fn validate_typed_batch_len(len: usize) -> Result<(), NodeError> {
    if (1..=MAX_WRITE_BATCH_MEMBERS).contains(&len) {
        Ok(())
    } else {
        Err(NodeError::InvalidRequest(format!(
            "write batch must contain 1..={MAX_WRITE_BATCH_MEMBERS} commands"
        )))
    }
}

fn validate_command_size(payload: &[u8]) -> Result<(), NodeError> {
    if payload.len() <= MAX_COMMAND_BYTES {
        Ok(())
    } else {
        Err(NodeError::InvalidRequest(format!(
            "command exceeds {MAX_COMMAND_BYTES} bytes"
        )))
    }
}

#[cfg(feature = "sql")]
fn canonical_put(request_id: &str, key: &str, value: &str) -> Result<Vec<u8>, NodeError> {
    validate_field("request_id", request_id, MAX_REQUEST_ID_BYTES, false)?;
    validate_key(key)?;
    validate_field("value", value, MAX_VALUE_BYTES, true)?;
    let payload = encode_put_request(request_id, key, value)
        .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
    validate_command_size(&payload)?;
    Ok(payload)
}

#[cfg(feature = "sql")]
fn encode_sql_command_with_index(command: &SqlCommand) -> Result<Vec<u8>, NodeError> {
    encode_sql_command(command).map_err(|error| {
        let message = error.to_string();
        match first_invalid_sql_statement(command, |prefix| encode_sql_command(prefix).is_err()) {
            Some(statement_index) => NodeError::InvalidSqlStatement {
                statement_index,
                message,
            },
            None => NodeError::InvalidRequest(message),
        }
    })
}

#[cfg(feature = "sql")]
fn first_invalid_sql_statement(
    command: &SqlCommand,
    mut invalid: impl FnMut(&SqlCommand) -> bool,
) -> Option<usize> {
    if command.statements.is_empty() || command.statements.len() > MAX_SQL_STATEMENTS {
        return None;
    }
    (0..command.statements.len()).find(|statement_index| {
        let prefix = SqlCommand {
            request_id: command.request_id.clone(),
            statements: command.statements[..=*statement_index].to_vec(),
        };
        invalid(&prefix)
    })
}

#[cfg(feature = "sql")]
fn validate_key(key: &str) -> Result<(), NodeError> {
    validate_field("key", key, MAX_KEY_BYTES, false)
}

#[cfg(feature = "sql")]
fn validate_field(
    name: &str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), NodeError> {
    if !allow_empty && value.is_empty() {
        return Err(NodeError::InvalidRequest(format!(
            "{name} must not be empty"
        )));
    }
    if value.len() > max_bytes {
        return Err(NodeError::InvalidRequest(format!(
            "{name} exceeds {max_bytes} bytes"
        )));
    }
    if value.contains('\t') {
        return Err(NodeError::InvalidRequest(format!(
            "{name} must not contain a tab"
        )));
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn write_response(outcome: RequestOutcome) -> WriteResponse {
    WriteResponse {
        applied_index: outcome.original_log_index(),
        hash: outcome.original_log_hash(),
    }
}

fn reconcile_local_storage(
    config: &NodeConfig,
    log_store: &FileLogStore,
    materializer: &Materializer,
) -> Result<(), NodeError> {
    let log_state = log_store
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let log_last_index = log_state.tip.as_ref().map_or(0, |tip| tip.index());
    let (reconciliation_base, mut materializer_configuration) = materializer
        .startup_reconciliation_base_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let applied_index = reconciliation_base.index();
    let applied_hash = reconciliation_base.hash();

    if let Some(anchor) = &log_state.anchor {
        if anchor.recovery_generation() != config.recovery_generation {
            return Err(NodeError::Reconciliation(format!(
                "qlog anchor recovery generation {} differs from runtime generation {}",
                anchor.recovery_generation(),
                config.recovery_generation
            )));
        }
        if applied_index < anchor.compacted().index() {
            return Err(NodeError::SnapshotRequired(Box::new(anchor.clone())));
        }
    }
    if applied_index > log_last_index {
        return Err(NodeError::Reconciliation(format!(
            "{} materializer is ahead at {applied_index}, qlog ends at {log_last_index}",
            materializer.profile()
        )));
    }
    if applied_index == 0 {
        if applied_hash != LogHash::ZERO {
            return Err(NodeError::Reconciliation(format!(
                "{} materializer genesis hash is not zero",
                materializer.profile()
            )));
        }
    } else if !log_state.anchor.as_ref().is_some_and(|anchor| {
        applied_index == anchor.compacted().index() && applied_hash == anchor.compacted().hash()
    }) {
        let entry = log_store
            .read(applied_index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!(
                    "qlog prefix is missing {} materializer index {applied_index}",
                    materializer.profile()
                ))
            })?;
        validate_entry_envelope(config, &entry, applied_index, entry.prev_hash)?;
        if entry.hash != applied_hash {
            return Err(NodeError::Reconciliation(format!(
                "{} materializer hash diverges from qlog at index {applied_index}",
                materializer.profile()
            )));
        }
    }

    let mut expected_prev_hash = applied_hash;
    for index in (applied_index + 1)..=log_last_index {
        let entry = log_store
            .read(index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!("qlog prefix is missing index {index}"))
            })?;
        match &materializer_configuration {
            Some(configuration) => {
                validate_runtime_entry(config, configuration, &entry, index, expected_prev_hash)?
            }
            None => validate_entry_envelope(config, &entry, index, expected_prev_hash)?,
        };
        materializer
            .apply_entry(&entry)
            .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
        if index < log_last_index {
            materializer_configuration = materializer
                .configuration_state()
                .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
        }
        expected_prev_hash = entry.hash;
    }
    let log_configuration = log_store
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    // Return to the normal public state boundary after suffix replay. This also
    // fails startup if a pending graph intent had no matching qlog entry to
    // redeliver and therefore remains unresolved.
    let final_materializer_configuration = materializer
        .configuration_state()
        .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    if final_materializer_configuration
        .as_ref()
        .is_some_and(|configuration| configuration != &log_configuration)
    {
        return Err(NodeError::Reconciliation(format!(
            "qlog and {} materializer configuration states disagree",
            materializer.profile()
        )));
    }
    Ok(())
}

fn recover_peer_candidates(
    config: &NodeConfig,
    consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    materializer: &Materializer,
    peer_candidates: &[&dyn LogPeer],
) -> Result<(), NodeError> {
    for peer in peer_candidates {
        let (last_index, last_hash) = static_log_tip(log_store)?;
        let candidates = match peer.fetch_log(FetchLogRequest {
            from_index: last_index.saturating_add(1),
            max_entries: MAX_FETCH_ENTRIES,
        }) {
            Ok(response) => validate_fetched_entries_with_configuration(
                last_index.saturating_add(1),
                last_hash,
                &config.cluster_id,
                config.epoch,
                log_store
                    .configuration_state()
                    .map_err(|error| NodeError::Storage(error.to_string()))?,
                response.entries,
            )
            .map_err(|error| {
                NodeError::Reconciliation(format!("peer candidate validation failed: {error}"))
            })?,
            Err(FetchLogError::Transport { .. }) => continue,
            Err(FetchLogError::SnapshotRequired { anchor }) => {
                return Err(NodeError::SnapshotRequired(anchor));
            }
            Err(error) => {
                return Err(NodeError::Reconciliation(format!(
                    "peer candidate validation failed: {error}"
                )));
            }
        };

        let mut expected_index = last_index.checked_add(1).ok_or_else(|| {
            NodeError::Reconciliation("qlog index is exhausted during peer catch-up".into())
        })?;
        let mut expected_prev_hash = last_hash;
        for candidate in candidates {
            match consensus
                .inspect_decision_at(expected_index, expected_prev_hash)
                .map_err(startup_consensus_error)?
            {
                DecisionInspection::Committed(committed) if committed == candidate => {
                    persist_startup_entry(
                        config,
                        log_store,
                        materializer,
                        &candidate,
                        expected_index,
                        expected_prev_hash,
                    )?;
                    expected_prev_hash = candidate.hash;
                    expected_index = expected_index.checked_add(1).ok_or_else(|| {
                        NodeError::Reconciliation(
                            "qlog index is exhausted during peer catch-up".into(),
                        )
                    })?;
                }
                DecisionInspection::Committed(_) => {
                    return Err(NodeError::Reconciliation(format!(
                        "peer candidate at index {expected_index} differs from committed decision"
                    )));
                }
                DecisionInspection::Unavailable => {
                    return Err(NodeError::Unavailable(format!(
                        "decision inspection unavailable for peer candidate at index {expected_index}"
                    )));
                }
                DecisionInspection::Empty | DecisionInspection::Pending => {
                    return Err(NodeError::Reconciliation(format!(
                        "peer candidate at index {expected_index} is not committed"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn recover_startup_decisions(
    config: &NodeConfig,
    consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    materializer: &Materializer,
) -> Result<(), NodeError> {
    for _ in 0..MAX_STARTUP_RECOVERY_ENTRIES {
        let (last_index, last_hash) = static_log_tip(log_store)?;
        let slot = last_index.checked_add(1).ok_or_else(|| {
            NodeError::Reconciliation("qlog index is exhausted during startup".into())
        })?;
        match consensus
            .inspect_decision_at(slot, last_hash)
            .map_err(startup_consensus_error)?
        {
            DecisionInspection::Committed(entry) => {
                persist_startup_entry(config, log_store, materializer, &entry, slot, last_hash)?;
            }
            DecisionInspection::Pending => {
                let entry = consensus
                    .propose_at(
                        slot,
                        last_hash,
                        Command::new(CommandKind::ReadBarrier, Vec::new()),
                    )
                    .map_err(startup_consensus_error)?;
                persist_startup_entry(config, log_store, materializer, &entry, slot, last_hash)?;
            }
            DecisionInspection::Empty => return Ok(()),
            DecisionInspection::Unavailable => {
                return Err(NodeError::Unavailable(
                    "decision inspection unavailable during startup".into(),
                ));
            }
        }
    }
    Err(NodeError::Reconciliation(format!(
        "startup recovery exceeded {MAX_STARTUP_RECOVERY_ENTRIES} entries"
    )))
}

fn persist_startup_entry(
    config: &NodeConfig,
    log_store: &FileLogStore,
    materializer: &Materializer,
    entry: &LogEntry,
    expected_index: LogIndex,
    expected_prev_hash: LogHash,
) -> Result<(), NodeError> {
    let configuration_state = log_store
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    validate_runtime_entry(
        config,
        &configuration_state,
        entry,
        expected_index,
        expected_prev_hash,
    )?;
    log_store
        .append(entry)
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    materializer
        .apply_entry(entry)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    Ok(())
}

fn static_log_tip(log_store: &FileLogStore) -> Result<(LogIndex, LogHash), NodeError> {
    Ok(log_store
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?
        .tip
        .map_or((0, LogHash::ZERO), |tip| (tip.index(), tip.hash())))
}

fn validate_runtime_entry(
    config: &NodeConfig,
    configuration_state: &ConfigurationState,
    entry: &LogEntry,
    expected_index: LogIndex,
    expected_prev_hash: LogHash,
) -> Result<(), NodeError> {
    validate_entry_envelope(config, entry, expected_index, expected_prev_hash)?;
    validate_profile_entry_shape(config.execution_profile(), entry)
        .map_err(NodeError::Invariant)?;
    #[cfg(feature = "graph")]
    if config.execution_profile() == ExecutionProfile::Graph
        && entry.entry_type == EntryType::Command
    {
        let effect = LadybugFileEffectV1::decode(&entry.payload)
            .map_err(|error| NodeError::Invariant(error.to_string()))?;
        if effect.recovery_generation != config.recovery_generation()
            || effect.materializer_fingerprint != rhiza_graph::graph_materializer_fingerprint()
        {
            return Err(NodeError::Reconciliation(format!(
                "graph LGFX entry {} has incompatible recovery generation or materializer fingerprint",
                entry.index
            )));
        }
    }
    configuration_state
        .validate_entry(entry)
        .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
    Ok(())
}

fn validate_entry_envelope(
    config: &NodeConfig,
    entry: &LogEntry,
    expected_index: LogIndex,
    expected_prev_hash: LogHash,
) -> Result<(), NodeError> {
    if entry.index != expected_index {
        return Err(NodeError::Reconciliation(format!(
            "expected decision index {expected_index}, got {}",
            entry.index
        )));
    }
    if entry.cluster_id != config.cluster_id || entry.epoch != config.epoch {
        return Err(NodeError::Reconciliation(format!(
            "decision {} has a foreign identity",
            entry.index
        )));
    }
    if entry.prev_hash != expected_prev_hash {
        return Err(NodeError::Reconciliation(format!(
            "decision {} has a conflicting predecessor",
            entry.index
        )));
    }
    if entry.recompute_hash() != entry.hash {
        return Err(NodeError::Reconciliation(format!(
            "decision {} has an invalid hash",
            entry.index
        )));
    }
    validate_entry_shape(entry).map_err(NodeError::Invariant)
}

fn validate_entry_shape(entry: &LogEntry) -> Result<(), String> {
    match entry.entry_type {
        EntryType::Command if entry.payload.len() <= MAX_COMMAND_BYTES => Ok(()),
        EntryType::Command => Err(format!("command exceeds {MAX_COMMAND_BYTES} bytes")),
        EntryType::Noop if entry.payload.is_empty() => Ok(()),
        EntryType::Noop => Err("Noop payload must be empty".into()),
        EntryType::ConfigChange => ConfigChange::recognize(&StoredCommand::new(
            EntryType::ConfigChange,
            entry.payload.clone(),
        ))
        .map(|_| ())
        .map_err(|error| error.to_string()),
        other => Err(format!("unsupported runtime entry type {other:?}")),
    }
}

pub(crate) fn validate_profile_entry_shape(
    _profile: ExecutionProfile,
    entry: &LogEntry,
) -> Result<(), String> {
    validate_entry_shape(entry)?;
    #[cfg(feature = "sql")]
    if _profile == ExecutionProfile::Sqlite && entry.entry_type == EntryType::Command {
        decode_qwal_v1(&entry.payload)
            .map_err(|error| format!("SQLite command is not canonical QWAL v1: {error}"))?;
    }
    #[cfg(feature = "graph")]
    if _profile == ExecutionProfile::Graph && entry.entry_type == EntryType::Command {
        let effect = LadybugFileEffectV1::decode(&entry.payload)
            .map_err(|error| format!("graph command is not canonical LGFX v1: {error}"))?;
        if effect.cluster_id != entry.cluster_id
            || effect.epoch != entry.epoch
            || effect.configuration_id != entry.config_id
            || effect.base_log_index.checked_add(1) != Some(entry.index)
            || effect.base_log_hash != entry.prev_hash
        {
            return Err(
                "graph LGFX identity or exact-base anchor does not match its qlog entry".into(),
            );
        }
    }
    Ok(())
}

fn startup_consensus_error(error: rhiza_quepaxa::Error) -> NodeError {
    match error {
        rhiza_quepaxa::Error::NoQuorum
        | rhiza_quepaxa::Error::CommandUnavailable
        | rhiza_quepaxa::Error::Io(_) => NodeError::Unavailable(error.to_string()),
        other => NodeError::Reconciliation(other.to_string()),
    }
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct E2eConfig {
    pub data_dir: PathBuf,
    pub object_store: ObjStoreConfig,
    pub cluster_id: String,
    pub node_id: String,
}

#[cfg(feature = "sql")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct E2eReport {
    pub applied_index: LogIndex,
    pub restored_value: String,
    pub object_keys: Vec<String>,
}

#[cfg(feature = "sql")]
pub async fn run_e2e(config: E2eConfig) -> Result<E2eReport, Box<dyn std::error::Error>> {
    let sqlite_dir = config.data_dir.join("sqlite");
    let log_dir = config.data_dir.join("consensus").join("log");
    ensure_fresh_e2e_data_dir(&config.data_dir, &sqlite_dir, &log_dir)?;

    fs::create_dir_all(&config.data_dir)?;
    let db_path = sqlite_dir.join("db.sqlite");
    let restore_path = config.data_dir.join("restore").join("db.sqlite");
    let db = SqliteStateMachine::open(&db_path, &config.cluster_id, &config.node_id, 1, 1)?;
    let recorder_dir = config.data_dir.join("consensus").join("recorder");
    let consensus = ThreeNodeConsensus::new(
        &config.cluster_id,
        &config.node_id,
        1,
        1,
        [
            recorder_dir.join("node-1"),
            recorder_dir.join("node-2"),
            recorder_dir.join("node-3"),
        ],
    )?;
    let base_request = canonical_put("e2e-base", "alpha", "bravo")?;
    let base_effect = db.prepare_put_effect(
        "e2e-base",
        "alpha",
        "bravo",
        &base_request,
        0,
        LogHash::ZERO,
    )?;
    let base_entry = consensus.propose(Command::new(CommandKind::Deterministic, base_effect))?;
    db.apply_entry(&base_entry)?;
    let snapshot = db.create_snapshot(base_entry.index)?;

    let tail_request = canonical_put("e2e-tail", "alpha", "charlie")?;
    let tail_effect = db.prepare_put_effect(
        "e2e-tail",
        "alpha",
        "charlie",
        &tail_request,
        base_entry.index,
        base_entry.hash,
    )?;
    let tail_entry = consensus.propose(Command::new(CommandKind::Deterministic, tail_effect))?;
    let segment_path = write_segment_file(&log_dir, std::slice::from_ref(&tail_entry))?;
    let segment = rhiza_log::SegmentFile::new(
        IndexRange::new(tail_entry.index, tail_entry.index)?,
        fs::read(&segment_path)?,
    );
    db.apply_entry(&tail_entry)?;

    let local_archive = matches!(&config.object_store, ObjStoreConfig::Local { .. });
    let store = ObjStore::new(config.object_store)?;
    let archive = if local_archive {
        rhiza_archive::ObjectArchiveStore::new_for_single_process(
            store.clone(),
            config.cluster_id.clone(),
        )
    } else {
        rhiza_archive::ObjectArchiveStore::new(store.clone(), config.cluster_id.clone())?
    };

    let segment_record = archive.publish_segment(tail_entry.epoch, &segment).await?;
    let snapshot_record = archive.publish_snapshot(&snapshot).await?;
    let (mut archive_manifest, expected_manifest_version) = match archive.load_manifest().await? {
        Some(loaded) => (loaded.manifest().clone(), Some(loaded.version().clone())),
        None => (
            rhiza_archive::ArchiveManifest::new(config.cluster_id.clone()),
            None,
        ),
    };
    archive_manifest.set_latest_snapshot(snapshot_record);
    archive_manifest.add_segment(segment_record);
    archive
        .publish_manifest(&archive_manifest, expected_manifest_version)
        .await?;

    let loaded_manifest = archive
        .load_manifest()
        .await?
        .ok_or("published archive manifest is missing")?;
    if loaded_manifest.manifest() != &archive_manifest {
        return Err("reloaded archive manifest did not match the published manifest".into());
    }
    let archived_snapshot = loaded_manifest
        .manifest()
        .latest_snapshot()
        .ok_or("archive manifest is missing its snapshot")?;
    let archived_segment = loaded_manifest
        .manifest()
        .segments()
        .iter()
        .find(|record| {
            record.start_index() == tail_entry.index && record.end_index() == tail_entry.index
        })
        .ok_or("archive manifest is missing its post-snapshot segment")?;

    let downloaded_segment = archive.download_segment(archived_segment).await?;
    let downloaded_entries = decode_segment_for_cluster(&downloaded_segment, &config.cluster_id)?;
    if downloaded_entries.as_slice() != std::slice::from_ref(&tail_entry) {
        return Err("downloaded qlog segment did not match written entry".into());
    }
    let downloaded_snapshot = rhiza_core::Snapshot::new(
        archived_snapshot.manifest().clone(),
        archive.download_snapshot(archived_snapshot).await?,
    );
    restore_snapshot_file(&restore_path, &downloaded_snapshot, &config.node_id)?;
    let restored_db = SqliteStateMachine::open_existing(&restore_path)?;
    if restored_db.get_value("alpha")?.as_deref() != Some("bravo") {
        return Err("restored base snapshot is missing alpha=bravo".into());
    }
    for entry in &downloaded_entries {
        restored_db.apply_entry(entry)?;
    }
    let restored_value = restored_db
        .get_value("alpha")?
        .ok_or("restored SQLite state is missing alpha")?;
    let applied_index = restored_db.applied_index_value()?;
    if applied_index != tail_entry.index || restored_value != "charlie" {
        return Err("restored SQLite state did not include the archived log tail".into());
    }
    let object_keys = store.list(&format!("rhiza/{}", config.cluster_id)).await?;

    Ok(E2eReport {
        applied_index,
        restored_value,
        object_keys,
    })
}

#[cfg(feature = "sql")]
fn ensure_fresh_e2e_data_dir(
    data_dir: &std::path::Path,
    sqlite_dir: &std::path::Path,
    log_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if directory_has_entries(sqlite_dir)? || directory_has_entries(log_dir)? {
        return Err(format!(
            "e2e data directory is not fresh: prior SQLite/qlog data exists in {}",
            data_dir.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(feature = "sql")]
fn directory_has_entries(path: &std::path::Path) -> Result<bool, std::io::Error> {
    match fs::read_dir(path) {
        Ok(mut entries) => entries.next().transpose().map(|entry| entry.is_some()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
