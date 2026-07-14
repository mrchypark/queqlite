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

use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Extension, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use queqlite_archive::SnapshotRecord;
use queqlite_core::{
    Command, CommandKind, ConfigChange, ConfigurationState, EntryType, LogAnchor, LogEntry,
    LogHash, LogIndex, RecoveryAnchor, StoredCommand,
};
use queqlite_log::{
    decode_segment_for_cluster, write_segment_file, FileLogStore, IndexRange, LogStore,
};
use queqlite_obj_store::{ObjStore, ObjStoreConfig};
use queqlite_quepaxa::{
    CertifiedDecisionInspection, Consensus, DecisionInspection, DecisionProof, Membership,
    RecordRequest, RecordSummary, RecorderFileStore, RecorderReply, RecorderRequest, RecorderRpc,
    RejectReason, ThreeNodeConsensus,
};
use queqlite_sqlite::{
    encode_sql_command, restore_snapshot_file, RecoverySnapshot, RequestConflict, RequestOutcome,
    SqlCommand, SqlCommandResult, SqlEffectPreparation, SqlQueryResult, SqlStatement, SqlValue,
    SqliteStateMachine, MAX_SQL_STATEMENTS, MAX_WRITE_BATCH_MEMBERS,
};

mod admin;
pub mod durability;
pub use admin::*;
pub use durability::{
    restore_checkpoint_to_fresh_data_dir, restore_checkpoint_to_fresh_data_dir_for_node,
    restore_successor_checkpoint_to_fresh_data_dir, CheckpointCoordinator, DurabilityError,
    DurabilityHealth, DurabilityMode, SuccessorRestorePreparation,
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
pub const DEFAULT_WRITER_BATCH_WINDOW: Duration = Duration::from_micros(500);
pub const PROTOCOL_VERSION: &str = "1";
pub const RECORDER_PROTOCOL_VERSION: &str = "2";
const RECORDER_WIRE_VERSION: u16 = 2;
pub const VERSION_HEADER: &str = "x-queqlite-version";
pub const NODE_ID_HEADER: &str = "x-queqlite-node-id";
pub const RECOVERY_GENERATION_HEADER: &str = "x-queqlite-recovery-generation";
pub const RECORDER_PATH: &str = "/v1/quepaxa/recorder";
pub const RECORDER_IDENTITY_PATH: &str = "/v2/quepaxa/recorder/identity";
pub const RECORDER_STORE_COMMAND_PATH: &str = "/v2/quepaxa/recorder/store-command";
pub const RECORDER_FETCH_COMMAND_PATH: &str = "/v2/quepaxa/recorder/fetch-command";
pub const RECORDER_INSPECT_PROOF_PATH: &str = "/v2/quepaxa/recorder/inspect-proof";
pub const RECORDER_INSPECT_RECORD_PATH: &str = "/v2/quepaxa/recorder/inspect-record";
pub const RECORDER_RECORD_PATH: &str = "/v2/quepaxa/recorder/record";
pub const RECORDER_INSTALL_PROOF_PATH: &str = "/v2/quepaxa/recorder/install-decision-proof";
pub const LOG_FETCH_PATH: &str = "/v1/log/fetch";
pub const WRITE_PATH: &str = "/v1/write";
pub const READ_PATH: &str = "/v1/read";
pub const SQL_EXECUTE_PATH: &str = "/v1/sql/execute";
pub const SQL_QUERY_PATH: &str = "/v1/sql/query";
pub const SQL_EXECUTE_RESPONSE_VERSION: u16 = 1;
pub const LIVEZ_PATH: &str = "/livez";
pub const READYZ_PATH: &str = "/readyz";
const MAX_STARTUP_RECOVERY_ENTRIES: usize = 100_000;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT_WRITE_WAIT_TIMEOUT: Duration = Duration::from_secs(1);
const SYNC_FLUSH_RETRY_INITIAL: Duration = Duration::from_millis(50);
const SYNC_FLUSH_RETRY_MAX: Duration = Duration::from_secs(1);
pub const DEFAULT_SQL_MAX_ROWS: u32 = 1_000;
pub const MAX_SQL_MAX_ROWS: u32 = 10_000;
pub const MAX_SQL_RESULT_BYTES: usize = 1024 * 1024;
pub const MAX_SQL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    EmptyClusterId,
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
    #[serde(alias = "Local")]
    Local,
    #[serde(alias = "ReadBarrier")]
    ReadBarrier,
    #[serde(alias = "AppliedIndex")]
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

    fn client(&self) -> queqlite_quepaxa::Result<&reqwest::blocking::Client> {
        if self.client.get().is_none() {
            let client = reqwest::blocking::Client::builder()
                .connect_timeout(HTTP_CONNECT_TIMEOUT)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .build()
                .map_err(|error| queqlite_quepaxa::Error::Io(error.to_string()))?;
            let _ = self.client.set(client);
        }
        self.client
            .get()
            .ok_or_else(|| queqlite_quepaxa::Error::Io("HTTP client initialization failed".into()))
    }

    fn post_v2<T, U>(&self, path: &str, body: T) -> queqlite_quepaxa::Result<U>
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
            .map_err(|error| queqlite_quepaxa::Error::Io(error.to_string()))?;
        let status = response.status();
        let wire = response
            .json::<RecorderWire<RecorderV2Result<U>>>()
            .map_err(|error| queqlite_quepaxa::Error::Decode(error.to_string()))?;
        if wire.version != RECORDER_WIRE_VERSION {
            return Err(queqlite_quepaxa::Error::Decode(
                "recorder wire version mismatch".into(),
            ));
        }
        match wire.body {
            RecorderV2Result::Ok(value) if status.is_success() => Ok(value),
            RecorderV2Result::Ok(_) => Err(queqlite_quepaxa::Error::Io(format!(
                "recorder rpc returned HTTP {status}"
            ))),
            RecorderV2Result::Rejected(reason) => Err(queqlite_quepaxa::Error::Rejected(reason)),
            RecorderV2Result::Error(message) => Err(queqlite_quepaxa::Error::Io(message)),
        }
    }
}

impl RecorderRpc for HttpRecorderClient {
    fn call(&self, _request: RecorderRequest) -> queqlite_quepaxa::Result<RecorderReply> {
        Err(queqlite_quepaxa::Error::MigrationRequired {
            format: "recorder HTTP transport",
            version: 1,
        })
    }

    fn recorder_id(&self) -> queqlite_quepaxa::Result<String> {
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
    ) -> queqlite_quepaxa::Result<()> {
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
    ) -> queqlite_quepaxa::Result<Option<StoredCommand>> {
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

    fn record(&self, request: RecordRequest) -> queqlite_quepaxa::Result<RecordSummary> {
        self.post_v2(RECORDER_RECORD_PATH, request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> queqlite_quepaxa::Result<()> {
        self.post_v2(
            RECORDER_INSTALL_PROOF_PATH,
            InstallProofV2 {
                proof,
                members: membership.members().to_vec(),
            },
        )
    }

    fn inspect_decision_proof(&self, slot: u64) -> queqlite_quepaxa::Result<Option<DecisionProof>> {
        self.post_v2(RECORDER_INSPECT_PROOF_PATH, InspectProofV2 { slot })
    }

    fn inspect_record_summary(&self, slot: u64) -> queqlite_quepaxa::Result<Option<RecordSummary>> {
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
}

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
    KeyValue(WriteResponse),
    Sql(SqlExecuteResponse),
}

struct QueuedWrite {
    request_id: String,
    payload: Vec<u8>,
    operation: QueuedOperation,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    sender: tokio::sync::watch::Sender<Option<WriteOperationResult>>,
}

enum QueuedOperation {
    KeyValue,
    Sql(SqlCommand),
}

struct RuntimeBatchMember {
    request_id: String,
    payload: Vec<u8>,
    sql: Option<SqlCommand>,
}

impl ClientWriteResponse {
    const fn applied_index(&self) -> LogIndex {
        match self {
            Self::KeyValue(response) => response.applied_index,
            Self::Sql(response) => response.applied_index,
        }
    }
}

#[derive(Clone)]
pub struct NodeService {
    runtime: Arc<NodeRuntime>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
}

impl NodeService {
    pub fn new(runtime: Arc<NodeRuntime>, coordinator: Option<Arc<CheckpointCoordinator>>) -> Self {
        Self {
            runtime,
            coordinator,
        }
    }

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

    pub async fn read(
        &self,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ReadResponse, NodeError> {
        let runtime = self.runtime.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || runtime.read(&key, consistency))
            .await
            .map_err(node_service_task_error)?
    }

    pub async fn query(
        &self,
        statement: SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        let runtime = self.runtime.clone();
        tokio::task::spawn_blocking(move || runtime.query_sql(&statement, consistency, max_rows))
            .await
            .map_err(node_service_task_error)?
    }

    fn write_allowed(&self) -> Result<(), NodeError> {
        self.coordinator
            .as_ref()
            .map_or(Ok(()), |coordinator| coordinator.write_allowed())
            .map_err(|error| NodeError::Unavailable(error.to_string()))
    }

    async fn confirm_committed(&self, index: LogIndex) -> Result<(), NodeError> {
        confirm_write_durability(self.runtime.as_ref(), self.coordinator.as_deref(), index)
            .await
            .map_err(|error| NodeError::Unavailable(error.to_string()))
    }
}

fn node_service_task_error(error: tokio::task::JoinError) -> NodeError {
    NodeError::Fatal(format!("node service task failed: {error}"))
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
    let client_routes = Router::new()
        .route(WRITE_PATH, post(handle_write))
        .route(READ_PATH, post(handle_read))
        .route(SQL_EXECUTE_PATH, post(handle_sql_execute))
        .route(SQL_QUERY_PATH, post(handle_sql_query))
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
        .with_state(RecorderRouteState { recorder })
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
    if request.version != RECORDER_WIRE_VERSION
        || request.body.command.payload.len() > MAX_COMMAND_BYTES
    {
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
    Json(request): Json<RecorderWire<RecordRequest>>,
) -> Response
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    if request.version != RECORDER_WIRE_VERSION
        || request.body.cluster_id.is_empty()
        || request.body.cluster_id.len() > MAX_REQUEST_ID_BYTES
    {
        return StatusCode::BAD_REQUEST.into_response();
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

async fn handle_recorder_install_proof<R>(
    State(state): State<RecorderRouteState<R>>,
    Extension(permit): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<RecorderWire<InstallProofV2>>,
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
            let membership = Membership::from_voters(request.body.members)?;
            recorder.install_decision_proof(request.body.proof, &membership)
        })
        .await,
    )
}

fn recorder_v2_response<T: serde::Serialize>(
    result: Result<queqlite_quepaxa::Result<T>, tokio::task::JoinError>,
) -> Response {
    let (status, body) = match result {
        Ok(Ok(value)) => (StatusCode::OK, RecorderV2Result::Ok(value)),
        Ok(Err(queqlite_quepaxa::Error::Rejected(reason))) => {
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
    let operation = QueuedOperation::KeyValue;
    coordinate_write(state, permit, request_id, payload, operation).await
}

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
            ClientWriteResponse::KeyValue(response) => Json(response).into_response(),
            ClientWriteResponse::Sql(response) => Json(response).into_response(),
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
            let sql = match queued.operation {
                QueuedOperation::KeyValue => None,
                QueuedOperation::Sql(command) => Some(command),
            };
            members.push(RuntimeBatchMember {
                request_id: queued.request_id.clone(),
                payload: queued.payload,
                sql,
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

        for ((request_id, sender, _permit), result) in dispatch.into_iter().zip(results) {
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
        Ok(Ok(response)) => Json(response).into_response(),
        Ok(Err(error)) => node_error_response(error),
        Err(error) => client_task_error(error),
    }
}

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
    if !recovery_generation_matches(request.headers(), state.recovery_generation)
        || !peer_authenticated(request.headers(), &state.peers, state.protocol_version)
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let permit = match state.slots.try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => return StatusCode::TOO_MANY_REQUESTS.into_response(),
    };
    request.extensions_mut().insert(permit);
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
    if matches!(request.uri().path(), WRITE_PATH | SQL_EXECUTE_PATH)
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

async fn confirm_write_durability(
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

fn peer_authenticated(headers: &HeaderMap, peers: &[PeerConfig], protocol_version: &str) -> bool {
    if header_text(headers, VERSION_HEADER) != Some(protocol_version) {
        return false;
    }
    let Some(node_id) = header_text(headers, NODE_ID_HEADER) else {
        return false;
    };
    let Some(token) = bearer_token(headers) else {
        return false;
    };
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
        NodeError::InvalidSqlStatement {
            statement_index, ..
        } => (
            StatusCode::BAD_REQUEST,
            "invalid_request",
            false,
            Some(*statement_index),
        ),
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
) -> Result<queqlite_quepaxa::ConfigurationState, NodeError> {
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
) -> Result<queqlite_quepaxa::ConfigurationState, NodeError> {
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
        )
        .map_err(|error| NodeError::Reconciliation(error.to_string()))
}

fn recorder_error_status(error: &queqlite_quepaxa::Error) -> StatusCode {
    match error {
        queqlite_quepaxa::Error::NoQuorum
        | queqlite_quepaxa::Error::CommandUnavailable
        | queqlite_quepaxa::Error::ContentionExhausted { .. }
        | queqlite_quepaxa::Error::Io(_)
        | queqlite_quepaxa::Error::RecorderRootLocked(_) => StatusCode::SERVICE_UNAVAILABLE,
        queqlite_quepaxa::Error::Rejected(_) => StatusCode::CONFLICT,
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
}

impl fmt::Debug for NodeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeConfig")
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
        Ok(Self::from_validated_parts(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            Vec::new(),
            String::new(),
        ))
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

        Ok(Self::from_validated_parts(
            cluster_id,
            node_id,
            data_dir,
            epoch,
            membership,
            configuration_state,
            peers,
            client_token,
        ))
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
    ) -> Self {
        let log_initial_configuration = ConfigurationState::active(
            configuration_state.config_id(),
            configuration_state.digest(),
        );
        Self {
            cluster_id,
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
        }
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
        queqlite_quepaxa::Error::DuplicateRecorderIdentity => {
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
        queqlite_quepaxa::Error::EmptyRecorderIdentity => ConfigError::EmptyPeerNodeId,
        _ => ConfigError::InvalidPeerCount(members.len()),
    })
}

fn membership_from_peers(peers: &[PeerConfig]) -> Result<Membership, ConfigError> {
    membership_from_node_ids(peers.iter().map(|peer| peer.node_id.clone()).collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    UnsupportedAckMode(AckMode),
    DataRootLocked(PathBuf),
    SnapshotRequired(Box<RecoveryAnchor>),
    Storage(String),
    Reconciliation(String),
    Invariant(String),
    Unavailable(String),
    ConfigurationTransition {
        state: Box<ConfigurationState>,
    },
    Contention(String),
    WinnerLimitExceeded,
    RequestConflict(RequestConflict),
    InvalidRequest(String),
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
            Self::ConfigurationTransition { state } => write!(
                f,
                "node unavailable during configuration transition: {state:?}"
            ),
            Self::Contention(message) => write!(f, "node contention: {message}"),
            Self::WinnerLimitExceeded => write!(f, "foreign winner retry limit exceeded"),
            Self::RequestConflict(conflict) => conflict.fmt(f),
            Self::InvalidRequest(message) => write!(f, "invalid request: {message}"),
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
    pub stop_anchor: Option<queqlite_core::LogAnchor>,
    pub active_config_id: u64,
    pub active_membership_digest: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StopInformation {
    pub version: u16,
    pub entry: LogEntry,
    pub proof: DecisionProof,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub request_id: String,
    pub key: String,
    pub value: String,
}

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

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub key: String,
    pub consistency: Option<ReadConsistency>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReadResponse {
    pub value: Option<String>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlExecuteRequest {
    pub request_id: String,
    pub statements: Vec<SqlStatement>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlExecuteResponse {
    pub version: u16,
    pub applied_index: LogIndex,
    pub hash: LogHash,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<SqlStatementResult>,
}

impl From<WriteResponse> for SqlExecuteResponse {
    fn from(response: WriteResponse) -> Self {
        sql_execute_response(response, None)
    }
}

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

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlStatementResult {
    pub statement_index: usize,
    pub rows_affected: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returning: Option<SqlQueryResult>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryRequest {
    pub statement: SqlStatement,
    pub consistency: Option<ReadConsistency>,
    pub max_rows: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

pub struct NodeRuntime {
    config: NodeConfig,
    consensus: Arc<ThreeNodeConsensus>,
    log_store: FileLogStore,
    sqlite: Mutex<SqliteStateMachine>,
    commit: Mutex<()>,
    checkpointing: AtomicBool,
    operation_cancelled: AtomicBool,
    operation_cancelled_notify: tokio::sync::Notify,
    ready: AtomicBool,
    fatal: AtomicBool,
    fatal_reason: Mutex<Option<String>>,
    _data_root_lock: fs::File,
}

struct ExecutedPayload {
    response: WriteResponse,
    sql_result: Option<SqlCommandResult>,
}

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
        let persisted_config_id = log_store
            .configuration_state()
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .config_id();
        let sqlite_path = config.data_dir.join("sqlite/db.sqlite");
        let sqlite = if sqlite_path.exists() {
            SqliteStateMachine::open(
                &sqlite_path,
                &config.cluster_id,
                &config.node_id,
                config.epoch,
                persisted_config_id,
            )
        } else {
            SqliteStateMachine::open_with_configuration(
                &sqlite_path,
                &config.cluster_id,
                &config.node_id,
                config.epoch,
                config.configuration_state.clone(),
            )
        }
        .map_err(|error| NodeError::Storage(error.to_string()))?;

        reconcile_local_storage(&config, &log_store, &sqlite)?;
        recover_peer_candidates(
            &config,
            consensus.as_ref(),
            &log_store,
            &sqlite,
            peer_candidates,
        )?;
        recover_startup_decisions(&config, consensus.as_ref(), &log_store, &sqlite)?;

        Ok(Self {
            config,
            consensus,
            log_store,
            sqlite: Mutex::new(sqlite),
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

    pub fn write(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, NodeError> {
        let payload = canonical_put(request_id, key, value)?;
        let _commit = self.lock_commit()?;
        self.execute_payload_locked(request_id, payload, None)
            .map(|outcome| outcome.response)
    }

    pub fn execute_sql(&self, command: SqlCommand) -> Result<WriteResponse, NodeError> {
        self.execute_sql_with_results(command)
            .map(|response| WriteResponse {
                applied_index: response.applied_index,
                hash: response.hash,
            })
    }

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

    fn execute_client_batch(
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
        for (index, member) in members.iter().enumerate() {
            match self.check_request(&member.request_id, &member.payload) {
                Ok(Some(outcome)) => {
                    results[index] = Some(self.member_response(member, outcome));
                }
                Ok(None) => pending.push(index),
                Err(error) => results[index] = Some(Err(error)),
            }
        }

        while !pending.is_empty() {
            if pending.len() == 1 {
                let index = pending[0];
                results[index] = Some(self.execute_single_member_locked(&members[index]));
                break;
            }
            let (last_index, last_hash) = match self.ensure_materialized_tip() {
                Ok(tip) => tip,
                Err(error) => {
                    for index in pending.drain(..) {
                        results[index] = Some(Err(error.clone()));
                    }
                    break;
                }
            };
            let original_payloads = pending
                .iter()
                .map(|index| members[*index].payload.clone())
                .collect::<Vec<_>>();
            let proposal_payload = {
                let sqlite = match self.lock_sqlite() {
                    Ok(sqlite) => sqlite,
                    Err(error) => {
                        for index in pending.drain(..) {
                            results[index] = Some(Err(error.clone()));
                        }
                        break;
                    }
                };
                sqlite.prepare_write_batch(&original_payloads, last_index, last_hash)
            };
            let proposal_payload = match proposal_payload {
                Ok(payload) if payload.len() <= MAX_COMMAND_BYTES => payload,
                Ok(_) | Err(_) => {
                    for index in pending.drain(..) {
                        results[index] = Some(self.execute_single_member_locked(&members[index]));
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
                match self.check_request(&member.request_id, &member.payload) {
                    Ok(Some(outcome)) => {
                        results[index] = Some(self.member_response(member, outcome));
                    }
                    Ok(None) => remaining.push(index),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }
            if entry.entry_type == EntryType::Command
                && entry.payload == proposal_payload
                && !remaining.is_empty()
            {
                let error = self.latch(NodeError::Invariant(
                    "committed write batch did not record every request".into(),
                ));
                for index in remaining.drain(..) {
                    results[index] = Some(Err(error.clone()));
                }
            }
            pending = remaining;
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(self.latch(NodeError::Invariant(
                        "writer batch omitted a request result".into(),
                    )))
                })
            })
            .collect()
    }

    fn execute_single_member_locked(
        &self,
        member: &RuntimeBatchMember,
    ) -> Result<ClientWriteResponse, NodeError> {
        if let Some(command) = member.sql.as_ref() {
            self.execute_sql_payload_locked(command, member.payload.clone())
                .map(|outcome| {
                    ClientWriteResponse::Sql(sql_execute_response(
                        outcome.response,
                        outcome.sql_result,
                    ))
                })
        } else {
            self.execute_payload_locked(&member.request_id, member.payload.clone(), None)
                .map(|outcome| ClientWriteResponse::KeyValue(outcome.response))
        }
    }

    fn member_response(
        &self,
        member: &RuntimeBatchMember,
        outcome: RequestOutcome,
    ) -> Result<ClientWriteResponse, NodeError> {
        if member.sql.is_some() {
            let result = self.replay_sql_result(&member.request_id, &member.payload, outcome)?;
            Ok(ClientWriteResponse::Sql(sql_execute_response(
                write_response(outcome),
                result,
            )))
        } else {
            Ok(ClientWriteResponse::KeyValue(write_response(outcome)))
        }
    }

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

    fn prepare_sql_proposal(
        &self,
        command: &SqlCommand,
        request_payload: &[u8],
        base_index: LogIndex,
        base_hash: LogHash,
    ) -> Result<Vec<u8>, NodeError> {
        let sqlite = self.lock_sqlite()?;
        match sqlite.prepare_sql_effect(command, request_payload, base_index, base_hash) {
            Ok(SqlEffectPreparation::Effect(payload)) if payload.len() <= MAX_COMMAND_BYTES => {
                Ok(payload)
            }
            Ok(SqlEffectPreparation::Effect(_)) => Err(NodeError::InvalidRequest(format!(
                "SQL effect exceeds {MAX_COMMAND_BYTES} bytes"
            ))),
            Ok(SqlEffectPreparation::StatementReplay) => Ok(request_payload.to_vec()),
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

    fn execute_payload_locked(
        &self,
        request_id: &str,
        payload: Vec<u8>,
        sql: Option<&SqlCommand>,
    ) -> Result<ExecutedPayload, NodeError> {
        self.ensure_ready()?;
        self.ensure_writes_active()?;

        if let Some(outcome) = self.check_request(request_id, &payload)? {
            let sql_result = if sql.is_some() {
                self.replay_sql_result(request_id, &payload, outcome)?
            } else {
                None
            };
            return Ok(ExecutedPayload {
                response: write_response(outcome),
                sql_result,
            });
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
            let sql_result = self.persist_entry(&entry, slot, last_hash)?;

            if let Some(outcome) = self.check_request(request_id, &payload)? {
                return Ok(ExecutedPayload {
                    response: write_response(outcome),
                    sql_result: sql.is_some().then_some(sql_result).flatten(),
                });
            }
            if entry.entry_type == EntryType::Command && entry.payload == payload {
                return Err(self.latch(NodeError::Invariant(
                    "committed request was not recorded by SQLite".into(),
                )));
            }
        }
    }

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
        let sqlite = self.lock_sqlite()?;
        sqlite
            .applied_index_value()
            .map_err(|error| self.map_sqlite_error(error))
    }

    pub fn applied_hash(&self) -> Result<LogHash, NodeError> {
        self.ensure_ready()?;
        let sqlite = self.lock_sqlite()?;
        sqlite
            .applied_hash_value()
            .map_err(|error| self.map_sqlite_error(error))
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

    fn read_local(
        &self,
        key: &str,
        required_index: Option<LogIndex>,
    ) -> Result<ReadResponse, NodeError> {
        self.ensure_ready()?;
        let sqlite = self.lock_sqlite()?;
        let applied_index = sqlite
            .applied_index_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        let hash = sqlite
            .applied_hash_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        let value = sqlite
            .get_value(key)
            .map_err(|error| self.map_sqlite_error(error))?;
        Ok(ReadResponse {
            value,
            applied_index,
            hash,
        })
    }

    fn query_sql_local(
        &self,
        statement: &SqlStatement,
        required_index: Option<LogIndex>,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, NodeError> {
        self.ensure_ready()?;
        let sqlite = self.lock_sqlite()?;
        let applied_index = sqlite
            .applied_index_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        if required_index.is_some_and(|required| applied_index < required) {
            return Err(NodeError::Unavailable(format!(
                "local applied index {applied_index} has not reached {}",
                required_index.expect("checked above")
            )));
        }
        let hash = sqlite
            .applied_hash_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        let SqlQueryResult { columns, rows } = sqlite
            .query_sql(
                statement,
                usize::try_from(max_rows).expect("u32 fits usize"),
                MAX_SQL_RESULT_BYTES,
            )
            .map_err(|error| NodeError::InvalidSqlStatement {
                statement_index: 0,
                message: error.to_string(),
            })?;
        let response = SqlQueryResponse {
            columns,
            rows,
            applied_index,
            hash,
        };
        let encoded_size = serde_json::to_vec(&response)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?
            .len();
        if encoded_size > MAX_SQL_RESPONSE_BYTES {
            return Err(NodeError::InvalidRequest(format!(
                "SQL response exceeds {MAX_SQL_RESPONSE_BYTES} bytes"
            )));
        }
        Ok(response)
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

    fn ensure_materialized_tip(&self) -> Result<(LogIndex, LogHash), NodeError> {
        let (last_index, last_hash) = self.durable_tip()?;
        let sqlite = self.lock_sqlite()?;
        let applied_index = sqlite
            .applied_index_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        let applied_hash = sqlite
            .applied_hash_value()
            .map_err(|error| self.map_sqlite_error(error))?;
        if (applied_index, applied_hash) != (last_index, last_hash) {
            return Err(self.latch(NodeError::Invariant(format!(
                "qlog tip {last_index}/{} differs from SQLite tip {applied_index}/{}",
                last_hash.to_hex(),
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
        let sqlite = self.lock_sqlite()?;
        sqlite
            .apply_entry_with_result(entry)
            .map(|outcome| outcome.sql_result().cloned())
            .map_err(|error| self.map_sqlite_error(error))
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

    fn lock_sqlite(&self) -> Result<MutexGuard<'_, SqliteStateMachine>, NodeError> {
        self.sqlite
            .lock()
            .map_err(|_| self.latch(NodeError::Invariant("SQLite mutex is poisoned".into())))
    }

    fn map_sqlite_error(&self, error: queqlite_sqlite::Error) -> NodeError {
        match error {
            queqlite_sqlite::Error::RequestConflict(conflict) => {
                NodeError::RequestConflict(conflict)
            }
            queqlite_sqlite::Error::InvalidCommand(message)
            | queqlite_sqlite::Error::IdentityMismatch(message)
            | queqlite_sqlite::Error::InvalidEntry(message)
            | queqlite_sqlite::Error::InvalidSnapshot(message) => {
                self.latch(NodeError::Invariant(message))
            }
            other => self.latch(NodeError::Storage(other.to_string())),
        }
    }

    fn map_consensus_error(&self, error: queqlite_quepaxa::Error) -> NodeError {
        match error {
            queqlite_quepaxa::Error::NoQuorum
            | queqlite_quepaxa::Error::CommandUnavailable
            | queqlite_quepaxa::Error::Cancelled
            | queqlite_quepaxa::Error::Io(_) => NodeError::Unavailable(error.to_string()),
            queqlite_quepaxa::Error::ContentionExhausted { .. } => {
                NodeError::Contention(error.to_string())
            }
            queqlite_quepaxa::Error::ConflictingCertificates
            | queqlite_quepaxa::Error::ChainConflict { .. } => {
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

    use super::{
        client_authenticated, next_sync_flush_retry, Duration, HeaderMap, PROTOCOL_VERSION,
        SYNC_FLUSH_RETRY_INITIAL, VERSION_HEADER,
    };

    #[test]
    fn client_authentication_rejects_empty_expected_token() {
        let mut headers = HeaderMap::new();
        headers.insert(VERSION_HEADER, HeaderValue::from_static(PROTOCOL_VERSION));
        headers.insert("authorization", HeaderValue::from_static("Bearer "));

        assert!(!client_authenticated(&headers, ""));
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
}

fn canonical_put(request_id: &str, key: &str, value: &str) -> Result<Vec<u8>, NodeError> {
    validate_field("request_id", request_id, MAX_REQUEST_ID_BYTES, false)?;
    validate_key(key)?;
    validate_field("value", value, MAX_VALUE_BYTES, true)?;
    let payload = format!("put\t{request_id}\t{key}\t{value}").into_bytes();
    if payload.len() > MAX_COMMAND_BYTES {
        return Err(NodeError::InvalidRequest(format!(
            "command exceeds {MAX_COMMAND_BYTES} bytes"
        )));
    }
    Ok(payload)
}

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

fn validate_key(key: &str) -> Result<(), NodeError> {
    validate_field("key", key, MAX_KEY_BYTES, false)
}

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

fn write_response(outcome: RequestOutcome) -> WriteResponse {
    WriteResponse {
        applied_index: outcome.original_log_index(),
        hash: outcome.original_log_hash(),
    }
}

fn reconcile_local_storage(
    config: &NodeConfig,
    log_store: &FileLogStore,
    sqlite: &SqliteStateMachine,
) -> Result<(), NodeError> {
    let log_state = log_store
        .logical_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let log_last_index = log_state.tip.as_ref().map_or(0, |tip| tip.index());
    let applied_index = sqlite
        .applied_index_value()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let applied_hash = sqlite
        .applied_hash_value()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    let mut sqlite_configuration = sqlite
        .configuration_state_value()
        .map_err(|error| NodeError::Storage(error.to_string()))?;

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
            "SQLite is ahead at {applied_index}, qlog ends at {log_last_index}"
        )));
    }
    if applied_index == 0 {
        if applied_hash != LogHash::ZERO {
            return Err(NodeError::Reconciliation(
                "SQLite genesis hash is not zero".into(),
            ));
        }
    } else if !log_state.anchor.as_ref().is_some_and(|anchor| {
        applied_index == anchor.compacted().index() && applied_hash == anchor.compacted().hash()
    }) {
        let entry = log_store
            .read(applied_index)
            .map_err(|error| NodeError::Storage(error.to_string()))?
            .ok_or_else(|| {
                NodeError::Reconciliation(format!(
                    "qlog prefix is missing SQLite index {applied_index}"
                ))
            })?;
        validate_entry_envelope(config, &entry, applied_index, entry.prev_hash)?;
        if entry.hash != applied_hash {
            return Err(NodeError::Reconciliation(format!(
                "SQLite hash diverges from qlog at index {applied_index}"
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
        validate_runtime_entry(
            config,
            &sqlite_configuration,
            &entry,
            index,
            expected_prev_hash,
        )?;
        sqlite
            .apply_entry(&entry)
            .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
        sqlite_configuration = sqlite
            .configuration_state_value()
            .map_err(|error| NodeError::Reconciliation(error.to_string()))?;
        expected_prev_hash = entry.hash;
    }
    let log_configuration = log_store
        .configuration_state()
        .map_err(|error| NodeError::Storage(error.to_string()))?;
    if sqlite_configuration != log_configuration {
        return Err(NodeError::Reconciliation(
            "qlog and SQLite configuration states disagree".into(),
        ));
    }
    Ok(())
}

fn recover_peer_candidates(
    config: &NodeConfig,
    consensus: &ThreeNodeConsensus,
    log_store: &FileLogStore,
    sqlite: &SqliteStateMachine,
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
                        sqlite,
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
    sqlite: &SqliteStateMachine,
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
                persist_startup_entry(config, log_store, sqlite, &entry, slot, last_hash)?;
            }
            DecisionInspection::Pending => {
                let entry = consensus
                    .propose_at(
                        slot,
                        last_hash,
                        Command::new(CommandKind::ReadBarrier, Vec::new()),
                    )
                    .map_err(startup_consensus_error)?;
                persist_startup_entry(config, log_store, sqlite, &entry, slot, last_hash)?;
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
    sqlite: &SqliteStateMachine,
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
    sqlite
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

fn startup_consensus_error(error: queqlite_quepaxa::Error) -> NodeError {
    match error {
        queqlite_quepaxa::Error::NoQuorum
        | queqlite_quepaxa::Error::CommandUnavailable
        | queqlite_quepaxa::Error::ContentionExhausted { .. }
        | queqlite_quepaxa::Error::Io(_) => NodeError::Unavailable(error.to_string()),
        other => NodeError::Reconciliation(other.to_string()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct E2eConfig {
    pub data_dir: PathBuf,
    pub object_store: ObjStoreConfig,
    pub cluster_id: String,
    pub node_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct E2eReport {
    pub applied_index: LogIndex,
    pub restored_value: String,
    pub object_keys: Vec<String>,
}

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
    let base_entry = consensus.propose(Command::new(
        CommandKind::Deterministic,
        b"put\talpha\tbravo".to_vec(),
    ))?;
    db.apply_entry(&base_entry)?;
    let snapshot = db.create_snapshot(base_entry.index)?;

    let tail_entry = consensus.propose(Command::new(
        CommandKind::Deterministic,
        b"put\talpha\tcharlie".to_vec(),
    ))?;
    let segment_path = write_segment_file(&log_dir, std::slice::from_ref(&tail_entry))?;
    let segment = queqlite_log::SegmentFile::new(
        IndexRange::new(tail_entry.index, tail_entry.index)?,
        fs::read(&segment_path)?,
    );
    db.apply_entry(&tail_entry)?;

    let local_archive = matches!(&config.object_store, ObjStoreConfig::Local { .. });
    let store = ObjStore::new(config.object_store)?;
    let archive = if local_archive {
        queqlite_archive::ObjectArchiveStore::new_for_single_process(
            store.clone(),
            config.cluster_id.clone(),
        )
    } else {
        queqlite_archive::ObjectArchiveStore::new(store.clone(), config.cluster_id.clone())?
    };

    let segment_record = archive.publish_segment(tail_entry.epoch, &segment).await?;
    let snapshot_record = archive.publish_snapshot(&snapshot).await?;
    let (mut archive_manifest, expected_manifest_version) = match archive.load_manifest().await? {
        Some(loaded) => (loaded.manifest().clone(), Some(loaded.version().clone())),
        None => (
            queqlite_archive::ArchiveManifest::new(config.cluster_id.clone()),
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
    let downloaded_snapshot = queqlite_core::Snapshot::new(
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
    let object_keys = store
        .list(&format!("queqlite/{}", config.cluster_id))
        .await?;

    Ok(E2eReport {
        applied_index,
        restored_value,
        object_keys,
    })
}

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

fn directory_has_entries(path: &std::path::Path) -> Result<bool, std::io::Error> {
    match fs::read_dir(path) {
        Ok(mut entries) => entries.next().transpose().map(|entry| entry.is_some()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
