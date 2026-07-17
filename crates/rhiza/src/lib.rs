#[cfg(feature = "graph")]
use std::collections::BTreeMap;
use std::{
    fmt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use rhiza_node::{
    confirm_write_durability, ConfigError, NodeConfig, NodeError, NodeRuntime, NodeService,
};
use rhiza_quepaxa::{Error as ConsensusError, ThreeNodeConsensus};
use tokio::{
    sync::{watch, OwnedRwLockReadGuard, RwLock},
    task::{JoinError, JoinHandle},
};

pub use rhiza_core::ExecutionProfile;
#[cfg(feature = "graph")]
pub use rhiza_graph::{
    CanonicalF64, GraphColumn, GraphCommandResultV1, GraphCommandV1, GraphInternalId,
    GraphLogicalType, GraphNode, GraphParameterValue, GraphQueryResult, GraphRel, GraphResultValue,
    GraphValueV1,
};
#[cfg(feature = "kv")]
pub use rhiza_kv::{KvCommandResultV1, KvCommandV1, KvReadTip, KvScanResult, KvScanRow};
pub use rhiza_node::{
    effective_cluster_id, CheckpointCoordinator, DurabilityError, DurabilityHealth, DurabilityMode,
    LogPeer, NodeStatus, ReadConsistency, ReadResponse, SqlExecuteResponse, SqlQueryResponse,
    SqlStatementResult, WriteRequest, WriteResponse,
};
#[cfg(feature = "graph")]
pub use rhiza_node::{GraphMutationOutcome, GraphReadResponse};
#[cfg(feature = "kv")]
pub use rhiza_node::{KvMutationOutcome, KvReadResponse};
pub use rhiza_quepaxa::RecorderRpc;
pub use rhiza_sql::{SqlCommand, SqlQueryResult, SqlStatement, SqlValue};

const MATERIALIZER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SHUTDOWN_RPC_TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddedIdentity {
    pub cluster_id: String,
    pub node_id: String,
    pub epoch: u64,
    pub config_id: u64,
}

impl EmbeddedIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        epoch: u64,
        config_id: u64,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            epoch,
            config_id,
        }
    }
}

pub struct EmbeddedConfig {
    pub identity: EmbeddedIdentity,
    pub data_dir: PathBuf,
    pub execution_profile: ExecutionProfile,
    pub members: Vec<String>,
    pub recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    pub log_peers: Vec<Box<dyn LogPeer>>,
    pub coordinator: Option<Arc<CheckpointCoordinator>>,
}

impl EmbeddedConfig {
    pub fn new(
        identity: EmbeddedIdentity,
        data_dir: impl Into<PathBuf>,
        execution_profile: ExecutionProfile,
        members: impl Into<Vec<String>>,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Box<dyn LogPeer>>,
        coordinator: Option<Arc<CheckpointCoordinator>>,
    ) -> Self {
        Self {
            identity,
            data_dir: data_dir.into(),
            execution_profile,
            members: members.into(),
            recorders,
            log_peers,
            coordinator,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Closed,
    ExecutionProfileMismatch {
        expected: ExecutionProfile,
        actual: ExecutionProfile,
    },
    Config(ConfigError),
    Consensus(ConsensusError),
    Node(NodeError),
    Durability(DurabilityError),
    PendingConsensusRpcs,
    Worker(JoinError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "rhiza is closed"),
            Self::ExecutionProfileMismatch { expected, actual } => write!(
                f,
                "execution profile mismatch: expected {expected}, got {actual}"
            ),
            Self::Config(error) => error.fmt(f),
            Self::Consensus(error) => error.fmt(f),
            Self::Node(error) => error.fmt(f),
            Self::Durability(error) => error.fmt(f),
            Self::PendingConsensusRpcs => {
                write!(
                    f,
                    "consensus RPCs did not finish before the shutdown deadline"
                )
            }
            Self::Worker(error) => write!(f, "embedded worker failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Closed | Self::ExecutionProfileMismatch { .. } => None,
            Self::Config(error) => Some(error),
            Self::Consensus(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::Durability(error) => Some(error),
            Self::PendingConsensusRpcs => None,
            Self::Worker(error) => Some(error),
        }
    }
}

impl From<ConfigError> for Error {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ConsensusError> for Error {
    fn from(error: ConsensusError) -> Self {
        Self::Consensus(error)
    }
}

impl From<NodeError> for Error {
    fn from(error: NodeError) -> Self {
        Self::Node(error)
    }
}

impl From<DurabilityError> for Error {
    fn from(error: DurabilityError) -> Self {
        Self::Durability(error)
    }
}

struct Inner {
    runtime: Arc<NodeRuntime>,
    service: NodeService,
    execution_profile: ExecutionProfile,
    coordinator: Option<Arc<CheckpointCoordinator>>,
    operations: Arc<RwLock<()>>,
    closed: AtomicBool,
    shutdown: watch::Sender<bool>,
}

pub struct Rhiza {
    inner: Option<Arc<Inner>>,
    workers: Vec<JoinHandle<Result<(), Error>>>,
}

#[derive(Clone)]
pub struct RhizaHandle {
    inner: Weak<Inner>,
}

impl Rhiza {
    pub async fn open(config: EmbeddedConfig) -> Result<Self, Error> {
        let EmbeddedConfig {
            identity,
            data_dir,
            execution_profile,
            members,
            recorders,
            log_peers,
            coordinator,
        } = config;
        let node_config = NodeConfig::new_embedded(
            identity.cluster_id.clone(),
            identity.node_id.clone(),
            data_dir,
            identity.epoch,
            identity.config_id,
            members,
        )?
        .with_execution_profile(execution_profile)?;
        let effective_cluster_id = node_config.cluster_id().to_owned();
        let consensus = Arc::new(ThreeNodeConsensus::from_recorders_with_ids(
            effective_cluster_id,
            identity.node_id,
            identity.epoch,
            identity.config_id,
            recorders,
        )?);
        if node_config.membership() != consensus.membership() {
            return Err(ConfigError::PeerMembershipMismatch.into());
        }
        let peers: Vec<&dyn LogPeer> = log_peers.iter().map(Box::as_ref).collect();
        let runtime = Arc::new(NodeRuntime::open(node_config, consensus, &peers)?);

        if let Some(coordinator) = &coordinator {
            coordinator.note_recovered_committed(runtime.applied_index()?);
        }

        let service = NodeService::new(runtime.clone(), coordinator.clone());
        let (shutdown, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            runtime,
            service,
            execution_profile,
            coordinator,
            operations: Arc::new(RwLock::new(())),
            closed: AtomicBool::new(false),
            shutdown,
        });
        let mut workers = vec![spawn_materializer(&inner)];
        if inner
            .coordinator
            .as_ref()
            .is_some_and(|coordinator| !matches!(coordinator.mode(), DurabilityMode::Sync))
        {
            workers.push(spawn_coordinator(&inner));
        }

        Ok(Self {
            inner: Some(inner),
            workers,
        })
    }

    pub fn handle(&self) -> RhizaHandle {
        RhizaHandle {
            inner: Arc::downgrade(self.inner.as_ref().expect("open owner has inner state")),
        }
    }

    pub async fn shutdown(mut self) -> Result<(), Error> {
        let inner = self.inner.take().expect("open owner has inner state");
        inner.closed.store(true, Ordering::Release);
        inner.runtime.cancel_operations();
        let operations = inner.operations.write().await;
        stop_inner(&inner);
        drop(operations);
        let mut worker_result = Ok(());
        for worker in self.workers.drain(..) {
            match worker.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if worker_result.is_ok() => worker_result = Err(error),
                Err(error) if worker_result.is_ok() => worker_result = Err(Error::Worker(error)),
                _ => {}
            }
        }
        let mut result = flush_applied_tip(&inner).await;
        let consensus_result = finish_pending_consensus_rpcs(&inner, SHUTDOWN_RPC_TIMEOUT);
        if result.is_ok() {
            result = consensus_result;
        }
        if result.is_ok() {
            result = worker_result;
        }
        drop(inner);
        result
    }
}

impl Drop for Rhiza {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            stop_inner(inner);
        }
    }
}

impl RhizaHandle {
    pub async fn put(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.put(request_id, key, value).await?)
    }

    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.write(request).await?)
    }

    pub async fn execute_sql(&self, command: SqlCommand) -> Result<SqlExecuteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.execute_sql(command).await?)
    }

    pub async fn read(
        &self,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ReadResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner.service.read(key, consistency).await?)
    }

    pub async fn query(
        &self,
        statement: SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Sqlite)?;
        Ok(inner
            .service
            .query(statement, consistency, max_rows)
            .await?)
    }

    #[cfg(feature = "graph")]
    pub async fn mutate_graph(
        &self,
        command: GraphCommandV1,
    ) -> Result<GraphMutationOutcome, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Graph)?;
        embedded_write_allowed(&inner)?;
        let runtime = inner.runtime.clone();
        let outcome = tokio::task::spawn_blocking(move || runtime.mutate_graph(command))
            .await
            .map_err(Error::Worker)??;
        confirm_embedded_write(&inner, outcome.applied_index()).await?;
        Ok(outcome)
    }

    #[cfg(feature = "graph")]
    pub async fn query_graph(
        &self,
        statement: impl Into<String>,
        parameters: BTreeMap<String, GraphParameterValue>,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<GraphQueryResult, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Graph)?;
        let runtime = inner.runtime.clone();
        let statement = statement.into();
        tokio::task::spawn_blocking(move || {
            runtime.query_graph(&statement, &parameters, consistency, max_rows)
        })
        .await
        .map_err(Error::Worker)?
        .map_err(Error::Node)
    }

    #[cfg(feature = "kv")]
    pub async fn put_kv(
        &self,
        request_id: impl Into<String>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<KvMutationOutcome, Error> {
        let command = KvCommandV1::put(request_id, key, value)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
        self.mutate_kv(command).await
    }

    #[cfg(feature = "kv")]
    pub async fn delete_kv(
        &self,
        request_id: impl Into<String>,
        key: Vec<u8>,
    ) -> Result<KvMutationOutcome, Error> {
        let command = KvCommandV1::delete(request_id, key)
            .map_err(|error| NodeError::InvalidRequest(error.to_string()))?;
        self.mutate_kv(command).await
    }

    #[cfg(feature = "kv")]
    pub async fn mutate_kv(&self, command: KvCommandV1) -> Result<KvMutationOutcome, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        embedded_write_allowed(&inner)?;
        let runtime = inner.runtime.clone();
        let outcome = tokio::task::spawn_blocking(move || runtime.mutate_kv(command))
            .await
            .map_err(Error::Worker)??;
        confirm_embedded_write(&inner, outcome.applied_index()).await?;
        Ok(outcome)
    }

    #[cfg(feature = "kv")]
    pub async fn get_kv(
        &self,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<KvReadResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        let runtime = inner.runtime.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || runtime.get_kv(&key, consistency))
            .await
            .map_err(Error::Worker)?
            .map_err(Error::Node)
    }

    #[cfg(feature = "kv")]
    pub async fn scan_kv_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        cursor: Option<&[u8]>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        let runtime = inner.runtime.clone();
        let start = start.to_vec();
        let end = end.map(<[u8]>::to_vec);
        let cursor = cursor.map(<[u8]>::to_vec);
        tokio::task::spawn_blocking(move || {
            runtime.scan_kv_range(
                &start,
                end.as_deref(),
                limit,
                cursor.as_deref(),
                consistency,
            )
        })
        .await
        .map_err(Error::Worker)?
        .map_err(Error::Node)
    }

    #[cfg(feature = "kv")]
    pub async fn scan_kv_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
        cursor: Option<&[u8]>,
        consistency: ReadConsistency,
    ) -> Result<KvScanResult, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        require_profile(&inner, ExecutionProfile::Kv)?;
        let runtime = inner.runtime.clone();
        let prefix = prefix.to_vec();
        let cursor = cursor.map(<[u8]>::to_vec);
        tokio::task::spawn_blocking(move || {
            runtime.scan_kv_prefix(&prefix, limit, cursor.as_deref(), consistency)
        })
        .await
        .map_err(Error::Worker)?
        .map_err(Error::Node)
    }

    pub async fn status(&self) -> Result<NodeStatus, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        let runtime = inner.runtime.clone();
        let mut status = tokio::task::spawn_blocking(move || runtime.status())
            .await
            .map_err(Error::Worker)??;
        if inner
            .coordinator
            .as_ref()
            .is_some_and(|coordinator| coordinator.health() == DurabilityHealth::Unavailable)
        {
            status.ready = false;
        }
        Ok(status)
    }

    async fn begin_operation(&self) -> Result<(Arc<Inner>, OwnedRwLockReadGuard<()>), Error> {
        let inner = self.inner.upgrade().ok_or(Error::Closed)?;
        let operation = inner.operations.clone().read_owned().await;
        if inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok((inner, operation))
    }
}

fn require_profile(inner: &Inner, expected: ExecutionProfile) -> Result<(), Error> {
    if inner.execution_profile == expected {
        Ok(())
    } else {
        Err(Error::ExecutionProfileMismatch {
            expected,
            actual: inner.execution_profile,
        })
    }
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn embedded_write_allowed(inner: &Inner) -> Result<(), Error> {
    if let Some(coordinator) = &inner.coordinator {
        coordinator.write_allowed()?;
    }
    Ok(())
}

#[cfg(any(feature = "graph", feature = "kv"))]
async fn confirm_embedded_write(
    inner: &Inner,
    applied_index: rhiza_core::LogIndex,
) -> Result<(), Error> {
    confirm_write_durability(
        inner.runtime.as_ref(),
        inner.coordinator.as_deref(),
        applied_index,
    )
    .await
    .map_err(Error::Durability)
}

fn spawn_materializer(inner: &Arc<Inner>) -> JoinHandle<Result<(), Error>> {
    let runtime = inner.runtime.clone();
    let shutdown = inner.shutdown.subscribe();
    tokio::spawn(async move {
        runtime
            .run_background_materializer(MATERIALIZER_POLL_INTERVAL, wait_for_shutdown(shutdown))
            .await
            .map_err(Error::Node)
    })
}

fn spawn_coordinator(inner: &Arc<Inner>) -> JoinHandle<Result<(), Error>> {
    let coordinator = inner.coordinator.as_ref().unwrap().clone();
    let runtime = inner.runtime.clone();
    let shutdown = inner.shutdown.subscribe();
    tokio::spawn(async move {
        coordinator
            .run_background(runtime, wait_for_shutdown(shutdown))
            .await
            .map_err(Error::Durability)
    })
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

async fn flush_applied_tip(inner: &Inner) -> Result<(), Error> {
    let Some(coordinator) = &inner.coordinator else {
        return Ok(());
    };
    let applied_tip = inner.runtime.applied_index()?;
    coordinator.note_committed(applied_tip);
    coordinator
        .flush_runtime(&inner.runtime, applied_tip)
        .await?;
    Ok(())
}

fn finish_pending_consensus_rpcs(inner: &Inner, timeout: Duration) -> Result<(), Error> {
    let consensus = inner.runtime.consensus();
    let finished = if matches!(
        tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    ) {
        tokio::task::block_in_place(|| consensus.finish_pending_rpcs(timeout))
    } else {
        consensus.finish_pending_rpcs(timeout)
    };
    if finished {
        Ok(())
    } else {
        Err(Error::PendingConsensusRpcs)
    }
}

fn stop_inner(inner: &Inner) {
    inner.closed.store(true, Ordering::Release);
    inner.runtime.cancel_operations();
    let _ = inner.shutdown.send(true);
}
