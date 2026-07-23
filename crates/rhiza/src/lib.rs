use std::{
    fmt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use rhiza_node::{confirm_write_durability, ConfigError, NodeConfig, NodeRuntime, NodeService};
use rhiza_quepaxa::{Error as ConsensusError, Membership, RecorderFileStore, ThreeNodeConsensus};
use tokio::{
    sync::{watch, OwnedRwLockReadGuard, RwLock},
    task::{JoinError, JoinHandle},
};

pub use rhiza_core::{ErrorCategory, ErrorClassification, ExecutionProfile};
pub use rhiza_node::{
    effective_cluster_id, CheckpointCoordinator, DurabilityError, DurabilityHealth, DurabilityMode,
    LogPeer, NodeError, NodeStatus, ReadConsistency, ReadResponse, SqlExecuteResponse,
    SqlQueryResponse, SqlStatementResult, WriteRequest, WriteResponse,
};
pub use rhiza_quepaxa::RecorderRpc;
pub use rhiza_sql::{SqlCommand, SqlQueryResult, SqlStatement, SqlValue};

const MATERIALIZER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SHUTDOWN_RPC_TIMEOUT: Duration = Duration::from_secs(25);
const LOCAL_RECORDER_IDS: [&str; 3] = ["recorder-1", "recorder-2", "recorder-3"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddedIdentity {
    cluster_id: String,
    node_id: String,
    epoch: u64,
    config_id: u64,
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
    identity: EmbeddedIdentity,
    data_dir: PathBuf,
    execution_profile: ExecutionProfile,
    members: Vec<String>,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Box<dyn LogPeer>>,
    coordinator: Option<Arc<CheckpointCoordinator>>,
}

impl EmbeddedConfig {
    /// Creates a fixed three-recorder configuration for one local process.
    ///
    /// This writes durable state below `root` and is not highly available: the node and all
    /// recorders share one process and failure domain. Use [`Self::new`] when transports or
    /// recorder membership must be supplied explicitly.
    pub fn local_file_backed(
        logical_cluster_id: impl Into<String>,
        root: impl Into<PathBuf>,
        execution_profile: ExecutionProfile,
    ) -> Result<Self, Error> {
        require_sql_embedded_profile(execution_profile)?;
        let logical_cluster_id = logical_cluster_id.into();
        let cluster_id = effective_cluster_id(execution_profile, &logical_cluster_id)?;
        let root = root.into();
        let membership = Membership::new(LOCAL_RECORDER_IDS)?;
        let recorders = membership
            .members()
            .iter()
            .map(|id| {
                let recorder = RecorderFileStore::new_with_membership(
                    root.join("recorders").join(id),
                    id.clone(),
                    &cluster_id,
                    1,
                    1,
                    membership.clone(),
                )?;
                Ok((id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>))
            })
            .collect::<Result<Vec<_>, ConsensusError>>()?;

        Ok(Self::new(
            EmbeddedIdentity::new(logical_cluster_id, LOCAL_RECORDER_IDS[0], 1, 1),
            root.join("node"),
            execution_profile,
            membership.members().to_vec(),
            recorders,
            vec![],
            None,
        ))
    }

    /// Creates an embedded configuration from explicitly supplied transports.
    ///
    /// This is an advanced extension point for custom or remote deployments. The facade
    /// re-exports its `RecorderRpc` and `LogPeer` trait boundaries, but implementing them or
    /// using the component-specific transport vocabulary requires direct dependencies on
    /// `rhiza-quepaxa` and `rhiza-node`. Most applications should use [`Self::local_file_backed`].
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

    /// Adds a checkpoint coordinator to this configuration.
    pub fn with_coordinator(mut self, coordinator: Arc<CheckpointCoordinator>) -> Self {
        self.coordinator = Some(coordinator);
        self
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

impl Error {
    /// Returns a stable machine-readable code, category, and retry guidance.
    pub fn classification(&self) -> ErrorClassification {
        match self {
            Self::Node(error) => error.classification(),
            Self::Closed => ErrorClassification::new("closed", ErrorCategory::Unavailable, true),
            Self::ExecutionProfileMismatch { .. } => ErrorClassification::new(
                "execution_profile_mismatch",
                ErrorCategory::Internal,
                false,
            ),
            Self::Config(_) => {
                ErrorClassification::new("config_error", ErrorCategory::Internal, false)
            }
            Self::Consensus(_) => {
                ErrorClassification::new("consensus_error", ErrorCategory::Unavailable, true)
            }
            Self::Durability(_) => {
                ErrorClassification::new("durability_error", ErrorCategory::Unavailable, true)
            }
            Self::PendingConsensusRpcs => {
                ErrorClassification::new("pending_consensus_rpcs", ErrorCategory::Unavailable, true)
            }
            Self::Worker(_) => {
                ErrorClassification::new("worker_error", ErrorCategory::Internal, false)
            }
        }
    }
}

/// An outer failure from an embedded typed batch write.
///
/// `NotAttempted` means the complete vector failed validation or admission before any command was
/// attempted. `Indeterminate` means execution may have committed commands but their durability
/// could not be confirmed. After `Indeterminate`, retry the entire unchanged vector with the same
/// request IDs; per-command idempotency makes that retry safe.
#[derive(Debug)]
pub enum BatchWriteError {
    NotAttempted(Error),
    Indeterminate(Error),
}

impl fmt::Display for BatchWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAttempted(error) => write!(f, "batch was not attempted: {error}"),
            Self::Indeterminate(error) => write!(f, "batch outcome is indeterminate: {error}"),
        }
    }
}

impl std::error::Error for BatchWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotAttempted(error) | Self::Indeterminate(error) => Some(error),
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

/// Owns the embedded node runtime and its background workers.
///
/// Keep this owner alive for the lifetime of the application server. During planned shutdown,
/// first drain the server, then call [`Self::shutdown`]. Dropping the owner only signals its
/// workers and cannot report drain or durability errors.
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
        require_sql_embedded_profile(execution_profile)?;
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

    /// Drains embedded work and returns any durability or worker failure.
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

    /// Executes an ordered, non-atomic SQL batch that may coalesce commands into fewer log entries.
    ///
    /// The returned vector has the same length and order as `commands`. An outer `NotAttempted`
    /// guarantees that no command was attempted. After `Indeterminate`, retry the entire unchanged
    /// vector with the same request IDs.
    pub async fn execute_sql_batch(
        &self,
        commands: Vec<SqlCommand>,
    ) -> Result<Vec<Result<SqlExecuteResponse, NodeError>>, BatchWriteError> {
        self.execute_typed_batch(
            ExecutionProfile::Sqlite,
            move |runtime| runtime.execute_sql_batch(commands),
            |response| response.applied_index,
        )
        .await
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

    async fn execute_typed_batch<T, F, I>(
        &self,
        profile: ExecutionProfile,
        execute: F,
        applied_index: I,
    ) -> Result<Vec<Result<T, NodeError>>, BatchWriteError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<NodeRuntime>) -> Result<Vec<Result<T, NodeError>>, NodeError>
            + Send
            + 'static,
        I: Fn(&T) -> rhiza_core::LogIndex,
    {
        let (inner, _operation) = self
            .begin_operation()
            .await
            .map_err(BatchWriteError::NotAttempted)?;
        require_profile(&inner, profile).map_err(BatchWriteError::NotAttempted)?;
        embedded_write_allowed(&inner).map_err(BatchWriteError::NotAttempted)?;
        let runtime = inner.runtime.clone();
        let results = tokio::task::spawn_blocking(move || execute(runtime))
            .await
            .map_err(|error| BatchWriteError::Indeterminate(Error::Worker(error)))?
            .map_err(|error| BatchWriteError::NotAttempted(Error::Node(error)))?;
        if let Some(index) = results
            .iter()
            .filter_map(|result| result.as_ref().ok().map(&applied_index))
            .max()
        {
            confirm_embedded_write(&inner, index)
                .await
                .map_err(BatchWriteError::Indeterminate)?;
        }
        Ok(results)
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

fn require_sql_embedded_profile(execution_profile: ExecutionProfile) -> Result<(), Error> {
    if execution_profile == ExecutionProfile::Sqlite {
        Ok(())
    } else {
        Err(Error::ExecutionProfileMismatch {
            expected: ExecutionProfile::Sqlite,
            actual: execution_profile,
        })
    }
}

fn embedded_write_allowed(inner: &Inner) -> Result<(), Error> {
    if let Some(coordinator) = &inner.coordinator {
        coordinator.write_allowed()?;
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn open_rejects_recorder_membership_before_creating_runtime_storage() {
        let root = tempfile::tempdir().unwrap();
        let mut config =
            EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Sqlite)
                .unwrap();
        config.members = vec![
            "recorder-1".into(),
            "recorder-2".into(),
            "recorder-4".into(),
        ];

        assert!(matches!(
            Rhiza::open(config).await,
            Err(Error::Config(ConfigError::PeerMembershipMismatch))
        ));
        assert!(!root.path().join("node").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_rejects_non_sql_profile_before_creating_runtime_storage() {
        let root = tempfile::tempdir().unwrap();
        let config = EmbeddedConfig::new(
            EmbeddedIdentity::new("cluster-a", "node-1", 1, 1),
            root.path().join("node"),
            ExecutionProfile::Graph,
            vec![],
            vec![],
            vec![],
            None,
        );

        assert!(matches!(
            Rhiza::open(config).await,
            Err(Error::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: ExecutionProfile::Graph,
            })
        ));
        assert!(!root.path().join("node").exists());
    }
}
