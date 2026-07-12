use std::{
    fmt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use queqlite_node::{ConfigError, NodeConfig, NodeError, NodeRuntime, NodeService};
use queqlite_quepaxa::{Error as ConsensusError, ThreeNodeConsensus};
use tokio::{
    sync::{watch, OwnedRwLockReadGuard, RwLock},
    task::{JoinError, JoinHandle},
};

pub use queqlite_node::{
    CheckpointCoordinator, DurabilityError, DurabilityHealth, DurabilityMode, LogPeer, NodeStatus,
    ReadConsistency, ReadResponse, SqlExecuteResponse, SqlQueryResponse, SqlStatementResult,
    WriteRequest, WriteResponse,
};
pub use queqlite_quepaxa::RecorderRpc;
pub use queqlite_sqlite::{SqlCommand, SqlQueryResult, SqlStatement, SqlValue};

const MATERIALIZER_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    pub members: Vec<String>,
    pub recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    pub log_peers: Vec<Box<dyn LogPeer>>,
    pub coordinator: Option<Arc<CheckpointCoordinator>>,
}

impl EmbeddedConfig {
    pub fn new(
        identity: EmbeddedIdentity,
        data_dir: impl Into<PathBuf>,
        members: impl Into<Vec<String>>,
        recorders: Vec<(String, Box<dyn RecorderRpc>)>,
        log_peers: Vec<Box<dyn LogPeer>>,
        coordinator: Option<Arc<CheckpointCoordinator>>,
    ) -> Self {
        Self {
            identity,
            data_dir: data_dir.into(),
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
    Config(ConfigError),
    Consensus(ConsensusError),
    Node(NodeError),
    Durability(DurabilityError),
    Worker(JoinError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "queqlite is closed"),
            Self::Config(error) => error.fmt(f),
            Self::Consensus(error) => error.fmt(f),
            Self::Node(error) => error.fmt(f),
            Self::Durability(error) => error.fmt(f),
            Self::Worker(error) => write!(f, "embedded worker failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Closed => None,
            Self::Config(error) => Some(error),
            Self::Consensus(error) => Some(error),
            Self::Node(error) => Some(error),
            Self::Durability(error) => Some(error),
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
    coordinator: Option<Arc<CheckpointCoordinator>>,
    operations: Arc<RwLock<()>>,
    closed: AtomicBool,
    shutdown: watch::Sender<bool>,
}

pub struct Queqlite {
    inner: Option<Arc<Inner>>,
    workers: Vec<JoinHandle<Result<(), Error>>>,
}

#[derive(Clone)]
pub struct QueqliteHandle {
    inner: Weak<Inner>,
}

impl Queqlite {
    pub async fn open(config: EmbeddedConfig) -> Result<Self, Error> {
        let EmbeddedConfig {
            identity,
            data_dir,
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
        )?;
        let consensus = Arc::new(ThreeNodeConsensus::from_recorders_with_ids(
            identity.cluster_id,
            identity.node_id,
            identity.epoch,
            identity.config_id,
            recorders,
        )?);
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

    pub fn handle(&self) -> QueqliteHandle {
        QueqliteHandle {
            inner: Arc::downgrade(self.inner.as_ref().expect("open owner has inner state")),
        }
    }

    pub async fn shutdown(mut self) -> Result<(), Error> {
        let inner = self.inner.take().expect("open owner has inner state");
        inner.closed.store(true, Ordering::Release);
        inner.runtime.cancel_operations();
        let operations = inner.operations.write().await;
        let mut result = flush_applied_tip(&inner).await;
        stop_inner(&inner);
        drop(operations);
        for worker in self.workers.drain(..) {
            match worker.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if result.is_ok() => result = Err(error),
                Err(error) if result.is_ok() => result = Err(Error::Worker(error)),
                _ => {}
            }
        }
        drop(inner);
        result
    }
}

impl Drop for Queqlite {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            stop_inner(inner);
        }
    }
}

impl QueqliteHandle {
    pub async fn put(
        &self,
        request_id: &str,
        key: &str,
        value: &str,
    ) -> Result<WriteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        Ok(inner.service.put(request_id, key, value).await?)
    }

    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        Ok(inner.service.write(request).await?)
    }

    pub async fn execute_sql(&self, command: SqlCommand) -> Result<SqlExecuteResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        Ok(inner.service.execute_sql(command).await?)
    }

    pub async fn read(
        &self,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ReadResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
        Ok(inner.service.read(key, consistency).await?)
    }

    pub async fn query(
        &self,
        statement: SqlStatement,
        consistency: ReadConsistency,
        max_rows: u32,
    ) -> Result<SqlQueryResponse, Error> {
        let (inner, _operation) = self.begin_operation().await?;
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

fn stop_inner(inner: &Inner) {
    inner.closed.store(true, Ordering::Release);
    inner.runtime.cancel_operations();
    let _ = inner.shutdown.send(true);
}
