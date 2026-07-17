use std::{
    error::Error,
    fmt, fs,
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use rhiza_archive::{
    CheckpointIdentity, CheckpointPublisher, CheckpointPublisherOptions, CheckpointTip,
    ObjectArchiveStore, RestoredCheckpoint,
};
#[cfg(any(feature = "graph", feature = "kv"))]
use rhiza_core::SnapshotIdentity;
use rhiza_core::{
    ConfigurationState, ExecutionProfile, LogAnchor, LogHash, LogIndex, RecoveryAnchor,
};
#[cfg(feature = "graph")]
use rhiza_graph::{
    decode_snapshot as decode_graph_snapshot, encode_snapshot as encode_graph_snapshot,
    restore_snapshot_file as restore_graph_snapshot_file,
};
#[cfg(feature = "kv")]
use rhiza_kv::{
    decode_snapshot as decode_kv_snapshot, encode_snapshot as encode_kv_snapshot,
    restore_snapshot_file as restore_kv_snapshot_file,
};
use rhiza_log::{FileLogStore, IndexRange, LogStore};
#[cfg(feature = "sql")]
use rhiza_sql::{restore_recovery_snapshot_file, sql_executor_fingerprint};
use serde::Serialize;

use crate::{Materializer, NodeConfig, NodeRuntime};

const FLUSH_BATCH_ENTRIES: LogIndex = 32;
const RESTORE_INTENT_FILE: &str = ".rhiza-restore-v1";
const RESTORE_INTENT_CONTENTS: &[u8] = b"rhiza restore in progress\n";
const RESTORE_STAGING_PREFIX: &str = ".restore-stage-";
const RESTORE_MARKER_TMP_PREFIX: &str = ".restore-marker-tmp-";
const SUCCESSOR_RESTORE_LOCK_FILE: &str = ".successor-restore.lock";
const SUCCESSOR_RESTORE_INTENT_FILE: &str = ".successor-restore.intent";
const SUCCESSOR_RESTORE_COMPLETE_FILE: &str = ".successor-restore.complete";
static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize)]
struct SuccessorRestoreIdentity<'a> {
    version: u32,
    cluster_id: &'a str,
    epoch: u64,
    target_config_id: u64,
    recovery_generation: u64,
    node_id: &'a str,
    membership_digest: String,
    predecessor_config_id: u64,
    stop_index: LogIndex,
    stop_hash: String,
    checkpoint_index: LogIndex,
    checkpoint_hash: String,
}

pub struct SuccessorRestorePreparation {
    tip: CheckpointTip,
    data_dir: PathBuf,
    identity: Vec<u8>,
    requires_recorder_install: bool,
    _lock: fs::File,
}

impl SuccessorRestorePreparation {
    pub const fn tip(&self) -> CheckpointTip {
        self.tip
    }

    pub const fn requires_recorder_install(&self) -> bool {
        self.requires_recorder_install
    }

    pub fn complete(mut self) -> Result<CheckpointTip, DurabilityError> {
        if !self.requires_recorder_install {
            return Ok(self.tip);
        }
        let intent = self.data_dir.join(SUCCESSOR_RESTORE_INTENT_FILE);
        if fs::read(&intent)? != self.identity {
            return Err(DurabilityError::SnapshotVerification(
                "successor restore intent changed before completion".into(),
            ));
        }
        fs::rename(intent, self.data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE))?;
        sync_directory(&self.data_dir)?;
        self.requires_recorder_install = false;
        Ok(self.tip)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurabilityMode {
    Sync,
    Bounded { max_lag: Duration },
    Periodic { interval: Duration },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityHealth {
    Available,
    Unavailable,
}

impl DurabilityMode {
    pub fn validate(&self) -> Result<(), DurabilityError> {
        match self {
            Self::Sync => Ok(()),
            Self::Bounded { max_lag } if max_lag.is_zero() => {
                Err(DurabilityError::InvalidDuration { mode: "bounded" })
            }
            Self::Periodic { interval } if interval.is_zero() => {
                Err(DurabilityError::InvalidDuration { mode: "periodic" })
            }
            Self::Bounded { .. } | Self::Periodic { .. } => Ok(()),
        }
    }
}

#[derive(Debug)]
pub enum DurabilityError {
    InvalidDuration {
        mode: &'static str,
    },
    MissingCheckpoint,
    Unavailable,
    LagExceeded {
        committed_index: LogIndex,
        durable_index: LogIndex,
        max_lag: Duration,
    },
    ArchiveAheadOfLocal {
        durable_index: LogIndex,
        local_index: LogIndex,
    },
    SnapshotRequired {
        anchor: Box<RecoveryAnchor>,
    },
    LocalLogGap {
        expected: LogIndex,
        actual: Option<LogIndex>,
    },
    LocalLogConflict {
        index: LogIndex,
    },
    SnapshotVerification(String),
    PreconditionFailed,
    DataDirNotFresh(PathBuf),
    Archive(rhiza_archive::Error),
    Log(rhiza_log::Error),
    Io(std::io::Error),
}

impl fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDuration { mode } => {
                write!(f, "{mode} durability duration must be non-zero")
            }
            Self::MissingCheckpoint => write!(f, "checkpoint manifest is missing"),
            Self::Unavailable => write!(f, "sync durability is unavailable"),
            Self::LagExceeded {
                committed_index,
                durable_index,
                max_lag,
            } => write!(
                f,
                "checkpoint lag exceeded {max_lag:?}: committed index {committed_index}, durable index {durable_index}"
            ),
            Self::ArchiveAheadOfLocal {
                durable_index,
                local_index,
            } => write!(
                f,
                "checkpoint tip {durable_index} is ahead of local qlog tip {local_index}"
            ),
            Self::SnapshotRequired { anchor } => write!(
                f,
                "snapshot restore required at qlog anchor {} before checkpoint flush",
                anchor.compacted().index()
            ),
            Self::LocalLogGap { expected, actual } => {
                write!(f, "local qlog gap: expected index {expected}, got {actual:?}")
            }
            Self::LocalLogConflict { index } => {
                write!(f, "local qlog hash chain conflicts at index {index}")
            }
            Self::SnapshotVerification(message) => {
                write!(f, "checkpoint snapshot verification failed: {message}")
            }
            Self::PreconditionFailed => write!(f, "checkpoint precondition failed"),
            Self::DataDirNotFresh(path) => write!(
                f,
                "restore data directory contains existing state: {}",
                path.display()
            ),
            Self::Archive(error) => error.fmt(f),
            Self::Log(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
        }
    }
}

impl Error for DurabilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Archive(error) => Some(error),
            Self::Log(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rhiza_archive::Error> for DurabilityError {
    fn from(error: rhiza_archive::Error) -> Self {
        Self::Archive(error)
    }
}

impl From<rhiza_log::Error> for DurabilityError {
    fn from(error: rhiza_log::Error) -> Self {
        Self::Log(error)
    }
}

impl From<std::io::Error> for DurabilityError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
enum PendingLag {
    New(Instant),
    Recovered,
}

#[derive(Debug)]
struct CoordinatorState {
    durable_tip: CheckpointTip,
    committed_index: LogIndex,
    pending_lag: Option<PendingLag>,
    health: DurabilityHealth,
}

pub struct CheckpointCoordinator {
    store: ObjectArchiveStore,
    publisher: CheckpointPublisher,
    mode: DurabilityMode,
    state: Mutex<CoordinatorState>,
    publication_attempts: AtomicU64,
}

struct RuntimeCheckpointSnapshot {
    anchor: RecoveryAnchor,
    archive_bytes: Vec<u8>,
}

#[cfg(any(feature = "graph", feature = "kv"))]
struct EngineSnapshotIdentity<'a> {
    cluster_id: &'a str,
    epoch: u64,
    config_id: u64,
    applied_index: LogIndex,
    applied_hash: LogHash,
}

impl CheckpointCoordinator {
    pub async fn open(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
    ) -> Result<Self, DurabilityError> {
        Self::open_with_holder(store, mode, "anonymous-node").await
    }

    pub async fn open_with_holder(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
        holder: impl AsRef<str>,
    ) -> Result<Self, DurabilityError> {
        Self::open_with_holder_and_options(
            store,
            mode,
            holder,
            CheckpointPublisherOptions::default(),
        )
        .await
    }

    pub async fn open_with_holder_and_options(
        store: ObjectArchiveStore,
        mode: DurabilityMode,
        holder: impl AsRef<str>,
        publisher_options: CheckpointPublisherOptions,
    ) -> Result<Self, DurabilityError> {
        mode.validate()?;
        store
            .load_checkpoint()
            .await?
            .ok_or(DurabilityError::MissingCheckpoint)?;
        let identity = store.checkpoint_identity()?;
        let holder = format!(
            "checkpoint-coordinator-{}-{}-{}-{}-{}",
            identity.cluster_id(),
            identity.epoch(),
            identity.config_id(),
            identity.recovery_generation(),
            holder.as_ref()
        );
        let publisher = store
            .open_checkpoint_publisher(holder, publisher_options)
            .await?;
        let loaded = publisher.cached_checkpoint().await;
        let durable_tip = *loaded.manifest().tip();
        let restored = store.restore_checkpoint_v2().await?;
        let restored_tip = *restored.tip();
        if restored_tip != durable_tip {
            return Err(DurabilityError::Archive(
                rhiza_archive::Error::InvalidCheckpoint(
                    "restored entries changed while verifying the loaded manifest".into(),
                ),
            ));
        }
        Ok(Self {
            store,
            publisher,
            mode,
            state: Mutex::new(CoordinatorState {
                durable_tip,
                committed_index: durable_tip.index(),
                pending_lag: None,
                health: DurabilityHealth::Available,
            }),
            publication_attempts: AtomicU64::new(0),
        })
    }

    pub const fn mode(&self) -> &DurabilityMode {
        &self.mode
    }

    pub fn durable_tip(&self) -> CheckpointTip {
        self.lock_state().durable_tip
    }

    pub async fn refresh_durable_tip(&self) -> Result<CheckpointTip, DurabilityError> {
        let loaded = self.publisher.observe_checkpoint().await?;
        let accepted = self.publisher.cache_observed_checkpoint(loaded).await?;
        observe_durable_tip(&self.state, *accepted.manifest().tip())
    }

    pub fn health(&self) -> DurabilityHealth {
        self.lock_state().health
    }

    #[doc(hidden)]
    pub fn checkpoint_publication_attempts(&self) -> u64 {
        self.publication_attempts.load(Ordering::Relaxed)
    }

    pub fn note_committed(&self, index: LogIndex) {
        let mut state = self.lock_state();
        if index <= state.committed_index {
            return;
        }
        state.committed_index = index;
        if index > state.durable_tip.index() && state.pending_lag.is_none() {
            state.pending_lag = Some(PendingLag::New(Instant::now()));
        }
    }

    pub fn note_recovered_committed(&self, index: LogIndex) {
        let mut state = self.lock_state();
        state.committed_index = state.committed_index.max(index);
        if state.committed_index > state.durable_tip.index() {
            state.pending_lag = Some(PendingLag::Recovered);
        }
    }

    pub fn write_allowed(&self) -> Result<(), DurabilityError> {
        if matches!(self.mode, DurabilityMode::Sync)
            && self.health() == DurabilityHealth::Unavailable
        {
            return Err(DurabilityError::Unavailable);
        }
        let DurabilityMode::Bounded { max_lag } = self.mode else {
            return Ok(());
        };
        let state = self.lock_state();
        let exceeded = state.committed_index > state.durable_tip.index()
            && match state.pending_lag {
                Some(PendingLag::Recovered) => true,
                Some(PendingLag::New(pending)) => pending.elapsed() >= max_lag,
                None => false,
            };
        if exceeded {
            return Err(DurabilityError::LagExceeded {
                committed_index: state.committed_index,
                durable_index: state.durable_tip.index(),
                max_lag,
            });
        }
        Ok(())
    }

    pub async fn flush_runtime(
        &self,
        runtime: &NodeRuntime,
        target_index: LogIndex,
    ) -> Result<CheckpointTip, DurabilityError> {
        let result = self.flush_runtime_inner(runtime, target_index).await;
        if result.is_err() {
            self.mark_unavailable();
        }
        result
    }

    async fn flush_runtime_inner(
        &self,
        runtime: &NodeRuntime,
        target_index: LogIndex,
    ) -> Result<CheckpointTip, DurabilityError> {
        let log_state = runtime.log_store().logical_state()?;
        let local_index = log_state.tip.as_ref().map_or(0, |tip| tip.index());
        let mut durable_tip = self.durable_tip();
        if durable_tip.index() > local_index {
            return Err(DurabilityError::ArchiveAheadOfLocal {
                durable_index: durable_tip.index(),
                local_index,
            });
        }
        let target_index = target_index.min(local_index);
        if target_index <= durable_tip.index() {
            return Ok(durable_tip);
        }
        if let Some(anchor) = log_state.anchor {
            if durable_tip.index() < anchor.compacted().index() {
                return Err(DurabilityError::SnapshotRequired {
                    anchor: Box::new(anchor),
                });
            }
        }

        let mut next =
            durable_tip
                .index()
                .checked_add(1)
                .ok_or_else(|| DurabilityError::LocalLogGap {
                    expected: durable_tip.index(),
                    actual: None,
                })?;
        while next <= target_index {
            let end = next
                .saturating_add(FLUSH_BATCH_ENTRIES - 1)
                .min(target_index);
            let entries = runtime
                .log_store()
                .read_range(IndexRange::new(next, end)?)?;
            validate_local_batch(&entries, next, end, durable_tip)?;
            self.publication_attempts.fetch_add(1, Ordering::Relaxed);
            let published = self.publisher.publish_committed(&entries).await?;
            durable_tip = *published.manifest().tip();
            self.mark_durable(durable_tip);
            if durable_tip.index() >= target_index {
                break;
            }
            next =
                durable_tip
                    .index()
                    .checked_add(1)
                    .ok_or_else(|| DurabilityError::LocalLogGap {
                        expected: durable_tip.index(),
                        actual: None,
                    })?;
        }
        Ok(durable_tip)
    }

    pub async fn checkpoint_compact(
        &self,
        runtime: &NodeRuntime,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        self.checkpoint_compact_inner(runtime, None).await
    }

    pub async fn checkpoint_compact_fenced(
        &self,
        runtime: &NodeRuntime,
        expected_config_id: u64,
        expected_recovery_generation: u64,
        expected_root: LogAnchor,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        self.checkpoint_compact_inner(
            runtime,
            Some((
                expected_config_id,
                expected_recovery_generation,
                expected_root,
            )),
        )
        .await
    }

    async fn checkpoint_compact_inner(
        &self,
        runtime: &NodeRuntime,
        fence: Option<(u64, u64, LogAnchor)>,
    ) -> Result<RecoveryAnchor, DurabilityError> {
        let (target, snapshot, _fence) = {
            let _commit = runtime.commit.lock().map_err(|_| {
                DurabilityError::SnapshotVerification("commit mutex is poisoned".into())
            })?;
            runtime
                .ensure_ready()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let configuration = runtime
                .configuration_state()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if !configuration.is_active() && configuration.stop().is_none() {
                return Err(DurabilityError::SnapshotVerification(
                    "runtime configuration is not compactable".into(),
                ));
            }
            if let Some((config_id, generation, root)) = fence {
                let actual_config_id = configuration.config_id();
                let actual_generation = runtime.config.recovery_generation();
                let actual_root = runtime.log_root_unlocked().ok();
                if actual_config_id != config_id
                    || actual_generation != generation
                    || actual_root != Some(root)
                {
                    eprintln!(
                        "checkpoint fence mismatch: config {actual_config_id}/{config_id}, \
                         generation {actual_generation}/{generation}, root {actual_root:?}/{root:?}"
                    );
                    return Err(DurabilityError::PreconditionFailed);
                }
            }
            if runtime
                .checkpointing
                .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                return Err(DurabilityError::SnapshotVerification(
                    "checkpoint transition is already in progress".into(),
                ));
            }
            let fence = CheckpointFence(&runtime.checkpointing);
            let (target, target_hash) = runtime
                .ensure_materialized_tip()
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            let snapshot =
                create_runtime_checkpoint_snapshot(runtime, target, target_hash, &configuration)?;
            (target, snapshot, fence)
        };
        self.flush_runtime(runtime, target).await?;
        let anchor = snapshot.anchor.clone();
        self.publisher
            .publish_checkpoint_snapshot(anchor.clone(), &snapshot.archive_bytes)
            .await?;
        let restored = self.store.restore_checkpoint_v2().await?;
        let published = restored.snapshot().ok_or_else(|| {
            DurabilityError::SnapshotVerification("published V2 root has no snapshot".into())
        })?;
        if published.anchor() != &anchor || published.bytes() != snapshot.archive_bytes {
            return Err(DurabilityError::SnapshotVerification(
                "read-back anchor or snapshot bytes differ".into(),
            ));
        }
        runtime.log_store.compact_prefix(&anchor)?;
        self.mark_durable(CheckpointTip::new(
            anchor.compacted().index(),
            anchor.compacted().hash(),
        ));
        Ok(anchor)
    }

    pub async fn run_background<F>(
        self: Arc<Self>,
        runtime: Arc<NodeRuntime>,
        shutdown: F,
    ) -> Result<(), DurabilityError>
    where
        F: Future<Output = ()> + Send,
    {
        let cadence = match self.mode {
            DurabilityMode::Sync => return Ok(()),
            DurabilityMode::Bounded { max_lag } => {
                let half = max_lag / 2;
                half.min(Duration::from_secs(1))
            }
            DurabilityMode::Periodic { interval } => interval,
        };
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                () = tokio::time::sleep(cadence) => {
                    match self.flush_runtime(&runtime, LogIndex::MAX).await {
                        Ok(_) | Err(DurabilityError::Archive(_) | DurabilityError::Io(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    fn mark_durable(&self, durable_tip: CheckpointTip) {
        let mut state = self.lock_state();
        mark_durable_state(&mut state, durable_tip);
    }

    fn mark_unavailable(&self) {
        let mut state = self.lock_state();
        if state.committed_index > state.durable_tip.index() {
            state.health = DurabilityHealth::Unavailable;
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, CoordinatorState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn create_runtime_checkpoint_snapshot(
    runtime: &NodeRuntime,
    target: LogIndex,
    target_hash: LogHash,
    configuration: &ConfigurationState,
) -> Result<RuntimeCheckpointSnapshot, DurabilityError> {
    #[cfg(not(any(feature = "graph", feature = "kv")))]
    let _ = target_hash;
    let materializer = runtime
        .lock_materializer()
        .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    match &*materializer {
        #[cfg(not(any(feature = "sql", feature = "graph", feature = "kv")))]
        Materializer::Unavailable => unreachable!("no execution profiles are compiled in"),
        #[cfg(feature = "sql")]
        Materializer::Sql(state) => {
            let snapshot = state
                .create_recovery_snapshot(runtime.config().recovery_generation())
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            if snapshot.anchor().compacted().index() != target
                || snapshot.anchor().configuration_state() != configuration
            {
                return Err(DurabilityError::SnapshotVerification(
                    "SQLite snapshot does not match the compacted runtime state".into(),
                ));
            }
            Ok(RuntimeCheckpointSnapshot {
                anchor: snapshot.anchor().clone(),
                archive_bytes: snapshot.db_bytes().to_vec(),
            })
        }
        #[cfg(feature = "graph")]
        Materializer::Graph(state) => {
            let snapshot = state
                .create_snapshot(target)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            validate_engine_snapshot_identity(
                runtime,
                configuration,
                EngineSnapshotIdentity {
                    cluster_id: snapshot.cluster_id(),
                    epoch: snapshot.epoch(),
                    config_id: snapshot.config_id(),
                    applied_index: snapshot.applied_index(),
                    applied_hash: snapshot.applied_hash(),
                },
                target,
                target_hash,
            )?;
            let archive_bytes = encode_graph_snapshot(&snapshot)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            Ok(RuntimeCheckpointSnapshot {
                anchor: engine_recovery_anchor(
                    runtime,
                    configuration,
                    target,
                    snapshot.applied_hash(),
                    snapshot.materializer_fingerprint(),
                    &archive_bytes,
                )?,
                archive_bytes,
            })
        }
        #[cfg(feature = "kv")]
        Materializer::Kv(state) => {
            let snapshot = state
                .create_snapshot(target)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            validate_engine_snapshot_identity(
                runtime,
                configuration,
                EngineSnapshotIdentity {
                    cluster_id: snapshot.cluster_id(),
                    epoch: snapshot.epoch(),
                    config_id: snapshot.config_id(),
                    applied_index: snapshot.applied_index(),
                    applied_hash: snapshot.applied_hash(),
                },
                target,
                target_hash,
            )?;
            let archive_bytes = encode_kv_snapshot(&snapshot)
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
            Ok(RuntimeCheckpointSnapshot {
                anchor: engine_recovery_anchor(
                    runtime,
                    configuration,
                    target,
                    snapshot.applied_hash(),
                    snapshot.materializer_fingerprint(),
                    &archive_bytes,
                )?,
                archive_bytes,
            })
        }
    }
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn validate_engine_snapshot_identity(
    runtime: &NodeRuntime,
    configuration: &ConfigurationState,
    snapshot: EngineSnapshotIdentity<'_>,
    expected_index: LogIndex,
    expected_hash: LogHash,
) -> Result<(), DurabilityError> {
    let config = runtime.config();
    if snapshot.cluster_id != config.cluster_id()
        || snapshot.epoch != config.epoch()
        || snapshot.config_id != configuration.config_id()
        || snapshot.applied_index == 0
        || snapshot.applied_index != expected_index
        || snapshot.applied_hash != expected_hash
    {
        return Err(DurabilityError::SnapshotVerification(
            "engine snapshot identity does not match the compacted runtime state".into(),
        ));
    }
    Ok(())
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn engine_recovery_anchor(
    runtime: &NodeRuntime,
    configuration: &ConfigurationState,
    applied_index: LogIndex,
    applied_hash: LogHash,
    materializer_fingerprint: LogHash,
    archive_bytes: &[u8],
) -> Result<RecoveryAnchor, DurabilityError> {
    let size_bytes = u64::try_from(archive_bytes.len()).map_err(|_| {
        DurabilityError::SnapshotVerification("snapshot envelope size exceeds u64".into())
    })?;
    Ok(RecoveryAnchor::new_with_configuration(
        runtime.config().cluster_id(),
        runtime.config().epoch(),
        configuration.clone(),
        runtime.config().recovery_generation(),
        LogAnchor::new(applied_index, applied_hash),
        SnapshotIdentity::new(
            format!("snapshot-{applied_index:020}"),
            LogHash::digest(&[archive_bytes]),
            size_bytes,
        )
        .with_executor_fingerprint(materializer_fingerprint),
    ))
}

fn mark_durable_state(state: &mut CoordinatorState, durable_tip: CheckpointTip) {
    if durable_tip.index() > state.durable_tip.index() {
        state.durable_tip = durable_tip;
    }
    if state.committed_index <= state.durable_tip.index() {
        state.pending_lag = None;
        state.health = DurabilityHealth::Available;
    }
}

fn observe_durable_tip(
    state: &Mutex<CoordinatorState>,
    observed: CheckpointTip,
) -> Result<CheckpointTip, DurabilityError> {
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let current = state.durable_tip;
    if observed.index() < current.index() {
        return Err(DurabilityError::SnapshotVerification(format!(
            "checkpoint tip rolled back from index {} to {}",
            current.index(),
            observed.index()
        )));
    }
    if observed.index() == current.index() && observed.hash() != current.hash() {
        return Err(DurabilityError::SnapshotVerification(format!(
            "checkpoint tip hash changed at index {}",
            observed.index()
        )));
    }
    mark_durable_state(&mut state, observed);
    Ok(state.durable_tip)
}

struct CheckpointFence<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for CheckpointFence<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub async fn restore_checkpoint_to_fresh_data_dir(
    store: ObjectArchiveStore,
    data_dir: impl AsRef<Path>,
) -> Result<CheckpointTip, DurabilityError> {
    restore_checkpoint_to_fresh_data_dir_with_target(store, data_dir.as_ref(), None).await
}

pub async fn restore_checkpoint_to_fresh_data_dir_for_node(
    store: ObjectArchiveStore,
    data_dir: impl AsRef<Path>,
    target_node_id: &str,
) -> Result<CheckpointTip, DurabilityError> {
    if target_node_id.is_empty() {
        return Err(DurabilityError::SnapshotVerification(
            "target node_id is empty".into(),
        ));
    }
    restore_checkpoint_to_fresh_data_dir_with_target(store, data_dir.as_ref(), Some(target_node_id))
        .await
}

async fn restore_checkpoint_to_fresh_data_dir_with_target(
    store: ObjectArchiveStore,
    data_dir: &Path,
    target_node_id: Option<&str>,
) -> Result<CheckpointTip, DurabilityError> {
    let identity = store.checkpoint_identity()?.clone();
    store
        .load_checkpoint()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    prepare_fresh_restore_data_dir(data_dir)?;
    let restored = store.restore_checkpoint_v2().await?;
    publish_restore_marker(data_dir, RESTORE_INTENT_FILE, RESTORE_INTENT_CONTENTS)?;
    install_restored_checkpoint(&identity, &restored, data_dir, target_node_id, true)
}

fn install_restored_checkpoint(
    identity: &CheckpointIdentity,
    restored: &RestoredCheckpoint,
    data_dir: &Path,
    target_node_id: Option<&str>,
    remove_generic_intent: bool,
) -> Result<CheckpointTip, DurabilityError> {
    let tip = *restored.tip();
    let staging = create_restore_staging_dir(data_dir)?;
    let result = (|| -> Result<(), DurabilityError> {
        if let Some(snapshot) = restored.snapshot() {
            install_profile_snapshot(identity, snapshot, &staging, target_node_id)?;
        }

        if restored.snapshot().is_some() || !restored.suffix().is_empty() {
            let initial_configuration = restored
                .snapshot()
                .map(|snapshot| snapshot.anchor().configuration_state().clone())
                .unwrap_or_else(|| ConfigurationState::active(identity.config_id(), LogHash::ZERO));
            let log = FileLogStore::open_with_configuration(
                staging.join("consensus/log"),
                identity.cluster_id(),
                identity.epoch(),
                initial_configuration,
            )?;
            if let Some(snapshot) = restored.snapshot() {
                log.install_recovery_anchor(
                    snapshot.anchor(),
                    identity.recovery_generation(),
                    snapshot.anchor().configuration_state(),
                )?;
            }
            for batch in restored.suffix().chunks(FLUSH_BATCH_ENTRIES as usize) {
                log.append_batch(batch)?;
            }
            let installed_tip = log.logical_state()?.tip;
            if installed_tip.as_ref().map(|tip| (tip.index(), tip.hash()))
                != Some((tip.index(), tip.hash()))
            {
                return Err(DurabilityError::SnapshotVerification(
                    "installed qlog tip does not match checkpoint tip".into(),
                ));
            }
        }
        publish_restore_staging(&staging, data_dir, remove_generic_intent)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    Ok(tip)
}

fn install_profile_snapshot(
    identity: &CheckpointIdentity,
    snapshot: &rhiza_archive::RestoredCheckpointSnapshot,
    staging: &Path,
    target_node_id: Option<&str>,
) -> Result<(), DurabilityError> {
    match snapshot_profile(identity.cluster_id())? {
        ExecutionProfile::Sqlite => {
            #[cfg(feature = "sql")]
            {
                validate_anchor_fingerprint(
                    snapshot.anchor(),
                    sql_executor_fingerprint().map_err(|error| {
                        DurabilityError::SnapshotVerification(error.to_string())
                    })?,
                )?;
                let path = staging.join("sqlite/db.sqlite");
                match target_node_id {
                    Some(node_id) => restore_recovery_snapshot_file(
                        path,
                        snapshot.bytes(),
                        snapshot.anchor(),
                        node_id,
                    )
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string())),
                    None => install_snapshot_bytes(&path, snapshot.bytes()),
                }
            }
            #[cfg(not(feature = "sql"))]
            Err(DurabilityError::SnapshotVerification(
                "sql execution profile is not compiled in".into(),
            ))
        }
        ExecutionProfile::Graph => {
            #[cfg(feature = "graph")]
            {
                let decoded = decode_graph_snapshot(snapshot.bytes())
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                validate_decoded_snapshot_anchor(
                    snapshot.anchor(),
                    decoded.cluster_id(),
                    decoded.epoch(),
                    decoded.config_id(),
                    decoded.applied_index(),
                    decoded.applied_hash(),
                    decoded.materializer_fingerprint(),
                )?;
                let target_node_id = target_node_id.unwrap_or(decoded.created_by());
                restore_graph_snapshot_file(
                    staging.join("ladybug/graph.lbug"),
                    &decoded,
                    target_node_id,
                )
                .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
            }
            #[cfg(not(feature = "graph"))]
            Err(DurabilityError::SnapshotVerification(
                "graph recovery support is not compiled in".into(),
            ))
        }
        ExecutionProfile::Kv => {
            #[cfg(feature = "kv")]
            {
                let decoded = decode_kv_snapshot(snapshot.bytes())
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
                validate_decoded_snapshot_anchor(
                    snapshot.anchor(),
                    decoded.cluster_id(),
                    decoded.epoch(),
                    decoded.config_id(),
                    decoded.applied_index(),
                    decoded.applied_hash(),
                    decoded.materializer_fingerprint(),
                )?;
                let target_node_id = target_node_id.unwrap_or(decoded.created_by());
                restore_kv_snapshot_file(staging.join("kv/data.redb"), &decoded, target_node_id)
                    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))
            }
            #[cfg(not(feature = "kv"))]
            Err(DurabilityError::SnapshotVerification(
                "KV recovery support is not compiled in".into(),
            ))
        }
    }
}

fn snapshot_profile(cluster_id: &str) -> Result<ExecutionProfile, DurabilityError> {
    if matches!(cluster_id.strip_prefix("rhiza:graph:"), Some(logical) if !logical.is_empty()) {
        Ok(ExecutionProfile::Graph)
    } else if matches!(cluster_id.strip_prefix("rhiza:kv:"), Some(logical) if !logical.is_empty()) {
        Ok(ExecutionProfile::Kv)
    } else if matches!(cluster_id.strip_prefix("rhiza:sql:"), Some(logical) if !logical.is_empty())
    {
        Ok(ExecutionProfile::Sqlite)
    } else {
        Err(DurabilityError::SnapshotVerification(
            "snapshot checkpoint identity has no canonical execution profile prefix".into(),
        ))
    }
}

fn validate_anchor_fingerprint(
    anchor: &RecoveryAnchor,
    expected: LogHash,
) -> Result<(), DurabilityError> {
    if anchor.executor_fingerprint() != Some(expected) {
        return Err(DurabilityError::SnapshotVerification(
            "snapshot executor fingerprint does not match this binary".into(),
        ));
    }
    Ok(())
}

#[cfg(any(feature = "graph", feature = "kv"))]
fn validate_decoded_snapshot_anchor(
    anchor: &RecoveryAnchor,
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    applied_index: LogIndex,
    applied_hash: LogHash,
    materializer_fingerprint: LogHash,
) -> Result<(), DurabilityError> {
    validate_anchor_fingerprint(anchor, materializer_fingerprint)?;
    if anchor.cluster_id() != cluster_id
        || anchor.epoch() != epoch
        || anchor.config_id() != config_id
        || anchor.compacted().index() != applied_index
        || anchor.compacted().hash() != applied_hash
    {
        return Err(DurabilityError::SnapshotVerification(
            "decoded snapshot identity does not match its recovery anchor".into(),
        ));
    }
    Ok(())
}

pub async fn restore_successor_checkpoint_to_fresh_data_dir(
    store: ObjectArchiveStore,
    config: &NodeConfig,
) -> Result<SuccessorRestorePreparation, DurabilityError> {
    let identity = store.checkpoint_identity()?;
    let loaded = store
        .load_checkpoint()
        .await?
        .ok_or(DurabilityError::MissingCheckpoint)?;
    let transition = loaded.manifest().successor_transition().ok_or_else(|| {
        DurabilityError::SnapshotVerification(
            "successor startup requires transition provenance".into(),
        )
    })?;
    let stop = LogAnchor::new(transition.stop_entry().index, transition.stop_entry().hash);
    let expected_stopped = ConfigurationState::stopped(
        transition.predecessor().config_id(),
        transition.successor().predecessor_config_digest(),
        stop,
    );
    let expected_initial = ConfigurationState::active(
        transition.predecessor().config_id(),
        transition.successor().predecessor_config_digest(),
    );
    if identity.cluster_id() != config.cluster_id()
        || identity.epoch() != config.epoch()
        || identity.config_id() != transition.successor().config_id()
        || identity.recovery_generation() != config.recovery_generation()
        || transition.successor().cluster_id() != config.cluster_id()
        || transition.successor().members() != config.membership().members()
        || transition.successor().digest() != config.membership().digest()
        || config.configuration_state() != &expected_stopped
        || config.log_initial_configuration() != &expected_initial
    {
        return Err(DurabilityError::SnapshotVerification(
            "successor checkpoint does not match target node configuration".into(),
        ));
    }
    let receipt = serde_json::to_vec(&SuccessorRestoreIdentity {
        version: 1,
        cluster_id: config.cluster_id(),
        epoch: config.epoch(),
        target_config_id: identity.config_id(),
        recovery_generation: config.recovery_generation(),
        node_id: config.node_id(),
        membership_digest: config.membership().digest().to_hex(),
        predecessor_config_id: transition.predecessor().config_id(),
        stop_index: transition.stop_entry().index,
        stop_hash: transition.stop_entry().hash.to_hex(),
        checkpoint_index: loaded.manifest().tip().index(),
        checkpoint_hash: loaded.manifest().tip().hash().to_hex(),
    })
    .map_err(|error| DurabilityError::SnapshotVerification(error.to_string()))?;
    let (lock, state) = prepare_successor_restore_root(config.data_dir(), &receipt)?;
    if state == SuccessorRestoreRootState::Complete {
        return Ok(SuccessorRestorePreparation {
            tip: *loaded.manifest().tip(),
            data_dir: config.data_dir().clone(),
            identity: receipt,
            requires_recorder_install: false,
            _lock: lock,
        });
    }

    let restored = store.restore_checkpoint_v2().await?;
    if restored.tip() != loaded.manifest().tip() {
        return Err(DurabilityError::SnapshotVerification(
            "successor checkpoint changed during restore".into(),
        ));
    }
    if state == SuccessorRestoreRootState::Fresh {
        publish_restore_marker(config.data_dir(), SUCCESSOR_RESTORE_INTENT_FILE, &receipt)?;
    }
    install_restored_checkpoint(
        identity,
        &restored,
        config.data_dir(),
        Some(config.node_id()),
        false,
    )?;
    Ok(SuccessorRestorePreparation {
        tip: *restored.tip(),
        data_dir: config.data_dir().clone(),
        identity: receipt,
        requires_recorder_install: true,
        _lock: lock,
    })
}

#[cfg(feature = "sql")]
fn install_snapshot_bytes(path: &Path, bytes: &[u8]) -> Result<(), DurabilityError> {
    let parent = path.parent().expect("SQLite restore path has a parent");
    fs::create_dir_all(parent)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    sync_directory(parent)
}

fn create_restore_staging_dir(data_dir: &Path) -> Result<PathBuf, DurabilityError> {
    fs::create_dir_all(data_dir)?;
    for _ in 0..128 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let staging = data_dir.join(format!(
            "{RESTORE_STAGING_PREFIX}{}-{sequence}",
            process::id()
        ));
        match fs::create_dir(&staging) {
            Ok(()) => return Ok(staging),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate restore staging directory",
    )
    .into())
}

fn publish_restore_staging(
    staging: &Path,
    data_dir: &Path,
    remove_generic_intent: bool,
) -> Result<(), DurabilityError> {
    sync_directory(staging)?;
    for name in ["sqlite", "ladybug", "kv", "consensus"] {
        let source = staging.join(name);
        if source.exists() {
            fs::rename(&source, data_dir.join(name))?;
        }
    }
    fs::remove_dir(staging)?;
    sync_directory(data_dir)?;
    if remove_generic_intent {
        fs::remove_file(data_dir.join(RESTORE_INTENT_FILE))?;
    }
    sync_directory(data_dir)
}

fn publish_restore_marker(
    data_dir: &Path,
    marker_name: &str,
    contents: &[u8],
) -> Result<(), DurabilityError> {
    fs::create_dir_all(data_dir)?;
    let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = data_dir.join(format!(
        "{RESTORE_MARKER_TMP_PREFIX}{}-{sequence}",
        process::id()
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(temporary, data_dir.join(marker_name))?;
    sync_directory(data_dir)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SuccessorRestoreRootState {
    Fresh,
    Intent,
    Complete,
}

fn prepare_successor_restore_root(
    data_dir: &Path,
    expected_identity: &[u8],
) -> Result<(fs::File, SuccessorRestoreRootState), DurabilityError> {
    fs::create_dir_all(data_dir)?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(data_dir.join(SUCCESSOR_RESTORE_LOCK_FILE))?;
    lock.lock()?;

    let intent = data_dir.join(SUCCESSOR_RESTORE_INTENT_FILE);
    let complete = data_dir.join(SUCCESSOR_RESTORE_COMPLETE_FILE);
    let state = match (fs::read(&intent), fs::read(&complete)) {
        (Ok(actual), Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            if actual != expected_identity {
                return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
            }
            SuccessorRestoreRootState::Intent
        }
        (Err(error), Ok(actual)) if error.kind() == std::io::ErrorKind::NotFound => {
            if !completed_successor_identity_matches(&actual, expected_identity) {
                return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
            }
            SuccessorRestoreRootState::Complete
        }
        (Err(intent_error), Err(complete_error))
            if intent_error.kind() == std::io::ErrorKind::NotFound
                && complete_error.kind() == std::io::ErrorKind::NotFound =>
        {
            SuccessorRestoreRootState::Fresh
        }
        _ => return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf())),
    };

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let common =
            name == SUCCESSOR_RESTORE_LOCK_FILE || name.starts_with(RESTORE_MARKER_TMP_PREFIX);
        let allowed = match state {
            SuccessorRestoreRootState::Fresh => common,
            SuccessorRestoreRootState::Intent => {
                common
                    || name == SUCCESSOR_RESTORE_INTENT_FILE
                    || name == "sqlite"
                    || name == "ladybug"
                    || name == "kv"
                    || name == "consensus"
                    || name == "recorder"
                    || name.starts_with(RESTORE_STAGING_PREFIX)
            }
            SuccessorRestoreRootState::Complete => {
                common
                    || name == SUCCESSOR_RESTORE_COMPLETE_FILE
                    || name == ".node.lock"
                    || name == "sqlite"
                    || name == "ladybug"
                    || name == "kv"
                    || name == "consensus"
                    || name == "recorder"
            }
        };
        if !allowed {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
    }

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(RESTORE_MARKER_TMP_PREFIX)
        {
            fs::remove_file(entry.path())?;
        }
    }
    if state == SuccessorRestoreRootState::Intent {
        for name in ["sqlite", "ladybug", "kv", "consensus", "recorder"] {
            let path = data_dir.join(name);
            if path.exists() {
                fs::remove_dir_all(path)?;
            }
        }
        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(RESTORE_STAGING_PREFIX)
            {
                fs::remove_dir_all(entry.path())?;
            }
        }
        sync_directory(data_dir)?;
    }
    Ok((lock, state))
}

fn completed_successor_identity_matches(actual: &[u8], expected: &[u8]) -> bool {
    let (Ok(mut actual), Ok(mut expected)) = (
        serde_json::from_slice::<serde_json::Value>(actual),
        serde_json::from_slice::<serde_json::Value>(expected),
    ) else {
        return false;
    };
    let Some(actual_index) = actual["checkpoint_index"].as_u64() else {
        return false;
    };
    let Some(expected_index) = expected["checkpoint_index"].as_u64() else {
        return false;
    };
    let (Some(actual_hash), Some(expected_hash)) = (
        actual["checkpoint_hash"].as_str(),
        expected["checkpoint_hash"].as_str(),
    ) else {
        return false;
    };
    if LogHash::from_hex(actual_hash).is_none() || LogHash::from_hex(expected_hash).is_none() {
        return false;
    }
    if actual_index > expected_index
        || (actual_index == expected_index && actual_hash != expected_hash)
    {
        return false;
    }
    for receipt in [&mut actual, &mut expected] {
        let Some(receipt) = receipt.as_object_mut() else {
            return false;
        };
        receipt.remove("checkpoint_index");
        receipt.remove("checkpoint_hash");
    }
    actual == expected
}

fn sync_directory(path: &Path) -> Result<(), DurabilityError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn validate_local_batch(
    entries: &[rhiza_core::LogEntry],
    start: LogIndex,
    end: LogIndex,
    durable_tip: CheckpointTip,
) -> Result<(), DurabilityError> {
    let expected_len =
        usize::try_from(end - start + 1).map_err(|_| DurabilityError::LocalLogGap {
            expected: start,
            actual: entries.first().map(|entry| entry.index),
        })?;
    if entries.len() != expected_len {
        let actual = entries
            .iter()
            .zip(start..=end)
            .find_map(|(entry, expected)| (entry.index != expected).then_some(entry.index));
        return Err(DurabilityError::LocalLogGap {
            expected: start + entries.len() as u64,
            actual,
        });
    }

    let mut expected_hash = durable_tip.hash();
    for (expected_index, entry) in (start..).zip(entries) {
        if entry.index != expected_index {
            return Err(DurabilityError::LocalLogGap {
                expected: expected_index,
                actual: Some(entry.index),
            });
        }
        if entry.prev_hash != expected_hash || entry.recompute_hash() != entry.hash {
            return Err(DurabilityError::LocalLogConflict { index: entry.index });
        }
        expected_hash = entry.hash;
    }
    Ok(())
}

fn prepare_fresh_restore_data_dir(data_dir: &Path) -> Result<(), DurabilityError> {
    if !path_has_state(data_dir)? {
        return Ok(());
    }

    let intent = data_dir.join(RESTORE_INTENT_FILE);
    if !intent.exists() {
        let entries = fs::read_dir(data_dir)?.collect::<Result<Vec<_>, _>>()?;
        if entries.iter().all(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(RESTORE_MARKER_TMP_PREFIX)
        }) {
            for entry in entries {
                fs::remove_file(entry.path())?;
            }
            sync_directory(data_dir)?;
            return Ok(());
        }
    }
    if !matches!(fs::read(&intent), Ok(contents) if contents == RESTORE_INTENT_CONTENTS) {
        return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
    }

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let owned = name == RESTORE_INTENT_FILE
            || name == "sqlite"
            || name == "ladybug"
            || name == "kv"
            || name == "consensus"
            || name.starts_with(RESTORE_MARKER_TMP_PREFIX)
            || name.starts_with(RESTORE_STAGING_PREFIX);
        if !owned {
            return Err(DurabilityError::DataDirNotFresh(data_dir.to_path_buf()));
        }
    }

    for name in ["sqlite", "ladybug", "kv", "consensus"] {
        let path = data_dir.join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
    }
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(RESTORE_STAGING_PREFIX) {
            fs::remove_dir_all(entry.path())?;
        } else if name.starts_with(RESTORE_MARKER_TMP_PREFIX) {
            fs::remove_file(entry.path())?;
        }
    }
    fs::remove_file(intent)?;
    sync_directory(data_dir)?;
    Ok(())
}

fn path_has_state(path: &Path) -> Result<bool, std::io::Error> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Ok(true);
    }
    fs::read_dir(path)?
        .next()
        .transpose()
        .map(|entry| entry.is_some())
}

#[cfg(test)]
mod tests {
    use super::{
        completed_successor_identity_matches, mark_durable_state, observe_durable_tip,
        snapshot_profile, CheckpointTip, CoordinatorState, DurabilityError, DurabilityHealth,
        ExecutionProfile, LogHash, PendingLag,
    };
    use std::sync::{Arc, Barrier, Mutex};

    #[test]
    fn snapshot_profile_requires_a_canonical_effective_cluster_identity() {
        assert_eq!(
            snapshot_profile("rhiza:sql:cluster-a").unwrap(),
            ExecutionProfile::Sqlite
        );
        assert_eq!(
            snapshot_profile("rhiza:graph:cluster-a").unwrap(),
            ExecutionProfile::Graph
        );
        assert_eq!(
            snapshot_profile("rhiza:kv:cluster-a").unwrap(),
            ExecutionProfile::Kv
        );
        assert!(matches!(
            snapshot_profile("cluster-a"),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert!(snapshot_profile("rhiza:graph:").is_err());
    }

    fn successor_receipt(index: u64, hash_byte: char, generation: u64) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "cluster_id": "cluster-a",
            "epoch": 1,
            "target_config_id": 2,
            "recovery_generation": generation,
            "node_id": "node-1",
            "membership_digest": "digest",
            "predecessor_config_id": 1,
            "stop_index": 4,
            "stop_hash": "stop",
            "checkpoint_index": index,
            "checkpoint_hash": hash_byte.to_string().repeat(64),
        }))
        .unwrap()
    }

    #[test]
    fn completed_successor_receipt_allows_only_forward_checkpoint_progress() {
        let expected = successor_receipt(8, '8', 1);

        assert!(completed_successor_identity_matches(
            &successor_receipt(4, '4', 1),
            &expected
        ));
        assert!(!completed_successor_identity_matches(
            &successor_receipt(8, '9', 1),
            &expected
        ));
        assert!(!completed_successor_identity_matches(
            &successor_receipt(9, '9', 1),
            &expected
        ));
        assert!(!completed_successor_identity_matches(
            &successor_receipt(4, '4', 2),
            &expected
        ));
        let mut malformed =
            serde_json::from_slice::<serde_json::Value>(&successor_receipt(4, '4', 1)).unwrap();
        malformed["checkpoint_hash"] = serde_json::json!(7);
        assert!(!completed_successor_identity_matches(
            &serde_json::to_vec(&malformed).unwrap(),
            &expected
        ));
    }

    #[test]
    fn concurrent_flush_completion_cannot_regress_the_durable_tip() {
        let newer = CheckpointTip::new(8, LogHash::digest(&[b"newer"]));
        let older = CheckpointTip::new(4, LogHash::digest(&[b"older"]));
        let mut state = CoordinatorState {
            durable_tip: newer,
            committed_index: 8,
            pending_lag: None,
            health: DurabilityHealth::Available,
        };

        mark_durable_state(&mut state, older);

        assert_eq!(state.durable_tip, newer);
    }

    #[test]
    fn checkpoint_observation_rejects_same_index_hash_conflict_without_mutation() {
        let current = CheckpointTip::new(8, LogHash::digest(&[b"current"]));
        let conflicting = CheckpointTip::new(8, LogHash::digest(&[b"conflicting"]));
        let state = Mutex::new(CoordinatorState {
            durable_tip: current,
            committed_index: 8,
            pending_lag: None,
            health: DurabilityHealth::Available,
        });

        assert!(matches!(
            observe_durable_tip(&state, conflicting),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert_eq!(state.lock().unwrap().durable_tip, current);
    }

    #[test]
    fn checkpoint_observation_rejects_remote_rollback_without_mutation() {
        let current = CheckpointTip::new(8, LogHash::digest(&[b"current"]));
        let older = CheckpointTip::new(7, LogHash::digest(&[b"older"]));
        let state = Mutex::new(CoordinatorState {
            durable_tip: current,
            committed_index: 8,
            pending_lag: None,
            health: DurabilityHealth::Available,
        });

        assert!(matches!(
            observe_durable_tip(&state, older),
            Err(DurabilityError::SnapshotVerification(_))
        ));
        assert_eq!(state.lock().unwrap().durable_tip, current);
    }

    #[test]
    fn concurrent_conflicting_checkpoint_observations_accept_exactly_one_tip() {
        let state = Arc::new(Mutex::new(CoordinatorState {
            durable_tip: CheckpointTip::new(0, LogHash::ZERO),
            committed_index: 0,
            pending_lag: None,
            health: DurabilityHealth::Available,
        }));
        let start = Arc::new(Barrier::new(3));
        let results = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|label| {
                let state = Arc::clone(&state);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let tip = CheckpointTip::new(1, LogHash::digest(&[label]));
                    start.wait();
                    (tip, observe_durable_tip(&state, tip))
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        let results = results
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_err()).count(),
            1
        );
        let accepted = results
            .iter()
            .find_map(|(tip, result)| result.is_ok().then_some(*tip))
            .unwrap();
        assert_eq!(state.lock().unwrap().durable_tip, accepted);
    }

    #[test]
    fn durable_progress_clears_lag_only_after_reaching_the_committed_index() {
        let mut state = CoordinatorState {
            durable_tip: CheckpointTip::new(2, LogHash::digest(&[b"two"])),
            committed_index: 4,
            pending_lag: Some(PendingLag::Recovered),
            health: DurabilityHealth::Unavailable,
        };
        let partial = CheckpointTip::new(3, LogHash::digest(&[b"three"]));
        mark_durable_state(&mut state, partial);
        assert!(state.pending_lag.is_some());
        assert_eq!(state.health, DurabilityHealth::Unavailable);

        let complete = CheckpointTip::new(4, LogHash::digest(&[b"four"]));
        mark_durable_state(&mut state, complete);

        assert_eq!(state.durable_tip, complete);
        assert!(state.pending_lag.is_none());
        assert_eq!(state.health, DurabilityHealth::Available);
    }
}
