//! Closed-file Ladybug effects for the graph feasibility slice.
//!
//! Native Ladybug WAL replay is useful for recovery, but direct checkpoint and
//! replay-then-checkpoint do not reliably produce byte-identical files. Native
//! WAL helpers in this module are therefore audit-only. `LGFX/1` uses bounded
//! final chunk images and exact closed-file digests as its canonical authority.

use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Mutex, RwLock},
};

use lbug::{Connection, Database};
use rhiza_core::{LogEntry, LogHash, LogIndex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use super::{
    ensure_parent, initialize_or_validate, io_error, ladybug_error, ladybug_sidecar,
    ladybug_sidecars, ladybug_system_config, Error, Identity, LadybugStateMachine, Result,
};

pub const LGFX_V1_MAGIC: &[u8; 6] = b"LGFX\0\x01";
pub const LGFX_CHUNK_BYTES: usize = 4096;
pub const MAX_LGFX_V1_BYTES: usize = 256 * 1024;

const MAX_ID_BYTES: usize = 256;
const CHUNK_DIGEST_DOMAIN: &[u8] = b"rhiza-ladybug-file-chunks-v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LadybugFileChunkV1 {
    /// Zero-based fixed-size chunk index.
    pub chunk_index: u64,
    pub after_image: Vec<u8>,
}

/// Canonical closed-file effect. It deliberately contains no native WAL bytes.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LadybugFileEffectV1 {
    pub cluster_id: String,
    pub configuration_id: u64,
    pub recovery_generation: u64,
    pub base_log_index: LogIndex,
    pub base_log_hash: LogHash,
    pub base_db_digest: LogHash,
    pub base_file_bytes: u64,
    pub target_db_digest: LogHash,
    pub target_file_bytes: u64,
    pub storage_version: u64,
    pub materializer_fingerprint: LogHash,
    pub request_id: String,
    pub request_digest: LogHash,
    pub result_encoding_version: u16,
    pub bounded_result: Vec<u8>,
    pub chunks_digest: LogHash,
    pub chunks: Vec<LadybugFileChunkV1>,
}

impl LadybugFileEffectV1 {
    pub fn validate(&self) -> Result<()> {
        validate_nonempty_bounded("cluster_id", &self.cluster_id, MAX_ID_BYTES)?;
        validate_nonempty_bounded("request_id", &self.request_id, MAX_ID_BYTES)?;
        if self.result_encoding_version == 0 {
            return invalid("result encoding version must be non-zero");
        }
        validate_file_length("base", self.base_file_bytes)?;
        validate_file_length("target", self.target_file_bytes)?;
        if self.bounded_result.len() > MAX_LGFX_V1_BYTES {
            return exhausted("LGFX result exceeds the envelope bound");
        }

        let mut previous = None;
        let mut chunk_bytes = 0usize;
        for chunk in &self.chunks {
            if previous.is_some_and(|value| chunk.chunk_index <= value) {
                return invalid("chunks must be strictly ordered without duplicates");
            }
            if chunk.after_image.len() != LGFX_CHUNK_BYTES {
                return invalid(format!(
                    "chunk after-image must be exactly {LGFX_CHUNK_BYTES} bytes"
                ));
            }
            let chunk_end = chunk
                .chunk_index
                .checked_add(1)
                .and_then(|count| count.checked_mul(LGFX_CHUNK_BYTES as u64))
                .ok_or_else(|| Error::ResourceExhausted("LGFX chunk offset overflows".into()))?;
            if chunk_end > self.target_file_bytes {
                return invalid("chunk lies outside the target file");
            }
            chunk_bytes = chunk_bytes
                .checked_add(chunk.after_image.len())
                .ok_or_else(|| Error::ResourceExhausted("LGFX chunk bytes overflow".into()))?;
            if chunk_bytes > MAX_LGFX_V1_BYTES {
                return exhausted("LGFX chunk images exceed the envelope bound");
            }
            previous = Some(chunk.chunk_index);
        }

        let base_chunks = self.base_file_bytes / LGFX_CHUNK_BYTES as u64;
        let target_chunks = self.target_file_bytes / LGFX_CHUNK_BYTES as u64;
        if target_chunks > base_chunks {
            let first_new = self
                .chunks
                .partition_point(|chunk| chunk.chunk_index < base_chunks);
            let new_chunks = &self.chunks[first_new..];
            let required = usize::try_from(target_chunks - base_chunks)
                .map_err(|_| Error::ResourceExhausted("LGFX growth count overflows".into()))?;
            if new_chunks.len() != required
                || new_chunks
                    .iter()
                    .enumerate()
                    .any(|(offset, chunk)| chunk.chunk_index != base_chunks + offset as u64)
            {
                return invalid("growth must include every newly allocated chunk");
            }
        }
        if self.chunks_digest != lgfx_chunks_digest(&self.chunks) {
            return invalid("chunk digest does not match the after-images");
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let body = postcard::to_allocvec(self)
            .map_err(|error| Error::InvalidEntry(format!("LGFX encode failed: {error}")))?;
        let encoded_len = LGFX_V1_MAGIC
            .len()
            .checked_add(body.len())
            .ok_or_else(|| Error::ResourceExhausted("LGFX encoded length overflows".into()))?;
        if encoded_len > MAX_LGFX_V1_BYTES {
            return exhausted(format!("LGFX envelope exceeds {MAX_LGFX_V1_BYTES} bytes"));
        }
        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(LGFX_V1_MAGIC);
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_LGFX_V1_BYTES {
            return exhausted(format!("LGFX envelope exceeds {MAX_LGFX_V1_BYTES} bytes"));
        }
        let Some(body) = bytes.strip_prefix(LGFX_V1_MAGIC) else {
            return invalid("wrong magic or version");
        };
        if body.is_empty() {
            return invalid("body is empty");
        }
        let effect: Self = postcard::from_bytes(body)
            .map_err(|error| Error::InvalidEntry(format!("LGFX decode failed: {error}")))?;
        effect.validate()?;
        let canonical = postcard::to_allocvec(&effect)
            .map_err(|error| Error::InvalidEntry(format!("LGFX re-encode failed: {error}")))?;
        if canonical.as_slice() != body {
            return invalid("body is not canonically encoded");
        }
        Ok(effect)
    }
}

pub fn lgfx_chunks_digest(chunks: &[LadybugFileChunkV1]) -> LogHash {
    let mut hasher = Sha256::new();
    hasher.update(CHUNK_DIGEST_DOMAIN);
    for chunk in chunks {
        hasher.update(chunk.chunk_index.to_be_bytes());
        hasher.update(&chunk.after_image);
    }
    LogHash::from_bytes(hasher.finalize().into())
}

/// Produces sorted final chunk images by comparing two closed Ladybug files.
pub fn diff_closed_ladybug_files(
    base_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
) -> Result<Vec<LadybugFileChunkV1>> {
    let base_path = base_path.as_ref();
    let target_path = target_path.as_ref();
    require_clean(base_path)?;
    require_clean(target_path)?;
    let base_len = file_length(base_path)?;
    let target_len = file_length(target_path)?;
    validate_file_length("base", base_len)?;
    validate_file_length("target", target_len)?;

    let mut base = File::open(base_path).map_err(io_error)?;
    let mut target = File::open(target_path).map_err(io_error)?;
    let base_chunks = base_len / LGFX_CHUNK_BYTES as u64;
    let target_chunks = target_len / LGFX_CHUNK_BYTES as u64;
    let mut chunks = Vec::new();
    let mut captured_bytes = 0usize;
    for chunk_index in 0..target_chunks {
        let mut target_chunk = vec![0; LGFX_CHUNK_BYTES];
        target.read_exact(&mut target_chunk).map_err(io_error)?;
        let changed = if chunk_index < base_chunks {
            let mut base_chunk = [0; LGFX_CHUNK_BYTES];
            base.read_exact(&mut base_chunk).map_err(io_error)?;
            base_chunk.as_slice() != target_chunk
        } else {
            true
        };
        if changed {
            captured_bytes = captured_bytes
                .checked_add(target_chunk.len())
                .ok_or_else(|| Error::ResourceExhausted("LGFX diff bytes overflow".into()))?;
            if captured_bytes > MAX_LGFX_V1_BYTES {
                return exhausted("LGFX changed chunks exceed the envelope bound");
            }
            chunks.push(LadybugFileChunkV1 {
                chunk_index,
                after_image: target_chunk,
            });
        }
    }
    Ok(chunks)
}

/// Applies an LGFX effect only through a temporary clone of its exact base.
pub fn apply_lgfx_to_exact_base(
    base_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    effect: &LadybugFileEffectV1,
) -> Result<LogHash> {
    effect.validate()?;
    let base_path = base_path.as_ref();
    let target_path = target_path.as_ref();
    if base_path == target_path {
        return invalid("base and target paths must differ");
    }
    require_clean(base_path)?;
    if file_length(base_path)? != effect.base_file_bytes
        || file_digest(base_path)? != effect.base_db_digest
    {
        return invalid("base file does not match the effect identity");
    }
    require_clean(target_path)?;
    if target_path.exists() {
        return invalid("target database already exists");
    }
    ensure_parent(target_path)?;
    let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent).map_err(io_error)?;
    let mut base = File::open(base_path).map_err(io_error)?;
    std::io::copy(&mut base, &mut temporary).map_err(io_error)?;
    for chunk in &effect.chunks {
        temporary
            .seek(SeekFrom::Start(chunk.chunk_index * LGFX_CHUNK_BYTES as u64))
            .map_err(io_error)?;
        temporary.write_all(&chunk.after_image).map_err(io_error)?;
    }
    temporary
        .as_file_mut()
        .set_len(effect.target_file_bytes)
        .map_err(io_error)?;
    temporary.as_file().sync_all().map_err(io_error)?;
    let target_digest = file_digest(temporary.path())?;
    if target_digest != effect.target_db_digest {
        return invalid("applied file digest does not match the target");
    }
    temporary.persist_noclobber(target_path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidEntry("LGFX target already exists".into())
        } else {
            io_error(error.error)
        }
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)?;
    Ok(target_digest)
}

/// Native WAL capture result retained only for recovery feasibility auditing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedNativeLadybugWal {
    pub target_db_digest: LogHash,
    pub wal_digest: LogHash,
    pub wal_payload: Vec<u8>,
}

/// Audit-only: capture a committed native WAL before explicitly checkpointing.
pub fn capture_graph_entry_native_wal(
    base_path: impl AsRef<Path>,
    staging_path: impl AsRef<Path>,
    cluster_id: &str,
    node_id: &str,
    epoch: u64,
    config_id: u64,
    entry: &LogEntry,
) -> Result<CapturedNativeLadybugWal> {
    let base_path = base_path.as_ref();
    let staging_path = staging_path.as_ref();
    let base_digest = clone_clean_base(base_path, staging_path)?;
    let state = open_feasibility_state(staging_path, cluster_id, node_id, epoch, config_id)?;
    state.apply_entry(entry)?;
    if file_digest(staging_path)? != base_digest {
        return invalid("staging data file changed before native WAL capture");
    }
    for suffix in [".wal.checkpoint", ".shadow", ".tmp"] {
        if ladybug_sidecar(staging_path, suffix).exists() {
            return invalid(format!("staging created forbidden sidecar {suffix}"));
        }
    }
    let wal_payload = fs::read(ladybug_sidecar(staging_path, ".wal")).map_err(io_error)?;
    if wal_payload.is_empty() {
        return invalid("staging did not produce a committed native WAL");
    }
    let wal_digest = LogHash::digest(&[&wal_payload]);
    {
        let guard = state.read_database()?;
        let database = guard.as_ref().ok_or(Error::Closed)?;
        let connection = Connection::new(database).map_err(ladybug_error)?;
        connection.query("CHECKPOINT").map_err(ladybug_error)?;
    }
    drop(state);
    require_clean(staging_path)?;
    Ok(CapturedNativeLadybugWal {
        target_db_digest: file_digest(staging_path)?,
        wal_digest,
        wal_payload,
    })
}

/// Audit-only: install a captured native WAL, reopen for recovery, and checkpoint.
pub fn replay_native_ladybug_wal(
    base_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    wal_payload: &[u8],
) -> Result<()> {
    if wal_payload.is_empty() {
        return invalid("native WAL payload is empty");
    }
    let target_path = target_path.as_ref();
    clone_clean_base(base_path.as_ref(), target_path)?;
    let wal_path = ladybug_sidecar(target_path, ".wal");
    let parent = wal_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent).map_err(io_error)?;
    temporary.write_all(wal_payload).map_err(io_error)?;
    temporary.as_file().sync_all().map_err(io_error)?;
    temporary.persist_noclobber(&wal_path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            Error::InvalidEntry("native WAL target already exists".into())
        } else {
            io_error(error.error)
        }
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)?;
    let database = Database::new(target_path, ladybug_system_config().auto_checkpoint(false))
        .map_err(ladybug_error)?;
    let connection = Connection::new(&database).map_err(ladybug_error)?;
    connection
        .query("CALL force_checkpoint_on_close=false")
        .map_err(ladybug_error)?;
    connection.query("CHECKPOINT").map_err(ladybug_error)?;
    drop(connection);
    drop(database);
    require_clean(target_path)
}

/// Opens a clean file for deterministic readback without checkpoint-on-close.
pub fn open_lgfx_readback(
    path: impl AsRef<Path>,
    cluster_id: &str,
    node_id: &str,
    epoch: u64,
    config_id: u64,
) -> Result<LadybugStateMachine> {
    let path = path.as_ref();
    require_clean(path)?;
    open_feasibility_state(path, cluster_id, node_id, epoch, config_id)
}

fn open_feasibility_state(
    path: &Path,
    cluster_id: &str,
    node_id: &str,
    epoch: u64,
    config_id: u64,
) -> Result<LadybugStateMachine> {
    let identity = Identity {
        cluster_id: cluster_id.into(),
        node_id: node_id.into(),
        epoch,
        config_id,
    };
    let database = Database::new(path, ladybug_system_config().auto_checkpoint(false))
        .map_err(ladybug_error)?;
    let setting = Connection::new(&database).map_err(ladybug_error)?;
    setting
        .query("CALL force_checkpoint_on_close=false")
        .map_err(ladybug_error)?;
    drop(setting);
    initialize_or_validate(&database, &identity)?;
    Ok(LadybugStateMachine {
        path: path.to_path_buf(),
        identity,
        database: RwLock::new(Some(database)),
        writer: Mutex::new(()),
    })
}

fn clone_clean_base(base_path: &Path, target_path: &Path) -> Result<LogHash> {
    if base_path == target_path {
        return invalid("base and target paths must differ");
    }
    require_clean(base_path)?;
    if !base_path.is_file() {
        return invalid("base database does not exist");
    }
    require_clean(target_path)?;
    ensure_parent(target_path)?;
    let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent).map_err(io_error)?;
    let mut base = File::open(base_path).map_err(io_error)?;
    std::io::copy(&mut base, temporary.as_file_mut()).map_err(io_error)?;
    temporary.as_file().sync_all().map_err(io_error)?;
    let cloned_digest = file_digest(temporary.path())?;
    let persisted = match temporary.persist_noclobber(target_path) {
        Ok(file) => file,
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            drop(error.file);
            return invalid("target database already exists");
        }
        Err(error) => {
            let tempfile::PersistError { error, file } = error;
            drop(file);
            return Err(io_error(error));
        }
    };
    persisted.sync_all().map_err(io_error)?;
    drop(persisted);
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(io_error)?;
    Ok(cloned_digest)
}

fn validate_file_length(name: &str, length: u64) -> Result<()> {
    if length == 0 || !length.is_multiple_of(LGFX_CHUNK_BYTES as u64) {
        return invalid(format!(
            "{name} file size must be a non-zero {LGFX_CHUNK_BYTES}-byte multiple"
        ));
    }
    Ok(())
}

fn require_clean(path: &Path) -> Result<()> {
    for sidecar in ladybug_sidecars(path) {
        match fs::symlink_metadata(&sidecar) {
            Ok(_) => {
                return invalid(format!("Ladybug sidecar exists: {}", sidecar.display()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Io(format!(
                    "cannot inspect Ladybug sidecar {}: {error}",
                    sidecar.display()
                )));
            }
        }
    }
    Ok(())
}

fn file_length(path: &Path) -> Result<u64> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(io_error)
}

fn file_digest(path: &Path) -> Result<LogHash> {
    let mut file = File::open(path).map_err(io_error)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(LogHash::from_bytes(hasher.finalize().into()))
}

fn validate_nonempty_bounded(name: &str, value: &str, maximum: usize) -> Result<()> {
    if value.is_empty() {
        return invalid(format!("{name} is empty"));
    }
    if value.len() > maximum {
        return invalid(format!("{name} exceeds {maximum} bytes"));
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidEntry(format!(
        "invalid LGFX v1: {}",
        message.into()
    )))
}

fn exhausted<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::ResourceExhausted(message.into()))
}
