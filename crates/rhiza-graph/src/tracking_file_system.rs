use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions as NativeOpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use lbug::{FileHandle, FileSystem, FileSystemError, FileSystemErrorKind, LockMode, OpenOptions};

use crate::LGFX_CHUNK_BYTES;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct DirtySnapshot {
    pub(crate) chunk_indices: BTreeSet<u64>,
    pub(crate) truncated: bool,
    pub(crate) unsafe_full_diff: bool,
}

impl DirtySnapshot {
    pub(crate) fn is_empty(&self) -> bool {
        self.chunk_indices.is_empty() && !self.truncated
    }

    pub(crate) fn needs_full_diff(&self, target_changed: bool) -> bool {
        self.truncated || self.unsafe_full_diff || (self.is_empty() && target_changed)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IoSnapshot {
    pub(crate) primary_opens: u64,
    pub(crate) sidecar_opens: u64,
    pub(crate) primary_writes: u64,
    pub(crate) sidecar_writes: u64,
    pub(crate) syncs: u64,
}

#[derive(Default)]
struct TrackerState {
    tracking: bool,
    dirty: DirtySnapshot,
    #[cfg(test)]
    io: IoSnapshot,
}

#[derive(Clone)]
pub(crate) struct DirtyTracker {
    primary: Arc<PathBuf>,
    state: Arc<Mutex<TrackerState>>,
}

impl DirtyTracker {
    pub(crate) fn reset(&self) {
        let mut state = lock(&self.state);
        state.tracking = true;
        state.dirty = DirtySnapshot::default();
    }

    pub(crate) fn dirty_snapshot(&self) -> DirtySnapshot {
        lock(&self.state).dirty.clone()
    }

    #[cfg(test)]
    pub(crate) fn io_snapshot(&self) -> IoSnapshot {
        lock(&self.state).io
    }

    fn is_primary(&self, path: &str) -> bool {
        Path::new(path) == self.primary.as_path()
    }

    #[cfg(test)]
    fn is_sidecar(&self, path: &str) -> bool {
        let primary = self.primary.to_string_lossy();
        path.strip_prefix(primary.as_ref())
            .is_some_and(|suffix| suffix.starts_with('.'))
    }

    #[cfg(test)]
    fn record_open(&self, path: &str) {
        let mut state = lock(&self.state);
        if self.is_primary(path) {
            state.io.primary_opens += 1;
        } else if self.is_sidecar(path) {
            state.io.sidecar_opens += 1;
        }
    }

    fn record_write(&self, path: &str, offset: u64, len: usize) {
        if !self.is_primary(path) {
            #[cfg(test)]
            if self.is_sidecar(path) {
                lock(&self.state).io.sidecar_writes += 1;
            }
            return;
        }
        let mut state = lock(&self.state);
        #[cfg(test)]
        {
            state.io.primary_writes += 1;
        }
        if state.tracking {
            mark_range(&mut state.dirty, offset, len);
        }
    }

    fn record_truncate(&self, path: &str) {
        if !self.is_primary(path) {
            return;
        }
        let mut state = lock(&self.state);
        if state.tracking {
            state.dirty.truncated = true;
        }
    }

    #[cfg(test)]
    fn record_sync(&self) {
        lock(&self.state).io.syncs += 1;
    }

    fn mark_unsafe_if_primary(&self, path: &str) {
        if !self.is_primary(path) {
            return;
        }
        let mut state = lock(&self.state);
        if state.tracking {
            state.dirty.unsafe_full_diff = true;
        }
    }
}

pub(crate) struct TrackingFileSystem {
    tracker: DirtyTracker,
}

impl TrackingFileSystem {
    pub(crate) fn new(primary: impl AsRef<Path>) -> (Self, DirtyTracker) {
        let tracker = DirtyTracker {
            primary: Arc::new(primary.as_ref().to_path_buf()),
            state: Arc::new(Mutex::new(TrackerState::default())),
        };
        (
            Self {
                tracker: tracker.clone(),
            },
            tracker,
        )
    }
}

impl FileSystem for TrackingFileSystem {
    fn open(
        &self,
        path: &str,
        options: OpenOptions,
    ) -> Result<Box<dyn FileHandle>, FileSystemError> {
        if options.create_or_truncate {
            self.tracker.mark_unsafe_if_primary(path);
        }
        let file = NativeOpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create || options.create_or_truncate)
            .truncate(options.create_or_truncate)
            .open(path)
            .map_err(|error| io_error("open", error))?;
        acquire_lock(&file, options.lock)?;
        let cursor = file
            .try_clone()
            .map_err(|error| io_error("clone file handle", error))?;
        #[cfg(test)]
        self.tracker.record_open(path);
        Ok(Box::new(TrackingFileHandle {
            file,
            cursor: Mutex::new(cursor),
            lock: options.lock,
            path: path.to_owned(),
            tracker: self.tracker.clone(),
        }))
    }

    fn exists(&self, path: &str) -> Result<bool, FileSystemError> {
        match fs::metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_error("inspect path", error)),
        }
    }

    fn create_dir(&self, path: &str) -> Result<(), FileSystemError> {
        fs::create_dir(path).map_err(|error| io_error("create directory", error))
    }

    fn remove_file_if_exists(&self, path: &str) -> Result<(), FileSystemError> {
        self.tracker.mark_unsafe_if_primary(path);
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("remove file", error)),
        }
    }

    fn overwrite(&self, from: &str, to: &str) -> Result<(), FileSystemError> {
        self.tracker.mark_unsafe_if_primary(to);
        if !self.exists(from)? || !self.exists(to)? {
            return Ok(());
        }
        fs::copy(from, to)
            .map(|_| ())
            .map_err(|error| io_error("overwrite file", error))
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), FileSystemError> {
        self.tracker.mark_unsafe_if_primary(from);
        self.tracker.mark_unsafe_if_primary(to);
        fs::rename(from, to).map_err(|error| io_error("rename file", error))
    }

    fn copy(&self, from: &str, to: &str) -> Result<(), FileSystemError> {
        self.tracker.mark_unsafe_if_primary(to);
        if !self.exists(from)? {
            return Ok(());
        }
        if self.exists(to)? {
            return Err(FileSystemError::new(
                FileSystemErrorKind::AlreadyExists,
                format!("copy destination already exists: {to}"),
            ));
        }
        fs::copy(from, to)
            .map(|_| ())
            .map_err(|error| io_error("copy file", error))
    }

    fn glob(&self, path: &str) -> Result<Vec<String>, FileSystemError> {
        Ok(if self.exists(path)? {
            vec![path.to_owned()]
        } else {
            Vec::new()
        })
    }

    fn expand_path(&self, path: &str) -> Result<String, FileSystemError> {
        Ok(path.to_owned())
    }
}

struct TrackingFileHandle {
    file: File,
    cursor: Mutex<File>,
    lock: LockMode,
    path: String,
    tracker: DirtyTracker,
}

impl Drop for TrackingFileHandle {
    fn drop(&mut self) {
        if self.lock != LockMode::None {
            let _ = File::unlock(&self.file);
        }
    }
}

impl FileHandle for TrackingFileHandle {
    fn read_at(&self, offset: u64, dst: &mut [u8]) -> Result<(), FileSystemError> {
        native_read_at(&self.file, offset, dst).map_err(|error| io_error("positional read", error))
    }

    fn write_at(&self, offset: u64, src: &[u8]) -> Result<(), FileSystemError> {
        native_write_at(&self.file, offset, src)
            .map_err(|error| io_error("positional write", error))?;
        self.tracker.record_write(&self.path, offset, src.len());
        Ok(())
    }

    fn read(&self, dst: &mut [u8]) -> Result<usize, FileSystemError> {
        lock(&self.cursor)
            .read(dst)
            .map_err(|error| io_error("sequential read", error))
    }

    fn seek(&self, position: SeekFrom) -> Result<u64, FileSystemError> {
        lock(&self.cursor)
            .seek(position)
            .map_err(|error| io_error("seek", error))
    }

    fn len(&self) -> Result<u64, FileSystemError> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| io_error("file length", error))
    }

    fn truncate(&self, len: u64) -> Result<(), FileSystemError> {
        self.file
            .set_len(len)
            .map_err(|error| io_error("truncate", error))?;
        self.tracker.record_truncate(&self.path);
        Ok(())
    }

    fn sync(&self) -> Result<(), FileSystemError> {
        self.file
            .sync_all()
            .map_err(|error| io_error("sync", error))?;
        #[cfg(test)]
        self.tracker.record_sync();
        Ok(())
    }
}

fn acquire_lock(file: &File, mode: LockMode) -> Result<(), FileSystemError> {
    let result = match mode {
        LockMode::None => return Ok(()),
        LockMode::Read => file.try_lock_shared(),
        LockMode::Write => file.try_lock(),
    };
    result.map_err(|error| io_error("acquire non-blocking file lock", error.into()))
}

fn mark_range(dirty: &mut DirtySnapshot, offset: u64, len: usize) {
    if len == 0 {
        return;
    }
    let Ok(len) = u64::try_from(len) else {
        dirty.unsafe_full_diff = true;
        return;
    };
    let Some(end) = offset.checked_add(len) else {
        dirty.unsafe_full_diff = true;
        return;
    };
    let chunk_bytes = LGFX_CHUNK_BYTES as u64;
    for chunk in offset / chunk_bytes..=(end - 1) / chunk_bytes {
        dirty.chunk_indices.insert(chunk);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn io_error(operation: &str, error: std::io::Error) -> FileSystemError {
    let kind = match error.kind() {
        std::io::ErrorKind::NotFound => FileSystemErrorKind::NotFound,
        std::io::ErrorKind::AlreadyExists => FileSystemErrorKind::AlreadyExists,
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::WouldBlock => {
            FileSystemErrorKind::PermissionDenied
        }
        std::io::ErrorKind::InvalidInput => FileSystemErrorKind::InvalidInput,
        std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof => {
            FileSystemErrorKind::Corruption
        }
        std::io::ErrorKind::Unsupported => FileSystemErrorKind::Unsupported,
        _ => FileSystemErrorKind::Other,
    };
    FileSystemError::new(kind, format!("{operation}: {error}"))
}

#[cfg(unix)]
fn native_read_at(file: &File, mut offset: u64, mut dst: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !dst.is_empty() {
        let read = file.read_at(dst, offset)?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        offset += read as u64;
        dst = &mut dst[read..];
    }
    Ok(())
}

#[cfg(windows)]
fn native_read_at(file: &File, mut offset: u64, mut dst: &mut [u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !dst.is_empty() {
        let read = file.seek_read(dst, offset)?;
        if read == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        offset += read as u64;
        dst = &mut dst[read..];
    }
    Ok(())
}

#[cfg(unix)]
fn native_write_at(file: &File, mut offset: u64, mut src: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !src.is_empty() {
        let written = file.write_at(src, offset)?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += written as u64;
        src = &src[written..];
    }
    Ok(())
}

#[cfg(windows)]
fn native_write_at(file: &File, mut offset: u64, mut src: &[u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !src.is_empty() {
        let written = file.seek_write(src, offset)?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += written as u64;
        src = &src[written..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TrackingFileSystem, LGFX_CHUNK_BYTES};
    use lbug::{Connection, Database, FileSystem, LockMode, OpenOptions, SystemConfig};
    use std::fs;

    #[test]
    fn tracker_coalesces_primary_writes_and_records_truncation_after_reset() {
        let directory = tempfile::tempdir().unwrap();
        let primary = directory.path().join("staging.lbug");
        fs::write(&primary, vec![0; LGFX_CHUNK_BYTES * 3]).unwrap();
        let (file_system, tracker) = TrackingFileSystem::new(&primary);
        tracker.reset();
        let handle = file_system
            .open(
                primary.to_str().unwrap(),
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

        handle.write_at(4095, &[1, 2]).unwrap();
        handle.write_at(8192, &[3]).unwrap();
        handle.truncate(8192).unwrap();

        let dirty = tracker.dirty_snapshot();
        assert_eq!(dirty.chunk_indices, [0, 1, 2].into());
        assert!(dirty.truncated);
        assert!(!dirty.unsafe_full_diff);
    }

    #[test]
    fn replacing_the_primary_path_requests_full_diff_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let primary = directory.path().join("staging.lbug");
        let replacement = directory.path().join("replacement.lbug");
        fs::write(&primary, b"old").unwrap();
        fs::write(&replacement, b"new").unwrap();
        let (file_system, tracker) = TrackingFileSystem::new(&primary);
        tracker.reset();

        file_system
            .overwrite(replacement.to_str().unwrap(), primary.to_str().unwrap())
            .unwrap();

        assert!(tracker.dirty_snapshot().needs_full_diff(false));
    }

    #[test]
    fn changed_target_without_tracked_writes_requests_full_diff_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let (_, tracker) = TrackingFileSystem::new(directory.path().join("staging.lbug"));
        tracker.reset();

        assert!(tracker.dirty_snapshot().needs_full_diff(true));
    }

    #[test]
    fn ladybug_staging_io_passes_through_the_tracking_file_system() {
        let directory = tempfile::tempdir().unwrap();
        let primary = directory.path().join("staging.lbug");
        let (file_system, tracker) = TrackingFileSystem::new(&primary);
        let database = Database::new_with_file_system(
            &primary,
            SystemConfig::default().auto_checkpoint(false),
            Box::new(file_system),
        )
        .unwrap();
        let connection = Connection::new(&database).unwrap();
        tracker.reset();
        connection
            .query("CREATE NODE TABLE T(id INT64 PRIMARY KEY)")
            .unwrap();
        connection.query("CREATE (:T {id: 1})").unwrap();
        connection.query("CHECKPOINT").unwrap();
        drop(connection);
        drop(database);

        let io = tracker.io_snapshot();
        assert!(io.primary_opens > 0);
        assert!(io.sidecar_opens > 0);
        assert!(io.primary_writes > 0);
        assert!(io.sidecar_writes > 0);
        assert!(io.syncs > 0);
        assert!(!tracker.dirty_snapshot().chunk_indices.is_empty());
    }
}
