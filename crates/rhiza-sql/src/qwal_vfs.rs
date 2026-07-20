//! A named, staging-only SQLite VFS which records candidate changed pages.
//!
//! The recorder is deliberately not a correctness boundary. Its output can
//! reduce the pages inspected by a closed-file diff, but callers must fall
//! back to that full diff whenever [`SealedQwalRecording::is_complete`] is
//! false. I/O failures and unexpected persistent databases fail the recording
//! closed instead of becoming fallback conditions.

use rusqlite::ffi;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{c_char, c_int, c_void, CStr};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub const QWAL_RECORDING_VFS_NAME: &str = "rhiza_qwal_recording_v1";

const VFS_NAME_NUL: &[u8] = b"rhiza_qwal_recording_v1\0";
// SQLite guarantees the allocation passed to xOpen is aligned for
// sqlite3_file. Platform VFS implementations are required to fit that ABI as
// well, so the embedded underlying handle uses the same alignment.
const FILE_ALIGNMENT: usize = std::mem::align_of::<ffi::sqlite3_file>();
const MAX_RECORDED_CANDIDATE_PAGES: u64 = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QwalVfsError(String);

impl QwalVfsError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for QwalVfsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for QwalVfsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRange {
    pub first_page: u64,
    pub last_page: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedQwalRecording {
    pub candidate_pages: Vec<u64>,
    pub candidate_page_ranges: Vec<PageRange>,
    pub main_sync_observed: bool,
    pub wal_opened: bool,
    pub wal_write_observed: bool,
    pub wal_truncate_observed: bool,
    pub wal_sync_observed: bool,
    pub commit_observed: bool,
    pub checkpoint_succeeded: bool,
    complete: bool,
}

impl SealedQwalRecording {
    /// Whether the candidate set is eligible to narrow a closed-file diff.
    /// Durability syncs are deliberately not required: this VFS observes a
    /// disposable speculative file whose bytes are consumed before ACK.
    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

#[derive(Debug)]
struct RecordingState {
    expected_main: PathBuf,
    expected_wal: PathBuf,
    page_size: u64,
    candidate_pages: BTreeSet<u64>,
    opening_main_handles: usize,
    opening_wal_handles: usize,
    open_main_handles: usize,
    open_wal_handles: usize,
    main_sync_observed: bool,
    wal_opened: bool,
    wal_write_observed: bool,
    wal_truncate_observed: bool,
    wal_sync_observed: bool,
    commit_observed: bool,
    checkpoint_succeeded: bool,
    incomplete_reason: Option<String>,
    fatal_error: Option<String>,
}

impl RecordingState {
    fn mark_fatal(&mut self, message: impl Into<String>) {
        if self.fatal_error.is_none() {
            self.fatal_error = Some(message.into());
        }
    }

    fn observe_main_write(&mut self, offset: i64, amount: c_int) -> Result<(), ()> {
        let bounds =
            page_bounds_for_write_range(offset, amount, self.page_size).ok_or_else(|| {
                self.mark_fatal("invalid or overflowing main-database write range");
            })?;
        if let Some((first, last)) = bounds {
            self.insert_candidate_range(first, last);
        }
        Ok(())
    }

    fn observe_truncate(&mut self, previous_size: i64, size: i64) -> Result<(), ()> {
        let previous_size = u64::try_from(previous_size).map_err(|_| {
            self.mark_fatal("negative pre-truncate main-database size");
        })?;
        let size = u64::try_from(size).map_err(|_| {
            self.mark_fatal("negative main-database truncate size");
        })?;
        if size % self.page_size != 0 {
            self.mark_fatal("main-database truncate was not page aligned");
            return Err(());
        }
        let last_page = size / self.page_size;
        if size < previous_size {
            self.candidate_pages.retain(|page| *page <= last_page);
        } else if size > previous_size {
            let first_new_page = previous_size / self.page_size + 1;
            self.insert_candidate_range(first_new_page, last_page);
        }
        Ok(())
    }

    fn insert_candidate_range(&mut self, first: u64, last: u64) {
        let count = last.saturating_sub(first).saturating_add(1);
        if count > MAX_RECORDED_CANDIDATE_PAGES
            || self.candidate_pages.len() as u64 + count > MAX_RECORDED_CANDIDATE_PAGES
        {
            self.mark_incomplete("recorded candidate-page set exceeded its bounded capacity");
            return;
        }
        self.candidate_pages.extend(first..=last);
    }

    fn mark_incomplete(&mut self, message: impl Into<String>) {
        if self.incomplete_reason.is_none() {
            self.incomplete_reason = Some(message.into());
        }
    }
}

type SharedRecording = Arc<Mutex<RecordingState>>;
type ActiveRecordings = Mutex<BTreeMap<PathBuf, Weak<Mutex<RecordingState>>>>;

fn active_recordings() -> &'static ActiveRecordings {
    static ACTIVE: OnceLock<ActiveRecordings> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// An armed recording scope for one staging database path.
///
/// The scope must outlive every connection opened with
/// [`QWAL_RECORDING_VFS_NAME`]. Dropping an unsealed scope simply disarms it.
pub struct QwalRecordingSession {
    path: PathBuf,
    state: SharedRecording,
    sealed: bool,
}

impl QwalRecordingSession {
    pub fn begin(path: impl AsRef<Path>, page_size: u32) -> Result<Self, QwalVfsError> {
        ensure_qwal_recording_vfs_registered()?;
        if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(QwalVfsError::new(
                "recording page size must be a SQLite power of two from 512 through 65536",
            ));
        }
        let path = normalize_path(path.as_ref())?;
        let actual_page_size = read_sqlite_page_size(&path)?;
        if actual_page_size != page_size {
            return Err(QwalVfsError::new(format!(
                "recording page size {page_size} does not match staging database page size {actual_page_size}"
            )));
        }
        let expected_wal = wal_path(&path);
        let state = Arc::new(Mutex::new(RecordingState {
            expected_main: path.clone(),
            expected_wal,
            page_size: u64::from(page_size),
            candidate_pages: BTreeSet::new(),
            opening_main_handles: 0,
            opening_wal_handles: 0,
            open_main_handles: 0,
            open_wal_handles: 0,
            main_sync_observed: false,
            wal_opened: false,
            wal_write_observed: false,
            wal_truncate_observed: false,
            wal_sync_observed: false,
            commit_observed: false,
            checkpoint_succeeded: false,
            incomplete_reason: None,
            fatal_error: None,
        }));

        let mut active = active_recordings()
            .lock()
            .map_err(|_| QwalVfsError::new("recording registry mutex is poisoned"))?;
        active.retain(|_, recording| recording.strong_count() > 0);
        if active.contains_key(&path) {
            return Err(QwalVfsError::new(format!(
                "a QWAL recording is already active for {}",
                path.display()
            )));
        }
        active.insert(path.clone(), Arc::downgrade(&state));
        drop(active);

        Ok(Self {
            path,
            state,
            sealed: false,
        })
    }

    /// Records the successful SQLite commit (normally from the WAL hook or the
    /// successful transaction API return).
    pub fn mark_commit_observed(&self) -> Result<(), QwalVfsError> {
        self.with_state(|state| state.commit_observed = true)
    }

    /// Records that an explicit checkpoint completed successfully.
    pub fn mark_checkpoint_succeeded(&self) -> Result<(), QwalVfsError> {
        self.with_state(|state| state.checkpoint_succeeded = true)
    }

    fn with_state(&self, update: impl FnOnce(&mut RecordingState)) -> Result<(), QwalVfsError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| QwalVfsError::new("recording state mutex is poisoned"))?;
        update(&mut state);
        Ok(())
    }

    /// Seals the recorder after all staging connections have closed.
    pub fn seal(mut self) -> Result<SealedQwalRecording, QwalVfsError> {
        self.remove_registration()?;
        let state = self
            .state
            .lock()
            .map_err(|_| QwalVfsError::new("recording state mutex is poisoned"))?;
        if let Some(error) = &state.fatal_error {
            return Err(QwalVfsError::new(error.clone()));
        }
        if state.opening_main_handles != 0
            || state.opening_wal_handles != 0
            || state.open_main_handles != 0
            || state.open_wal_handles != 0
        {
            return Err(QwalVfsError::new(
                "cannot seal QWAL recording while SQLite files remain open",
            ));
        }

        let candidate_pages: Vec<_> = state.candidate_pages.iter().copied().collect();
        let candidate_page_ranges = collapse_page_ranges(&candidate_pages);
        let complete = state.incomplete_reason.is_none()
            && state.commit_observed
            && state.checkpoint_succeeded;
        Ok(SealedQwalRecording {
            candidate_pages,
            candidate_page_ranges,
            main_sync_observed: state.main_sync_observed,
            wal_opened: state.wal_opened,
            wal_write_observed: state.wal_write_observed,
            wal_truncate_observed: state.wal_truncate_observed,
            wal_sync_observed: state.wal_sync_observed,
            commit_observed: state.commit_observed,
            checkpoint_succeeded: state.checkpoint_succeeded,
            complete,
        })
    }

    fn remove_registration(&mut self) -> Result<(), QwalVfsError> {
        let mut active = active_recordings()
            .lock()
            .map_err(|_| QwalVfsError::new("recording registry mutex is poisoned"))?;
        active.remove(&self.path);
        self.sealed = true;
        Ok(())
    }
}

fn read_sqlite_page_size(path: &Path) -> Result<u32, QwalVfsError> {
    let mut file = File::open(path).map_err(|error| {
        QwalVfsError::new(format!(
            "open existing staging database {}: {error}",
            path.display()
        ))
    })?;
    let mut header = [0_u8; 100];
    file.read_exact(&mut header).map_err(|error| {
        QwalVfsError::new(format!(
            "read SQLite header from existing staging database {}: {error}",
            path.display()
        ))
    })?;
    if &header[..16] != b"SQLite format 3\0" {
        return Err(QwalVfsError::new(format!(
            "staging database {} has an invalid SQLite header",
            path.display()
        )));
    }
    let encoded = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded == 1 {
        65_536
    } else {
        u32::from(encoded)
    };
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(QwalVfsError::new(format!(
            "staging database {} declares invalid page size {page_size}",
            path.display()
        )));
    }
    let bytes = file
        .metadata()
        .map_err(|error| QwalVfsError::new(format!("read staging metadata: {error}")))?
        .len();
    if bytes % u64::from(page_size) != 0 {
        return Err(QwalVfsError::new(format!(
            "staging database byte length {bytes} is not page aligned"
        )));
    }
    Ok(page_size)
}

impl Drop for QwalRecordingSession {
    fn drop(&mut self) {
        if !self.sealed {
            if let Ok(mut active) = active_recordings().lock() {
                active.remove(&self.path);
            }
        }
    }
}

fn normalize_path(path: &Path) -> Result<PathBuf, QwalVfsError> {
    if let Ok(path) = path.canonicalize() {
        return Ok(path);
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| QwalVfsError::new(format!("resolve current directory: {error}")))?
            .join(path)
    };
    let parent = absolute
        .parent()
        .ok_or_else(|| QwalVfsError::new("staging database has no parent directory"))?;
    let file_name = absolute
        .file_name()
        .ok_or_else(|| QwalVfsError::new("staging database has no file name"))?;
    let parent = parent
        .canonicalize()
        .map_err(|error| QwalVfsError::new(format!("canonicalize staging directory: {error}")))?;
    Ok(parent.join(file_name))
}

fn wal_path(main: &Path) -> PathBuf {
    let mut value = main.as_os_str().to_os_string();
    value.push("-wal");
    PathBuf::from(value)
}

fn page_bounds_for_write_range(
    offset: i64,
    amount: c_int,
    page_size: u64,
) -> Option<Option<(u64, u64)>> {
    let offset = u64::try_from(offset).ok()?;
    let amount = u64::try_from(amount).ok()?;
    if page_size == 0 {
        return None;
    }
    if amount == 0 {
        return Some(None);
    }
    let end = offset.checked_add(amount)?;
    let first = offset / page_size + 1;
    let last = end.checked_sub(1)? / page_size + 1;
    Some(Some((first, last)))
}

#[cfg(test)]
fn pages_for_write_range(offset: i64, amount: c_int, page_size: u64) -> Option<Vec<u64>> {
    page_bounds_for_write_range(offset, amount, page_size)
        .map(|bounds| bounds.map_or_else(Vec::new, |(first, last)| (first..=last).collect()))
}

fn collapse_page_ranges(pages: &[u64]) -> Vec<PageRange> {
    let mut ranges = Vec::new();
    let Some(&first) = pages.first() else {
        return ranges;
    };
    let mut range_first = first;
    let mut previous = first;
    for &page in &pages[1..] {
        if previous.checked_add(1) == Some(page) {
            previous = page;
        } else {
            ranges.push(PageRange {
                first_page: range_first,
                last_page: previous,
            });
            range_first = page;
            previous = page;
        }
    }
    ranges.push(PageRange {
        first_page: range_first,
        last_page: previous,
    });
    ranges
}

struct AppData {
    underlying: *mut ffi::sqlite3_vfs,
}

// SQLite owns and invokes this process-global registration on arbitrary
// connection threads. The underlying VFS itself has the same SQLite lifetime.
unsafe impl Send for AppData {}
unsafe impl Sync for AppData {}

struct RegisteredVfs {
    _app: Box<AppData>,
    vfs: Box<ffi::sqlite3_vfs>,
}

unsafe impl Send for RegisteredVfs {}
unsafe impl Sync for RegisteredVfs {}

fn registration() -> &'static OnceLock<Result<RegisteredVfs, QwalVfsError>> {
    static REGISTRATION: OnceLock<Result<RegisteredVfs, QwalVfsError>> = OnceLock::new();
    &REGISTRATION
}

/// Registers the opt-in recorder without changing SQLite's default VFS.
///
/// Only bundled platform VFS names with the alignment and callback semantics
/// audited by this wrapper are accepted. Callers must treat an error as a
/// request to use the full closed-file diff path.
pub fn ensure_qwal_recording_vfs_registered() -> Result<&'static str, QwalVfsError> {
    let registered = registration().get_or_init(register_vfs);
    match registered {
        Ok(_) => Ok(QWAL_RECORDING_VFS_NAME),
        Err(error) => Err(error.clone()),
    }
}

fn register_vfs() -> Result<RegisteredVfs, QwalVfsError> {
    // SAFETY: SQLite returns a process-global VFS pointer which remains valid
    // while SQLite is initialized. The bundled SQLite is not shut down by this
    // crate.
    let underlying = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
    if underlying.is_null() {
        return Err(QwalVfsError::new("SQLite has no default VFS"));
    }
    let underlying_ref = unsafe { &*underlying };
    if underlying_ref.zName.is_null() {
        return Err(QwalVfsError::new("SQLite default VFS has no name"));
    }
    let underlying_name = unsafe { CStr::from_ptr(underlying_ref.zName) };
    if !supported_underlying_vfs(underlying_name) {
        return Err(QwalVfsError::new(format!(
            "SQLite default VFS {:?} is not audited for QWAL recording; use full-diff fallback",
            underlying_name.to_string_lossy()
        )));
    }
    // A foreign VFS with our private name is a hard collision.
    let existing = unsafe { ffi::sqlite3_vfs_find(VFS_NAME_NUL.as_ptr().cast()) };
    if !existing.is_null() {
        return Err(QwalVfsError::new(
            "QWAL recording VFS name is already registered",
        ));
    }

    let mut app = Box::new(AppData { underlying });
    let header_size = align_up(std::mem::size_of::<WrapperFile>(), FILE_ALIGNMENT)
        .ok_or_else(|| QwalVfsError::new("wrapper file header size overflow"))?;
    let os_file_size = header_size
        .checked_add(
            usize::try_from(underlying_ref.szOsFile)
                .map_err(|_| QwalVfsError::new("underlying VFS reported a negative file size"))?,
        )
        .ok_or_else(|| QwalVfsError::new("wrapper VFS file size overflow"))?;
    let os_file_size = c_int::try_from(os_file_size)
        .map_err(|_| QwalVfsError::new("wrapper VFS file size exceeds SQLite ABI"))?;

    let mut vfs = Box::new(ffi::sqlite3_vfs {
        iVersion: underlying_ref.iVersion.min(3),
        szOsFile: os_file_size,
        mxPathname: underlying_ref.mxPathname,
        pNext: ptr::null_mut(),
        zName: VFS_NAME_NUL.as_ptr().cast(),
        pAppData: (&mut *app as *mut AppData).cast(),
        xOpen: Some(vfs_open),
        xDelete: underlying_ref.xDelete.map(|_| vfs_delete as _),
        xAccess: underlying_ref.xAccess.map(|_| vfs_access as _),
        xFullPathname: underlying_ref.xFullPathname.map(|_| vfs_full_pathname as _),
        xDlOpen: underlying_ref.xDlOpen.map(|_| vfs_dl_open as _),
        xDlError: underlying_ref.xDlError.map(|_| vfs_dl_error as _),
        xDlSym: underlying_ref.xDlSym.map(|_| vfs_dl_sym as _),
        xDlClose: underlying_ref.xDlClose.map(|_| vfs_dl_close as _),
        xRandomness: underlying_ref.xRandomness.map(|_| vfs_randomness as _),
        xSleep: underlying_ref.xSleep.map(|_| vfs_sleep as _),
        xCurrentTime: underlying_ref.xCurrentTime.map(|_| vfs_current_time as _),
        xGetLastError: underlying_ref
            .xGetLastError
            .map(|_| vfs_get_last_error as _),
        xCurrentTimeInt64: underlying_ref
            .xCurrentTimeInt64
            .map(|_| vfs_current_time_int64 as _),
        xSetSystemCall: underlying_ref
            .xSetSystemCall
            .map(|_| vfs_set_system_call as _),
        xGetSystemCall: underlying_ref
            .xGetSystemCall
            .map(|_| vfs_get_system_call as _),
        xNextSystemCall: underlying_ref
            .xNextSystemCall
            .map(|_| vfs_next_system_call as _),
    });
    let rc = unsafe { ffi::sqlite3_vfs_register(&mut *vfs, 0) };
    if rc != ffi::SQLITE_OK {
        return Err(QwalVfsError::new(format!(
            "register QWAL recording VFS: SQLite error {rc}"
        )));
    }
    Ok(RegisteredVfs { _app: app, vfs })
}

fn supported_underlying_vfs(name: &CStr) -> bool {
    let name = name.to_bytes();
    #[cfg(unix)]
    {
        matches!(
            name,
            b"unix" | b"unix-none" | b"unix-dotfile" | b"unix-excl"
        )
    }
    #[cfg(windows)]
    {
        matches!(name, b"win32" | b"win32-longpath" | b"win32-none")
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = name;
        false
    }
}

impl Drop for RegisteredVfs {
    fn drop(&mut self) {
        // This value lives in a process-global OnceLock and is not normally
        // dropped. Keeping the implementation makes teardown ordering explicit.
        unsafe {
            let _ = ffi::sqlite3_vfs_unregister(&mut *self.vfs);
        }
    }
}

#[repr(C)]
struct WrapperFile {
    base: ffi::sqlite3_file,
    underlying_offset: usize,
    context: *mut FileContext,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileKind {
    Main,
    Wal,
    Other,
}

struct FileContext {
    kind: FileKind,
    recording: Option<SharedRecording>,
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    debug_assert!(alignment.is_power_of_two());
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
}

unsafe fn app_data(vfs: *mut ffi::sqlite3_vfs) -> &'static AppData {
    unsafe { &*((*vfs).pAppData.cast::<AppData>()) }
}

unsafe fn underlying_vfs(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    unsafe { app_data(vfs).underlying }
}

unsafe fn wrapper(file: *mut ffi::sqlite3_file) -> *mut WrapperFile {
    file.cast()
}

unsafe fn underlying_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    let wrapper = unsafe { &*wrapper(file) };
    unsafe { (file.cast::<u8>()).add(wrapper.underlying_offset).cast() }
}

unsafe fn context(file: *mut ffi::sqlite3_file) -> Option<&'static FileContext> {
    let context = unsafe { (*wrapper(file)).context };
    unsafe { context.as_ref() }
}

fn guard<T>(fallback: T, callback: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).unwrap_or(fallback)
}

fn name_path(name: *const c_char) -> Option<PathBuf> {
    if name.is_null() {
        return None;
    }
    let bytes = unsafe { CStr::from_ptr(name) }.to_bytes();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Some(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
    }
    #[cfg(not(unix))]
    {
        Some(PathBuf::from(String::from_utf8_lossy(bytes).as_ref()))
    }
}

struct OpenReservation {
    recording: SharedRecording,
    kind: FileKind,
    active: bool,
}

impl OpenReservation {
    fn commit(mut self) -> Result<SharedRecording, QwalVfsError> {
        let mut state = self
            .recording
            .lock()
            .map_err(|_| QwalVfsError::new("recording state mutex is poisoned"))?;
        match self.kind {
            FileKind::Main => {
                state.opening_main_handles = state.opening_main_handles.saturating_sub(1);
                state.open_main_handles = state
                    .open_main_handles
                    .checked_add(1)
                    .ok_or_else(|| QwalVfsError::new("main handle count overflow"))?;
            }
            FileKind::Wal => {
                state.opening_wal_handles = state.opening_wal_handles.saturating_sub(1);
                state.open_wal_handles = state
                    .open_wal_handles
                    .checked_add(1)
                    .ok_or_else(|| QwalVfsError::new("WAL handle count overflow"))?;
                state.wal_opened = true;
            }
            FileKind::Other => {}
        }
        self.active = false;
        drop(state);
        Ok(Arc::clone(&self.recording))
    }
}

impl Drop for OpenReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.recording.lock() {
            match self.kind {
                FileKind::Main => {
                    state.opening_main_handles = state.opening_main_handles.saturating_sub(1)
                }
                FileKind::Wal => {
                    state.opening_wal_handles = state.opening_wal_handles.saturating_sub(1)
                }
                FileKind::Other => {}
            }
        }
    }
}

fn reserve_recording(path: &Path, kind: FileKind) -> Result<Option<OpenReservation>, QwalVfsError> {
    let path = normalize_path(path)?;
    let mut active = active_recordings()
        .lock()
        .map_err(|_| QwalVfsError::new("recording registry mutex is poisoned"))?;
    active.retain(|_, recording| recording.strong_count() > 0);
    let recording = match kind {
        FileKind::Main => active.get(&path).and_then(Weak::upgrade),
        FileKind::Wal => active.values().find_map(|recording| {
            let recording = recording.upgrade()?;
            let matches = recording.lock().ok()?.expected_wal == path;
            matches.then_some(recording)
        }),
        FileKind::Other => None,
    };
    let Some(recording) = recording else {
        return Ok(None);
    };
    let mut state = recording
        .lock()
        .map_err(|_| QwalVfsError::new("recording state mutex is poisoned"))?;
    match kind {
        FileKind::Main => {
            state.opening_main_handles = state
                .opening_main_handles
                .checked_add(1)
                .ok_or_else(|| QwalVfsError::new("opening main handle count overflow"))?
        }
        FileKind::Wal => {
            state.opening_wal_handles = state
                .opening_wal_handles
                .checked_add(1)
                .ok_or_else(|| QwalVfsError::new("opening WAL handle count overflow"))?
        }
        FileKind::Other => return Ok(None),
    }
    drop(state);
    drop(active);
    Ok(Some(OpenReservation {
        recording,
        kind,
        active: true,
    }))
}

fn poison_recordings_for_path(path: Option<&Path>, message: String) {
    if let Ok(mut active) = active_recordings().lock() {
        active.retain(|_, recording| recording.strong_count() > 0);
        for recording in active.values().filter_map(Weak::upgrade) {
            if let Ok(mut recording) = recording.lock() {
                let same_scope = path.is_none()
                    || recording.expected_main.parent() == path.and_then(Path::parent);
                if same_scope {
                    recording.mark_fatal(message.clone());
                }
            }
        }
    }
}

unsafe extern "C" fn vfs_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    guard(ffi::SQLITE_CANTOPEN, || unsafe {
        let header = wrapper(file);
        let Some(offset) = align_up(std::mem::size_of::<WrapperFile>(), FILE_ALIGNMENT) else {
            return ffi::SQLITE_CANTOPEN;
        };
        ptr::write(
            header,
            WrapperFile {
                base: ffi::sqlite3_file {
                    pMethods: ptr::null(),
                },
                underlying_offset: offset,
                context: ptr::null_mut(),
            },
        );
        let underlying = underlying_file(file);
        let underlying_size = usize::try_from((*underlying_vfs(vfs)).szOsFile).unwrap_or(0);
        ptr::write_bytes(underlying.cast::<u8>(), 0, underlying_size);

        let kind = if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
            FileKind::Main
        } else if flags & ffi::SQLITE_OPEN_WAL != 0 {
            FileKind::Wal
        } else {
            FileKind::Other
        };
        let path = name_path(name);
        let normalized_path = path.as_deref().and_then(|path| normalize_path(path).ok());
        let mut reservation = match path.as_deref() {
            Some(path) => match reserve_recording(path, kind) {
                Ok(reservation) => reservation,
                Err(_) => return ffi::SQLITE_CANTOPEN,
            },
            None => None,
        };

        if kind == FileKind::Main
            && flags & ffi::SQLITE_OPEN_MEMORY == 0
            && (path.is_none() || reservation.is_none())
        {
            let display = path
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<null>".to_owned());
            poison_recordings_for_path(
                normalized_path.as_deref(),
                format!(
                    "unexpected persistent main database opened through recording VFS: {display}"
                ),
            );
            return ffi::SQLITE_CANTOPEN;
        }

        let open = match (*underlying_vfs(vfs)).xOpen {
            Some(open) => open,
            None => return ffi::SQLITE_CANTOPEN,
        };
        let rc = open(underlying_vfs(vfs), name, underlying, flags, out_flags);
        let mut underlying_guard = UnderlyingOpenGuard::new(underlying);
        if rc != ffi::SQLITE_OK {
            let close_rc = underlying_guard.close();
            if close_rc != ffi::SQLITE_OK {
                if let Some(reservation) = &reservation {
                    if let Ok(mut state) = reservation.recording.lock() {
                        state.mark_fatal(format!(
                            "partial underlying xClose failed with SQLite error {close_rc}"
                        ));
                    }
                }
            }
            (*header).base.pMethods = ptr::null();
            (*header).context = ptr::null_mut();
            return rc;
        }

        if (*underlying).pMethods.is_null() {
            let _ = underlying_guard.close();
            return ffi::SQLITE_CANTOPEN;
        }
        let methods = match (*(*underlying).pMethods).iVersion {
            version if version >= 3 => &IO_METHODS_V3,
            version if version >= 2 => &IO_METHODS_V2,
            _ => &IO_METHODS_V1,
        };
        let recording = match reservation.take() {
            Some(reservation) => match reservation.commit() {
                Ok(recording) => Some(recording),
                Err(_) => {
                    let _ = underlying_guard.close();
                    return ffi::SQLITE_CANTOPEN;
                }
            },
            None => None,
        };
        let context = Box::into_raw(Box::new(FileContext { kind, recording }));
        (*header).context = context;
        // Publish pMethods last. From this point SQLite may invoke xClose.
        (*header).base.pMethods = methods;
        underlying_guard.disarm();
        ffi::SQLITE_OK
    })
}

struct UnderlyingOpenGuard {
    file: *mut ffi::sqlite3_file,
    armed: bool,
}

impl UnderlyingOpenGuard {
    unsafe fn new(file: *mut ffi::sqlite3_file) -> Self {
        Self {
            file,
            armed: !file.is_null() && !unsafe { (*file).pMethods.is_null() },
        }
    }

    fn close(&mut self) -> c_int {
        if !self.armed {
            return ffi::SQLITE_OK;
        }
        self.armed = false;
        unsafe { close_underlying_file(self.file) }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnderlyingOpenGuard {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

unsafe fn close_underlying_file(file: *mut ffi::sqlite3_file) -> c_int {
    if file.is_null() || unsafe { (*file).pMethods.is_null() } {
        return ffi::SQLITE_OK;
    }
    let methods = unsafe { (*file).pMethods };
    let rc = match unsafe { (*methods).xClose } {
        Some(close) => unsafe { close(file) },
        None => ffi::SQLITE_IOERR_CLOSE,
    };
    unsafe { (*file).pMethods = ptr::null() };
    rc
}

macro_rules! io_delegate {
    ($file:expr, $method:ident, $fallback:expr $(, $arg:expr)*) => {{
        let underlying = unsafe { underlying_file($file) };
        if underlying.is_null() || unsafe { (*underlying).pMethods.is_null() } {
            $fallback
        } else {
            match unsafe { (*(*underlying).pMethods).$method } {
                Some(callback) => unsafe { callback(underlying $(, $arg)*) },
                None => $fallback,
            }
        }
    }};
}

unsafe extern "C" fn file_close(file: *mut ffi::sqlite3_file) -> c_int {
    guard(ffi::SQLITE_IOERR_CLOSE, || unsafe {
        let underlying = underlying_file(file);
        let rc = if !underlying.is_null() && !(*underlying).pMethods.is_null() {
            match (*(*underlying).pMethods).xClose {
                Some(close) => close(underlying),
                None => ffi::SQLITE_IOERR_CLOSE,
            }
        } else {
            ffi::SQLITE_OK
        };
        let context_ptr = (*wrapper(file)).context;
        if !context_ptr.is_null() {
            let context = Box::from_raw(context_ptr);
            if rc != ffi::SQLITE_OK {
                if let Some(recording) = &context.recording {
                    if let Ok(mut state) = recording.lock() {
                        state
                            .mark_fatal(format!("underlying xClose failed with SQLite error {rc}"));
                    }
                }
            }
            if let Some(recording) = &context.recording {
                if let Ok(mut state) = recording.lock() {
                    match context.kind {
                        FileKind::Main => {
                            state.open_main_handles = state.open_main_handles.saturating_sub(1)
                        }
                        FileKind::Wal => {
                            state.open_wal_handles = state.open_wal_handles.saturating_sub(1)
                        }
                        FileKind::Other => {}
                    }
                }
            }
        }
        (*wrapper(file)).context = ptr::null_mut();
        (*wrapper(file)).base.pMethods = ptr::null();
        rc
    })
}

unsafe extern "C" fn file_read(
    file: *mut ffi::sqlite3_file,
    buffer: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    guard(ffi::SQLITE_IOERR_READ, || {
        io_delegate!(file, xRead, ffi::SQLITE_IOERR_READ, buffer, amount, offset)
    })
}

#[allow(unused_unsafe)]
unsafe extern "C" fn file_write(
    file: *mut ffi::sqlite3_file,
    buffer: *const c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    guard(ffi::SQLITE_IOERR_WRITE, || unsafe {
        if offset < 0
            || amount < 0
            || u64::try_from(offset)
                .ok()
                .and_then(|v| v.checked_add(amount as u64))
                .is_none()
        {
            if let Some(context) = context(file) {
                if let Some(recording) = &context.recording {
                    if let Ok(mut state) = recording.lock() {
                        state.mark_fatal("invalid or overflowing VFS write range");
                    }
                }
            }
            return ffi::SQLITE_IOERR_WRITE;
        }
        let rc = io_delegate!(
            file,
            xWrite,
            ffi::SQLITE_IOERR_WRITE,
            buffer,
            amount,
            offset
        );
        if let Some(context) = context(file) {
            if let Some(recording) = &context.recording {
                if let Ok(mut state) = recording.lock() {
                    if rc != ffi::SQLITE_OK {
                        state
                            .mark_fatal(format!("underlying xWrite failed with SQLite error {rc}"));
                    } else {
                        match context.kind {
                            FileKind::Main => {
                                let _ = state.observe_main_write(offset, amount);
                            }
                            FileKind::Wal => state.wal_write_observed = true,
                            FileKind::Other => {}
                        }
                    }
                }
            }
        }
        rc
    })
}

#[allow(unused_unsafe)]
unsafe extern "C" fn file_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    guard(ffi::SQLITE_IOERR_TRUNCATE, || unsafe {
        if size < 0 {
            if let Some(context) = context(file) {
                if let Some(recording) = &context.recording {
                    if let Ok(mut state) = recording.lock() {
                        state.mark_fatal("negative VFS truncate size");
                    }
                }
            }
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let observed_file = context(file).and_then(|context| {
            context
                .recording
                .clone()
                .map(|recording| (context.kind, recording))
        });
        let mut previous_size = 0_i64;
        if let Some((FileKind::Main, recording)) = &observed_file {
            let size_rc =
                io_delegate!(file, xFileSize, ffi::SQLITE_IOERR_FSTAT, &mut previous_size);
            if size_rc != ffi::SQLITE_OK {
                if let Ok(mut state) = recording.lock() {
                    state.mark_fatal(format!(
                        "underlying xFileSize before truncate failed with SQLite error {size_rc}"
                    ));
                }
                return size_rc;
            }
        }
        let rc = io_delegate!(file, xTruncate, ffi::SQLITE_IOERR_TRUNCATE, size);
        if let Some((kind, recording)) = &observed_file {
            if let Ok(mut state) = recording.lock() {
                if rc != ffi::SQLITE_OK {
                    state.mark_fatal(format!(
                        "underlying xTruncate failed with SQLite error {rc}"
                    ));
                } else {
                    match kind {
                        FileKind::Main => {
                            let _ = state.observe_truncate(previous_size, size);
                        }
                        FileKind::Wal => state.wal_truncate_observed = true,
                        FileKind::Other => {}
                    }
                }
            }
        }
        rc
    })
}

#[allow(unused_unsafe)]
unsafe extern "C" fn file_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    guard(ffi::SQLITE_IOERR_FSYNC, || unsafe {
        let rc = io_delegate!(file, xSync, ffi::SQLITE_IOERR_FSYNC, flags);
        if let Some(context) = context(file) {
            if let Some(recording) = &context.recording {
                if let Ok(mut state) = recording.lock() {
                    if rc != ffi::SQLITE_OK {
                        state.mark_fatal(format!("underlying xSync failed with SQLite error {rc}"));
                    } else {
                        match context.kind {
                            FileKind::Main => state.main_sync_observed = true,
                            FileKind::Wal => state.wal_sync_observed = true,
                            FileKind::Other => {}
                        }
                    }
                }
            }
        }
        rc
    })
}

unsafe extern "C" fn file_size(file: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    guard(ffi::SQLITE_IOERR_FSTAT, || {
        io_delegate!(file, xFileSize, ffi::SQLITE_IOERR_FSTAT, size)
    })
}
unsafe extern "C" fn file_lock(file: *mut ffi::sqlite3_file, lock: c_int) -> c_int {
    guard(ffi::SQLITE_IOERR_LOCK, || {
        io_delegate!(file, xLock, ffi::SQLITE_IOERR_LOCK, lock)
    })
}
unsafe extern "C" fn file_unlock(file: *mut ffi::sqlite3_file, lock: c_int) -> c_int {
    guard(ffi::SQLITE_IOERR_UNLOCK, || {
        io_delegate!(file, xUnlock, ffi::SQLITE_IOERR_UNLOCK, lock)
    })
}
unsafe extern "C" fn file_check_reserved(
    file: *mut ffi::sqlite3_file,
    result: *mut c_int,
) -> c_int {
    guard(ffi::SQLITE_IOERR_CHECKRESERVEDLOCK, || {
        io_delegate!(
            file,
            xCheckReservedLock,
            ffi::SQLITE_IOERR_CHECKRESERVEDLOCK,
            result
        )
    })
}
unsafe extern "C" fn file_control(
    file: *mut ffi::sqlite3_file,
    op: c_int,
    argument: *mut c_void,
) -> c_int {
    guard(ffi::SQLITE_IOERR, || {
        io_delegate!(file, xFileControl, ffi::SQLITE_IOERR, op, argument)
    })
}
unsafe extern "C" fn file_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    guard(0, || io_delegate!(file, xSectorSize, 0))
}
unsafe extern "C" fn file_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    guard(0, || io_delegate!(file, xDeviceCharacteristics, 0))
}
unsafe extern "C" fn file_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    page_size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    guard(ffi::SQLITE_IOERR_SHMMAP, || {
        io_delegate!(
            file,
            xShmMap,
            ffi::SQLITE_IOERR_SHMMAP,
            page,
            page_size,
            extend,
            out
        )
    })
}
unsafe extern "C" fn file_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    count: c_int,
    flags: c_int,
) -> c_int {
    guard(ffi::SQLITE_IOERR_SHMLOCK, || {
        io_delegate!(
            file,
            xShmLock,
            ffi::SQLITE_IOERR_SHMLOCK,
            offset,
            count,
            flags
        )
    })
}
unsafe extern "C" fn file_shm_barrier(file: *mut ffi::sqlite3_file) {
    guard((), || {
        let underlying = unsafe { underlying_file(file) };
        if !underlying.is_null() && !unsafe { (*underlying).pMethods.is_null() } {
            if let Some(callback) = unsafe { (*(*underlying).pMethods).xShmBarrier } {
                unsafe { callback(underlying) };
            }
        }
    });
}
unsafe extern "C" fn file_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    guard(ffi::SQLITE_IOERR_SHMOPEN, || {
        io_delegate!(file, xShmUnmap, ffi::SQLITE_IOERR_SHMOPEN, delete)
    })
}
unsafe extern "C" fn file_fetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    amount: c_int,
    out: *mut *mut c_void,
) -> c_int {
    guard(ffi::SQLITE_IOERR, || {
        io_delegate!(file, xFetch, ffi::SQLITE_IOERR, offset, amount, out)
    })
}
unsafe extern "C" fn file_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    value: *mut c_void,
) -> c_int {
    guard(ffi::SQLITE_IOERR, || {
        io_delegate!(file, xUnfetch, ffi::SQLITE_IOERR, offset, value)
    })
}

static IO_METHODS_V1: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(file_close),
    xRead: Some(file_read),
    xWrite: Some(file_write),
    xTruncate: Some(file_truncate),
    xSync: Some(file_sync),
    xFileSize: Some(file_size),
    xLock: Some(file_lock),
    xUnlock: Some(file_unlock),
    xCheckReservedLock: Some(file_check_reserved),
    xFileControl: Some(file_control),
    xSectorSize: Some(file_sector_size),
    xDeviceCharacteristics: Some(file_device_characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};
static IO_METHODS_V2: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xShmMap: Some(file_shm_map),
    xShmLock: Some(file_shm_lock),
    xShmBarrier: Some(file_shm_barrier),
    xShmUnmap: Some(file_shm_unmap),
    ..IO_METHODS_V1
};
static IO_METHODS_V3: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xFetch: Some(file_fetch),
    xUnfetch: Some(file_unfetch),
    ..IO_METHODS_V2
};

macro_rules! vfs_delegate {
    ($vfs:expr, $method:ident, $fallback:expr $(, $arg:expr)*) => {{
        let underlying = unsafe { underlying_vfs($vfs) };
        match unsafe { (*underlying).$method } {
            Some(callback) => unsafe { callback(underlying $(, $arg)*) },
            None => $fallback,
        }
    }};
}

unsafe extern "C" fn vfs_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    guard(ffi::SQLITE_IOERR_DELETE, || {
        vfs_delegate!(vfs, xDelete, ffi::SQLITE_IOERR_DELETE, name, sync_dir)
    })
}
unsafe extern "C" fn vfs_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    result: *mut c_int,
) -> c_int {
    guard(ffi::SQLITE_IOERR_ACCESS, || {
        vfs_delegate!(vfs, xAccess, ffi::SQLITE_IOERR_ACCESS, name, flags, result)
    })
}
unsafe extern "C" fn vfs_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    length: c_int,
    out: *mut c_char,
) -> c_int {
    guard(ffi::SQLITE_CANTOPEN, || {
        vfs_delegate!(vfs, xFullPathname, ffi::SQLITE_CANTOPEN, name, length, out)
    })
}
unsafe extern "C" fn vfs_dl_open(vfs: *mut ffi::sqlite3_vfs, name: *const c_char) -> *mut c_void {
    guard(ptr::null_mut(), || {
        vfs_delegate!(vfs, xDlOpen, ptr::null_mut(), name)
    })
}
unsafe extern "C" fn vfs_dl_error(vfs: *mut ffi::sqlite3_vfs, length: c_int, out: *mut c_char) {
    guard((), || {
        vfs_delegate!(vfs, xDlError, (), length, out);
    });
}
unsafe extern "C" fn vfs_dl_sym(
    vfs: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    name: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    guard(None, || vfs_delegate!(vfs, xDlSym, None, handle, name))
}
unsafe extern "C" fn vfs_dl_close(vfs: *mut ffi::sqlite3_vfs, handle: *mut c_void) {
    guard((), || {
        vfs_delegate!(vfs, xDlClose, (), handle);
    });
}
unsafe extern "C" fn vfs_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    length: c_int,
    out: *mut c_char,
) -> c_int {
    guard(0, || vfs_delegate!(vfs, xRandomness, 0, length, out))
}
unsafe extern "C" fn vfs_sleep(vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    guard(0, || vfs_delegate!(vfs, xSleep, 0, micros))
}
unsafe extern "C" fn vfs_current_time(vfs: *mut ffi::sqlite3_vfs, result: *mut f64) -> c_int {
    guard(ffi::SQLITE_IOERR, || {
        vfs_delegate!(vfs, xCurrentTime, ffi::SQLITE_IOERR, result)
    })
}
unsafe extern "C" fn vfs_get_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    length: c_int,
    out: *mut c_char,
) -> c_int {
    guard(0, || vfs_delegate!(vfs, xGetLastError, 0, length, out))
}
unsafe extern "C" fn vfs_current_time_int64(vfs: *mut ffi::sqlite3_vfs, result: *mut i64) -> c_int {
    guard(ffi::SQLITE_IOERR, || {
        vfs_delegate!(vfs, xCurrentTimeInt64, ffi::SQLITE_IOERR, result)
    })
}
unsafe extern "C" fn vfs_set_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    call: ffi::sqlite3_syscall_ptr,
) -> c_int {
    guard(ffi::SQLITE_NOTFOUND, || {
        vfs_delegate!(vfs, xSetSystemCall, ffi::SQLITE_NOTFOUND, name, call)
    })
}
unsafe extern "C" fn vfs_get_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    guard(None, || vfs_delegate!(vfs, xGetSystemCall, None, name))
}
unsafe extern "C" fn vfs_next_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> *const c_char {
    guard(ptr::null(), || {
        vfs_delegate!(vfs, xNextSystemCall, ptr::null(), name)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OpenFlags};
    use std::ffi::CString;
    use std::fs;
    use std::sync::Barrier;

    fn open_recorded(path: &Path) -> Connection {
        Connection::open_with_flags_and_vfs(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            QWAL_RECORDING_VFS_NAME,
        )
        .unwrap()
    }

    fn create_staging_database(path: &Path) {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch("PRAGMA page_size=4096; CREATE TABLE seed(id INTEGER PRIMARY KEY);")
            .unwrap();
    }

    #[test]
    fn registration_does_not_change_the_default_vfs() {
        let before = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
        ensure_qwal_recording_vfs_registered().unwrap();
        let after = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
        assert_eq!(before, after);
        assert!(!unsafe { ffi::sqlite3_vfs_find(VFS_NAME_NUL.as_ptr().cast()) }.is_null());
    }

    #[test]
    fn page_ranges_cover_every_overlapped_page() {
        let offsets = [0, 1, 4094, 4095, 4096, 4097, 8191, 8192];
        let amounts = [0, 1, 2, 4095, 4096, 4097, 8192];
        for offset in offsets {
            for amount in amounts {
                let expected: BTreeSet<_> = (offset..offset + amount)
                    .map(|byte| (byte / 4096 + 1) as u64)
                    .collect();
                let actual: BTreeSet<_> = pages_for_write_range(offset, amount as c_int, 4096)
                    .unwrap()
                    .into_iter()
                    .collect();
                assert_eq!(actual, expected, "offset={offset}, amount={amount}");
            }
        }
        assert_eq!(pages_for_write_range(-1, 1, 4096), None);
        assert_eq!(pages_for_write_range(0, 1, 0), None);
    }

    #[test]
    fn named_vfs_records_wal_write_and_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        create_staging_database(&path);
        let recording = QwalRecordingSession::begin(&path, 4096).unwrap();
        let connection = open_recorded(&path);
        assert_eq!(
            connection
                .query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0))
                .unwrap()
                .to_lowercase(),
            "wal"
        );
        connection.execute_batch("CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item(value) VALUES ('a');").unwrap();
        recording.mark_commit_observed().unwrap();
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        recording.mark_checkpoint_succeeded().unwrap();
        drop(connection);

        let sealed = recording.seal().unwrap();
        assert!(sealed.wal_opened);
        assert!(sealed.wal_write_observed);
        assert!(sealed.main_sync_observed);
        assert!(!sealed.candidate_pages.is_empty());
        assert!(sealed.is_complete());
    }

    #[test]
    fn synchronous_off_recording_is_complete_without_a_durability_sync() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        create_staging_database(&path);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(connection);
        let recording = QwalRecordingSession::begin(&path, 4096).unwrap();
        let connection = open_recorded(&path);
        connection
            .execute_batch(
                "PRAGMA synchronous=OFF; \
                 INSERT INTO seed DEFAULT VALUES;",
            )
            .unwrap();
        recording.mark_commit_observed().unwrap();
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        recording.mark_checkpoint_succeeded().unwrap();
        drop(connection);

        let sealed = recording.seal().unwrap();
        assert!(sealed.commit_observed);
        assert!(sealed.checkpoint_succeeded);
        assert!(!sealed.main_sync_observed);
        assert!(sealed.is_complete());
    }

    #[test]
    fn observed_candidates_cover_the_closed_file_diff() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("base.db");
        let target = temp.path().join("target.db");
        {
            let connection = Connection::open(&base).unwrap();
            connection.execute_batch("PRAGMA page_size=4096; CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item(value) VALUES ('before'); VACUUM;").unwrap();
        }
        fs::copy(&base, &target).unwrap();

        let recording = QwalRecordingSession::begin(&target, 4096).unwrap();
        let connection = open_recorded(&target);
        connection
            .execute("UPDATE item SET value = ?1 WHERE id = 1", ["after"])
            .unwrap();
        recording.mark_commit_observed().unwrap();
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        recording.mark_checkpoint_succeeded().unwrap();
        drop(connection);
        let sealed = recording.seal().unwrap();

        let before = fs::read(base).unwrap();
        let after = fs::read(target).unwrap();
        let changed: BTreeSet<u64> = before
            .chunks(4096)
            .zip(after.chunks(4096))
            .enumerate()
            .filter_map(|(index, (before, after))| (before != after).then_some(index as u64 + 1))
            .collect();
        let candidates: BTreeSet<_> = sealed.candidate_pages.into_iter().collect();
        assert!(!changed.is_empty());
        assert!(
            changed.is_subset(&candidates),
            "changed={changed:?}, candidates={candidates:?}"
        );
    }

    #[test]
    fn unexpected_persistent_main_database_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let expected = temp.path().join("expected.db");
        let unexpected = temp.path().join("unexpected.db");
        create_staging_database(&expected);
        let recording = QwalRecordingSession::begin(expected, 4096).unwrap();
        let result = Connection::open_with_flags_and_vfs(
            unexpected,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            QWAL_RECORDING_VFS_NAME,
        );
        assert!(result.is_err());
        let error = recording.seal().unwrap_err();
        assert!(error
            .to_string()
            .contains("unexpected persistent main database"));
    }

    #[test]
    fn session_rejects_a_page_size_that_differs_from_the_existing_database() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        create_staging_database(&path);

        let error = match QwalRecordingSession::begin(path, 8192) {
            Ok(_) => panic!("mismatched page size must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn failed_xopen_leaves_wrapper_unpublished_and_cancels_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        create_staging_database(&path);
        let recording = QwalRecordingSession::begin(&path, 4096).unwrap();
        fs::remove_file(&path).unwrap();

        let path = CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        let vfs = unsafe { ffi::sqlite3_vfs_find(VFS_NAME_NUL.as_ptr().cast()) };
        assert!(!vfs.is_null());
        let size = unsafe { (*vfs).szOsFile as usize };
        for _ in 0..32 {
            let mut storage = vec![0_u64; size.div_ceil(std::mem::size_of::<u64>())];
            let file = storage.as_mut_ptr().cast::<ffi::sqlite3_file>();
            let mut out_flags = 0;
            let rc = unsafe {
                ((*vfs).xOpen.unwrap())(
                    vfs,
                    path.as_ptr(),
                    file,
                    ffi::SQLITE_OPEN_MAIN_DB | ffi::SQLITE_OPEN_READONLY,
                    &mut out_flags,
                )
            };
            assert_ne!(rc, ffi::SQLITE_OK);
            assert!(unsafe { (*file).pMethods.is_null() });
        }
        {
            let state = recording.state.lock().unwrap();
            assert_eq!(state.opening_main_handles, 0);
            assert_eq!(state.open_main_handles, 0);
        }
        let sealed = recording.seal().unwrap();
        assert!(!sealed.is_complete());
    }

    #[test]
    fn seal_fails_while_an_xopen_reservation_is_in_flight() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("staging.db");
        create_staging_database(&path);
        let recording = QwalRecordingSession::begin(&path, 4096).unwrap();
        let reserved = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let worker_path = path.clone();
        let worker_reserved = Arc::clone(&reserved);
        let worker_release = Arc::clone(&release);
        let worker = std::thread::spawn(move || {
            let reservation = reserve_recording(&worker_path, FileKind::Main)
                .unwrap()
                .unwrap();
            worker_reserved.wait();
            worker_release.wait();
            drop(reservation);
        });

        reserved.wait();
        let error = match recording.seal() {
            Ok(_) => panic!("seal must reject an in-flight xOpen reservation"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("remain open"));
        release.wait();
        worker.join().unwrap();
    }
}
