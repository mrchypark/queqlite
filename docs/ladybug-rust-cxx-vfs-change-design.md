# ladybug-rust CXX VFS 표면 변경 설계

> 상태: **변경 제안, 미구현**
>
> 대상: `LadybugDB/ladybug-rust`
>
> 선행 변경: [LadybugDB construction-time VFS 변경 설계](./ladybug-construction-time-vfs-change-design.md)
>
> 연관 문서: [Ladybug WAL replication design](./ladybug-wal-replication-design.md)

## 1. 결론

현재 `ladybug-rust`의 CXX bridge에는 `FileSystem`, `VirtualFileSystem`,
`Database::registerFileSystem()`이 노출되어 있지 않다. C++ core의 virtual class를 Rust가 직접
상속할 수도 없으므로 함수 하나를 bridge에 추가하는 것으로는 충분하지 않다.

필요한 최소 구조는 다음과 같다.

```text
Rust Arc<dyn FileSystem>
  -> RustFileSystemBridge (concrete opaque Rust type)
  -> cxx extern "Rust" callbacks
  -> C++ RustFileSystemAdapter : lbug::common::FileSystem
  -> Ladybug Database construction-time defaultFS
```

기존 `Database::new()`는 그대로 유지하고, opt-in `Database::new_with_file_system()`을 추가한다.
adapter ownership은 C++ `Database -> VirtualFileSystem -> RustFileSystemAdapter -> rust::Box` 방향으로
한 번만 이동시킨다.

## 2. 현재 bridge 범위

현재 공개 Rust API는 `Database`, `SystemConfig`, `Connection`, `PreparedStatement`,
`QueryResult`, `Value`, `LogicalType` 중심이다. `src/ffi.rs`, `include/lbug_rs.h`,
`src/lbug_rs.cpp`, `database.rs` 어디에도 filesystem type이나 register 함수가 없다.

- [ladybug-rust `ffi.rs`](https://github.com/LadybugDB/ladybug-rust/blob/main/src/ffi.rs)
- [ladybug-rust `database.rs`](https://github.com/LadybugDB/ladybug-rust/blob/main/src/database.rs)
- [ladybug-rust C++ shim](https://github.com/LadybugDB/ladybug-rust/blob/main/src/lbug_rs.cpp)
- [ladybug-rust header](https://github.com/LadybugDB/ladybug-rust/blob/main/include/lbug_rs.h)

`cxx`는 opaque C++ type과 함수를 안전하게 연결하지만 Rust object가 C++ virtual base class를 직접
상속하도록 생성해 주지 않는다. C++ derived adapter와 `extern "Rust"` forwarding 함수가 필요하다.

## 3. 목표와 비목표

### 목표

- Rust filesystem을 DB construction 이전에 전달한다.
- CXX 경계에서 path, flags, byte buffer, error, ownership을 명시적으로 변환한다.
- file handle이 filesystem과 DB보다 먼저 파괴되도록 lifetime을 고정한다.
- callback panic, reentrancy, concurrent access가 UB로 이어지지 않게 한다.
- 기존 `Database::new()`와 prebuilt/source/shared build 흐름을 보존한다.

### 비목표

- C++ `FileSystem`의 raw pointer나 `VirtualFileSystem*`을 public Rust API로 노출하지 않는다.
- Rust caller가 열린 DB의 primary filesystem을 교체하게 하지 않는다.
- 첫 버전에서 async filesystem trait을 제공하지 않는다.
- WAL bytes나 replay internals를 generic filesystem API로 의미 해석하지 않는다.
- object store semantics를 local filesystem과 같다고 선언하지 않는다.

## 4. Rust public API

### 4.1 object-safe trait

첫 API는 synchronous하고 object-safe하게 유지한다. Ladybug의 storage path가 synchronous이므로
async runtime을 bridge 안에 넣지 않는다.

```rust
use std::io::SeekFrom;

pub trait FileSystem: Send + Sync + 'static {
    fn open(&self, path: &str, options: OpenOptions)
        -> Result<Box<dyn FileHandle>, FileSystemError>;

    fn exists(&self, path: &str) -> Result<bool, FileSystemError>;
    fn create_dir(&self, path: &str) -> Result<(), FileSystemError>;
    fn remove_file_if_exists(&self, path: &str) -> Result<(), FileSystemError>;
    fn overwrite(&self, from: &str, to: &str) -> Result<(), FileSystemError>;
    fn rename(&self, from: &str, to: &str) -> Result<(), FileSystemError>;
    fn copy(&self, from: &str, to: &str) -> Result<(), FileSystemError>;
    fn sync_directory(&self, path: &str) -> Result<(), FileSystemError>;

    fn glob(&self, path: &str) -> Result<Vec<String>, FileSystemError>;
    fn expand_path(&self, path: &str) -> Result<String, FileSystemError>;
}

pub trait FileHandle: Send + 'static {
    fn read_at(&mut self, offset: u64, dst: &mut [u8])
        -> Result<(), FileSystemError>;
    fn write_at(&mut self, offset: u64, src: &[u8])
        -> Result<(), FileSystemError>;
    fn read(&mut self, dst: &mut [u8]) -> Result<usize, FileSystemError>;
    fn seek(&mut self, position: SeekFrom) -> Result<u64, FileSystemError>;
    fn len(&self) -> Result<u64, FileSystemError>;
    fn truncate(&mut self, len: u64) -> Result<(), FileSystemError>;
    fn sync(&mut self, mode: SyncMode) -> Result<(), FileSystemError>;
}
```

positional `read_at`만으로는 C++의 abstract `readFile`과 `seek`를 구현할 수 없다. C++ adapter가 별도
cursor를 재구현하는 대신 첫 버전부터 sequential `read`/`seek`를 handle 계약에 포함한다. `glob`과
path expansion은 user file query도 primary filesystem을 통과할 수 있어 포함한다. 반면
`handleFileViaFunction()`은 첫 버전에서 `false`, `canPerformSeek()`는 `true`로 고정하고 해당 조합에서
불필요한 table-function 객체는 Rust로 넘기지 않는다. 최종 trait은 전체 virtual method inventory와
call-site test로 확정하며, C++ 기본 구현의 `UNREACHABLE_CODE`에 떨어지는 entry point가 없어야 한다.

`glob`/`expand_path`가 `ClientContext` 설정을 필요로 할 때 raw context pointer를 Rust로 넘기지 않는다.
C++ shim이 home directory와 file search path처럼 필요한 값만 owned string으로 복사한 작은
`PathContext` shared struct를 만들어 callback에 전달한다.

### 4.2 configuration types

```rust
#[derive(Clone, Copy, Debug)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub create_or_truncate: bool,
    pub temporary: bool,
    pub lock: LockMode,
}

#[derive(Clone, Copy, Debug)]
pub enum SyncMode {
    Data,
    Full,
}
```

unknown C++ flag bit는 silently drop하지 않는다. bridge가 이해하지 못하는 flag는 open 전에
`Unsupported`로 거부한다.

`FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS`는 Rust `create_new`와 의미가 다르므로 별도
`create_or_truncate` bit로 보존한다. compression type도 adapter가 처리할 값인지 VFS가 바깥에서
처리할 값인지 한 곳에서 결정하고 중복 적용하지 않는다.

### 4.3 Database constructor

```rust
impl Database {
    pub fn new_with_file_system<P: AsRef<Path>>(
        path: P,
        config: SystemConfig,
        file_system: Arc<dyn FileSystem>,
    ) -> Result<Self, Error>;
}
```

규칙:

- `file_system`은 DB construction 전에 C++로 이동한다.
- `Database::new()`는 기존 native local filesystem 경로를 유지한다.
- primary filesystem은 DB가 열린 뒤 교체할 수 없다.
- `Arc`는 Rust caller와 adapter가 동일 implementation을 공유할 필요가 있을 때만 사용한다.
  공유가 필요 없는 내부 경로는 `Box<dyn FileSystem>`으로 한 번만 소유해도 된다.
- 첫 production consumer가 observer를 함께 읽어야 하므로 public API는 `Arc`를 허용하되, CXX에는
  concrete bridge box 하나만 넘긴다.

## 5. Rust opaque bridge types

`dyn Trait`을 CXX signature에 직접 넣지 않는다.

```rust
pub(crate) struct RustFileSystemBridge {
    inner: Arc<dyn FileSystem>,
}

pub(crate) struct RustFileHandleBridge {
    inner: Box<dyn FileHandle>,
}
```

`ffi.rs`의 `extern "Rust"` block은 concrete type과 forwarding 함수만 노출한다.

```rust
#[cxx::bridge]
mod ffi {
    extern "Rust" {
        type RustFileSystemBridge;
        type RustFileHandleBridge;

        fn rust_fs_open(
            fs: &RustFileSystemBridge,
            path: &str,
            flags: u32,
        ) -> Result<Box<RustFileHandleBridge>>;

        fn rust_file_read_at(
            file: &mut RustFileHandleBridge,
            offset: u64,
            dst: &mut [u8],
        ) -> Result<()>;

        fn rust_file_write_at(
            file: &mut RustFileHandleBridge,
            offset: u64,
            src: &[u8],
        ) -> Result<()>;

        // len, truncate, sync, exists, rename, remove, directory sync ...
    }

    unsafe extern "C++" {
        fn new_database_with_file_system(
            database_path: StringView,
            // 기존 SystemConfig fields
            file_system: Box<RustFileSystemBridge>,
        ) -> Result<UniquePtr<Database>>;
    }
}
```

현재 crate가 고정한 `cxx = 1.0.138`에서 `Result<Box<OpaqueRustType>>`, `&[u8]`, `&mut [u8]`는 지원된다.
생성된 C++ signature는 각각 `rust::Box<T>`, `rust::Slice<const uint8_t>`, `rust::Slice<uint8_t>`가 된다.
따라서 이 형태를 먼저 사용하고 raw `void*`와 pointer-length pair를 public Rust API에 노출하지 않는다.

## 6. C++ forwarding adapter

`include/lbug_rs.h`와 `src/lbug_rs.cpp`에 concrete derived class를 둔다.

```cpp
class RustFileSystemAdapter final : public lbug::common::FileSystem {
public:
    explicit RustFileSystemAdapter(rust::Box<RustFileSystemBridge> inner);

    std::unique_ptr<lbug::common::FileInfo> openFile(
        const std::string& path,
        lbug::common::FileOpenFlags flags,
        lbug::main::ClientContext* context) override;

    void syncFile(const lbug::common::FileInfo& fileInfo) const override;
    bool canHandleFile(std::string_view) const override { return true; }

    // required path operations forward to extern Rust callbacks

private:
    rust::Box<RustFileSystemBridge> inner;
};
```

`RustFileInfo`는 Ladybug `FileInfo`를 상속하거나 core가 요구하는 concrete handle wrapper가 되며,
해당 open에서 반환된 `rust::Box<RustFileHandleBridge>`를 소유한다.

```text
Database
  owns VirtualFileSystem
    owns RustFileSystemAdapter
      owns RustFileSystemBridge

storage/WAL
  owns RustFileInfo
    owns RustFileHandleBridge
```

`new_database_with_file_system()`은 adapter를 만든 뒤 선행 core 변경의 construction-time constructor에
전달한다. `Database`를 먼저 만들고 `registerFileSystem()`을 호출하는 방식은 startup recovery를
놓치므로 사용하지 않는다.

core가 normalized database path를 filesystem base에 bind한 뒤 첫 open을 수행한다. Rust backend가
root 자체를 별도로 알 필요가 없도록 모든 canonical storage callback에는 완전한 path를 전달한다.
따라서 첫 버전에는 mutable `bind_root` public trait method를 추가하지 않는다. root-relative backend가
실제 요구가 되면 construction-only context로 별도 확장하며, runtime root 변경은 허용하지 않는다.

## 7. error와 panic 경계

filesystem error는 안정적인 category와 message로 나눈다.

```rust
pub enum FileSystemErrorKind {
    NotFound,
    AlreadyExists,
    PermissionDenied,
    InvalidInput,
    Unsupported,
    Corruption,
    Other,
}
```

- Rust callback의 `Err`는 CXX의 `rust::Error`로 나오므로 adapter 각 entry point에서 잡아 Ladybug
  `IOException`으로 변환한다.
- Rust panic이 CXX callback 경계를 넘어가면 안 된다. forwarding function에서
  `catch_unwind(AssertUnwindSafe(...))` 후 deterministic I/O error로 바꾼다.
- CXX 1.0.138은 `extern "Rust"` panic을 `Result` 여부와 무관하게 process abort로 처리하므로
  `catch_unwind`는 방어적 선택이 아니라 필수 경계다.
- error message에 secret path parameter나 file content를 포함하지 않는다.
- partial write를 성공으로 보고하지 않는다.
- callback failure 뒤 C++ commit이 성공했다고 응답하지 않는지 integration test로 고정한다.

## 8. concurrency와 reentrancy

- public `FileSystem`은 `Send + Sync`다.
- `FileHandle`은 우선 `Send`만 요구하고, 한 handle에 대한 병렬 callback 여부는 C++ adapter가
  serialize한다.
- filesystem callback 안에서 같은 `Database`에 query를 재진입하는 동작은 금지한다.
- callback은 Ladybug 내부 lock을 잡은 상태에서 호출될 수 있으므로 사용자 code 실행 시간을
  bounded하게 유지한다.
- async runtime의 `block_on`을 callback에서 사용하지 않는다.
- observer나 metrics가 필요하면 filesystem 구현 안에서 bounded non-blocking channel을 사용하되,
  overflow가 correctness data 손실이면 I/O를 실패시키고 telemetry 손실이면 drop counter를 남긴다.

## 9. local-compatible 첫 구현

범용 remote filesystem보다 먼저 표준 library 기반 local implementation과 recording decorator를
제공한다.

```text
StdFileSystem
  -> actual local file semantics

RecordingFileSystem<StdFileSystem>
  -> delegates every operation
  -> records bounded open/write/sync/rename lifecycle
```

첫 Rhiza consumer는 `RecordingFileSystem`을 사용해 WAL capture와 crash boundary를 관찰한다.
recording 결과는 qlog가 아니며 VFS callback만 보고 commit을 판정하지 않는다. engine commit 성공,
WAL durability, complete transaction evidence가 별도로 필요하다.

새 third-party VFS crate는 추가하지 않는다. Rust standard library와 현재 `cxx` dependency로 local
correctness path를 먼저 완성한다.

## 10. build 변경

`build.rs`의 기존 흐름을 유지한다.

- external `LBUG_LIBRARY_DIR` / `LBUG_INCLUDE_DIR`
- downloaded prebuilt static library
- bundled/source CMake fallback
- optional Arrow bridge

추가 사항:

- compile-time header check로 construction-time core constructor 존재 여부를 확인한다.
- 구버전 external Ladybug library와 link할 때 undefined symbol이 늦게 발생하지 않도록 명확한 build
  error 또는 feature gate를 제공한다.
- 예: `vfs` Cargo feature가 켜졌을 때만 새 symbol을 요구하고, bundled matching source에서는
  기본 활성화 여부를 release 정책으로 결정한다.
- core ABI/fingerprint와 crate version을 runtime diagnostics에서 조회할 수 있게 한다.

권장 feature:

```toml
[features]
default = []
arrow = ["dep:arrow"]
vfs = []
```

기존 사용자가 custom VFS code를 링크하지 않으면 callback/adapter code가 동작 경로에 들어가지 않는다.

## 11. 안전성 검증

### ownership/lifetime

- DB construction 실패 중 Rust filesystem과 열린 handle이 정확히 한 번 drop
- `Connection`/`QueryResult`가 남아 있을 때 DB drop ordering 검증
- recovery exception, open exception, callback panic에서 leak/double-free 없음
- sanitizer와 Miri 적용 가능한 Rust-only bridge tests

### I/O contract

- read/write offset와 length 경계, short read, EOF, truncate/grow
- file sync와 directory sync 호출 전달
- rename source/target와 same-backend error 전달
- unicode/non-UTF8 path 정책을 명시적으로 검사
- unsupported C++ flag를 silent downgrade하지 않음

### recovery

- pre-existing complete WAL을 Rust filesystem을 통해 replay
- torn/corrupt WAL failure
- `.wal.checkpoint`와 `.shadow` recovery
- sync/rename/directory-sync fault injection
- callback panic 시 DB serving 금지

### concurrency

- 여러 read connection의 병렬 I/O
- serialized writer와 optional multi-write mode
- filesystem-wide lock과 file-handle lock deadlock 검사
- callback 중 DB reentrancy rejection

### compatibility

- `Database::new()` 기존 test 전체 통과
- `Database::new_with_file_system(StdFileSystem)` 결과가 기존 local path와 동등
- bundled static, downloaded prebuilt, external shared build
- `arrow`, `vfs`, `arrow+vfs` feature matrix

## 12. 공개 API 안정화 순서

1. crate-private bridge와 C++ adapter로 local integration test 작성
2. `StdFileSystem`과 recording decorator로 startup recovery 검증
3. trait method inventory를 실제 core call site에 맞춰 축소/확정
4. `vfs` feature 아래 experimental public API 공개
5. Rhiza restart-per-effect POC와 crash matrix 통과
6. ownership/error/flag contract가 안정된 뒤 semver-stable API 승격

첫 release에서 WAL export/import API를 함께 공개하지 않는다. VFS bridge가 안정된 뒤에도 replication
effect는 별도의 versioned engine API와 qlog envelope로 설계한다.

## 13. 파일별 변경 범위

| 파일 | 변경 |
|---|---|
| `src/database.rs` | `new_with_file_system`, bridge ownership 생성 |
| `src/ffi.rs` | opaque Rust bridge types, callbacks, C++ factory declaration |
| `include/lbug_rs.h` | `RustFileSystemAdapter`, `RustFileInfo`, factory 선언 |
| `src/lbug_rs.cpp` | FileSystem virtual method forwarding과 error 변환 |
| `src/error.rs` | public filesystem error 또는 mapping |
| `src/file_system.rs` | trait, flags, local implementation; 새 모듈 |
| `src/lib.rs` | `vfs` feature 아래 public export |
| `build.rs` | core symbol/version guard와 feature-specific build |
| `Cargo.toml` | `vfs` feature; 새 runtime dependency는 없음 |
| tests/examples | local recording VFS, startup recovery, fault injection |

완료 기준은 Rust filesystem이 **LadybugDB construction 전에 소유권과 함께 전달되고**, 최초 data
open부터 WAL recovery/checkpoint까지 호출을 받으며, failure/panic/drop 경계가 안전하게 검증되는 것이다.
