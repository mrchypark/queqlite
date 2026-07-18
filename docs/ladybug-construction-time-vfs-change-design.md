# LadybugDB construction-time VFS 변경 설계

> 상태: **변경 제안, 미구현**
>
> 대상: LadybugDB C++ core (`LadybugDB/ladybug`)
>
> 기준: Rhiza가 사용하는 `lbug` 0.18.x 계열과 2026-07-18 upstream 구조
>
> 연관 문서: [Ladybug WAL replication design](./ladybug-wal-replication-design.md)

## 1. 결론

LadybugDB에는 이미 `FileSystem`과 `VirtualFileSystem`이 있고 data file, WAL,
`WALReplayer`, shadow file, checkpoint I/O가 대부분 이 계층을 통과한다. 부족한 것은 VFS의
존재가 아니라 **설치 시점과 파일 연산 계약**이다.

현재 `Database::registerFileSystem()`은 `Database` 생성이 끝난 뒤에만 호출할 수 있다. 하지만
constructor 안에서 이미 기본 `LocalFileSystem`, buffer/storage manager, WAL, startup recovery가
만들어진다. 따라서 등록된 filesystem은 최초 data-file open과 startup WAL replay를 안정적으로
가로챌 수 없다.

최소 변경 방향은 다음과 같다.

1. `Database` 생성 시 primary/default filesystem을 move-in 할 수 있는 overload를 추가한다.
2. `VirtualFileSystem`이 전달받은 filesystem을 처음부터 `defaultFS`로 소유하고 normalized DB
   path를 첫 I/O 전에 한 번 bind한다.
3. rename/copy/overwrite가 source와 target의 backend를 검증하고 같은 backend로 dispatch하게 한다.
4. parent directory sync API를 추가해 durable file publication을 표현한다.
5. 기존 constructor와 `registerFileSystem()`은 호환성을 위해 유지한다.

VFS는 저장 I/O 경계일 뿐 consensus나 replication authority가 아니다. Rhiza에서는 QuePaxa/qlog가
유일한 권위이고, VFS는 결정 전 staging capture와 결정 후 apply를 돕는 엔진 adapter다.

## 2. 현재 구조와 문제

### 2.1 이미 존재하는 표면

LadybugDB C++ core는 다음 API를 갖는다.

- `common::FileSystem`: open/read/write/truncate/sync/rename/remove/exists 등
- `common::VirtualFileSystem`: filesystem routing과 기본 local filesystem 소유
- `main::Database::registerFileSystem(std::unique_ptr<FileSystem>)`
- `main::Database::getVFS()`

근거:

- [Database C++ API](https://github.com/LadybugDB/ladybug/blob/main/src/include/main/database.h)
- [FileSystem API](https://github.com/LadybugDB/ladybug/blob/main/src/include/common/file_system/file_system.h)
- [VirtualFileSystem API](https://github.com/LadybugDB/ladybug/blob/main/src/include/common/file_system/virtual_file_system.h)

### 2.2 등록이 너무 늦다

현재 생성 순서는 개념적으로 다음과 같다.

```text
Database constructor
  -> initMembers()
     -> VirtualFileSystem(default LocalFileSystem)
     -> BufferManager / MemoryManager
     -> StorageManager / WAL / TransactionManager
     -> StorageManager::recover()
        -> WALReplayer
     -> constructor return

caller
  -> Database::registerFileSystem(custom)
```

custom filesystem이 등록될 때는 startup recovery가 이미 끝났다. WAL이 첫 write까지 lazy-open되는
경우 일부 I/O를 우연히 관찰할 수는 있지만, main data handle과 recovery가 기본 filesystem으로
열린 상태와 섞이므로 correctness 계약으로 사용할 수 없다.

[Database initialization](https://github.com/LadybugDB/ladybug/blob/main/src/main/database.cpp)

### 2.3 routing이 file operation마다 일관되지 않다

현재 `VirtualFileSystem`의 일부 연산은 path resolver로 filesystem을 찾지만 rename은 기본
filesystem으로 직접 전달되는 구현이 존재한다. custom filesystem이 WAL path를 맡아도
`.wal -> .wal.checkpoint` rotation이 다른 backend로 전달될 수 있다.

또한 열린 `FileInfo`는 자신을 만든 `FileSystem*`에 의존한다. 같은 local path를 DB open 뒤에
다른 filesystem이 claim하면 path 기반 연산과 기존 handle 연산이 서로 다른 backend를 볼 수 있다.

### 2.4 directory durability를 표현할 수 없다

`FileSystem::syncFile(FileInfo&)`는 있지만 parent directory를 sync하는 API는 없다. 다음과 같은
crash-safe publication을 VFS 계약만으로 완결할 수 없다.

```text
write temp file
-> sync temp file
-> rename temp to final
-> sync parent directory
```

## 3. 목표와 비목표

### 목표

- DB의 최초 path access보다 먼저 primary filesystem을 설치한다.
- data/WAL/shadow/checkpoint/recovery가 하나의 일관된 filesystem routing을 사용한다.
- 기존 constructor의 source 사용법과 local filesystem 동작을 보존한다.
- Rust/CXX adapter가 ownership과 error boundary를 안전하게 구현할 수 있게 한다.
- file sync와 directory sync를 분리해 crash test에서 각 barrier를 주입·관찰할 수 있게 한다.

### 비목표

- VFS 안에 consensus, qlog, replication protocol을 넣지 않는다.
- WAL record format을 public stable API로 선언하지 않는다.
- 첫 변경에서 object store나 네트워크 filesystem의 완전한 POSIX emulation을 보장하지 않는다.
- 열린 database에 외부 WAL을 live import하는 API를 이 변경에 섞지 않는다.
- 기존 `registerFileSystem()` 기반 extension filesystem을 제거하지 않는다.

## 4. 제안 API

### 4.1 `VirtualFileSystem` primary filesystem 주입

기존 constructor는 유지한다.

```cpp
class VirtualFileSystem final : public FileSystem {
public:
    explicit VirtualFileSystem(std::string homeDir);

    VirtualFileSystem(
        std::string homeDir,
        std::unique_ptr<FileSystem> defaultFileSystem);
};
```

규칙:

- `defaultFileSystem`은 non-null이어야 한다.
- `VirtualFileSystem`이 `unique_ptr` ownership을 가진다.
- 기존 constructor는 현재처럼 `LocalFileSystem(homeDir)`를 생성해 새 constructor에 위임한다.
- `registerFileSystem()`으로 추가되는 scheme/subsystem은 primary filesystem과 별도로 유지한다.

현재 `FileSystem`의 protected `dbPath`는 local backend의 sidecar 삭제 허용 범위 등에 쓰인다. caller가
filesystem을 만드는 시점에는 `Database`가 확정한 normalized path를 알 수 없으므로, VFS constructor는
ownership을 받은 직후 `defaultFileSystem`에 이 값을 **한 번만** bind해야 한다. 가장 작은 core 변경은
`VirtualFileSystem`만 호출할 수 있는 non-virtual `FileSystem::bindDatabasePath()`와 friend 관계다.
이 함수는 최초 open 전만 허용하고 public runtime 교체 API로 노출하지 않는다.

remote URI 자체를 database path로 허용하려면 현재 `StorageUtils::expandPath()`의
`std::filesystem::absolute()` 전제부터 바꿔야 한다. 첫 변경은 기존과 같은 local-compatible path
normalization을 유지한다. URI-aware database root가 실제 두 번째 요구가 되면 그때
`FileSystemFactory(normalizedPath)` 또는 별도 path policy를 추가한다.

`FileSystem` 자체에 factory abstraction을 추가하지 않는다. 실제로 construction-time factory가 두
번째 소비자에게도 필요해질 때까지 move-only filesystem 한 개가 가장 작은 API다.

### 4.2 `Database` constructor overload

```cpp
class Database {
public:
    explicit Database(
        std::string_view databasePath,
        SystemConfig systemConfig = SystemConfig());

    Database(
        std::string_view databasePath,
        SystemConfig systemConfig,
        std::unique_ptr<common::FileSystem> defaultFileSystem);
};
```

기존 constructor는 `defaultFileSystem == nullptr`인 내부 경로로 위임하며 기존
`LocalFileSystem`을 선택한다. 새 overload는 다음 순서를 보장해야 한다.

```text
validate arguments
-> normalize database path without opening storage files
-> construct VirtualFileSystem(path, injected default filesystem)
-> validate path through that VFS
-> construct buffer/storage/WAL managers using that VFS
-> run startup recovery using that VFS
```

`std::filesystem::is_directory()`처럼 injected filesystem을 우회하는 DB-path 접근도 이 과정에서
제거하거나 local-only precondition으로 명확히 제한해야 한다. 일반 path validation은
`VirtualFileSystem::fileOrPathExists()`와 filesystem metadata API를 사용한다.

### 4.3 same-backend two-path 연산 계약

`VirtualFileSystem`의 두-path 연산은 source와 target을 각각 resolve한다.

```cpp
void VirtualFileSystem::renameFile(const std::string& from, const std::string& to) {
    auto* srcFS = findFileSystem(from);
    auto* dstFS = findFileSystem(to);
    if (srcFS != dstFS) {
        throw IOException("Cross-filesystem atomic rename is unsupported");
    }
    srcFS->renameFile(from, to);
}
```

같은 규칙을 atomicity가 필요한 overwrite/replace에 적용한다. 현재 VFS header에는 `copyFile`
override도 없으므로 이를 명시적으로 추가해야 한다. copy는 source backend가 target backend까지 쓸 수
있다는 암묵적 가정을 두지 않는다. 첫 버전은 same-backend copy만 dispatch하고 cross-backend copy가
필요한 호출자가 read/write + sync + remove 순서를 명시적으로 구현하게 한다. 이를 atomic rename으로
가장하지 않는다.

### 4.4 directory sync

```cpp
class FileSystem {
public:
    virtual void syncDirectory(const std::string& directoryPath) const;
};

class VirtualFileSystem final : public FileSystem {
public:
    void syncDirectory(const std::string& directoryPath) const override;
};
```

`LocalFileSystem`은 지원되는 플랫폼에서 directory fd를 열고 `fsync`한다. 플랫폼이 안전한 directory
sync를 제공하지 않으면 성공으로 가장하지 말고 capability 또는 명시적 unsupported error를
반환한다.

기본 구현도 명시적인 unsupported error를 반환해야 한다. pure virtual로 시작하면 기존 외부
filesystem 구현의 source compatibility까지 즉시 깨진다. 다만 default virtual method라도 vtable이
바뀌므로 **binary ABI는 깨진다**. 이 변경을 포함하는 core와 `ladybug-rust` CXX shim은 같은 ABI
fingerprint로 함께 rebuild해야 하며, 기존 shared library에 새 crate만 연결하는 조합은 build 또는
startup에서 거부한다.

checkpoint/WAL rotation처럼 crash 후 filename publication이 중요한 호출부는 다음 순서를 갖는다.

```text
sync source file as required
-> rename on same filesystem
-> sync parent directory
```

기존 모든 rename을 무조건 더 비싼 durable rename으로 바꾸지 않는다. durability가 필요한
호출부만 `syncDirectory()`를 명시적으로 호출한다.

## 5. ownership과 lifetime

- `Database`가 `VirtualFileSystem`을 소유한다.
- `VirtualFileSystem`이 injected primary filesystem과 registered subsystem을 소유한다.
- `StorageManager`와 `FileInfo`의 filesystem pointer는 non-owning이다.
- 모든 storage/WAL/shadow/file handle은 filesystem보다 먼저 파괴되어야 한다.
- constructor가 recovery 중 실패해도 partially-created file handles보다 filesystem이 오래 살아야 한다.
- filesystem 교체 API는 제공하지 않는다. DB open 뒤 primary filesystem은 immutable하다.

이 규칙을 class comment와 destructor/member-order test로 고정한다. `registerFileSystem()`은 새 scheme을
추가하는 용도이며 이미 열린 path의 backend 교체 수단이 아니다.

## 6. error와 thread-safety 계약

- filesystem callback은 Ladybug `IOException` 또는 명시된 storage exception으로 변환된다.
- sync 실패는 WAL poison/fail-closed 흐름을 유지한다.
- callback 실패 뒤 commit 성공으로 응답하면 안 된다.
- filesystem 구현은 Ladybug가 동일 instance를 여러 thread에서 호출할 수 있음을 전제로 한다.
- `FileInfo`별 offset/handle state와 filesystem 전역 state의 lock 책임을 문서화한다.
- partial read는 요청된 나머지 bytes를 임의 데이터로 남기지 않는다.
- write/truncate/rename의 성공은 해당 backend가 정의한 결과가 실제로 완료된 뒤에만 반환한다.

## 7. WAL replication과의 관계

construction-time VFS는 다음을 가능하게 한다.

- staging WAL open/write/sync 관찰
- startup `WALReplayer` I/O fault injection
- WAL/checkpoint/shadow lifecycle 추적
- 동일 filesystem 안의 durable WAL install

하지만 VFS callback만으로 transaction commit을 판정하지 않는다. WAL export는 complete commit과
durability boundary를 엔진이 명시적으로 알려주는 별도 versioned API가 더 안전하다. 따라서 향후
최적화 순서는 다음과 같다.

1. construction-time VFS로 I/O와 crash boundary를 관찰한다.
2. 현재 restart-per-effect WAL capture/reopen/checkpoint 경로를 검증한다.
3. 성능상 필요할 때 `exportCommittedLocalWAL()` 같은 좁은 engine API를 별도 설계한다.
4. 열린 DB live import는 transaction manager/cache/catalog 원자성까지 다루는 별도 단계로 둔다.

## 8. 호환성과 rollout

- 기존 `Database(path, config)` source behavior와 호출 결과를 유지한다.
- 새 constructor는 opt-in이다.
- constructor symbol 추가만으로 기존 호출자의 ABI는 유지되지만 `FileSystem` virtual surface 변경은
  ABI-breaking이다. core ABI/version을 올리고 모든 filesystem extension과 CXX shim을 함께 rebuild한다.
- directory sync를 pure virtual로 바로 추가하면 외부 filesystem 구현이 깨진다. 호환성이 필요하면
  첫 release에서는 기본 `UNSUPPORTED` 구현을 제공하고 구현별 capability를 점진적으로 강제한다.
- injected filesystem을 사용하는 DB는 snapshot/materializer fingerprint에 filesystem implementation
  ID와 version을 포함해야 한다.

## 9. 검증 계획

### construction ordering

- injected filesystem이 최초 main DB open을 관찰한다.
- existing WAL이 있을 때 startup replay의 모든 open/read/truncate/sync가 injected filesystem을 탄다.
- recovery 실패 시 filesystem이 file handle보다 먼저 파괴되지 않는다.

### routing

- main DB, `.wal`, `.wal.checkpoint`, `.shadow`, temp path가 같은 backend로 resolve된다.
- same-backend rename은 선택된 filesystem에 한 번만 전달된다.
- cross-backend rename은 copy로 조용히 fallback하지 않고 거부된다.
- 기존 secondary scheme filesystem은 계속 동작한다.

### durability/fault injection

- file sync, rename, directory sync 각각의 직전/직후 process kill matrix
- WAL sync failure가 writer를 fail closed로 전환
- checkpoint rotation 중 directory sync failure가 성공으로 보고되지 않음
- partial/torn WAL과 shadow recovery

### compatibility

- filesystem을 전달하지 않은 기존 DB test 전체 통과
- in-memory/read-only/compression/checksum/multi-write 조합
- external shared library와 bundled static library build

## 10. 구현 순서

1. `FileSystem`의 construction-only DB path binding과 ABI fingerprint 갱신
2. `VirtualFileSystem(homeDir, defaultFS)` overload와 기존 constructor delegation
3. `Database(path, config, defaultFS)` overload 및 pre-recovery 설치
4. DB path의 direct `std::filesystem` 우회 제거/제한
5. same-backend two-path dispatch와 cross-backend rejection
6. `syncDirectory` 및 local implementation
7. recovery/routing/durability fault tests
8. ladybug-rust CXX adapter 연결

완료 기준은 custom filesystem이 **최초 DB open부터 startup recovery와 checkpoint 종료까지** 모든
canonical storage I/O를 일관되게 관찰·처리하고, 기존 constructor 동작이 회귀하지 않는 것이다.
