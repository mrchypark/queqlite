# LadybugDB WAL Replication Design

> 상태: **설계 방향 승인, 미구현**
>
> 대상: Rhiza graph materializer, LadybugDB/lbug `0.18.1`
>
> 범위: QuePaxa가 결정한 단일 Cypher 쓰기의 LadybugDB WAL effect 복제

## 결정 요약

Rhiza graph의 첫 WAL 복제 버전은 LadybugDB가 이미 제공하는 시작 시점
`WALReplayer`를 그대로 사용한다. WAL을 열린 데이터베이스에 주입하는 공개 API는
없지만, 데이터베이스를 닫고 결정된 `.wal`을 설치한 뒤 다시 열면
`StorageManager::recover()`가 `WALReplayer`를 실행한다. 따라서 **재시작 기반
capture/install/reopen 복제는 LadybugDB fork 없이 구현 가능**하다.

v1은 WAL suffix를 계속 이어 붙이지 않는다. 매 effect를 다음과 같이 독립적으로
처리한다.

```text
clean checkpointed canonical base
  -> staging clone
  -> one explicit write transaction
  -> capture one complete committed WAL
  -> QuePaxa decides WAL effect
  -> follower quiesce + close
  -> install WAL + reopen/recover
  -> explicit CHECKPOINT
  -> clean checkpointed canonical target
```

이 방식은 매 effect가 하나의 clean base와 clean target 사이를 이동하게 한다.
WAL 세대, 누적 offset, checkpoint 중간 파일 조합을 합의 프로토콜에 노출하지
않는 것이 v1의 핵심 단순화다. 열린 DB에 대한 live apply와 연속 WAL streaming은
후속 최적화다.

### QuePaxa 권위와 slot binding

qlog가 복제 상태의 유일한 권위다. Ladybug WAL, staging 파일, VFS hook, blob은
결정 후보 또는 결정된 qlog entry가 참조하는 artifact일 뿐 독립적인 권위를 갖지
않는다.

- 영구 leader는 없다. preferred proposer는 latency hint일 뿐 exclusive writer가
  아니며, 현재 base를 가진 eligible proposer라면 effect를 만들 수 있다.
- effect는 정확한 `base_log_index/base_log_hash`에 결합되고, 제안 slot은 반드시
  `base_log_index + 1`이어야 한다.
- 다른 command가 그 slot을 차지하면 패배한 WAL은 stale artifact다. WAL bytes나
  result를 새 base에 rebase하거나 재사용하지 않고 즉시 폐기한다.
- retry는 새 clean canonical base clone에서 query를 다시 실행해 새 WAL과 result를
  만들어야 한다.
- follower는 qlog가 결정하지 않은 WAL을 canonical DB에 설치하지 않는다.

## 목표

- 쓰기 Cypher를 winning proposer의 격리된 staging DB에서 정확히 한 번 실행한다.
- 엔진이 생성한 WAL과 bounded query result를 QuePaxa의 결정 대상으로 만든다.
- 모든 replica가 동일한 canonical seed와 WAL effect에서 동일한 graph state로
  수렴하게 한다.
- DDL, insert/update/delete, sequence와 **강제 checkpoint가 발생하지 않는
  WAL-logged 범위의 index** 등 `WALReplayer`가 지원하는 엔진 내부 mutation을
  statement replay 없이 보존한다.
- duplicate delivery, 프로세스 중단, checkpoint 중단, snapshot 설치를 명시적인
  idempotency 규칙으로 복구한다.
- WAL 및 저장 파일 호환성을 capability와 materializer fingerprint로 fail closed
  한다.

## 비목표

- v1에서 열린 LadybugDB handle에 WAL을 적용하지 않는다.
- v1에서 여러 effect의 WAL suffix를 연결하거나 offset만 복제하지 않는다.
- 여러 Cypher transaction을 한 effect로 묶지 않는다.
- `LOAD EXTENSION`, 외부 파일/네트워크 I/O, database attach/export/import 같은
  DB 외부 효과를 복제하지 않는다.
- LadybugDB의 local crash recovery를 QuePaxa consensus로 대체하지 않는다.
- WAL 포맷을 Rhiza가 재해석하거나 자체 구현하지 않는다.

## 확인된 LadybugDB 0.18.1 동작

### Commit과 내구성

쓰기 transaction마다 `LocalWAL`이 생성된다. commit 시 `COMMIT_RECORD`를 추가한
뒤 local WAL 전체를 shared `.wal`에 append하고, WAL sync가 완료될 때까지 기다린
후 in-memory 변경을 publish한다.

- [`Transaction`이 `LocalWAL`을 만들고 commit하는 경로](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/transaction/transaction.cpp#L31-L95)
- [`WAL::logCommittedWAL`과 group commit fsync](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal.cpp#L30-L43)
- [`WAL::waitForDurabilityNoLock`](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal.cpp#L137-L173)

따라서 successful commit 반환 후, writer를 정지한 상태의 `.wal`은 이미 sync된
complete transaction을 포함한다. 그래도 Rhiza는 캡처한 bytes에 별도 digest를
계산하고, effect를 만들기 전에 staging의 `.wal.checkpoint`와 `.shadow`가 없음을
검증한다.

여기에는 중요한 예외가 있다. LadybugDB 0.18.1의 ART `CREATE INDEX`는 serialized
tree size가 `LBUG_CREATE_INDEX_WAL_THRESHOLD`보다 크면 index를 WAL record로 남기는
대신 transaction에 `forceCheckpoint`를 설정한다. threshold 기본값은 256 MiB이며
환경변수로 바뀔 수 있다. `TransactionManager::commit()`은
`auto_checkpoint(false)`와 별개로 publish 뒤 강제 `checkpoint()`를 수행하고, 이
checkpoint가 끝난 후에야 commit이 반환한다. 따라서 이 경로에서는 capture하려던
active WAL이 이미 rotation/cleanup되어 사라질 수 있다.

- [ART CREATE INDEX WAL threshold와 기본값](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/processor/operator/ddl/create_index.cpp#L23-L36)
- [threshold 초과 시 WAL 대신 force checkpoint 선택](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/processor/operator/ddl/create_index.cpp#L110-L136)
- [commit 내부의 force/auto checkpoint 분기](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/transaction/transaction_manager.cpp#L104-L174)

v1은 `forceCheckpoint`를 유발할 수 있는 mutation을 허용하지 않는다. ART
`CREATE INDEX`는 staging 실행 결과가 threshold 이하의 완전한 WAL-logged commit임을
검증할 수 있는 경우에만 effect를 만들며, threshold 초과 또는 검증 불가능이면
qlog proposal 전에 명시적으로 거부한다. staging DB에서 query를 실행했더라도
active WAL이 사라졌거나 base data file이 checkpoint로 변경됐다면 artifact 전체를
폐기하고 canonical DB에는 적용하지 않는다.

### 시작 시점 복구

디스크 DB를 여는 `Database::initMembers()`는 catalog를 사용하기 전에
`StorageManager::recover()`를 호출한다. 이 함수가 `WALReplayer::replay()`를
실행한다.

- [`Database` open과 recover 호출](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/main/database.cpp#L103-L145)
- [`StorageManager::recover`](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/storage_manager.cpp#L120-L124)
- [`WALReplayer`의 active/frozen WAL 선택](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp#L77-L149)

`dryReplay()`는 마지막으로 완전히 역직렬화된 `COMMIT_RECORD`의 끝까지만 replay
offset을 전진시킨다. 실제 replay는 `BEGIN`, `COMMIT`, catalog DDL, index,
table insertion, node/relationship delete/update, copy, sequence 등을 처리한다.

- [마지막 complete commit 선택](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp#L193-L234)
- [WAL record dispatch](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp#L237-L292)

Rhiza는 `throw_on_wal_replay_failure=true`를 유지한다. checksum 또는 decoding 오류를
무시하고 일부까지만 적용하는 best-effort 모드는 replica 수렴 계약에 사용할 수
없다.

### WAL header와 호환성

WAL header에는 database UUID와 `enableChecksums`가 들어간다. Replayer는 WAL의
UUID가 base DB의 UUID와 같은지 확인하고 checksum 설정이 다르면 거부한다.

- [WAL header 생성](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal.cpp#L226-L231)
- [header 및 checksum 설정 검사](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp#L35-L67)
- [database UUID 검사](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp#L168-L175)

그러므로 각 replica를 별도로 생성해서는 안 된다. 모든 replica와 staging은 같은
canonical seed의 byte clone에서 출발해야 한다. DB digest가 같더라도 UUID가 다른
독립 생성 DB는 WAL 대상이 아니다.

단, lbug 0.18.1의 Rust/C API에는 database UUID getter가 없다. fork-free v1은
apply 전에 UUID를 직접 읽어 비교한다고 가정하지 않는다. canonical base의 전체
file digest를 먼저 검증하고, reopen 중 `WALReplayer`가 수행하는 UUID 검사를
fail-closed 최종 방어선으로 사용한다. envelope의 UUID는 디버깅용 optional
provenance로만 둘 수 있으며 correctness 검증 입력이 아니다. 공개 UUID getter는
향후 선택적 preflight 강화 항목이다.

### VFS 적용 범위와 v1 판단

LadybugDB C++ storage는 I/O의 상당 부분을 `VirtualFileSystem`으로 통과시킨다.
main data file은 VFS로 열리고 `FileInfo`를 통해 read/write/sync되며, WAL writer와
`WALReplayer`, shadow file, checkpoint도 VFS 또는 VFS가 만든 `FileInfo`를 사용한다.

- [StorageManager가 WAL과 ShadowFile에 VFS를 전달](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/storage_manager.cpp#L40-L79)
- [persistent data `FileHandle`의 VFS open](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/file_handle.cpp#L31-L50)
- [WAL lazy open과 sync](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal.cpp#L137-L173)
- [WALReplayer의 VFS open/remove/truncate](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp#L77-L182)
- [shadow file read/write/sync](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/shadow_file.cpp#L66-L193)

그러나 v1의 WAL wire/apply 경로를 custom VFS 위에 만들 수는 없다.

- C++에는 `Database::registerFileSystem(unique_ptr<FileSystem>)`가 있지만 constructor가
  startup recovery와 data-file open을 끝낸 뒤에야 호출할 수 있다. DB 자체의 첫
  open/recovery를 가로채지 못한다.
- Rust `lbug` 0.18.1의 `Database`와 CXX bridge에는 filesystem 타입이나 register
  함수가 공개되지 않는다.
- 등록 filesystem은 `canHandleFile(path)` 기준으로 기본 local filesystem과 함께
  동작한다. 이미 local filesystem으로 열린 data handle과 같은 local path를 나중에
  override하면 path dispatch와 `FileInfo`가 보유한 filesystem pointer가 섞일 수 있다.
  기본 `LocalFileSystem`도 `final`이라 단순 subclass wrapper로 교체할 수 없다.
- `VirtualFileSystem::renameFile()`은 path resolver를 사용하지 않고 항상
  `defaultFS`로 보낸다. 따라서 `.wal`에서 `.wal.checkpoint`로의 rotation을 custom
  filesystem이 일관되게 가로챌 수 없다.
- `FileSystem`에는 file sync만 있고 parent-directory fsync API가 없다. 기본
  `LocalFileSystem`도 file `F_FULLFSYNC`/`fdatasync`/`fsync`는 수행하지만 rename 뒤
  directory를 sync하지 않는다.
- VFS와 등록 filesystem은 `unique_ptr`로 DB가 소유하지만 `StorageManager`와
  `FileInfo`는 raw pointer를 보유한다. 모든 file handle보다 filesystem이 오래
  살아야 하며 Rust callback ownership/lifetime 계약도 별도로 필요하다.

근거:

- [C++ register API와 DB member ownership](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/include/main/database.h#L107-L209)
- [constructor recovery 이후 register 구현](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/main/database.cpp#L103-L160)
- [VFS subsystem 선택과 defaultFS rename](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/common/file_system/virtual_file_system.cpp#L40-L102)
- [FileInfo의 filesystem lifetime 의존](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/include/common/file_system/file_info.h#L15-L64)
- [Rust Database 공개 표면](https://github.com/LadybugDB/ladybug-rust/blob/ea283cd1bf5473cd5c233944e3b281eb0d758a45/src/database.rs#L9-L161)
- [Rust CXX bridge의 database 표면](https://github.com/LadybugDB/ladybug-rust/blob/ea283cd1bf5473cd5c233944e3b281eb0d758a45/src/ffi.rs#L155-L175)
- [기본 local rename](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/common/file_system/local_file_system.cpp#L218-L225)
- [기본 local file sync](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/common/file_system/local_file_system.cpp#L533-L565)

따라서 fork-free v1은 VFS interception을 사용하지 않는다. staging commit이 WAL을
sync한 뒤 Rhiza가 외부에서 bytes를 capture하고, follower의 DB handle을 닫은 뒤
Rhiza가 temp write → file fsync → sibling atomic rename → parent-directory fsync로
설치한 다음 기존 startup `WALReplayer`를 사용한다.

향후 live capture가 실제 병목을 해결한다고 측정될 때 필요한 최소 hook은 다음과
같다.

- construction-time, pre-recovery VFS injection
- canonical local path에 대한 명시적이고 일관된 routing/override
- rename을 선택된 filesystem으로 dispatch하는 계약
- parent-directory sync API
- Rust FFI와 filesystem/FileInfo lifetime·thread-safety 계약

이 hook도 capture/apply 최적화일 뿐 qlog와 wire protocol의 권위를 대신하지 않는다.

## 현재 Rhiza graph 수명주기와 필요한 변경

현재 `LadybugStateMachine`은 `RwLock<Option<Database>>`와 단일 writer mutex를
가지므로 handle을 교체하는 lifecycle boundary가 이미 있다.

- [state machine과 lifecycle lock](../crates/rhiza-graph/src/lib.rs#L858-L912)
- [snapshot의 checkpoint, close, sidecar 검사, reopen](../crates/rhiza-graph/src/lib.rs#L1069-L1115)
- [현재 Ladybug config](../crates/rhiza-graph/src/lib.rs#L1490-L1505)
- [현재 인식하는 sidecar 목록](../crates/rhiza-graph/src/lib.rs#L3913-L3920)

다만 현재 graph DB 안에는 `node_id`, `applied_index`, `applied_hash`, request
receipt/result 같은 Rhiza control state도 들어간다. physical effect payload가
이 값을 포함하면 다음 문제가 생긴다.

- `node_id`가 replica마다 달라 canonical base bytes가 달라진다.
- qlog entry hash가 effect payload를 포함하는데 DB 안의 `applied_hash`도 그 entry
  hash를 포함하면 순환 의존이 생긴다.
- 동일 WAL을 적용해도 node-local receipt 상태 때문에 target digest가 달라진다.

WAL mode의 선행 조건은 이 control state를 canonical Ladybug data file 밖의
Rhiza control sidecar로 옮기는 것이다. canonical graph DB에는 사용자 graph와
cluster 전체에 동일한 engine state만 둔다. control sidecar는 최소한 다음을
원자적으로 보존한다.

- cluster/config/recovery generation identity
- node identity
- committed qlog index와 hash
- pending effect identity, base/target digest
- request digest와 persisted bounded result
- snapshot generation과 materializer fingerprint

control sidecar는 qlog와 snapshot으로 재구성 가능해야 하며, LadybugDB가 읽는
`.wal`, `.wal.checkpoint`, `.shadow`, `.tmp`와 다른 파일명과 atomic replace
프로토콜을 사용한다.

## v1 전제 조건

각 effect 시작 시 canonical DB는 반드시 다음 clean-base invariant를 만족한다.

- Ladybug `Database` handle이 정상적으로 열리며 이전 recovery가 완료됐다.
- `.wal`, `.wal.checkpoint`, `.shadow`, `.tmp`가 없다.
- DB file digest가 control sidecar의 committed target digest와 같다.
- committed qlog position과 canonical seed generation이 일치한다.
- 같은 Ladybug storage/build fingerprint와 runtime 설정을 사용한다.
- mutation이 v1 force-checkpoint deny policy를 통과하고, ART index이면 effective
  `LBUG_CREATE_INDEX_WAL_THRESHOLD` 이하의 WAL-logged 결과임이 확인된다.

하나라도 어긋나면 새 query를 실행하거나 WAL을 설치하지 않는다. 먼저 pending
recovery를 끝내거나 trusted snapshot으로 reseed한다.

## WAL effect envelope

다음은 wire contract의 개념 형식이다. 실제 encoding은 기존 qlog command envelope
관례를 따라 명시적 magic/version과 bounded length를 사용한다.

```text
LadybugWalEffectV1 {
  magic: "LGWX",
  version: 1,

  cluster_id,
  config_id,
  recovery_generation,
  base_log_index,
  base_log_hash,

  base_db_digest,
  target_db_digest,
  database_uuid_provenance?: optional,
  storage_version,
  materializer_fingerprint,

  request_id,
  request_digest,
  result_encoding_version,
  bounded_result,

  wal_size,
  wal_digest,
  wal_payload: Inline(bytes) | BlobRef { digest, size, chunking },
}
```

필수 검증 순서는 envelope bounds → magic/version → cluster/config/generation → exact
base position → capability/fingerprint → base DB digest → WAL size/digest 순이다.
검증이 모두 끝나기 전에는 Ladybug 파일을 바꾸지 않는다.

`database_uuid_provenance`는 현재 공개 API로 신뢰성 있게 채우거나 preflight할 수
없으므로 optional 진단 정보다. follower는 이를 correctness 조건으로 사용하지 않고,
reopen 중 Ladybug `WALReplayer`의 내부 UUID 검사를 신뢰한다.

`target_db_digest`는 staging에서 WAL bytes를 별도 보관한 다음 명시적
`CHECKPOINT`를 수행해 clean target을 만들고 계산한다. 이 과정은 WAL capture가
replay 가능한지와 최종 파일이 deterministic한지를 사전에 확인하는 역할도 한다.
target digest가 replica의 checkpoint 결과와 다르면 materializer를 중지하고
snapshot reseed를 요구한다.

## Proposer: staging에서 effect 생성

1. writer/lifecycle boundary에서 canonical DB가 clean-base invariant를 만족하고
   base qlog position이 정확한지 확인한다.
2. canonical data file을 sibling staging 경로로 reflink 또는 byte-copy한다. 원본
   DB와 sidecar는 변경하지 않는다.
3. staging을 동일한 materializer fingerprint로 열되
   `SystemConfig::auto_checkpoint(false)`로 자동 checkpoint를 끈다.
4. staging connection을 만든 직후 `CALL force_checkpoint_on_close=false`를 실행한다.
   Rust `SystemConfig`에는 이 항목의 builder가 없으므로 현재 0.18.1에서는 setting
   query가 필요하다.
5. admission을 통과한 단일 Cypher write를 Rhiza가 만든 explicit transaction 안에서
   한 번 실행하고 bounded result를 수집한 뒤 commit한다.
6. 더 이상 staging query를 실행하지 않는다. `.wal.checkpoint` 또는 `.shadow`가
   있거나, active `.wal`에 완전한 commit이 없거나, staging data file이 강제
   checkpoint로 base digest에서 바뀌었으면 force-checkpoint mutation으로 보고
   캡처를 폐기한다. 이 검증은 특히 큰 ART `CREATE INDEX`에 필수다.
7. **staging `Database`를 drop하기 전에** `.wal` bytes를 읽고 size/digest를
   계산해 Rhiza-owned immutable 임시 artifact에 저장한다. commit은 WAL sync를
   기다리므로 이 시점의 complete WAL을 캡처할 수 있다.
8. 보관한 WAL로부터 effect envelope를 만들 수 있도록 staging에 명시적
   `CHECKPOINT`를 실행하고, close 후 sidecar 부재와 target DB digest를 구한다.
9. proposal이 패배하거나 다른 command가 `base_log_index + 1` slot을 차지하면
   staging과 임시 WAL artifact를 폐기한다. stale WAL/result는 새 base에 재사용하지
   않으며 canonical DB에는 아무 mutation도 없어야 한다.
10. proposal이 결정되면 proposer도 다른 replica와 같은 follower apply 경로로
    canonical DB에 effect를 적용한다.

관련 근거:

- [`SystemConfig::auto_checkpoint`](https://github.com/LadybugDB/ladybug-rust/blob/ea283cd1bf5473cd5c233944e3b281eb0d758a45/src/database.rs#L38-L45)
- [`auto_checkpoint(false)` 및 `force_checkpoint_on_close=false` 사용 예](https://github.com/LadybugDB/ladybug-rust/blob/ea283cd1bf5473cd5c233944e3b281eb0d758a45/src/database.rs#L301-L323)
- [`force_checkpoint_on_close` setting](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/main/settings.cpp#L193-L200)
- [`Database` destructor의 기본 checkpoint](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/main/database.cpp#L147-L154)

`force_checkpoint_on_close=false`가 없으면 staging handle을 drop할 때 WAL이 DB에
합쳐지고 캡처 대상이 사라질 수 있다. 설정 여부와 무관하게 v1은 capture 전에
staging을 drop하지 않는다.

## Follower: install, reopen, replay, checkpoint

결정된 effect 하나를 다음 순서로 적용한다.

1. control sidecar에 effect identity, base/target digest, qlog position을 `pending`으로
   fsync한다.
2. graph writer mutex를 잡고 lifecycle write lock으로 신규 read/write를 차단한다.
   기존 `Connection`과 `QueryResult`가 남아 있지 않음을 보장한다.
3. canonical handle을 `Option`에서 꺼내 drop한다.
4. clean-base sidecar 부재와 base DB digest를 다시 확인한다. 공개 UUID getter는
   없으므로 pre-open UUID 비교는 하지 않는다.
5. WAL bytes를 sibling temp file에 쓰고 file fsync한다.
6. temp file을 canonical `<db>.wal`로 atomic rename하고 parent directory를
   fsync한다. 기존 `.wal`을 덮어쓰지 않는다.
7. canonical DB를 기존과 같은 config로 다시 연다. open 중
   `StorageManager::recover()` → `WALReplayer::replay()`가 complete commit까지
   적용하고 WAL header의 database UUID를 base DB와 비교한다. 모든 replay 오류는
   fatal이다.
8. open 성공 후 즉시 explicit `CHECKPOINT`를 실행한다. 완료 후 handle을 닫고
   `.wal`, `.wal.checkpoint`, `.shadow`, `.tmp`가 모두 없는지 검사한다.
9. clean target data file의 digest가 `target_db_digest`와 같은지 확인한다.
10. DB를 다시 열어 serving 가능한 handle로 설치한다.
11. control sidecar에 committed qlog position, request receipt/result, target digest를
    atomic replace + fsync하고 `pending`을 지운다.
12. lifecycle lock을 풀고 read/write serving을 재개한다.

이 순서에서 Ladybug data file과 Rhiza control sidecar 사이에 하나의 파일시스템
transaction은 없다. 대신 durable `pending` marker와 base/target digest가 두 상태를
연결한다.

## Crash recovery와 idempotency

재시작 시 qlog/control sidecar와 quiesced DB 상태를 다음처럼 분류한다.

| 관찰 상태 | 처리 |
|---|---|
| control이 effect를 committed로 기록 | duplicate delivery로 보고 persisted result 반환 |
| DB digest가 base이고 pending/decided effect 존재 | WAL install부터 다시 수행 |
| `.wal`이 존재하고 DB가 base | Ladybug open recovery를 실행한 뒤 checkpoint |
| `.wal.checkpoint` 또는 `.shadow` 존재 | digest가 target처럼 보여도 먼저 Ladybug open recovery와 checkpoint로 clean invariant를 재확립 |
| DB digest가 target이고 Ladybug sidecar가 없으며 control은 pending/base | DB 적용은 완료된 것으로 보고 control만 finalize |
| base도 target도 아니거나 fingerprint 불일치 | 로컬 추측 복구 금지, serving 중지 후 snapshot reseed |

같은 WAL을 이미 target DB에 다시 설치해서는 안 된다. `.wal.checkpoint` 또는
`.shadow`가 있으면 target digest 관찰보다 engine recovery가 우선한다. sidecar 없는
clean 상태를 재확립한 뒤에만 control을 finalize한다. UUID는 state position을
증명하지 않으므로, 항상 control position과 base/target digest를 먼저 본다.

주요 crash point는 다음과 같다.

- pending marker fsync 전/후
- canonical handle drop 전/후
- WAL temp write/fsync/rename/directory-fsync 각 단계
- reopen recovery 도중
- checkpoint가 `.wal.checkpoint`로 rotate된 직후
- shadow pages 생성/적용 도중
- clean target 생성 후 control finalize 전

Ladybug가 만든 `.wal.checkpoint`와 `.shadow`는 checkpoint crash recovery 세트다.
Rhiza가 둘 중 하나를 독자적으로 복사·삭제·조합하지 않는다. v1 effect에는 active
`.wal`만 들어가며, staging capture 시 checkpoint sidecar가 보이면 effect 생성을
실패시킨다.

## Snapshot 계약

Snapshot은 WAL chain을 포함하지 않고 clean checkpointed DB만 포함한다.

1. writer와 lifecycle lock으로 materializer를 quiesce한다.
2. explicit `CHECKPOINT`를 실행하고 handle을 닫는다.
3. Ladybug sidecar가 전혀 없는지 확인한다.
4. DB bytes/digest, storage version, materializer fingerprint, committed qlog position,
   request recovery metadata를 snapshot envelope에 넣는다. UUID provenance는 필요하면
   optional 진단 정보로만 둔다.
5. 설치 시 DB temp write + fsync + atomic rename + directory fsync를 수행하고 기존
   Ladybug sidecar를 제거한 clean location에서 연다.
6. node-local identity는 snapshot DB 안을 수정해 rebind하지 않고 새 control
   sidecar에 기록한다.

Snapshot 설치 후 첫 WAL effect의 `base_db_digest`는 설치한 snapshot digest와
정확히 같아야 한다.

## 크기 제한과 blob 전달

QuePaxa command payload 한도가 256 KiB이므로 다음 정책을 사용한다.

- envelope과 WAL 합계가 inline 한도 이하면 qlog entry에 직접 싣는다.
- 한도를 넘는 WAL은 blob transport가 구현되기 전까지 `resource exhausted`로
  명시적으로 거부한다. 같은 요청을 logical replay로 자동 fallback하지 않는다.
- blob transport 도입 후에는 recorder quorum에 content-addressed blob 전체를
  먼저 내구화하고, 단일 qlog entry에는 digest/size/chunk manifest만 결정한다.
- 하나의 transaction WAL을 여러 개의 독립 qlog command로 나누지 않는다.
- follower는 전체 blob의 size와 digest를 확인한 뒤에만 install한다.
- blob retention은 해당 qlog entry가 snapshot으로 대체되고 모든 필요한 replica가
  복구 가능한 시점까지 유지한다.

## Capability와 materializer fingerprint

WAL은 엔진 내부 포맷이므로 SQL/Cypher text보다 강한 동질성 검사가 필요하다.
fingerprint에는 최소한 다음이 들어간다.

- effect encoding `LGWX/1`
- `lbug` crate 및 bundled LadybugDB version (`0.18.1`)
- `lbug::get_storage_version()`
- checksums, compression, multi-write, page/storage 관련 config
- effective `LBUG_CREATE_INDEX_WAL_THRESHOLD` 값과 force-checkpoint deny policy version
- Rhiza graph schema/materializer version
- WAL effect admission policy version
- 필요하면 target/endianness 및 storage-affecting build feature

모든 voter와 materializer가 `ladybug-wal-effect-v1` capability 및 같은 fingerprint를
광고하기 전에는 WAL effect admission을 활성화하지 않는다. rolling upgrade 중
불일치는 proposal 전에 거부하며, apply 중 발견되면 replica를 중지하고 compatible
snapshot 또는 binary로 복구한다.

## 보안 및 admission 경계

WAL replay가 어떤 record type을 구현한다는 사실은 해당 query를 Rhiza가 허용해야
한다는 뜻이 아니다. 특히 `LOAD_EXTENSION_RECORD`도 replay 대상이므로 별도
admission이 반드시 필요하다.

v1에서 계속 금지할 범주:

- extension install/load 및 임의 native code
- extension/UDF/vtable이 수행하는 외부 파일, 네트워크, process I/O
- `COPY TO`, export/import, attach/detach 등 외부 경로나 다른 DB를 변경하는 명령
- 모든 local-file 및 HTTP/HTTPS `LOAD FROM`, file scan, CSV/JSON/Parquet 등 외부
  source scan/import. DB 밖의 입력을 읽기만 하더라도 replica가 결과를 독립 검증할
  수 없고 proposer 권한으로 임의 I/O를 수행하므로 금지한다.
- `forceCheckpoint`를 유발하는 mutation. 특히 effective threshold를 초과하거나
  WAL-logged 결과임을 확인할 수 없는 ART `CREATE INDEX`는 v1에서 거부한다.
- 사용자 제어 `BEGIN`, `COMMIT`, `ROLLBACK`, `CHECKPOINT`
- credential/secret 및 노드별 설정 변경
- DB 밖의 상태를 읽어 graph mutation을 결정하는 비결정적 extension

시간·random처럼 **DB 내부 결과만** 비결정적인 표현은 winning proposer가 한 번
실행하고 WAL과 result를 복제하므로 원칙적으로 허용할 수 있다. 단, 외부 효과가
없고 result 크기 제한과 type encoding을 통과해야 한다.

WAL payload는 합의된 내부 artifact라도 손상되거나 악의적으로 구성될 수 있다.
envelope bounds와 digest를 먼저 검사하고, Ladybug checksum을 켠 채 전용
materializer process/권한 경계에서 replay한다.

## 검증 계획

### 기본 동작

- 동일 seed의 3개 replica에 create/alter/drop, node/relationship
  insert/update/delete, sequence와 강제 checkpoint가 없는 WAL-logged index effect를
  적용해 동일 target digest 확인
- 작은 test threshold를 사용해 ART `CREATE INDEX`의 threshold 이하 WAL capture와
  threshold 초과 force-checkpoint 거부를 각각 검증
- bounded result와 duplicate request가 원래 결과를 그대로 반환하는지 확인
- losing proposal이 canonical DB와 control sidecar를 변경하지 않고, stale WAL/result를
  다음 slot에 재사용하지 않는지 확인
- `slot == base_log_index + 1` 위반과 stale base를 proposal/apply 전에 거부
- independent-created DB의 UUID mismatch가 reopen 중 `WALReplayer`에서 fatal인지 확인

### WAL 무결성

- WAL 중간/끝 truncation, byte corruption, checksum 설정 mismatch
- `COMMIT_RECORD` 직전 truncation이 state로 노출되지 않는지 확인
- WAL digest mismatch가 Ladybug open 전에 거부되는지 확인
- storage/build fingerprint mismatch와 unsupported effect version 거부

### Crash matrix

- follower 절차의 모든 fsync/rename/reopen/checkpoint/control-finalize 경계에서
  process kill 후 재시작
- `.wal.checkpoint`만 존재, `.shadow`까지 존재, target DB만 완료된 상태 복구
- 같은 decided effect 반복 적용과 recovery 반복의 idempotency
- base/target 어느 것과도 다른 DB에서 fail closed + snapshot reseed

### 동시성과 snapshot

- 장시간 read와 `QueryResult`가 있을 때 quiesce가 안전하게 drain하는지 확인
- apply 중 신규 read/write가 serving되지 않는지 확인
- snapshot 생성/설치 직전·직후 effect와 qlog position 일치
- snapshot 설치 후 첫 effect의 base digest 검증과 WALReplayer 내부 UUID mismatch 검증

### 제한과 보안

- inline 경계 바로 아래/위 WAL, oversized WAL/result의 `resource exhausted`, malformed
  length 필드
- force-checkpoint 뒤 active WAL 부재 또는 staging base file 변경을 proposal 전에 거부
- blob missing/corruption/partial availability와 retention
- extension load, external copy/export, attach, transaction/checkpoint control 거부
- fuzzing으로 effect envelope parser가 allocation/overflow bounds를 지키는지 확인

## 구현 단계

### 1단계: restart-per-effect correctness

1. graph control metadata를 Ladybug data file 밖으로 이동한다.
2. clean-base invariant와 canonical seed/snapshot 계약을 구현한다.
3. staging clone, checkpoint controls, one-transaction WAL capture를 구현한다.
4. `LadybugWalEffectV1` inline envelope와 follower restart apply를 구현한다.
5. crash/idempotency/internal-UUID/checksum/fingerprint/sidecar 테스트를 통과시킨다.

이 단계가 본 문서의 승인된 구현 목표다.

### 2단계: large effect blob

Recorder quorum의 content-addressed blob 선저장과 단일 manifest decision을 추가한다.
운영 측정 전에는 별도 범용 blob 추상화를 만들지 않는다.

### 3단계: live import 최적화

restart 비용이 실제 병목이라는 측정이 나온 경우에만 LadybugDB fork/FFI의 작은
API를 검토한다.

- committed `LocalWAL` 또는 exact shared-WAL transaction bytes export
- force checkpoint가 실행되기 전 committed `LocalWAL`을 export하는 hook. 이 hook이
  검증되기 전에는 threshold 초과 ART `CREATE INDEX`를 지원하지 않는다.
- 열린 DB의 writer gate 아래 recovery WAL import
- apply 완료/실패 시 cache, catalog, transaction manager 상태의 원자적 갱신
- WAL generation/offset 및 checkpoint와의 동기화

이 단계에서는 기존 private C++ type을 Rust에 그대로 노출하지 않고, versioned
byte envelope과 명확한 ownership을 가진 최소 API를 upstream 가능한 형태로 만든다.
continuous WAL suffix streaming은 이 API와 checkpoint generation 프로토콜이
검증된 뒤에만 고려한다.

## 열린 쟁점

- 직접 실행 후 checkpoint한 staging과 WAL recovery 후 checkpoint한 follower의
  data file bytes가 지원할 모든 mutation에서 항상 동일한지 통합/property test로
  확정해야 한다.
- target file digest 대신 안정적인 engine-provided logical checksum이 필요한지
  측정해야 한다.
- WAL/result가 256 KiB를 넘는 실제 분포를 수집해 blob 단계의 우선순위를 정한다.
- restart-per-effect의 reader drain 및 reopen latency가 목표 처리량을 만족하는지
  benchmark로 확인한다.

이 쟁점은 v1의 correctness 경계를 완화하지 않는다. 검증 실패 시 해당 mutation을
admission에서 막거나 snapshot reseed하며, 자동으로 statement replay에 섞지 않는다.

## 참고 자료

- [LadybugDB transactions](https://docs.ladybugdb.com/cypher/transaction/)
- [LadybugDB 0.18.1 `WALReplayer`](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal_replayer.cpp)
- [LadybugDB 0.18.1 WAL writer](https://github.com/LadybugDB/ladybug/blob/v0.18.1/src/storage/wal/wal.cpp)
- [LadybugDB 0.18.1에 고정된 Rust API revision의 `SystemConfig`](https://github.com/LadybugDB/ladybug-rust/blob/ea283cd1bf5473cd5c233944e3b281eb0d758a45/src/database.rs)
