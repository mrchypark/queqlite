# Rhiza SQLite WAL/Page Replication 설계

> 상태: **QWAL-only 브레이킹 전환 핵심 경로 구현 완료**
>
> 범위: QuePaxa가 결정한 SQLite 물리 효과의 생성·전달·적용·복구 계약
>
> 1차 형식: `QWAL v1 / sqlite_page_after_image_v1`
>
> 후속 최적화 후보: VFS candidate-only diff 및 SQLite native WAL frame 캡처

## 1. 결정 요약

Rhiza의 SQLite 쓰기는 장기적으로 SQL 문장 재실행이나 SQLite Session changeset 대신,
**결정된 슬롯에서 이긴 제안자가 한 번 실행해 만든 물리 효과**를 복제한다. QuePaxa는
여전히 SQLite를 해석하지 않고 versioned opaque payload만 순서화한다.

첫 구현은 SQLite의 raw `-wal` 파일을 그대로 운반하지 않는다. 정확한 qlog base에서 만든
staging DB를 정상 commit·checkpoint·close한 뒤, closed base/target 전체를 page 단위로 비교해
각 changed page의 최종 이미지(after-image)를 canonical envelope로 만든다. 이 full closed-file
diff가 현재 correctness 경로다. named `QwalRecordingVfs`는 production prepare staging에
shadow/audit mode로 연결되어 changed-page 후보와 commit·checkpoint·close·sync evidence를
수집한다. 다만 wire page source는 아직 항상 full closed-file diff이며, VFS candidate만으로
diff 범위를 줄이는 최적화는 미구현이다. 이 문서에서는 이 전송 계층 전체를
`QWAL`이라 부르고 첫 codec을 `sqlite_page_after_image_v1`이라 부른다.

이 선택의 이유는 다음과 같다.

- raw WAL frame은 WAL salt, 선행 frame checksum chain, commit marker, checkpoint generation에
  결합된다.
- `-shm`의 WAL-index는 지속 복제 대상이 아니며 프로세스가 다시 만들 수 있는 파생 상태다.
- closed-file page after-image는 WAL generation을 wire protocol에 노출하지 않으면서 같은
  최종 SQLite 파일을 만든다.
- full closed-file diff가 changed-page 탐색의 기준 결과를 제공하고, target 전체 digest와
  `integrity_check`가 최종 correctness를 증명한다.
- staging 전용 VFS shadow/audit는 구현되었지만 wire format과 apply 검증을
  바꾸지 않는다. candidate-only diff를 추가하더라도 full diff fallback과 target
  검증은 유지한다.
- 적용은 authoritative DB를 직접 덮지 않고 temp clone에서 검증한 뒤 atomic rename할 수 있다.

native WAL frame codec은 page codec의 정확성과 운영성이 검증되고, 측정으로 복사·diff 비용이
병목임이 확인된 뒤에만 추가한다.

## 2. 목표와 비목표

### 목표

- winning slot proposer가 실행한 SQLite 결과를 모든 replica에 byte-identical하게 반영한다.
- DDL, trigger, foreign key cascade, PK 없는 테이블, `AUTOINCREMENT`, SQLite 내부
  비결정성, bounded `RETURNING`을 statement replay 없이 지원한다.
- 패배한 제안과 프로세스 crash가 authoritative DB에 부분 상태를 남기지 않게 한다.
- duplicate delivery와 apply 재시작을 멱등하게 처리한다.
- qlog, snapshot, recorder durability 계약을 유지한다.
- SQLite 버전·page size·compile options가 다른 노드의 잘못된 적용을 fail closed로 막는다.

### 비목표

- QuePaxa에 영구 leader나 전용 SQLite writer leader를 추가하지 않는다.
- `ATTACH`된 여러 DB, TEMP schema, connection-local 상태를 복제하지 않는다.
- extension, virtual table, 사용자 함수가 만든 파일·네트워크 등 DB 밖의 효과를 복제하지 않는다.
- `-shm` 파일이나 WAL reader lock 상태를 복제하지 않는다.
- 첫 버전에서 임의 크기 effect, live page patch, raw WAL frame streaming을 지원하지 않는다.
- physical effect를 서로 다른 SQLite storage format 사이의 논리 migration 수단으로 쓰지 않는다.

## 3. 구현 상태와 코드 경계

현재 `rhiza-sql`은 레거시 모드 선택지 없이 QWAL-only로 열린다. 기존 DB를 자동
변환하거나 결정된 QSQL/QEFX를 QWAL과 함께 decode하는 rolling 경로는 없다.

| 상태 | 구현 책임 | 코드 기준점 |
|---|---|---|
| 구현 | canonical user DB와 mandatory `.control` sidecar를 함께 생성·검증 | [`SqliteStateMachine::open`](../crates/rhiza-sql/src/lib.rs), [`ControlStore`](../crates/rhiza-sql/src/control.rs) |
| 구현 | `DELETE` journal·`FULL` synchronous control store에 identity, tip, receipt, pending intent 저장 | [`control.rs`](../crates/rhiza-sql/src/control.rs) |
| 구현 | `Mutex<Option<Connection>>`과 lifecycle gate로 checkpoint·close·replace·reopen 직렬화 | [`SqliteStateMachine`](../crates/rhiza-sql/src/lib.rs) |
| 구현 | exact-base clone에서 commit한 후 closed base/target 전체 diff로 QWAL page after-image 생성 | [`prepare_sql_effect`](../crates/rhiza-sql/src/lib.rs), [`diff_closed_databases`](../crates/rhiza-sql/src/qwal.rs) |
| 구현 | canonical QWAL codec, 256 KiB envelope 상한, page/size/digest 검증 | [`QwalEnvelopeV1`](../crates/rhiza-sql/src/qwal.rs) |
| 구현 | pending intent 후 temp clone에 page를 적용·검증하고 authoritative DB를 atomic rename | [`apply_entry_with_result`](../crates/rhiza-sql/src/lib.rs), [`apply_qwal_to_file`](../crates/rhiza-sql/src/qwal.rs) |
| 구현 | user DB와 replicated control state를 `QSNP v1` container로 snapshot/restore | [`create_snapshot`](../crates/rhiza-sql/src/lib.rs), [`restore_snapshot_file`](../crates/rhiza-sql/src/lib.rs) |
| 구현 | 기존 DB의 `__rhiza_meta`/`__rhiza_requests`, direct QSQL/QEFX apply 거부 | [`reject_legacy_user_database`](../crates/rhiza-sql/src/lib.rs), [QWAL contracts](../crates/rhiza-sql/tests/qwal_contracts.rs) |
| 구현 | named non-default `QwalRecordingVfs`를 production staging에 shadow/audit로 연결, candidate coverage를 full diff와 대조 | [`qwal_vfs.rs`](../crates/rhiza-sql/src/qwal_vfs.rs), [`prepare_sql_effect`](../crates/rhiza-sql/src/lib.rs) |
| 구현 | Node runtime/startup/restore가 SQLite command를 qlog append 또는 staged publish 전에 canonical QWAL로 검증 | [`validate_profile_entry_shape`](../crates/rhiza-node/src/lib.rs), [`durability.rs`](../crates/rhiza-node/src/durability.rs) |
| 구현 | Node SQL batching·legacy put을 요청별 exact-base QWAL 제안으로 전환 | [`rhiza-node`](../crates/rhiza-node/src/lib.rs), [Node contracts](../crates/rhiza-node/tests/contracts.rs) |
| 미구현 | VFS candidate page만 읽어 diff 범위를 줄이는 최적화 | wire page source는 아직 항상 full closed-file diff |
| 미구현 | 256 KiB를 넘는 recorder-quorum blob, native WAL frame codec | 후속 단계 |

QSQL v2는 client request의 canonical preparation input으로만 남아 있다. qlog에 결정되는
`EntryType::Command` payload는 `QWAL\0\x01`이어야 하며, QSQL/QEFX/QBCH payload를 direct apply하는
호환 decoder는 제공하지 않는다. 레거시 write-batch preparation API도 제거되었다.

## 4. 핵심 불변식

### 4.1 영구 leader가 아니라 winning slot proposer

QuePaxa에는 영구 leader가 없다. 여러 노드가 같은 다음 slot에 서로 다른 payload를 제안할 수
있고, 결정된 payload를 만든 제안자만 그 slot의 winning proposer다. 따라서 문서와 구현은
`leader WAL`이 아니라 `winning-slot effect`를 다룬다.

### 4.2 exact-base 실행

effect의 base는 반드시 다음을 모두 만족해야 한다.

- `base_index == local_control.applied_index`
- `base_hash == local_control.applied_hash`
- `base_db_digest == digest(canonical_user_db)`
- 제안 slot은 `base_index + 1`

경합으로 다른 payload가 slot을 차지하면 기존 staging과 effect를 폐기한다. 새 qlog tip을
적용한 뒤 새 base에서 요청의 receipt를 확인하고, 필요할 때만 다시 실행한다. stale effect를
다음 slot에 재사용하지 않는다.

### 4.3 authoritative DB에서 speculative execution 금지

제안이 결정되기 전에 authoritative DB에서 SQL을 실행하면 패배한 제안도 로컬 상태를
변경한다. transaction rollback만으로 충분하지 않은 statement나 extension 부작용도 있다.
모든 제안 실행은 exact base의 별도 staging clone에서 수행한다.

```text
canonical DB@base --clone--> staging DB
                              |
                              +-- execute + commit + checkpoint + close
                              +-- diff(base, target) -> QWAL proposal

QuePaxa slot 결정
  - staging payload 승리: 결정된 effect를 표준 apply 경로로 반영
  - 다른 payload 승리: staging 및 effect 폐기
```

winning proposer도 자신의 staging 파일을 곧바로 authoritative DB로 승격하지 않는다. 모든
노드가 같은 validation·crash-recovery 경로를 지나도록 결정된 envelope를 apply한다. 이후
최적화로 이미 검증된 staging 파일을 재사용하더라도 target digest와 envelope byte equality를
먼저 증명해야 한다.

## 5. canonical user DB와 Rhiza control sidecar

### 5.1 분리 이유

모든 replica의 canonical SQLite bytes가 같아야 page effect의 base digest가 의미가 있다.
현재 DB 내부의 `node_id`는 노드마다 다르므로 이 조건을 깨뜨린다. `applied_hash`를 effect로
수정하려 하면 다음 순환 의존도 생긴다.

```text
effect payload -> qlog entry hash -> DB 안 applied_hash -> effect payload
```

따라서 QWAL 활성화 전에 Rhiza control state를 user DB 밖으로 이동한다.

### 5.2 파일 책임

`sqlite.db`는 다음만 포함한다.

- 사용자가 만든 main schema와 데이터
- SQLite 자체 schema와 file-format metadata
- DB 내부 효과만 내는, 명시적으로 허용된 built-in virtual table의 shadow table

`sqlite.control` sidecar는 다음을 포함한다.

- cluster/epoch/configuration identity와 recovery generation
- node-local `node_id`
- `applied_index`, `applied_hash`
- canonical DB digest; pending apply의 target byte length
- request ID, request digest, original log anchor, bounded result receipt
- pending apply intent와 target digest
- materializer fingerprint

현재 sidecar는 SQLite `ControlStore`로 구현되며 `journal_mode=DELETE`,
`synchronous=FULL`을 사용한다. user DB page effect와 같은 파일에 넣지 않고 page
effect payload의 일부로도 싣지 않는다. snapshot은 user DB bytes와 replicated control state를
같은 qlog anchor에 결합하되, restore 시 node-local identity를 대상 노드 값으로 다시 바인딩한다.

### 5.3 cross-file crash 원자성

user DB atomic rename과 sidecar commit은 하나의 filesystem transaction이 아니다. 다음 상태
판별이 protocol의 원자성 경계다.

- DB digest가 effect의 `base_db_digest`: physical apply가 아직 안 됨
- DB digest가 `target_db_digest`: physical apply는 끝났고 sidecar만 완료하면 됨
- 둘 다 아님: 자동 patch를 금지하고 snapshot reseed가 필요한 divergence로 격리

DB rename 전에 sidecar에 pending intent를 fsync하고, rename과 parent-directory fsync 뒤에
sidecar를 committed로 전환한다. 재시작은 intent와 실제 DB digest를 함께 검사한다.

## 6. QWAL v1 envelope

QWAL은 canonical binary encoding을 사용한다. JSON은 page bytes의 크기와 canonicality 때문에
사용하지 않는다. 정수 byte order, 배열 정렬, 중복 금지, reserved field 처리까지 codec
contract로 고정한다.

```text
QwalEnvelopeV1 {
  magic: "QWAL\\0\\x01",

  cluster_id,
  epoch,
  configuration_id,
  recovery_generation,

  base_index,
  base_hash,
  base_db_digest,
  base_file_bytes,

  materializer_fingerprint,
  page_size,

  request_id,
  request_digest,
  result_blob,

  target_file_bytes,
  target_db_digest,
  pages: [{ page_no, after_image }...]
}
```

검증 규칙은 다음과 같다.

- `page_size`는 base SQLite header와 local capability가 허용하는 값이어야 한다.
- page number는 1부터 시작하며 오름차순이고 중복이 없어야 한다.
- 모든 `after_image` 길이는 정확히 `page_size`여야 한다. 마지막 부분 page는 허용하지 않는다.
- 모든 page는 `target_file_bytes` 범위 안에 있어야 한다.
- 파일 축소는 `target_file_bytes`로 표현하고, 잘려 나간 page image를 싣지 않는다.
- page 1을 포함하면 그 안의 page size와 SQLite format header를 다시 검사한다.
- `base_file_bytes`와 `target_file_bytes`는 page size의 배수여야 한다.
- 동일 base/target인 no-op transaction은 빈 page 배열을 허용한다.
- `request_digest`는 preparation input으로 받은 canonical QSQL v2 request bytes에 대한 digest다.
- `result_blob`은 현재 QRES의 row/byte limit를 계승하고 decode/canonical re-encode를 검증한다.
- `materializer_fingerprint`는 SQLite source ID, compile options, Rhiza policy와
  QWAL codec version을 결합한다.
- envelope은 별도 `body_digest`를 두지 않고 canonical payload를 결정한 qlog entry hash에
  결합된다. entry hash 자체를 envelope 안에 넣지 않는다.
- decode 후 canonical re-encode한 bytes가 입력과 다르면 거부한다.

`target_db_digest`는 page 적용 후 전체 closed DB 파일의 digest다. 첫 버전은 전체 파일 digest
비용을 받아들인다. 성능 측정 없이 Merkle tree나 incremental hash를 먼저 도입하지 않는다.

QWAL wire envelope은 page를 찾은 방법과 무관하다. `QwalRecordingVfs`, full closed-file diff,
향후 다른 recorder가 같은 base/target에서 같은 page after-image를 만들면 동일 wire codec을
사용한다. VFS 내부 path, file handle, write 순서, WAL-index 상태는 envelope에 넣지 않는다.

### staging 전용 `QwalRecordingVfs`

> 현재 상태: **production shadow/audit 연결 구현**. QWAL v1의 correctness와 wire page
> source는 아직 closed base/target 전체 diff에 의존한다. VFS candidate는 모든 actual
> changed page를 포함하는지 full diff와 대조하며, candidate-only diff 최적화는 미구현이다.

현재 rusqlite 0.40.1은 등록된 VFS를 이름으로 선택하는
[`Connection::open_with_flags_and_vfs`](https://docs.rs/rusqlite/0.40.1/rusqlite/struct.Connection.html#method.open_with_flags_and_vfs)를
제공한다. `libsqlite3-sys` 0.38.1은
`sqlite3_vfs`, `sqlite3_file`, `sqlite3_io_methods` raw ABI와
[`sqlite3_vfs_register`](https://docs.rs/libsqlite3-sys/0.38.1/libsqlite3_sys/fn.sqlite3_vfs_register.html)를
포함한 find/register/unregister 함수를 노출한다. 안전한 Rust VFS trait은 없으므로
`QwalRecordingVfs`는 작은 semantic hook이지만 구현은 audited unsafe FFI 경계다.

현재 `QwalRecordingSession` 초기화와 named VFS open은 production
`prepare_sql_effect` staging 경로에서 수행된다. recorder 등록·open·evidence·seal 오류,
incomplete recording, candidate coverage mismatch는 모두 recording을 사용하지 않고 full closed-file
diff 결과를 권위 값으로 사용하게 한다.

VFS는 `makeDflt=0`으로 한 번 등록하고 Rhiza staging connection만 이름으로 선택한다.
authoritative DB, 다른 crate, 프로세스의 기본 SQLite VFS를 바꾸지 않는다. lower VFS는
audited built-in 이름만 허용한다: Unix의 `unix`, `unix-none`, `unix-dotfile`, `unix-excl`과
Windows의 `win32`, `win32-longpath`, `win32-none`다. 그 외의 lower VFS에서는 recorder를
사용하지 않고 full diff로 fallback한다. wrapper는 underlying platform VFS를 저장하고
다음을 정확히 delegate한다.

- open/close/read/write/truncate/sync와 file-size
- lock/unlock/reserved-lock 및 모든 file-control
- `xShmMap`, `xShmLock`, `xShmBarrier`, `xShmUnmap`
- `xFetch`/`xUnfetch`, sector size와 device characteristics
- pathname, access/delete, randomness, `xSleep`, `xCurrentTime`, `xGetLastError`와 동적 로더 정책
- underlying `iVersion`이 제공하는 v2/v3 optional method 전체: `xCurrentTimeInt64`,
  `xSetSystemCall`, `xGetSystemCall`, `xNextSystemCall`

등록된 VFS, 이름, method table, `pAppData`와 wrapper state는 모든 사용 connection보다 오래
살아야 한다. wrapper `szOsFile`은 최소
`align_up(wrapper_header) + underlying.szOsFile`이고 underlying `sqlite3_file` storage의
alignment도 만족해야 한다. 각 file callback은 wrapper 안에 embedded된 underlying
`sqlite3_file` pointer로 위임한다. 실패한 `xOpen` 뒤 `xClose` 가능성까지 SQLite ABI를
따르며, Rust panic은 어떤 C callback 경계도 넘어가면 안 된다.

recording이 arm된 staging request 동안 다음을 수집한다.

- `SQLITE_OPEN_MAIN_DB`의 `xWrite(offset, length)`, `xTruncate`, `xSync`
- `SQLITE_OPEN_WAL`의 open/write/truncate/sync lifecycle
- 허용된 staging DB 외의 예상하지 않은 persistent main DB open

main DB의 arbitrary write range `[offset, offset + length)`와 겹치는 모든 page를 changed set에
넣는다. write가 page-aligned이거나 한 호출이 한 page라는 가정은 금지한다. truncate된 뒤
존재하는 changed page의 최종 bytes는 모든 connection을 닫은 staging 파일에서 읽는다.
recorder overflow, write-range 누락 가능성, callback/evidence/seal 오류, unexpected persistent DB
open, incomplete recording, full-diff coverage mismatch에서는 VFS set을 wire page source로 신뢰하지
않고 closed base/target 전체 diff를 사용한다.

`xShm*`은 WAL-index lock과 shared-memory coordination이며 복제 payload가 아니다. wrapper는
관측 metric을 남길 수 있지만 값이나 `-shm` bytes를 QWAL에 싣지 않는다. `xWrite`나 `xSync`
한 번만 보고 transaction commit을 판정해서도 안 된다. 실제 DB change가 있는 transaction은
successful SQLite transaction commit, connection profile이 요구한 main/WAL sync, explicit
checkpoint 성공, 모든 main/WAL handle close를 확인한 뒤 recording을 seal한다. 반면
`SQLite changes == 0`이고 closed base와 target digest가 같은 검증된 no-op/result-only
transaction은 WAL hook이 없어도 빈 page effect와 persisted result/receipt로 seal할 수 있다.

### raw WAL을 v1으로 쓰지 않는 이유

SQLite WAL header와 frame에는 page size, checkpoint sequence, salt, 누적 checksum이 있다.
commit은 commit frame의 database-size 필드로 표시되며, WAL-index는 `-shm`에 있는 별도 파생
상태다. 단순히 한 노드의 WAL suffix를 다른 노드의 열린 DB에 붙이면 base generation,
checksum chain, reader/checkpoint 상태가 어긋날 수 있다.

후속 `sqlite_wal_frames_v1` codec은 다음 조건을 충족할 때만 검토한다.

- recording VFS가 arbitrary split WAL write를 재조립하고, WAL hook과 함께 transaction
  boundary와 정확한 frame 범위를 제공한다.
- base WAL salt/checksum/checkpoint generation을 envelope로 검증한다.
- follower DB를 quiesce·close하고 WAL을 atomic install한 뒤 reopen하여 WAL-index를 안전하게
  재생성하는 절차가 있다.
- page codec 대비 end-to-end 성능 개선이 측정된다.

public SQLite API에는 열린 connection에 외부 WAL frame을 주입하고 WAL-index를 publish하는
기능이 없다. 따라서 raw WAL의 첫 apply도 `close -> WAL install/fsync -> stale -shm 제거 ->
reopen/recovery`여야 한다. live apply는 SQLite fork나 upstream에 validated frame append와
WAL-index refresh API가 생길 때만 검토한다. `xShm*`을 직접 고쳐 live apply를 흉내 내지 않는다.

## 7. 제안 알고리즘

쓰기 요청을 받은 어느 노드든 proposer가 될 수 있다.

1. request ID와 canonical QSQL payload를 검증한다.
2. control sidecar에서 이미 같은 request digest의 receipt가 있으면 저장된 결과를 반환한다.
   같은 request ID와 다른 digest면 conflict로 거부한다.
3. materializer base lease를 잡아 qlog apply와 checkpoint를 잠시 막고, qlog tip과 sidecar tip,
   canonical DB digest가 exact base인지 확인한다.
4. 열린 connection을 quiesce하고 canonical DB가 closed/checkpointed 상태일 때 같은 filesystem의
   immutable base artifact를 만든다. 열린 DB 본체만 복사하지 않는다. artifact digest가
   base digest와 같은지 확인하고, artifact의 별도 복제본을 writable staging target으로 만든
   뒤에만 base lease를 푼다. 우선 portable file copy를 사용하고 reflink는 검증된 최적화로만
   사용한다. 이후 canonical tip이 전진해도 immutable artifact는 proposal 완료까지 보관하며,
   slot이 이미 결정됐으면 이 제안은 패배한 것으로 처리한다.
5. staging에 `QwalRecordingSession`을 arm하고 named non-default VFS와 고정 connection
   profile로 열어 auto-checkpoint를 끈다. recorder를 사용할 수 없으면 기본 VFS로
   staging을 열고 full diff 경로를 계속한다.
6. 명시적 transaction에서 요청 전체를 한 번 실행하고 bounded `RETURNING` 결과를 수집한다.
7. DB change가 있으면 SQLite commit, WAL hook 통지와 요구된 sync를 확인하고
   `sqlite3_wal_checkpoint_v2(..., TRUNCATE, ...)` 상당 절차를 수행한다. VFS `xWrite` 하나를
   commit 증거로 사용하지 않는다. `changes == 0`인 후보는 closed target이 immutable base와
   byte-identical한지 뒤에서 검증해야 no-op으로 인정한다.
8. 모든 connection을 닫고 `-wal`이 없거나 길이 0이며 `-shm`이 제거 가능한 파생 상태인지
   확인한다. checkpoint busy나 sync 실패면 불완전 target을 사용하지 않는다.
9. staging DB에 `quick_check` 또는 구현 단계에서 정한 동등한 검증을 수행한 뒤 다시 닫는다.
10. 보관한 immutable base artifact와 closed target을 page 단위로 전체 비교하고, target에서
    최종 after-image를 읽어 page number로 정렬한다. sealed VFS candidate가 complete인
    경우 actual full-diff changed page를 모두 포함하는지 audit한다. 현재 wire page set은
    audit 결과와 무관하게 항상 full diff에서 만든다. 검증된 no-op은 빈 page set이다.
11. target 전체 digest와 envelope body digest를 계산한다.
12. inline 상한을 만족하면 QWAL bytes를 slot `base_index + 1`에 제안한다. 초과하면 v1에서는
    명시적으로 거부한다.
13. QuePaxa 결정 후 결정된 payload를 일반 apply 경로로 넘긴다.

checkpoint와 검증을 위해 staging을 재개방할 때 쓰이는 SQLite 자체 header 변화도 target bytes의
일부다. 구현이 이해하지 못하는 header byte를 임의로 0으로 만드는 식의 정규화는 금지한다.
여기서 “normalized page effect”는 raw WAL chain 대신 최종 page after-image를 canonical하게
정렬한다는 뜻이다.

## 8. 적용 알고리즘

결정된 QWAL entry를 적용할 때 다음 순서를 지킨다.

1. qlog entry hash와 cluster/epoch/configuration/recovery identity를 기존 규칙대로 검증한다.
2. materializer write gate를 닫고 새 SQLite read를 막은 뒤 열린 connection과 reader를 모두
   quiesce한다.
3. envelope canonicality, fingerprint, request/result bounds를 검증한다.
4. sidecar가 이미 같은 index/hash를 committed로 가리키면 receipt를 확인하고 성공을 반환한다.
5. 현재 closed user DB의 size와 digest를 계산한다.
6. digest가 target이면 11단계로 간다. base가 아니면 divergence로 격리한다.
7. sidecar에 `(entry anchor, base digest, target digest)` pending intent를 기록하고 fsync한다.
8. canonical DB를 같은 directory의 temp 파일로 clone하고 page after-image를 `pwrite`한다.
   `target_file_bytes`에 맞춰 truncate/extend한다.
9. temp file 전체 digest, SQLite header와 `integrity_check`를 검증한다. 실패하면 temp만 폐기한다.
10. temp file fsync -> authoritative path로 atomic rename -> parent directory fsync 순서로
    durable하게 설치한다.
11. sidecar transaction에서 applied anchor, DB digest, request receipt/result를 committed로 쓰고
    pending intent를 지운 뒤 fsync한다.
12. canonical DB를 다시 열고 serving gate를 연다.

authoritative DB에 page를 in-place patch하지 않는다. 적용 중에는 read도 멈춘다. 열린 SQLite
connection의 page cache, mmap, WAL-index가 교체 전 inode를 계속 참조할 수 있기 때문이다.

## 9. 패배한 제안과 retry

- 제안 전 오류: staging과 sidecar의 proposal scratch만 삭제하고 client에 오류를 반환한다.
- 제안했지만 다른 payload가 결정: staging을 삭제하고 winner를 먼저 적용한다.
- winner 적용 뒤 같은 request receipt 발견: 원래 결과를 반환한다.
- receipt가 없으면 새 exact base에서 새 staging을 만들어 다시 제안한다.
- 같은 request ID와 다른 request digest는 어떤 base에서도 conflict다.

staging path나 proposal-local UUID는 envelope의 semantic identity가 아니다. cleanup은 안전하게
재시도할 수 있어야 하며 오래된 staging은 base digest와 결정 증거 없이 승격할 수 없다.

## 10. crash matrix와 멱등성

| crash 지점 | 재시작 처리 |
|---|---|
| staging 생성/실행 중 | authoritative DB 불변. orphan staging 삭제 |
| 제안 전 effect 생성 후 | 결정 증거가 없으므로 scratch 삭제 |
| 결정 후 apply 시작 전 | qlog에서 effect를 다시 읽어 적용 |
| pending intent fsync 후, temp 쓰기 전 | DB가 base이므로 temp부터 재적용 |
| temp write/fsync 중 | authoritative DB는 base. temp 삭제 후 재적용 |
| rename 후 directory fsync 전 | path의 실제 digest를 검사. base면 재적용, target이면 sidecar 완료; 그 외 reseed |
| rename 후 sidecar commit 전 | DB가 target이므로 physical write를 반복하지 않고 receipt와 tip만 완료 |
| sidecar commit 후 응답 전 | duplicate request가 저장된 result를 반환 |
| serving 재개 전 | sidecar와 target digest 확인 후 reopen |

멱등성 판별은 SQL 재실행이나 “page가 비슷해 보임”에 의존하지 않는다.

```text
current == base   -> apply pages, verify target, finalize sidecar
current == target -> skip physical apply, finalize/verify sidecar
otherwise         -> stop materializer, fetch/install trusted snapshot
```

## 11. checkpoint, snapshot, recovery

### 정상 checkpoint

- checkpoint는 materializer gate 아래에서 write와 read를 quiesce한다.
- WAL을 `TRUNCATE` checkpoint하고 connection을 닫는다.
- user DB digest가 sidecar의 committed digest와 같은지 확인한다.
- `-wal`에는 미checkpoint commit이 없어야 하고 `-shm`은 snapshot에 포함하지 않는다.
- checkpoint 중 qlog apply와 proposal base 획득을 허용하지 않는다.

### `QSNP v1` snapshot 생성

현재 `create_snapshot`은 `QSNP\0\x01` magic 뒤에 canonical user DB bytes와 exported replicated
control state를 하나의 container로 encoding한다. snapshot manifest는 최소한 다음을
함께 bind한다.

- full canonical user DB bytes와 digest
- cluster/epoch/configuration/recovery identity
- compacted qlog index/hash
- SQLite materializer fingerprint; page size는 container의 canonical DB header와 QWAL codec으로 검증
- snapshot에 포함된 replicated control receipt 범위

snapshot 생성은 checkpoint·close 후 sidecar의 committed digest와 user DB bytes를 비교하고,
pending apply가 있으면 control export를 거부한다. legacy standalone DB snapshot bytes는 `QSNP`가
아니므로 QWAL restore input으로 인정하지 않는다.

### snapshot restore

1. temp user DB에 snapshot bytes를 쓰고 fsync한다.
2. digest, manifest identity, fingerprint, `integrity_check`를 검증한다.
3. user DB를 atomic rename하고 parent를 fsync한다.
4. replicated control state를 snapshot anchor로 설치한다.
5. target node의 `node_id`를 sidecar에 별도로 bind한다.
6. 이후 qlog QWAL entry를 순서대로 replay한다.

DB가 base도 target도 아닌 apply divergence, 필요한 blob 소실, incompatible fingerprint는
best-effort page 수리 대신 이 snapshot restore 경로로 보낸다.

## 12. payload 크기와 recorder-quorum blob

### 1단계: inline only

현재 노드 command 상한은 256 KiB다. QWAL v1은 envelope 전체가
`MAX_COMMAND_BYTES` 이하일 때만 inline으로 제안한다. page effect가 이를 넘으면 요청을
statement replay로 조용히 fallback하지 않고 명확한 resource-exhausted 오류를 반환한다.

### 2단계: content-addressed blob

큰 effect를 여러 qlog command로 나누면 한 SQLite transaction이 여러 합의 단위로 찢어진다.
따라서 큰 payload 지원은 다음 순서를 사용한다.

1. proposer가 canonical QWAL body를 고정 크기 chunk로 나눈다.
2. 각 chunk와 전체 body를 content digest로 주소화한다.
3. 현재 configuration의 recorder quorum에 모든 chunk를 먼저 저장하고 durability proof를 얻는다.
4. qlog에는 `QwalBlobManifestV1` 한 개만 제안한다.
5. manifest는 body digest, 총 길이, chunk digest/length 순서, codec metadata와 recorder proof를
   포함한다.
6. apply 노드는 chunk를 병렬 fetch할 수 있지만, 전체 body를 canonical 순서로 재조립하고
   digest를 검증한 뒤에만 apply한다.

blob이 quorum에 없으면 manifest를 제안하지 않는다. 결정 뒤 일시적으로 fetch할 수 없으면
materializer는 catch-up 상태에 머물며 다른 SQL을 실행해 건너뛰지 않는다. blob GC는 해당
entry를 덮는 durable snapshot과 qlog/recorder retention 증거가 모두 생긴 뒤에만 수행한다.

이 기능은 기존 recorder command 저장 API의 단순 크기 상향이 아니라 별도 capability다.

## 13. 브레이킹 전환과 snapshot bootstrap

### capability

향후 voter capability 교환을 추가할 때는 최소한 다음을 구분한다.

- `sqlite_control_sidecar_v1`
- `sqlite_qwal_page_v1_inline`
- `sqlite_qwal_blob_manifest_v1`
- 선택적 `sqlite_qwal_frames_v1`

새 effect를 제안하는 배포에서는 현재 voter configuration 전체가 page-v1 apply와
동일 materializer fingerprint를 지원해야 한다. 다만 현재 코드에 voter capability
negotiation이 완료되었다고 간주하지 않는다.

### rolling dual decoder는 없다

QWAL 배포는 다음 호환 경로를 제공하지 않는 브레이킹 체크포인트다.

- `.control` 없이 기존 SQLite DB를 열어 sidecar를 자동 생성하지 않는다.
- `__rhiza_meta` 또는 `__rhiza_requests`가 있는 DB를 in-place migration하지 않는다.
- old qlog의 QSQL/QEFX/QBCH command를 QWAL apply 경로에서 decode하지 않는다.
- QWAL payload 상한 초과 시 statement replay로 fallback하지 않는다.
- QWAL entry가 결정된 뒤 old binary로 auto-downgrade하지 않는다.

### bootstrap 절차

1. 기존 writer를 중지하고 모든 old QSQL/QEFX entry가 반영된 하나의 qlog anchor를
   선택한다.
2. 운영 전환 도구로 그 anchor의 user schema/data만 담은 canonical DB와 replicated
   receipt/control state를 만든다. QWAL runtime이 기존 내부 table을 자동 변환하지는
   않는다.
3. 이 결과를 `QSNP v1`으로 각 voter에 restore하고, target node ID만 sidecar에
   재bind한다.
4. 모든 voter의 user DB digest, snapshot anchor, configuration, fingerprint가 같음을 확인한다.
5. 이 anchor 이후는 QWAL entry만 replay한다. old QSQL/QEFX history를 새 materializer로
   재실행하지 않는다.

즉 기존 DB와 old command history의 지원 경계는 dual decoder가 아니라 trusted `QSNP`
anchor다. 전환 이전 data export/정규화 도구는 별도 운영 작업이며 QWAL runtime의
호환 mode가 아니다.

## 14. 보안과 계속 차단할 동작

physical replication은 “SQLite가 실행할 수 있는 모든 것”을 안전하게 만들지 않는다. staging
밖에 영향을 줄 수 있는 동작은 admission과 SQLite authorizer 양쪽에서 계속 차단한다.

- `ATTACH`/`DETACH`와 여러 database file에 걸친 transaction
- TEMP table/index/trigger/view 및 connection-local pragma
- `load_extension`과 임의 extension 로딩
- 파일·network·process에 접근하는 virtual table 또는 사용자 함수
- `VACUUM INTO`, backup/export 등 별도 파일을 쓰는 문장
- plain `VACUUM`도 별도의 offline maintenance·전체-file effect 계약이 생기기 전까지 금지
- `writable_schema`, journal/VFS/page-size를 임의로 바꾸는 pragma
- transaction, WAL checkpoint, locking mode를 사용자가 직접 제어하는 문장
- Rhiza가 등록하지 않은 collation/function/module

FTS5처럼 변경이 main DB와 shadow table 안에만 머무는 built-in module도 별도 allowlist와
통합 테스트를 통과한 경우에만 연다. “virtual table”이라는 문법 분류만으로 허용하지 않는다.

## 15. 기대되는 query 지원 확대

QWAL page effect가 구현되면, 동일 SQL 재실행의 결정성이나 SQLite Session capture 범위가
아니라 staging의 최종 DB bytes가 합의 대상이 된다. 따라서 main DB 내부 효과에 한해 다음
지원이 가능해진다.

- table/index/view/trigger를 포함한 DDL
- trigger와 foreign-key cascade의 indirect write
- 명시적 PK가 없는 table과 ROWID allocation
- `AUTOINCREMENT`와 `sqlite_sequence` 변화
- `random()`, 시간 함수 등 winning execution에서 확정된 비결정적 값
- 여러 statement로 된 원자적 request
- bounded `RETURNING` 결과의 exact retry
- allowlist된 in-file virtual table과 shadow-table 변화

지원 확대는 external effect 차단을 완화하지 않는다. `ATTACH`, TEMP, extension side effect는
page replication으로 표현할 수 없으므로 계속 비지원이다.

## 16. 관측성

최소 metric과 structured event는 다음을 제공한다.

- proposal staging 생성/성공/실패/폐기 횟수와 시간
- base/target DB bytes, changed page 수, effect bytes, page amplification ratio
- checkpoint 시간과 busy/failure 횟수
- inline 거부 횟수와 blob upload/fetch bytes·latency·quorum 실패
- apply 단계별 시간: quiesce, clone, patch, digest, integrity, fsync, reopen
- `current==base`, `current==target`, divergence 판정 횟수
- crash recovery에서 pending intent를 완료한 횟수
- snapshot reseed 원인과 마지막 valid qlog anchor
- capability/fingerprint mismatch
- request duplicate/conflict와 persisted-result replay

로그에는 SQL parameter 값, page 내용, result row를 남기지 않는다. request ID도 운영 정책에
따라 hash 또는 bounded identifier로 기록한다.

## 17. 검증 상태와 계획

### codec/property test

- canonical encode/decode round trip
- page 정렬, 중복, 0번 page, 잘못된 길이, overflow, trailing bytes 거부
- base/target file shrink와 grow
- 한 bit 손상 시 body/target digest 불일치
- arbitrary bytes decoder가 panic하지 않음

### 실제 SQLite integration test

- 1/4/8/16/32/64 KiB page size 중 local SQLite가 지원하는 조합
- page 1 변경, freelist 변화, file grow/truncate
- WAL hook이 없는 검증된 no-op/result-only transaction의 빈 page effect와 receipt 재생 검증
- DDL, trigger, FK cascade, PK 없는 table, AUTOINCREMENT
- random/time 값을 포함한 write와 exact `RETURNING` retry
- allowlist된 FTS5 write와 shadow table 검증
- no-op transaction과 result-only receipt
- base clone과 applied target의 full digest equality 및 `integrity_check`

### 구현된 recording VFS test

`qwal_vfs` 모듈의 8개 test는 다음 행동을 검증한다.

- named VFS 등록이 default VFS를 바꾸지 않음
- arbitrary write range와 page boundary에서 candidate page 계산
- WAL write, main/WAL sync, commit, checkpoint, close 후 seal evidence
- observed candidate set이 full closed-file diff의 changed page를 모두 포함함
- unexpected persistent main DB open의 fail-closed 처리
- existing DB header와 다른 page size 거부
- 실패한 `xOpen`의 unpublished wrapper와 reservation cleanup
- in-flight `xOpen` reservation이 있을 때 seal 거부

최신 검증에서 `cargo test -p rhiza-sql`은 이 VFS 8개를 포함해 전체 52개 test가
모두 통과했다.

### QuePaxa/다중 노드 test

- 서로 다른 proposer가 같은 slot에 서로 다른 effect를 제안하고 winner만 반영
- 패배한 proposer staging이 authoritative DB를 바꾸지 않음
- slot contention 뒤 새 exact base에서 재생성
- duplicate decided entry와 duplicate client request의 멱등성
- permanent leader 없이 proposer가 바뀌어도 동일 동작
- capability가 하나라도 부족한 configuration에서 QWAL 제안 거부

### crash/fault injection test

- 10절 matrix의 각 fsync/rename/sidecar 경계에서 process kill
- short write, disk full, corrupt temp page, corrupt envelope/blob chunk
- checkpoint busy와 reopen 실패
- target DB는 설치됐지만 sidecar가 base인 상태의 자동 완료
- base/target 어느 쪽도 아닌 DB의 fail-closed 및 snapshot reseed

### snapshot/blob test

- snapshot restore 후 다음 QWAL entry catch-up
- 다른 node ID로 restore해도 user DB digest 동일
- recorder 한 곳 장애와 quorum durability
- missing/corrupt chunk에서 apply 정지
- snapshot retention 전 blob GC 금지, 이후 안전한 GC

## 18. 단계별 구현

### Phase 0 — control plane 분리

- **핵심 구현됨**: `.control` durable format, pending intent, request receipt/result, node-local
  identity, applied tip을 user DB 밖으로 분리했다.
- **핵심 구현됨**: `Option<Connection>`과 lifecycle gate로 close/replace/reopen을 직렬화했다.
- **구현됨**: snapshot/restore를 `QSNP` user DB + replicated control anchor 계약으로
  변경했다.
- **남음**: process-kill fault injection을 포함한 crash matrix와 다중 voter 운영 검증.

완료 기준: QWAL-only 모드에서 replica user DB digest와 control anchor가 일치하고,
모든 pending/rename 경계가 crash 후 멱등적으로 회복된다.

### Phase 1 — QWAL page v1 inline

- **구현됨**: staging clone 실행기와 fixed connection profile.
- **구현됨**: closed base/target full diff, canonical codec, target full digest/integrity,
  apply-to-temp/atomic-rename.
- **구현됨**: envelope 전체 256 KiB inline 상한. 초과 시 fallback 없이
  resource-exhausted로 거부.
- **구현됨**: staging-only named non-default `QwalRecordingVfs`를 production prepare에
  shadow/audit로 연결하고, seal evidence와 candidate coverage를 full diff와 대조.
- **미구현 최적화**: complete VFS candidate만으로 changed page 읽기 범위를 줄이는
  candidate-only diff. 현재 wire page source는 항상 full diff.
- **구현됨**: Node의 statement fallback과 legacy write batching을 제거하고 요청별 exact-base
  QWAL을 순차 제안. runtime/startup/restore suffix는 qlog append 또는 publish 전에 canonical
  QWAL decode를 통과해야 한다.
- **남음**: 더 넓은 3-node slot contention 및 process-kill crash matrix.

완료 기준: 지원 대상으로 선언한 SQL corpus가 모든 replica에서 같은 DB digest와 결과를 낸다.

### Phase 2 — query surface 확대

- **contract 추가됨**: DDL, trigger/FK cascade, PK-less table, AUTOINCREMENT,
  non-deterministic value, bounded RETURNING을 하나의 exact-base QWAL effect로 검증하는
  integration contract가 있다.
- **구현됨**: broad read families와 observational PRAGMA allowlist 통합 계약.
- **남음**: built-in in-file virtual table은 module별 통합 테스트 후 allowlist.

완료 기준: 각 gate에 positive/negative test와 외부 효과 차단 test가 있다.

### Phase 3 — recorder-quorum blob

- content-addressed chunk API, quorum proof, manifest codec, retention/GC 구현
- inline과 blob apply가 동일 QWAL body validation을 공유
- 큰 transaction과 disk-pressure fault test 통과

완료 기준: partial blob availability가 partial DB state로 이어지지 않는다.

### Phase 4 — 선택적 native WAL codec

- recording VFS의 WAL byte capture와 raw WAL hook을 결합한 frame-boundary POC 및 benchmark
- salt/checksum/checkpoint-generation validation과 close/install/reopen apply 설계
- live apply는 public API가 추가되거나 SQLite fork API의 소유 비용을 정당화할 때만 검토
- 명확한 성능 우위가 있을 때만 capability로 추가

완료 기준: page-v1과 결과·crash semantics가 같고 운영 복잡성을 상쇄할 측정 이득이 있다.

## 19. 공식 SQLite 자료

- [Write-Ahead Logging](https://www.sqlite.org/wal.html)
- [WAL-mode File Format](https://www.sqlite.org/walformat.html)
- [Database File Format](https://www.sqlite.org/fileformat.html)
- [The SQLite OS Interface or VFS](https://www.sqlite.org/vfs.html)
- [`sqlite3_vfs` OS Interface Object](https://www.sqlite.org/c3ref/vfs.html)
- [`sqlite3_io_methods` File Virtual Methods](https://www.sqlite.org/c3ref/io_methods.html)
- [`sqlite3_vfs_find/register/unregister`](https://www.sqlite.org/c3ref/vfs_find.html)
- [`sqlite3_database_file_object`](https://www.sqlite.org/c3ref/database_file_object.html)
- [`sqlite3_wal_hook`](https://www.sqlite.org/c3ref/wal_hook.html)
- [`sqlite3_wal_checkpoint_v2`](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html)
- [Atomic Commit](https://www.sqlite.org/atomiccommit.html)
- [How To Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html)
- [`ATTACH DATABASE`](https://www.sqlite.org/lang_attach.html)
- [Temporary Files Used By SQLite](https://www.sqlite.org/tempfiles.html)

## 20. 승인된 결론

- SQLite의 첫 physical replication format은 raw WAL suffix가 아니라 closed-file page
  after-image인 `QWAL v1`이다.
- 현재 v1은 exact-base staging을 closed target으로 만든 뒤 full closed-file diff로 changed
  page를 찾는다. target full digest와 integrity 검증은 항상 수행한다.
- named non-default recording VFS는 production prepare staging에 shadow/audit mode로 연결되었다.
  audited built-in lower VFS만 허용하고 commit·checkpoint·sync·close/seal evidence와
  candidate coverage를 수집한다.
- recorder 오류·incomplete·coverage mismatch는 full diff로 fallback한다. wire page source와
  correctness는 아직 항상 full diff·digest·integrity 검증이며, candidate-only diff는
  미구현이다.
- speculative SQL은 authoritative DB가 아닌 exact-base staging clone에서만 실행한다.
- 영구 leader를 만들지 않으며, 결정된 slot payload의 proposer가 그 effect의 실행 결과를
  확정한다.
- canonical user DB와 Rhiza control/receipt sidecar는 필수 한 쌍이다. 한 편만 있으면
  자동 복구·생성하지 않고 snapshot bootstrap을 요구한다.
- 레거시 모드, rolling dual decoder, direct QSQL/QEFX apply, in-place DB migration은 없다.
  기존 DB와 old qlog history는 trusted `QSNP` anchor를 설치한 뒤에만 QWAL을 시작한다.
- 현재는 envelope 전체 256 KiB까지 inline으로만 제안하며 초과 시 명시적으로
  거부한다. recorder-quorum content-addressed blob은 미구현 후속 기능이다.
- DB 밖의 효과는 physical replication으로도 지원하지 않는다.
- `rhiza-sql`의 핵심 QWAL과 VFS shadow/audit 경로는 구현되었지만, Node 전체
  통합·candidate-only diff·blob·완전한 crash matrix는 완료로 표기하지 않는다.
