# Rhiza SQLite QWAL v2 복제 계약

> 상태: **QWAL v2 브레이킹 전환 구현**
>
> 범위: QuePaxa가 결정한 SQLite 물리 효과의 준비, 배치, 적용, 재시도, 복구
>
> 배포: **클린 설치 전용**. QWAL v1, control v1, QSNP v1 마이그레이션 없음

## 1. 결정

Rhiza SQL은 follower에서 SQL 문장을 다시 실행하지 않는다. 정확한 qlog base의 staging
SQLite에서 winning proposer가 SQL을 한 번 실행하고, 닫힌 base와 target DB의 최종 page
after-image를 `QWAL v2` canonical payload로 만든다. QuePaxa는 이 opaque payload를
순서화하고, 모든 replica는 같은 payload를 검증·적용한다.

현재 correctness 경로는 closed-file full diff다. staging 전용 `QwalRecordingVfs`는 변경
page 후보와 commit/checkpoint/close evidence를 audit하지만, candidate 누락이 correctness를
바꾸지 않도록 wire page set은 full diff에서 만든다. raw `-wal`과 `-shm`은 복제하지 않는다.

이 계약은 다음을 보장한다.

- trigger, foreign-key cascade, rowid 할당, `AUTOINCREMENT`, 허용된 SQLite 비결정성을
  statement replay 없이 winning execution의 결과로 확정한다.
- 패배한 제안은 canonical DB를 변경하지 않는다.
- ordered batch의 성공한 subset과 그 결과 receipt가 하나의 물리 target과 하나의 qlog
  anchor에 결합된다.
- crash 중간 상태는 base/target digest와 durable pending intent로 판별한다.
- 다른 SQLite build, identity, base 또는 비canonical payload는 fail closed다.

## 2. 코드 경계

| 책임 | 구현 기준점 |
| --- | --- |
| QWAL v2 codec과 512 KiB/1,024 receipt 상한 | [`QwalEnvelopeV2`, `QwalReceiptV2`, `QwalPageV2`](../crates/rhiza-sql/src/qwal.rs) |
| ordered batch 준비와 member savepoint | [`SqliteStateMachine::prepare_sql_batch_effect`](../crates/rhiza-sql/src/lib.rs) |
| receipt bulk lookup과 exact retry 판정 | [`SqliteStateMachine::check_sql_requests`](../crates/rhiza-sql/src/lib.rs) |
| pending intent, shared-anchor receipt commit | [`ControlStore::commit_applied`](../crates/rhiza-sql/src/control.rs) |
| physical temp apply와 검증 | [`apply_qwal_to_file`](../crates/rhiza-sql/src/qwal.rs) |
| runtime batch, prefix 축소, winner 재분류 | [`NodeRuntime::execute_sql_batch`](../crates/rhiza-node/src/lib.rs) |
| QWAL contract tests | [`qwal_contracts.rs`](../crates/rhiza-sql/tests/qwal_contracts.rs) |

공개 이름 `QWAL_V2_MAGIC`은 canonical v2 의미를 유지하지만 현재 framing generation byte는
3이다. snapshot과 control/QCTL generation도 3이며 이전 generation을 같은 구조로 해석하는
호환 decoder는 없다.

## 3. QWAL v2 canonical envelope

```text
QwalEnvelopeV2 {
  cluster_id, epoch, configuration_id, recovery_generation,

  base_index, base_hash,
  base_db_digest, base_file_bytes,

  target_db_digest, target_file_bytes,
  materializer_fingerprint, page_size,

  receipts: [
    { request_id, request_digest, result_blob }, ...
  ],
  pages: [
    { page_no, after_image }, ...
  ]
}
```

한 envelope에는 **1..=1,024개의 성공 receipt**가 입력 순서대로 들어간다. request ID는
envelope 안에서 고유하며, 실패한 member와 이미 저장되어 proposal이 필요 없는 retry는
receipt subset에 들어가지 않는다. 각 receipt에는 anchor를 중복 저장하지 않는다. envelope을
운반한 단 하나의 decided entry anchor가 모든 receipt의 공통 original anchor다.
QWAL envelope, control receipt commit, bulk lookup, SQL preparation의 duplicate 검증은 각각
pre-sized `HashSet`을 사용해 request ID를 한 번만 순회한다. 이전 member slice를 매번
재탐색하는 quadratic validation은 없다.

receipt 배열, base/target identity와 digest, final page effect는 같은 canonical payload다.
따라서 receipt만 다른 target에 붙이거나 page effect와 receipt를 따로 결정할 수 없다.
decode 뒤 canonical re-encode bytes가 입력과 다르면 거부한다.

추가 검증 규칙은 다음과 같다.

- 전체 encoded envelope, page bytes 합계, result bytes 합계는 각각 512 KiB 경계 안에 있다.
- page number는 1부터 시작하고 엄격히 증가하며, after-image 길이는 `page_size`와 같다.
- grow 시 base EOF 뒤의 모든 새 page가 gap 없이 포함되어야 한다.
- base/target 크기는 page-size 배수이며 page 1의 SQLite header와 page count를 검증한다.
- 빈 page 배열은 DB bytes가 같은 성공/result-only batch에만 허용된다. receipt는 비어 있을
  수 없다.
- target 전체 digest와 materializer fingerprint를 적용 전에 검증한다.
- local path, staging UUID, WAL salt, `-shm`, entry hash는 envelope에 넣지 않는다.

## 4. ordered non-atomic batch 준비

`prepare_sql_batch_effect(&[SqlBatchMember], base_index, base_hash)`는 한 exact-base
staging DB와 한 outer SQLite transaction을 사용한다. 각 member는 nested savepoint에서
실행된다.

외부 `execute_sql_batch` 호출 하나는 1..=256 member뿐 아니라 aggregate canonical encoded
input **512 KiB 이하**여야 enqueue된다. FIFO pending queue의 encoded-byte 예산은 설정된 call
개수와 별개로 **고정 32 MiB**다. leader가 한 번에 drain하는 active physical group은
**2 MiB 이하, 1..=1,024 member**다. 따라서 queue가 여러 active group을 보유할 수는 있어도
하나의 QWAL preparation이 무제한으로 커지지 않는다.

1. 1..=1,024 internal member, canonical QSQL v2 bytes, request ID와 digest를 검증한다.
2. control sidecar를 bulk 조회해 stored exact retry와 request-ID conflict를 분류한다.
3. 각 unseen member를 입력 순서대로 savepoint에서 실행한다.
4. 성공하면 savepoint를 commit하고 result receipt를 successful subset에 추가한다.
5. 실패하면 해당 savepoint만 rollback하고 aligned result에 오류를 둔다. 다음 member는
   계속되며, 앞선 성공은 볼 수 있다.
6. 성공 receipt가 하나 이상이면 outer transaction을 commit해 하나의 target을 만든다.
7. 전부 실패하면 outer transaction을 rollback하고 `effect=None`을 반환한다. 이 경로는
   consensus와 qlog를 호출하지 않는다.

따라서 batch 자체는 원자적이지 않다. 각 `SqlCommand` 안의 여러 statement는 기존처럼 한
transaction 단위지만, batch member 실패는 다른 성공 member를 취소하지 않는다. 현재 runtime
coalescing fast path는 단일-statement command들에 적용되고, multi-statement command의 자체
원자성은 한-member 경로로 유지한다.

successful subset은 하나의 target digest, 하나의 page effect, 하나의 QuePaxa slot, 하나의
qlog entry를 공유한다. 내구성 원본은 complete QWAL과 receipt를 보존한 2/3 Recorder WAL이다.
로컬 apply 뒤 control sidecar의 한 `synchronous=OFF` transaction이 applied tip, receipt,
비내구 embedded qlog mirror를 같은 anchor로 게시한다.

## 5. retry, duplicate, conflict, slot 경쟁

클라이언트는 불확실한 batch를 같은 순서와 같은 request ID/bytes로 다시 보낼 수 있다.

- **batch 안 exact duplicate**: 첫 canonical member의 aligned 결과를 alias한다. 별도 SQL
  실행이나 receipt를 만들지 않는다.
- **stored exact retry**: 저장된 digest가 같으면 original result와 original anchor를
  반환한다. 새 consensus는 없다.
- **같은 ID, 다른 bytes**: request conflict로 해당 member를 격리한다.
- **exact proposal wins**: 준비한 result를 공통 decided anchor로 반환하고, byte-exact
  prepared target을 표준 검증 경로로 설치한다.
- **foreign payload wins**: winner를 먼저 persist/apply한 뒤 pending member receipt를 bulk
  재조회한다. winner가 저장한 retry는 반환하고, 여전히 unseen인 member만 새 exact base에서
  다시 준비한다. stale effect를 다음 slot에 재사용하지 않는다.

QWAL effect가 512 KiB command 상한을 넘으면 runtime은 eligible prefix 길이를 절반으로
줄여 다시 준비한다. fitting prefix만 한 entry로 제안하고 나머지는 다음 exact base에서
처리한다. 한 member도 맞지 않으면 그 member에 `resource_exhausted`를 반환한다. SQL replay나
logical-effect fallback은 없다.

## 6. exact-base preparation

effect base는 다음을 모두 만족한다.

- `base_index == control.applied_index`
- `base_hash == control.applied_hash`
- `base_db_digest == digest(canonical_user_db)`
- proposal slot은 `base_index + 1`

materializer lifecycle gate 아래에서 pending apply가 없는지 확인하고 canonical connection을
close한다. common path는 canonical DB에 매 요청 checkpoint를 실행하지 않는다. 작은 DB는 같은
filesystem의 staging file로 복사하고, 256 KiB 이상은 macOS clonefile/Linux FICLONE을 먼저
사용한 뒤 지원되지 않으면 copy로 fallback한다. speculative connection은 `synchronous=OFF`다.
결정 전 canonical DB, qlog, control state는 바뀌지 않는다.

staging transaction을 commit한 뒤 WAL checkpoint와 close를 끝내야 proposal이 될 수 있다.
VFS가 commit과 checkpoint를 완전히 관찰하면 기록된 candidate page만 base와 비교한다. 새로
증가한 모든 page가 candidate에 없으면 fail closed하며, recording이 불완전하면 full diff로
fallback한다. 현재 target 전체 digest scan은 exact replay 검증을 위해 유지한다. hot path의
SQLite integrity scan과 별도 promotion-time target digest 재scan은 제거했다.
proposal 준비 중 canonical tip이 바뀌거나 다른 payload가 slot을 차지하면 staging은
폐기하거나 foreign winner의 새 base에 맞춰 재준비한다.

## 7. Recorder-authoritative apply와 crash recovery

authoritative DB를 in-place patch하지 않는다. apply는 read/write gate를 닫고 connection을
quiesce한 상태에서 다음 순서로 수행한다.

1. entry identity, QWAL canonicality, fingerprint, receipt/result bounds를 검증한다.
2. 현재 DB가 envelope base digest인지 target digest인지 확인한다. 둘 다 아니면 divergence로
   중지하고 trusted same-version snapshot recovery를 요구한다.
3. exact local prepared target이면 owned inode, size, ctime/mtime seal을 다시 검증한다.
4. prepared target이 없으면 base에 after-image와 target length를 적용한 temp를 재구축하고
   target digest와 SQLite integrity를 검증한다.
5. canonical path로 atomic rename한다. staging sync와 rename 뒤 directory fsync는 하지 않는다.
6. 한 비내구 control transaction에서 embedded entry, applied tip, target digest, ordered receipt를
   같은 anchor로 게시한다. common path에는 pre-install pending transaction이 없다.
7. canonical DB를 reopen한 뒤에만 ACK/read visibility gate를 연다.

crash로 DB rename과 control 게시 사이가 갈리거나 DB/control 중 하나가 손상되면 pair digest와
tip 검증이 실패한다. compaction anchor가 없으면 로컬 materializer를 격리하고 Recorder quorum
tail을 genesis부터 재생한다. anchor가 있으면 verified checkpoint restore를 요구한다. remote
rejoin은 Recorder server를 열기 전에 전체 data directory를 sibling quarantine으로 옮기고
verified checkpoint를 복원한 뒤, checkpoint index가 0이어도 peer catch-up을 먼저 수행한다.
readiness는 이 과정과 applied tip 일치가 끝날 때까지 닫혀 있다. 혼합된 empty/occupied Recorder
증거가 quorum certificate를 만들지 못하면 `Unavailable`로 fail closed한다.

재시작 판정은 단순하다.

```text
current digest == base   -> temp부터 다시 적용
current digest == target -> physical apply를 반복하지 않고 sidecar commit 완료
otherwise                -> fail closed; trusted QSNP v2 recovery 필요
```

`QSNP v2`는 canonical user DB bytes와 replicated control state를 같은 qlog anchor에 묶는다.
restore는 같은 v2 contract 안의 crash recovery/catch-up 기능이다. v1 데이터를 v2로 바꾸는
migration 수단이 아니다.

## 8. 클린 설치 전용 브레이킹 계약

QWAL v2 도입은 rolling upgrade가 아니다. 새 cluster/data directory로 시작하고 모든 voter를
같은 binary와 fingerprint로 동시에 구성한다.

지원하지 않는 항목은 다음과 같다.

- QWAL v1 payload, QSNP v1 snapshot, control v1 sidecar decode
- 기존 `.control`이나 user DB의 in-place schema/data migration
- `__rhiza_meta`/`__rhiza_requests` legacy DB의 자동 변환
- old QSQL/QEFX/QBCH qlog history replay
- v1/v2 dual writer 또는 rolling dual decoder
- v2 effect 거부 시 statement replay나 old format fallback
- 구버전 binary로의 자동 downgrade

운영 절차는 old data directory를 재사용하거나 snapshot bootstrap으로 변환하는 것이 아니라,
old cluster를 중지하고 별도의 빈 v2 data directory에 clean install하는 것이다. 보존이 필요한
기존 데이터 변환은 이 runtime 계약 밖이며, 현재 릴리스에는 migration 경로가 없다.

## 9. SQL 범위와 계속 차단할 효과

QWAL은 main DB 내부의 final bytes를 복제하므로 DDL, trigger/FK cascade, PK 없는 table,
`AUTOINCREMENT`, bounded `RETURNING`, winning execution의 `random()`/시간 함수 결과를 지원할
수 있다. 하지만 DB 밖의 효과는 물리 page 복제로 안전해지지 않는다.

다음은 admission과 SQLite authorizer에서 계속 차단한다.

- `ATTACH`/`DETACH`, TEMP schema와 connection-local persistent state
- extension loading, arbitrary virtual table, 사용자 함수의 file/network/process 효과
- `VACUUM INTO`, backup/export, 별도 file을 쓰는 statement
- user-controlled journal, checkpoint, locking, page-size, `writable_schema`
- Rhiza가 등록하지 않은 collation/function/module

## 10. 성능 관측과 현재 증거

`rhiza-profile`은 logical command 처리량과 physical batch call을 분리한다.

- `operations_per_second`, attempts/successes/errors: logical member 기준
- `batch_calls_per_second`, `batch_call_latency_us`: physical API call 기준
- `logical_item_latency_us`: batch service time을 submitted member 수로 나눈 amortized 값
- `qlog_entries`, `logical_operations_per_qlog`: consensus/qlog coalescing 효율
- `qwal_prepare_latency_us`, `qwal_apply_latency_us`: direct QWAL 단계별 비용
- `qwal_envelope_bytes`: 실제 canonical envelope 크기

현재 release evidence에서 runtime c4는 네 개의 concurrent public 256-member call을 bounded
FIFO group commit으로 최대 1,024 member까지 합쳐 median **15,824 logical ops/s**를 기록했다.
102,400 writes당 median QLog entry는 101개였다. raw evidence와 exact command는 ignored 경로
`target/rhiza-bench/write-v3-group-window-idle/20260719T032700/README.md`에 있다. direct QWAL
256/512/1,024 medians은 각각 16,313/22,109/25,730 logical ops/s였다.

`HashSet` single-pass validation 뒤 첫 c4 진단은 seven-run median **17,974 logical ops/s**로
15,824보다 **13.6%** 높았다. 하지만 raw 경로
`target/rhiza-bench/write-v4-hashset/20260719T042000/`에는 orphan Virtualization VM이 있었고,
post-run 시점 `syspolicyd`와 `trustd` 부하가 기록됐다. 이 수치는 최적화 방향을 재측정할
신호일 뿐이며 공식 15,824 evidence를 대체하지 않는다.

성능 비교에서는 logical throughput만 비교하고, batch-call latency를 256개의 독립 end-to-end
latency 표본처럼 복제하지 않는다. 같은 durability mode, 3 voters, concurrency, operation 수,
payload와 host 조건을 함께 기록한다.

## 11. 최적화 경계

correctness를 유지하면서 검토할 수 있는 후속 최적화는 다음으로 제한한다.

- verified recording-VFS candidate set으로 diff read 범위를 줄이되 full-diff fallback과 target
  digest/integrity 검증 유지
- clone/reflink 또는 prepared-target 재사용으로 base copy 비용 축소
- independent slot pipelining은 prev-hash reservation 안전성을 먼저 증명한 뒤 별도 benchmark
- 512 KiB를 넘는 transaction은 recorder-quorum content-addressed blob의 별도 durability
  protocol이 있을 때만 지원
- native WAL frame codec은 page codec과 같은 crash semantics를 보이고 측정상 우위가 있을
  때만 도입

ACK-before-durability, pending intent 제거, target digest 생략, in-place patch, SQL replay fallback은
성능 최적화가 아니다.

## 12. 검증 기준

- canonical v2 encode/decode, v1/trailing/duplicate/unordered/oversized payload fail closed
- mixed success/failure/success savepoint 격리와 aligned result
- 1..=1,024 ordered receipt, shared anchor, chunked control SQL in one zero-or-all transaction
- all-failed batch의 consensus/qlog 0
- exact duplicate alias, stored retry, digest conflict, foreign winner 재준비
- public aggregate input 512 KiB, pending 32 MiB, active 2 MiB/1,024 member 경계
- HashSet single-pass duplicate validation과 1,024 unique/duplicate 경계
- oversized prefix halving과 single-member exhaustion
- fused full-diff target digest가 별도 digest와 같고 canonical QWAL bytes를 보존
- base/target grow/shrink, wrong base, corrupt page/digest, pending crash replay
- snapshot restore 뒤 다음 v2 QWAL catch-up
- 3-voter slot contention에서 winner만 반영되고 replica target digest 일치

## 13. 공식 SQLite 자료

- [Write-Ahead Logging](https://www.sqlite.org/wal.html)
- [WAL-mode File Format](https://www.sqlite.org/walformat.html)
- [Database File Format](https://www.sqlite.org/fileformat.html)
- [The SQLite OS Interface or VFS](https://www.sqlite.org/vfs.html)
- [`sqlite3_wal_checkpoint_v2`](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html)
- [Atomic Commit](https://www.sqlite.org/atomiccommit.html)
- [How To Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html)
