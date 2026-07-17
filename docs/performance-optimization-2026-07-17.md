# rhiza 성능 최적화 검증 — 2026-07-17

이 문서는 `c9cf59024689fb3a30812dbf17ff018dc053fd28` 위의 미커밋 후보를
계층별로 프로파일링하고, 정확한 이전 바이너리 및 transport 후보와 A/B한 결과를
기록한다. 이번 변경은 QuePaxa의 합의 상태 머신, quorum 조건,
proposal/decision 의미, 공개 `RecorderRpc` 계약을 바꾸지 않는다. 변경 지점은
Recorder 영속화 구현과 rhiza의 transport·읽기·배치 API 경계다. 아래 결과는 dirty
worktree의 진단 자료이므로 release 수치로 인용할 수 없다.

## 통신·QuePaxa 후속 최적화 요약

### 실제 write 병목

2026-07-17 후속 `rhiza-profile`은 같은 KV write를 raw materializer,
consensus+3 file Recorder, 전체 runtime으로 분리했다. 200 measured + 20 warmup,
3회 중앙값은 다음과 같다.

| 경로 | ops/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| raw materializer | 330.0 | 2.999 ms | 7.120 ms |
| QuePaxa + 3 file Recorder | 129.1 | 7.868 ms | 13.038 ms |
| full runtime, c=1 | 84.5 | 11.346 ms | 19.163 ms |
| full runtime, c=8 | 82.0 | 93.739 ms | 309.406 ms |

c=1 p50 기준으로 consensus/Recorder가 약 69%, materializer가 약 26%, qlog와
runtime 잔여 비용이 약 4%였다. 5초 표본에서 Recorder worker 샘플 약 75%가
`RecorderFileStore::persist_state_transition_with_command_unlocked` 아래
`File::sync_all`/`fcntl(F_FULLFSYNC)`에 있었고 proposer는 Recorder 응답을 기다렸다.
Tokio worker는 대부분 parked 상태였고 hashing은 0.24%였다. 즉 현재 durable write의
첫 병목은 codec이나 Tokio mutex가 아니라 **직렬화된 Recorder slot에서 수행하는
내구성 flush**다. transport-only 결과를 durable QuePaxa 처리량으로 해석하면 안 된다.

원시 JSON과 `sample` 결과는
`/private/tmp/rhiza-profile-current-20260717/`에 있다. source provenance는 dirty
`c9cf59024689fb3a30812dbf17ff018dc053fd28`, rustc 1.95.0, Apple M3/macOS
26.3이다.

### RecordWorker liveness race

최종 회귀 실행에서 queue capacity 1인 `RecordWorker`의 `Full`과 `Disconnected`가
같은 fatal 경로로 처리돼, retry 중 간헐적으로 `ProposeFailed`가 발생하던 기존
liveness race를 확인했다. `Full`은 이제 transient unavailable로 분류해 해당 RPC를
`Pending`으로 유지하고, `Disconnected`와 worker panic만 fatal로 처리한다. queue
submission은 계속 nonblocking이고 느린 minority Recorder는 quorum critical path에서
제외된다.

이 변경은 quorum, Recorder acceptance, decision proof 등 QuePaxa safety 규칙을
바꾸지 않고 포화 순간의 liveness만 개선한다. deterministic latch 회귀 테스트와 기존
flaky 시나리오 100회가 수정 후 100/100 통과했고, `rhiza-quepaxa` 전체 suite 77개도
통과했다.

### Linux Recorder WAL `sync_data`

steady `recorder.wal` append는 Linux에서 `File::sync_data`를 사용한다. Linux의
`fdatasync`는 append로 늘어난 file size처럼 이후 데이터를 읽는 데 필요한
메타데이터도 함께 내구화한다. 비 Linux는 보수적으로 `File::sync_all`을 유지한다.
다음 메타데이터 경로는 플랫폼과 무관하게 계속 `sync_all`이다.

- checkpoint/rotation 시 stable WAL inode truncate
- recovery에서 incomplete, ACK되지 않은 tail을 잘라내는 truncate
- command, slot, configuration, recorded-head의 atomic replace와 directory barrier

ACK 순서도 `frame write -> WAL sync 성공 -> in-memory state publish -> ACK`로
유지된다. checkpoint는 command/slot/configuration/head를 먼저 내구화하고 그 다음
WAL을 truncate+full-sync한다. 따라서 이 변경은 QuePaxa 메시지나 수락 규칙이 아닌
Linux steady persistence syscall만 바꾼다.

Docker Desktop Linux/aarch64에서 같은 release binary의 native `fdatasync`와
`fdatasync(fd)`를 `fsync(fd)`로 전달하는 `LD_PRELOAD` 기준을 12개 balanced pair로
비교했다. 각 run은 100 warmup + 800 measured record이며 24 run, 19,200 measured
record가 모두 성공했다.

| 측정 | native `fdatasync` | `fsync` preload | 비교 |
| --- | ---: | ---: | ---: |
| median throughput | 2,983.9011487711614 ops/s | 1,911.5215089204817 ops/s | aggregate medians 1.561008408666x |
| median p50 | 240,437.5 ns | 398,624.5 ns | native가 작음 |
| median p95 | 793,479 ns | 1,239,624.5 ns | native가 작음 |
| median p99 | 1,603,021 ns | 2,123,125 ns | native가 작음 |

aggregate median native/preload 비율은 1.561008408666x다. 그러나 paired
`fsync-preload/native` throughput ratio 중앙값은 0.9278500671968066이었고 native와
preload가 각각 6/12쌍에서 이겼다. aggregate throughput과 집계 latency percentile은
native 우세 신호지만 paired 결과와 승률은 mixed라 robust한 성능 승리를 입증하지
않는다.
preload 12 run 모두 expected/observed intercept가 900/900이었다. 모든 run은 WAL
frame 900개, generation 1을 확인했고 checkpoint는 발생하지 않았다.

재현 runner는 [`bench/run-recorder-sync-linux.py`](../bench/run-recorder-sync-linux.py),
shim은 [`bench/support/fdatasync-as-fsync.c`](../bench/support/fdatasync-as-fsync.c)다.
감사 가능한 tracked artifact는
[`raw.jsonl`](benchmarks/recorder-sync-linux-20260717/raw.jsonl)(49,782 bytes, 24 rows)과
[`summary.json`](benchmarks/recorder-sync-linux-20260717/summary.json)(9,603 bytes)이다.
summary는 exact commands, source/binary/shim hashes, dirty Git state, container image
digest를 포함한다. QuePaxa source SHA-256은
`54ca511bd8be35e1b2deeb50a1f8f9ced66bb336194e4d7ba07c4473a9d60c1d`, benchmark
binary SHA-256은 `7bc075b29e7d49524ea51555b5cc95a0f6d1eea4b9eccff7d634caa27893459d`다.
기록된 runner SHA-256
`bbe7d010c56fae73cc2d65d252093e2e547b4c191a8e14c9ccd7aa7454ed0b7d`는 현재 runner와
일치한다. fresh build provenance는
`target/recorder-sync-linux-build-final-v3-20260717`이며 runner의 full reuse gate가 이를
검증했다.

`sync_data`는 Linux에서 더 작은 durability syscall이라는 correctness-preserving
후보 구현으로 유지한다. Docker aggregate 진단은 성능 개선 신호도 보이지만 paired
결과는 inconclusive이고 artifact는
`production_valid=false`다. dirty worktree와 Docker Desktop의 가상 filesystem은
host power loss, ext4/XFS journal ordering, 실제 Kubernetes CSI flush를 재현하지
못한다. 따라서 1.5610x를 production speedup으로 일반화하지 않는다. 성능 채택에는
clean revision의 physical crash/reopen 및 ext4·XFS·목표 CSI 측정이 필요하다.

### 실제 Recorder adapter: postcard-rpc는 `HOLD`

`postcard-rpc` 후보는 QuePaxa domain crate에 의존성을 넣지 않고 `rhiza-node` 내부
private wire DTO, typed endpoint, sequence/pending map, 기존 TCP/TLS/ALPN/HELLO,
deadline, overload 정책을 조합했다. 실제 공개 `RecorderRpc` adapter를 공유 client로
측정한 canonical plaintext 결과는 3 balanced A/B pair, 총 144,000 measured calls,
오류 0이다. 다음 비율은 `tcp-postcard-rpc / tcp-postcard` 중앙값이다.

| workload | c | throughput | p99 | gate |
| --- | ---: | ---: | ---: | --- |
| record | 1 | 0.739x | 1.083x | FAIL |
| record | 4 | 0.862x | 0.159x | FAIL: throughput |
| record | 32 | 1.525x | 0.047x | PASS |
| inspect | 1 | 0.736x | 1.394x | FAIL |
| inspect | 4 | 0.754x | 0.353x | FAIL: throughput |
| inspect | 32 | 2.047x | 0.043x | PASS |

postcard-rpc는 multiplexing이 포화되는 c=32에서는 이겼지만 QuePaxa의 현재
per-recorder critical path에 가까운 c=1과 기본 promotion cell c=4의 처리량에서
졌다. 따라서 feature-gated 후보와 runtime selector는 유지하되 기본 transport로
승격하지 않는다. raw/aggregate artifact는
`/private/tmp/rhiza-recorder-balanced-final-l8-canonical-20260717/`,
`/private/tmp/rhiza-recorder-balanced-final-l8-canonical-summary-20260717.json`, TLS
smoke는 `/private/tmp/rhiza-recorder-tls-final-l8-canonical-smoke-20260717.json`에
있다. binary SHA-256은
`41e076914c413af13bb98d05dc6302afe80934aaac66b760c1a823f2f8635c05`다.

4-byte frame prefix와 payload를 `write_vectored` loop로 합치는 후보도 실제 adapter
matched release A/B에서 이득을 입증하지 못해 되돌렸다. 별도 `write_all` framing을
유지한다. 이 vectored 후보의 raw artifact는 보존되지 않았으므로 성능 수치로
인용하지 않는다.

legacy server에는 candidate와 같은 advertised remaining-deadline cap, dispatch 전
expiry 검사, admitted mutation의 exactly-once 실행과 shutdown drain을 적용했다.
리뷰에서 발견된 sender-owned absolute-deadline P1도 수정했다. plain socket과 TLS
`StreamOwned`의 underlying socket을 `DeadlineStream`으로 감싸 각 read/write/flush
직전에 `absolute_deadline - now`로 socket timeout을 줄여 갱신한다. send 직전에는
전달할 `remaining_deadline_ms`도 다시 계산한다. timeout이면 해당 연결을 pool에
반환하지 않고 폐기하며, 자동 replay하지 않는다.

slow partial fake는 read/write timeout이 100→70→40→10 ms로 감소함을 검증했고,
실제 slow-drip loopback은 sender deadline 안에 종료됐다. HELLO 후 advertised
remaining 감소, semaphore 포화 시 Recorder no-call, dispatch 전 만료 no-call,
admission 이후 mutation exactly-once와 shutdown drain도 회귀 테스트로 고정했다.
legacy unit 9/9와 integration 11/11(TLS 포함), candidate unit 7/7와 integration
7/7, 양 feature Clippy `-D warnings`가 통과했다. 남은 제한은 동기
`to_socket_addrs()` DNS 해석이 socket absolute deadline 바깥이라는 점이다.

### 48-cell typed batch sweep

KV/SQL/Graph 각각 batch 1/8/16/32를 네 가지 순서로 회전해 48 cell을 측정했다.
각 cell은 20 warmup + 200 measured logical command, concurrency 1, 128-byte value다.
9,600 measured command가 모두 성공했고 오류는 0이었다. qlog entry는 매 run마다
각각 200/25/13/7로 정확했다.

| profile | batch 1 | batch 8 | batch 16 | batch 32 | 권고 |
| --- | ---: | ---: | ---: | ---: | --- |
| KV logical ops/s | 85.4 | 635.4 | 1,180.3 | 1,896.8 | 32 |
| SQL logical ops/s | 60.1 | 491.5 | 811.1 | 1,285.6 | 16 균형, 32 처리량 |
| Graph logical ops/s | 36.5 | 227.6 | 343.5 | 397.9 | 8 |

KV batch 32의 p50/p99는 12.86/16.72 ms, SQL batch 16은 18.68/23.39 ms다.
Graph는 batch 8의 p50/p99가 34.82/44.73 ms지만 batch 32에서는
76.43/88.58 ms로 늘어 처리량 증가 대비 tail 비용이 크다. batch 16/32는 run당
독립 응답이 13/7개뿐이므로 p95/p99 해상도도 낮다.

따라서 explicit typed batch는 KV 32, SQL 16, Graph 8을 기본 권고로 한다. 다만 이
harness는 이미 만들어진 batch를 제출하므로 timed admission queue의 채움 정도를
측정하지 않는다. 자동 writer admission은 현재 max 8 / window 500 us를 유지한다.
raw JSON은 `/private/tmp/rhiza-profile-current-20260717/{batch-sweep,
sql-batch-sweep,graph-batch-sweep}/`에 있고 profile binary SHA-256은
`758cd117a072dbfbb46c3ca2e963900859c2af0c231c86cdf4b04a5db8667482`다.

## 결론

- QuePaxa 단독 합의 처리량 중앙값은 24.1 ops/s에서 85.4 ops/s로 3.55배
  향상됐다. p50은 40.1 ms에서 10.1 ms로 74.8% 감소했다.
- 같은 시점의 정확한 이전 바이너리 대비 공개 embedded 읽기는 KV 약 4.90배,
  SQL 약 2.68배 빨랐다.
- 일반 Cypher는 반복 A/B에서 c=1 약 23%, c=8 약 39% 향상됐다. 마지막 열부하
  상태의 짝지은 확인에서도 각각 16%, 52% 향상이 유지됐다.
- Graph document+tip 고정 조회는 동일 프로세스 교차 A/B에서 실행 시간이
  24.9–25.6% 감소했다.
- 후속 48-cell sweep은 typed batch를 KV 32, SQL 16(처리량 우선이면 32), Graph
  8로 권고한다. 자동 admission 기본값은 max 8 / 500 us를 유지한다.
- 후속 batch 9,600 measured command가 모두 성공했고 오류는 0이었다. qlog entry
  수는 각 입력 batch 수와 정확히 일치했다.

위 읽기·합의·batch 결과는 Apple M3 단일 호스트의 embedded 경로 결과다. 별도
Recorder adapter 표 역시 loopback transport다. HTTP, 물리 노드 간 네트워크, 원격
checkpoint, 다중 호스트 장애·재접속 비용은 어느 표에도 포함하지 않는다.

## 병목 분해

새 `rhiza-profile`은 네 계층을 분리한다.

| 계층 | 포함 범위 |
| --- | --- |
| `handle` | 공개 `RhizaHandle` async API, embedded 수명·내구성 확인 |
| `runtime` | worker당 하나의 blocking loop를 사용하는 `NodeRuntime` API |
| `raw` | command encode, `LogEntry` 구성, materializer apply/read |
| `consensus` | 범용 `ThreeNodeConsensus::propose_at` + 3개 file recorder만 포함 |

초기 분해에서 KV c=1 읽기는 raw 1.03M, runtime 843k, handle 105k ops/s였다.
SQL c=1은 raw 333k, runtime 130k, handle 59.5k ops/s였다. 반면 Graph Cypher는
raw/runtime/handle이 모두 약 2.65–2.72k ops/s여서 Ladybug query 실행 자체가
지배적이었다. 쓰기는 raw KV materializer가 약 356 ops/s인데 runtime은 약
24 ops/s였고, consensus-only도 약 26 ops/s여서 Recorder 내구화가 주 병목임을
확인했다.

## 채택한 변경

### RecorderFileStore bounded WAL

기존 hot Record는 authoritative head와 slot cache를 임시 파일에 쓰고 rename한 뒤
directory barrier를 반복했다. 새 경로는 미리 생성된 안정적인 `recorder.wal` inode에
frame 하나를 append하고 file sync 한 번이 성공한 뒤에만 ACK한다.

각 frame은 length, generation, global sequence, previous digest, frame digest, slot,
configuration, recorded head와 필요한 inline command를 포함한다. 재시작 시 연속된
digest chain만 재생한다. 구조적으로 잘린 마지막 frame은 ACK되지 않은 tail로
절단하지만, 완전한 길이의 마지막 frame checksum 오류와 interior corruption은 모두
fail-closed한다.

WAL은 16 MiB soft bound 또는 1,024 frame hard bound에서 회전한다. 회전은 command,
slot, configuration, QRHD v3 checkpoint를 먼저 내구화한 뒤 같은 WAL inode를
truncate하고 재사용한다. 구성 변경은 WAL을 drain한 후 기존 intent 기반 전이 경로를
사용한다. 정상 Record의 계측된 barrier는 file sync 1회, directory sync 0회다.

| consensus-only 5-run 중앙값 | 이전 | WAL | 변화 |
| --- | ---: | ---: | ---: |
| ops/s | 24.06 | 85.43 | 3.55x |
| p50 | 40.14 ms | 10.12 ms | -74.8% |
| p99 | 65.73 ms | 31.11 ms | -52.7% |

### 공개 읽기 경로

- KV는 redb가 반환한 owned value를 응답으로 이동해 두 번째 복사를 제거했다.
- multi-thread Tokio의 Local/AppliedIndex KV 읽기는 `block_in_place` fast path를
  사용하고, current-thread와 ReadBarrier는 blocking pool을 유지한다.
- SQL은 applied index/hash를 한 SQLite statement/snapshot에서 읽는다.
- SQL 단독 읽기는 inline fast path, 겹친 읽기는 blocking pool을 쓰는 adaptive
  in-flight counter를 사용한다. cancellation, panic, JoinError에도 RAII guard가
  counter를 복구한다.
- SQL HTTP JSON byte 제한은 HTTP 경계로 이동했고, 직렬화된 body를 그대로 재사용한다.
  embedded API의 raw result 제한은 그대로다.
- Graph empty-parameter query의 중복 prepare를 제거하고 applied tip을 한 query로
  읽는다. document hit는 한 Ladybug query snapshot에서 document와 tip을 같이
  반환하고, miss는 explicit read transaction에서 재확인한다.

Ladybug worker thread를 2개에서 4개로 늘린 후보는 c=1이 2.9% 하락하고 c=8이
6.9%만 향상돼 기각했다. SQL을 전부 inline 처리한 후보는 c=8 tail을 악화시켰고,
별도 semaphore/상주 worker 후보는 c=1 처리량을 기존 수준으로 되돌려 기각했다.

### typed embedded batch의 초기 matrix

`NodeRuntime`과 `RhizaHandle`에 SQL, Graph, KV별 typed batch API를 추가했다. 배치는
원자적 cross-command transaction API가 아니라, 기존 ordered writer batch를 공개해
여러 logical command가 한 QuePaxa entry를 공유하게 하는 최적화다.

- 전체 vector를 profile, 길이 1..=64, encoding, command size까지 먼저 검증한다.
- 결과 vector는 입력 순서와 길이를 유지하고 item별 오류를 격리한다.
- 같은 request ID와 같은 payload는 기존 idempotency 결과를 재생한다.
- `BatchWriteError::NotAttempted`와 `Indeterminate`를 구분한다.
- 성공 item 중 가장 큰 applied index에 대해 embedded durability를 한 번만 확인한다.
- `Indeterminate`이면 동일 vector와 동일 request ID 전체를 재시도하도록 계약한다.
- Graph/KV 결과는 내부에서 domain outcome을 유지하고 HTTP 경계에서만 DTO로 바꾼다.

256 logical operation, concurrency 1의 초기 단일 matrix는 다음과 같다. 이 표는
역사적 구현 검증으로 남기며, batch 크기 권고는 위의 순서 회전 48-cell 후속 측정이
대체한다.

| batch | KV ops/s | SQL ops/s | Graph ops/s | qlog entries |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 58 | 82 | 38 | 256 |
| 2 | 107 | 168 | 68 | 128 |
| 4 | 225 | 121* | 118 | 64 |
| 8 | 430 | 542 | 125 | 32 |
| 16 | 540 | 1,149 | 281 | 16 |
| 32 | 1,667 | 1,783 | 264 | 8 |
| 64 | 3,931 | 2,452 | 297 | 4 |

`*` SQL batch 4는 host-load outlier로 tail이 비정상적으로 커 판정에 사용하지 않았다.
이 초기 표만 보면 더 큰 batch가 유리했지만 single matrix라 host load와 낮은 독립
응답 수에 민감했다. 최종 운영 권고는 KV 32, SQL 16, Graph 8이며, SQL 32는 tail보다
처리량을 우선할 때만 명시적으로 선택한다.

## 정확한 읽기 A/B

이전 커밋 `c9cf590`에서 빌드한 고정 바이너리와 최종 후보 바이너리를 같은 호스트에서
비교했다. 최종 후보 SHA-256은
`574ad674064be0b43be65583674c0f2f7784f41647863c81551122021ff352c0`다.

| 경로 | 이전 ops/s | 최종 ops/s | 변화 |
| --- | ---: | ---: | ---: |
| KV handle c=1 | 68,896 | 337,673 중앙값 | 4.90x |
| SQL handle c=1 | 34,535 | 92,590 중앙값 | 2.68x |
| Graph Cypher c=1, 최신 pair | 1,280 | 1,487 | +16.2% |
| Graph Cypher c=8, 최신 pair | 3,105 | 4,712 | +51.8% |

Graph의 더 이른 3-run exact A/B 중앙값은 c=1 +23.2%, c=8 +39.2%였다. 절대
ops/s는 장시간 release build 이후 열·I/O 상태에 따라 크게 변했지만 방향은 모든
paired run에서 같았고 오류는 0이었다.

## 검증과 제한

- workspace Clippy: `--workspace --all-targets --all-features -D warnings` 통과
- bench Clippy 및 `rhiza-profile` 테스트 10개 통과
- Node all-feature unit 36개 통과
- embedded Graph/KV feature 테스트 16개 통과
- Graph unit/integration 53개 통과
- QuePaxa unit 15, dynamic membership 16, genuine consensus 29,
  Recorder durability fast path 12개 통과
- format 및 `git diff --check` 통과

전체 workspace 직렬 실행 중 다른 build가 겹친 한 차례에 KV HTTP 테스트 2개가
response deadline을 넘겼다. 두 테스트를 단독 직렬 재실행했을 때 8/8 통과했으며,
동시 build가 없는 최종 `cargo test --workspace --all-features --
--test-threads=1` 재실행은 모든 unit, integration, doc test를 오류 없이 통과했다.

이번 후보는 아직 미커밋 상태이므로 release benchmark가 아니다. multi-host network,
HTTP serialization, TLS transport, remote checkpoint, failure/reconnect, 30분 soak와
Kubernetes 자원 제한은 별도의 최종 채택 gate로 남는다.

## 다음 최적화 후보

Graph tip을 naive 장기 `RwLock`으로 캐시하는 설계는 긴 Cypher가 consensus write를
막고 snapshot lock order를 복잡하게 하므로 기각했다. 향후 필요하면 generation을
가진 optimistic cache를 사용하고, writer 경합 시 같은 Ladybug read transaction에서
DB tip으로 fallback해야 한다. exact A/B에서 cheap-read 10–15% 이상 개선되고 mixed
read/write의 write p99 악화가 5% 이내일 때만 채택한다.

WAL hot append의 Linux `sync_data` fast path는 syscall semantics와 독립 durability
review를 근거로 correctness-preserving 후보로 구현했다. 비 Linux steady append와
모든 metadata 변경은 `sync_all`을 유지한다. provenance-complete Docker 진단에서
aggregate median과 p50/p95/p99는 native가 유리했지만 paired `fsync/native` 중앙값
0.9279와 6/12 대 6/12 승률은 mixed다. 가상 filesystem 진단이므로 clean physical
ext4, XFS, 목표 Kubernetes CSI에서 순서 교차 측정과 power-loss/reopen matrix를
통과해야 production 성능 채택으로 올릴 수 있다.
