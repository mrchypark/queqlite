# Rhiza strong-read 성능 격차 해소 계획

작성일: 2026-07-18

## 결론과 현재 상태

현재 `ReadBarrier`는 선형화 가능한 읽기를 제공하지만, 읽기마다 새 empty Noop을 합의하고 Recorder WAL을 동기화한다. 따라서 strong-read 처리량이 durable write/fsync 처리량에 묶이는 것은 구현 결함이 아니라 현재 프로토콜의 직접적인 비용이다.

진행 중인 최적화 가운데 다음 항목은 비용을 줄이지만 근본 해결책은 아니다.

- typed `RecordSummary.decided`를 재사용하여 barrier 사전 검사를 2 quorum round에서 1 round로 줄임
- SQL empty Noop materialization을 두 메타데이터 트랜잭션에서 한 CAS 트랜잭션으로 줄임
- materializer의 applied index/hash를 한 snapshot의 `applied_tip`으로 조회
- 같은 시점에 도착한 barrier를 한 generation으로 묶는 coalescing 상태기계 개발

앞의 세 항목은 한 번의 barrier가 쓰는 CPU·RPC·로컬 DB 비용을 줄일 뿐, 요청마다 Recorder WAL sync와 QLog append를 수행하는 상한을 제거하지 않는다. Coalescing은 c4/c16을 완화하지만 c1은 개선하지 못한다.

근본 목표는 empty-slot common path를 **read-only quorum fence**로 바꾸는 것이다. 이 경로는 quorum 상태를 검증하되 새 Noop, Recorder WAL append/sync, QLog append, materializer apply를 만들지 않아야 한다.

## 3-peer 진단 기준선

사용자가 제공한 3회 교차 실행 중앙값은 다음과 같다.

| Concurrency | Rhiza `ReadBarrier` | Hiqlite `query_consistent` | 처리량 차이 |
|---|---:|---:|---:|
| c1 | 44.6 ops/s, p50 18.0 ms | 5,278 ops/s, p50 0.173 ms | 118배 |
| c4 | 46.2 ops/s, p50 83.6 ms | 13,770 ops/s, p50 0.267 ms | 298배 |

Rhiza c1 한 실행에는 약 5초 outlier가 있었다.

조건:

- Rhiza: 3개 file-backed Recorder, 2/3 quorum, transport 제외
- Hiqlite: 3개 Raft node, 실제 loopback socket, 2/3 quorum
- Hiqlite log policy: `LogSync::Immediate`
- 각 조건 3회 교차 실행 후 중앙값

이 결과는 transport가 주원인이 아님을 보여준다. Rhiza는 transport를 제외했는데도 c4에서 처리량이 거의 증가하지 않고 p50만 약 4.6배 늘었다. durable Noop이 commit mutex와 fsync queue를 직렬화하기 때문이다.

보고된 원본 위치는 아래와 같지만 모두 ignored `target/` 경로이므로 삭제되기 전에 tracked benchmark artifact로 복사해야 한다.

- `target/side-strong-read-bench/results-2026-07-18.json`
- `target/side-strong-read-bench/src/bin/rhiza_strong_read.rs`
- `target/side-strong-read-bench/src/bin/hiqlite_strong_read.rs`

현재 worktree에서는 위 세 파일이 확인되지 않았다. 최종 성능 판정 전 원본 명령, revision, host 정보와 raw run을 다시 보존해야 한다.

## 목표 계약

strong read는 다음을 모두 만족해야 한다.

1. 읽기 호출이 시작되기 전에 완료된 write는 반드시 보인다.
2. 읽기와 겹친 write는 읽기 전이나 후 어느 쪽으로든 선형화될 수 있지만, 반환된 값과 applied index/hash는 하나의 실제 snapshot이어야 한다.
3. 로컬 QLog/materializer가 모르는 committed prefix가 있으면 먼저 따라잡는다.
4. pending/accepted 상태를 empty로 오판하지 않는다.
5. epoch, config id/digest, membership transition을 가로질러 fence를 재사용하지 않는다.
6. quorum을 얻지 못하면 stale 값을 반환하지 않고 retryable `Unavailable`로 실패한다.
7. write의 기존 ACK-before-fsync 내구성은 변경하지 않는다.

## 목표 프로토콜: read-only quorum fence

아래는 구현 후보이며, 테스트와 안전성 증명 없이 단순히 `Empty`에서 조기 반환해서는 안 된다.

1. commit lane에서 로컬 QLog와 materializer가 정확히 같은 anchor `(N, H)`인지 확인한다.
2. next slot `N + 1`에 대해 typed Recorder summary를 quorum에서 읽는다.
3. 각 Recorder의 summary 읽기는 `record`, promise/accept, decision-proof 설치와 같은 직렬화 순서에 놓여야 한다. 응답 생성이 durable state publish보다 앞서면 안 된다.
4. 결과에 따라 처리한다.
   - `Committed`: 인증된 entry를 QLog/materializer에 적용하고 새 next slot에서 반복한다.
   - `Pending`: empty로 처리하지 않는다. 기존 결정을 회복하거나 bounded retry 후 실패한다.
   - `Unavailable`: strong read를 실패시킨다.
   - quorum 전체가 현재 configuration에서 검증된 `Empty`: `(N, H)`를 read fence로 반환한다. Noop을 제안하지 않는다.
5. fence 이후 backend query는 fence anchor 이상인 하나의 snapshot에서 값과 applied index/hash를 함께 반환한다.

선형화 후보 지점은 empty summary quorum의 마지막 유효 응답이 수집되는 시점이다. 이 주장이 성립하려면 다음 proof obligation을 코드와 테스트로 고정해야 한다.

- 완료된 write quorum과 read quorum은 적어도 한 Recorder에서 교차한다.
- 교차 Recorder의 summary는 write ACK를 가능하게 한 durable accept/decision보다 뒤에서 선형화된다.
- hash-chained slot에는 gap이 없으므로 `N + 1`이 quorum에서 empty라면 `N + 2` 이상의 완료된 결정도 존재할 수 없다.
- summary 수집 중 시작해 이후 완료된 write는 read fence 뒤로 선형화할 수 있다.
- configuration transition entry와 새 membership의 quorum을 혼합하지 않는다.

이 proof obligation을 만족하지 못한다면 durable Noop을 제거할 수 없다. leader/clock lease를 임의로 추가하는 것도 대안이 아니다.

## 즉시 완화책: generation coalescing

read-only fence와 별개로 동시 barrier는 하나의 durable Noop을 공유할 수 있다.

필수 generation 규칙:

- `Collecting` 단계에 등록된 요청만 같은 barrier를 공유한다.
- leader가 `Running`으로 전환한 뒤 도착한 요청은 반드시 다음 generation에 들어간다.
- 완료된 generation 결과는 이후 요청에 재사용하지 않는다.
- predecessor generation이 실패하면 다음 generation은 독립적으로 다시 시도한다.
- leader drop, shutdown cancellation, no-quorum 실패는 모든 waiter를 깨운다.
- 공유 결과에는 exact `LogAnchor`가 포함되어 각 backend query가 그 anchor 이상인 snapshot인지 검증한다.

Coalescing은 c4/c16의 QLog/fsync 수를 낮출 수 있지만, c1 44.6 ops/s의 구조적 상한은 그대로다. 따라서 최종 해법이 아니라 P0 완화책이다.

## 구현 순서

### P0: 증거와 완화책 고정

1. 위 benchmark harness와 raw JSON을 tracked artifact로 보존한다.
2. coalescing generation 상태기계를 실제 SQL/Graph/KV strong-read 경로에 연결한다.
3. `qlog_delta`, Recorder WAL bytes, sync count, coalesced group size를 benchmark 결과에 기록한다.
4. c1/c4/c16을 각각 3회 이상 교차 실행한다.

### P1: read-only fence를 테스트 우선으로 구현

1. Recorder operation ordering을 명시하고 deterministic race tests를 추가한다.
2. `inspect_read_fence_at(slot, prev_hash)`처럼 읽기 전용 결과를 기존 decision recovery API와 분리한다.
3. Node common path의 `Empty`에서 Noop proposal 없이 anchor를 반환한다.
4. `Committed`와 `Pending` 경로는 기존 인증·복구 코드를 재사용한다.
5. SQL/Graph/KV query가 fence 이상의 단일 snapshot을 반환하도록 backend별 계약 테스트를 추가한다.

### P2: 긴 query와 commit lane 분리

read fence를 얻은 뒤 사용자 query 동안 global commit mutex를 계속 보유할 필요가 있는지 backend별로 판단한다.

- Graph/KV: engine read transaction이 anchor 이상임을 검증한 뒤 snapshot을 pin할 수 있는지 확인한다.
- SQL: QWAL atomic rename으로 connection이 이전 inode를 가리킬 수 있으므로 generation-fenced connection/snapshot 없이는 mutex를 해제하지 않는다.

## 필수 회귀 테스트

- read 호출 전에 완료된 write가 항상 보임
- write/read 동시 실행의 허용 가능한 두 선형화 순서
- historical identical empty Noop 뒤에서도 최신 prefix까지 catch-up
- accepted-but-undecided slot을 `Empty`로 오판하지 않음
- 서로 다른 valid certificate 발견 시 fail closed
- 한 peer 중단 시 2/3으로 성공, 두 peer 중단 시 stale read 없이 실패
- configuration transition 직전·도중·직후 fence
- cancellation과 leader/participant drop 시 waiter 누수 없음
- empty common path에서 QLog entry, Recorder WAL bytes, sync count가 모두 증가하지 않음
- restart 후 기존 write와 materializer tip 불변
- SQL/Graph/KV 반환 값과 index/hash가 같은 snapshot임

## 성능 합격 기준

동일 host와 동일 3-peer 조건에서 다음을 모두 기록한다.

- throughput c1/c4/c16
- p50/p95/p99/p99.9 및 최대 latency
- 요청당 quorum RPC, Recorder WAL bytes, sync count, QLog delta
- CPU/op와 coalesced group size
- 성공·timeout·retry 수

1차 gate:

- read-only empty common path: `0` Noop, `0` Recorder WAL append/sync, `0` QLog delta
- c1: 최소 4,000 ops/s, p50 0.5 ms 이하
- c4: 최소 10,000 ops/s, p50 1 ms 이하
- 100 ms 이상 unexplained outlier 없음
- 모든 consistency/failover/restart 회귀 테스트 통과

최종 parity gate는 같은 실행에서 Hiqlite `query_consistent` 처리량의 80% 이상이며, 차이가 남으면 transport, worker scheduling, backend snapshot 비용을 각각 분해한다.

## 채택하면 안 되는 단축안

- Recorder RPC와 선형화 순서가 증명되지 않은 상태에서 `Empty`를 바로 strong-read 성공으로 처리
- completed barrier anchor를 이후 요청에 캐시해 재사용
- `AppliedIndex(N)`을 다른 peer의 strong read와 동일하게 간주
- wall-clock lease를 clock bound와 leader ownership 증명 없이 도입
- write ACK 전에 Recorder sync를 제거하거나 interval durability로 변경
- SQL QWAL generation fencing 없이 query 전에 commit mutex 해제

핵심 우선순위는 명확하다. Coalescing으로 동시성 병목을 즉시 줄이고, 이어서 leaderless QuePaxa의 quorum-intersection 및 Recorder ordering을 증명한 read-only fence를 도입해야 한다. 전자가 c4/c16 완화책이고 후자가 c1과 Hiqlite parity를 해결하는 경로다.
