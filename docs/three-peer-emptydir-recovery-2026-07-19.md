# 3-peer emptyDir / no-PVC recovery matrix

검증일: 2026-07-19
대상: Rhiza SQL 전체 매트릭스 (`rhiza-sql:recovery-20260719b`), no-quorum 회귀 (`rhiza-sql:recovery-20260719h`), Hiqlite `0.14.0` (`c8316c53799c`)
토폴로지: voter 3개, 데이터 볼륨 `emptyDir`, PVC 0개

## 결론

- 1 peer 손실에서는 두 시스템 모두 quorum을 유지하고 RPO 0으로 복구했다.
- 2 peer 손실에서는 강한 읽기와 쓰기가 닫혔다. Rhiza는 남은 peer의 로컬 읽기가 가능했다. 최초 전체 매트릭스에서는 1분 셀이 자동 재가입하고 3·5분 셀이 checkpoint DR로 전환했지만, 후속 반복에서는 hold와 무관하게 자동/DR 결과가 바뀌어 제어면 tail 변동임을 확인했다. Hiqlite는 세 셀 모두 backup DR이 필요했다.
- 3 peer 전체 손실에서는 두 시스템 모두 외부 복구 원본이 필수다. Rhiza는 sync checkpoint, Hiqlite는 completed backup에서 fresh 0→3 복구했다.
- 장애 유지시간은 복구시간의 주된 변수가 아니었다. 복구 방식과 제어면 재수렴이 RTO를 결정했다.

## 결과

`서비스 RTO`는 장애 해제 후 첫 강한 읽기/쓰기 성공까지, `완전 RTO`는 3 voter 수렴과 데이터 검증까지다.
1 peer 손실에서는 서비스가 중단되지 않았으므로 Rhiza의 8초는 outage가 아니라
해제 후 probe가 성공을 기록한 탐지 지연이다.

| 시스템 | 손실 | hold | 서비스 RTO | 완전 RTO | 장애 중 동작 | 복구 | RPO |
|---|---:|---:|---:|---:|---|---|---|
| Rhiza | 1 | 60s | 8s | 27s | write/ReadBarrier 성공 | peer 재생성 | 0 |
| Rhiza | 1 | 180s | 8s | 31s | write/ReadBarrier 성공 | peer 재생성 | 0 |
| Rhiza | 1 | 300s | 8s | 35s | write/ReadBarrier 성공 | peer 재생성 | 0 |
| Rhiza | 2 | 60s | 15s | 36s | local read 성공, write/ReadBarrier 실패 | 자동 재가입 | 0 |
| Rhiza | 2 | 180s | 44s | 65s | local read 성공, write/ReadBarrier 실패 | operator DR | last sync checkpoint |
| Rhiza | 2 | 300s | 50s | 71s | local read 성공, write/ReadBarrier 실패 | operator DR | last sync checkpoint |
| Rhiza | 3 | 60s | 13s | 34s | endpoint 0, 전 요청 실패 | operator DR | last sync checkpoint |
| Rhiza | 3 | 180s | 17s | 40s | endpoint 0, 전 요청 실패 | operator DR | last sync checkpoint |
| Rhiza | 3 | 300s | 13s | 34s | endpoint 0, 전 요청 실패 | operator DR | last sync checkpoint |
| Hiqlite | 1 | 60s | 0s | 9s | write/local/consistent 성공 | learner→voter | 0 |
| Hiqlite | 1 | 180s | 0s | 9s | write/local/consistent 성공 | learner→voter | 0 |
| Hiqlite | 1 | 300s | 0s | 10s | write/local/consistent 성공 | learner→voter | 0 |
| Hiqlite | 2 | 60s | 42s | 85s | write/consistent 실패 | backup DR | backup |
| Hiqlite | 2 | 180s | 40s | 84s | write/consistent 실패 | backup DR | backup |
| Hiqlite | 2 | 300s | 42s | 86s | write/consistent 실패 | backup DR | backup |
| Hiqlite | 3 | 60s | 7s | 51s | stable failure 뒤 전 요청 실패 | backup DR | backup |
| Hiqlite | 3 | 180s | 8s | 56s | stable failure 뒤 전 요청 실패 | backup DR | backup |
| Hiqlite | 3 | 300s | 10s | 55s | stable failure 뒤 전 요청 실패 | backup DR | backup |

모든 채택 셀은 요청한 hold 60/180/300초를 실제로 동일하게 유지했고, 복구 후 voter 3개, ACK 경계, 새 emptyDir marker 손실, PVC 0을 검증했다. Rhiza는 세 node의 최종 qlog tip hash 일치도 검증했다.

## 장애별 복구 단계

### 1 peer 손실

1. 손실 Pod의 `emptyDir`이 함께 사라진다.
2. 남은 2 voter가 quorum을 유지하므로 강한 읽기와 쓰기를 계속 처리한다.
3. StatefulSet이 fresh `emptyDir`로 peer를 재생성한다.
4. Rhiza는 남은 qlog/checkpoint에서 상태를 재구성한다. Hiqlite는 learner로 따라잡은 뒤 voter가 된다.
5. voter 3개, 장애 중 ACK, 최종 tip/멤버십을 검증한다.

### 2 peer 손실

1. 남은 1 peer는 로컬 상태를 보유하지만 quorum은 없다.
2. 강한 읽기와 쓰기를 fail-closed 한다. Rhiza local SQL read는 stale 허용 경로로 성공한다.
3. Rhiza는 새 peer 2개의 자동 재가입을 30초 기다린다. 성공하면 RPO 0이다. 실패하면 0→3으로 전환해 last sync checkpoint에서 재부트스트랩한다.
4. Hiqlite는 이 매트릭스에서 자동 복구하지 않고, completed backup을 지정해 0→3 restore한다.
5. ACK/RPO 경계와 3 voter 수렴을 검증한다.

### 3 peer 전체 손실

Rhiza:

1. object store의 sync checkpoint index/hash와 recorder identity를 확인한다.
2. 기존 StatefulSet을 0으로 유지하고 fresh `emptyDir` 세 개를 만든다.
3. 세 recorder를 checkpoint root에서 복구하고 consensus 시작점을 `checkpoint index + 1`로 초기화한다.
4. 3 voter를 활성화하고 write/ReadBarrier, ACK sentinel, 동일 qlog tip을 확인한다.

Hiqlite:

1. completed external backup과 그 시점의 RPO 경계를 확인한다.
2. 기존 voter를 0으로 유지하고 fresh `emptyDir` 세 개를 만든다.
3. backup restore 설정으로 DB/Raft state를 복원한다.
4. voter `[1,2,3]` 수렴을 기다린다.
5. backup 이전 sentinel은 존재하고 backup 이후 marker는 없음을 확인한 뒤 정상 시작 설정으로 되돌린다.

외부 checkpoint/backup이 없으면 emptyDir 전체 손실에서 lossless 복구는 불가능하다.

## 발견된 문제

1. **해결 — Rhiza no-quorum 지연과 fatal 오분류**: 원인은 public write가 1초에 timeout된 뒤 내부 제안이 긴 Recorder RPC와 commit mutex를 계속 점유하고, ReadBarrier가 그 뒤에서 막히는 것이었다. Recorder read fence 전용 worker lane, HTTP/TCP/Postcard record의 250ms quorum deadline, 전송 실패의 `ProposeFailed` 변환, `ProposeFailed`의 retryable `Unavailable` 매핑을 적용했다. 최종 F2/60s 회귀에서 write와 ReadBarrier가 모두 명시적 HTTP 503 `unavailable`을 반환했고 `read_no_quorum_latency_defect=false`, node non-fatal, local read 성공을 확인했다.
2. **Rhiza 2-peer 자동 복구 변동성**: 동일 30초 정책의 후속 전체 F2 반복에서는 60초가 operator DR, 180/300초가 자동 복구되어 최초 결과와 반전됐다. 최종 F2/60s 회귀는 다시 자동 복구했다. 따라서 hold 길이는 원인이 아니며 startup/rejoin 제어면의 tail 변동을 별도 추적해야 한다.
3. **Hiqlite 전체 손실 전환 ACK 창**: voter Pod가 모두 사라진 직후 안정적 failure가 성립하기 전 2건의 write ACK와 local read가 반복 관측됐다. 세 번째 probe에서 write/local/consistent가 모두 실패했다. 이 transition ACK는 backup DR 뒤 사라질 수 있으므로, 클라이언트가 받은 ACK와 backup RPO 사이의 노출로 취급해야 한다.
4. **제어면 RTO 차이**: 2 peer DR의 Hiqlite 완전 RTO는 84–86초, Rhiza checkpoint DR은 65–71초였다. 전체 손실 서비스 RTO는 Rhiza 13–17초, Hiqlite 7–10초였지만 Hiqlite 완전 voter 수렴은 51–56초로 Rhiza 34–40초보다 길었다.

## 범위와 한계

- fault target은 StatefulSet의 높은 ordinal부터 scale-down한 것이다. 임의 leader kill, 같은 Pod UID의 container restart, 네트워크 partition은 이 결과에 포함되지 않는다.
- abrupt Pod 삭제와 `emptyDir` 소실을 검증했으며 PVC는 만들지 않았다.
- object store도 같은 로컬 Kubernetes의 별도 namespace에 있는 RustFS `emptyDir`였다. 이는 복구 프로토콜 검증에는 유효하지만, 실제 독립 AZ/계정에 둔 durable object store의 내구성 증명은 아니다.
- Hiqlite는 upstream `0.14.0`의 proxy route 호환 문제만 별도 proxy 이미지로 수정했고 voter 이미지는 정확한 upstream commit/lock으로 빌드했다.

## 원시 증거

- Rhiza: `artifacts/rhiza-recovery-full-f1-direct`, `artifacts/rhiza-recovery-full-f2-h*-direct*`, `artifacts/rhiza-recovery-full-f3-h*-direct*`
- Rhiza no-quorum 최종 회귀: `target/rhiza-e2e/sql/20260719-142701-2968/recovery-matrix.jsonl` (F2/60s, service/full RTO 17/39s, RPO 0)
- Hiqlite: `artifacts/hiqlite-recovery-full-f1c`, `artifacts/hiqlite-recovery-full-f2`, `artifacts/hiqlite-recovery-full-f2-h300b`, `artifacts/hiqlite-recovery-full-f3-h*`
