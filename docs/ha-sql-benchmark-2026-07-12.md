# HA SQLite 계열 비교 메모 (2026-07-12)

## 범위와 원칙

이 문서는 2026-07-12 시점의 `Queqlite`, `Hiqlite`, `HaQLite`를 비교한 간단한 엔지니어링 메모다.

- `측정됨`: 작성 당시 로컬 실행이나 공개 벤치에서 관측된 항목
- `미측정`: 아직 재현하지 못했거나, 컴파일 실패/실험 미실시로 수치가 없는 항목
- 서로 다른 프로젝트의 내구성 모델, 배포 토폴로지, 벤치 조건이 같지 않으므로 절대 성능 순위로 읽으면 안 된다

중요: `target/queqlite-bench/...`로 표시한 실행 디렉터리는 저장소에 포함되지 않은
unversioned 로컬 관측이다. 현재 checkout만으로 수치를 독립 감사할 수 없으며, 릴리스나
성능 주장의 근거로 쓰려면 벤치를 다시 실행해 아티팩트를 별도로 보존해야 한다.

## 한줄 결론

`Queqlite`는 현재 로컬 3노드, `no PVC`, RustFS 로컬 S3 시뮬레이터 분리 배치에서 `sync`, `bounded(1s)`, `periodic(1s)` 쓰기를 측정했다. `Hiqlite`는 같은 Mac에서 3개 서버와 remote client, 동시성 4의 단일 INSERT를 측정했지만 로컬 Raft WAL `interval_200`이므로 OSS 내구성 조건은 다르다. `HaQLite`는 기준 커밋과 `v0.4.0` 모두 공식 의존성 조합이 빌드되지 않아 수치가 없다.

## 1. 시스템별 상태

### Queqlite

`측정됨`

- 구성: 3노드, `normal sync`, PVC 없음
- 객체 저장소: RustFS를 로컬 S3 시뮬레이터로 별도 배치
- 주의: RustFS는 `Queqlite` 컴포넌트가 아니라 외부 시뮬레이터다. CPU/메모리와 저장소 비용을 `Queqlite` 자체 비용으로 합산하면 안 된다

### Hiqlite

`로컬 smoke 측정됨`

- 공개 설명상 `openraft` 기반 로컬 영속 스토리지와 SQLite 상태머신을 사용한다
- S3 원격 백업 기능을 제공한다

### HaQLite

`미측정`

- 공개 README 기준 실험적 상태다
- `SingleWriter + Continuous`는 로컬 쓰기 후 `walrust`로 WAL을 S3에 비동기 전송한다
- `SingleWriter + Cloud`는 매 커밋을 S3에 업로드한 뒤 응답한다고 설명한다
- 이번 검토 기준 커밋 `db6db54...` 및 `v0.4.0`은 공식 `walrust` 의존성 조합에서 바로 빌드되지 않아 측정값을 만들지 못했다

## 2. 측정값 비교

### 2.1 Queqlite 기본 쓰기

`측정됨`

- 당시 로컬 실행 디렉터리(미포함): `target/queqlite-bench/20260711-170514-5240`
- 워크로드: `c4`, `20s`, write, `INSERT ... RETURNING`
- 결과:
  - 성공 `1282`, 오류 `0`
  - 처리율 `64.1 tx/s`
  - 지연 `p50 102.4ms / p95 204.8ms / p99 204.8ms`
- 히스토그램은 지수형 버킷이라 분위수는 거친 근사치다

### 2.2 Queqlite 노드 삭제 중 쓰기

`측정됨`

- 당시 로컬 실행 디렉터리(미포함): `target/queqlite-bench/20260711-171848-31702`
- 워크로드: `c4`, `60s`, write, 노드 교체 포함
- 구간별 결과:
  - 장애 전 `10s`: `85.7 tx/s`, 오류 `0`
  - 대체 노드 `Ready`까지: `45.825s`
  - 장애 중: `28.63 tx/s`, 오류 `437`
  - 오류 구성: `419 x HTTP 429`, `18 x HTTP 503`
  - 장애 중 지연: `p95 409.6ms / p99 1638.4ms`
  - 장애 후: `4.285s`, `27.30 tx/s`, 오류 `0`
- 해석:
  - 장애 중 쓰기 자체는 계속 처리됐지만 처리율과 tail latency가 크게 악화됐다
  - 장애 후 구간이 `4.285s`뿐이라 회복 후 정상 처리율을 주장하기에는 표본이 부족하다

복구 command 재구성 결함 수정 후 재실험:

- 당시 로컬 실행 디렉터리(미포함): `target/queqlite-bench/20260711-180230-71577`
- 조건: `c4`, `70s`, write, 10초 시점 follower Pod 삭제
- 장애 전: `81.2 tx/s`, 오류 `0`, `p95 102.4ms`, `p99 204.8ms`
- replacement Ready: `47.927s`
- 장애 중: `33.57 tx/s`, 오류 `381` (`429` 363, `503` 18), `p95 204.8ms`, `p99 819.2ms`
- 장애 후 12.168초: `34.19 tx/s`, 오류 `0`, 모든 관측 지연이 `204.8ms` 버킷

첫 기동의 `command bytes unavailable` 재시작은 사라졌지만 Ready까지 약 48초가 걸렸고 after 처리율도 장애 전 수준으로 돌아오지 않았다. 현재 no-PVC rejoin은 기능적으로 완료되더라도 빠른 HA 복구 성능은 충족하지 못한다.

### 2.3 Queqlite 리소스

`측정됨`

기본 쓰기 런 `20260711-170514-5240`의 3개 `Queqlite` 노드 합산 기준:

- CPU 누적 사용량: `16.231s`
- 샘플 구간: `24s`
- 평균 CPU 사용: `16.231 / 24 = 0.676 core`
- 최고 합산 메모리: `107,728,896 bytes`
- 평균 합산 메모리: `74,322,330 bytes`

주의:

- 위 CPU/메모리는 `Queqlite` 세 노드만 합산한 값이다
- 같은 런의 RustFS 리소스는 별도 계정이다

### 2.4 Hiqlite 로컬 smoke

`측정됨, 단 Queqlite와 내구성 비동등`

- 기준 커밋: `c8316c53799c509990475ea8e2aa2ef8679e070e`
- 임베디드 네이티브:
  - `1c`, `1000` single inserts: `2463/s`
  - transaction: `142857/s`
  - fresh select: `435us`
  - `4c`, `10k` single inserts: `8368/s`
  - transaction: `144927/s`
- 공식 server/client의 로컬 3-process smoke:
  - `c1`, `r100` single inserts: `2777/s`
  - transaction: `100000/s`
  - fresh query: `195us`
  - cached query: `64-147us`
- 단, 이후 cache PUT 단계에서 모든 서버가 `Option::unwrap`으로 크래시해 smoke 결과 이상으로 일반화하면 안 된다

### 2.5 Durability mode와 Hiqlite remote 비교

`측정됨, 단 내구성 및 실행 계층 비동등`

공통 조건은 Apple M3 Mac 한 대, 3개 DB 서버, 별도 remote client, 동시성 4, 단일 행 INSERT다. Queqlite는 vind 안의 3개 Pod와 RustFS 계측 proxy를 사용해 5초 warmup 후 20초 동안 `INSERT ... RETURNING`을 실행했다. Hiqlite는 native 3-process, no TLS, 공식 remote runner로 100,000 single INSERT를 실행했다.

| System / mode | ACK의 의미 | Committed single-row writes/s | 오류 | 지연 |
|---|---|---:|---:|---:|
| Queqlite `sync` | qlog와 manifest가 OSS에 반영됨 | 53.0 | 0/1060 | p50 102.4ms, p95 102.4ms, p99 204.8ms |
| Queqlite `bounded(1s)` | 로컬 quorum 적용, OSS lag 초과 시 backpressure | 100.2 | 1005/3009 (33.4% HTTP 503) | p50 25.6ms, p95 102.4ms, p99 102.4ms |
| Queqlite `periodic(1s)` | 로컬 quorum 적용, OSS flush는 1초 주기 | 162.55 | 0/3251 | p50 25.6ms, p95 51.2ms, p99 51.2ms |
| Hiqlite `interval_200` | 로컬 Raft WAL, 약 200ms 주기 flush | 9,795 | 0/100,000 | 공식 runner가 percentile 미제공; c4 상각 평균 약 0.408ms |
| Hiqlite `interval_1000` | 로컬 Raft WAL, 약 1초 주기 flush | 7,829 | 0/100,000 | 공식 runner가 percentile 미제공; c4 상각 평균 약 0.511ms |

이 표의 공통 단위는 성공한 단일 행 INSERT다. Queqlite 내부 JSON 필드가 `committed_transactions_per_second`인 이유는 한 요청이 여러 statement의 원자적 transaction일 수도 있기 때문이다. 이번 workload는 요청마다 `INSERT ... RETURNING` 한 문장뿐이므로 `tx/s`와 `single-row writes/s`가 수치상 같다.

당시 로컬 실행 디렉터리(Queqlite 항목은 저장소에 미포함):

- Queqlite sync: `target/queqlite-bench/20260712-022156-66465`
- Queqlite bounded: `target/queqlite-bench/20260712-022432-68236`
- Queqlite periodic: `target/queqlite-bench/20260712-023024-83282`
- Hiqlite raw summary: `docs/hiqlite-benchmark-artifact-2026-07-12.md`

Hiqlite 측정은 commit `c8316c5`, release/LTO/jemalloc, `HQL_WAL_SIZE=8MiB`, `HQL_LOGS_UNTIL_SNAPSHOT=10000`, `HQL_CACHE_STORAGE_DISK=false`다. `interval_200`은 10.209초, Queqlite periodic과 주기를 맞춘 `interval_1000`은 12.772초가 걸렸고 두 실행 모두 세 SQLite 복제본에서 정확히 100,000행을 확인했다. CPU/RSS는 `interval_200`에서만 측정했으며 서버 합산 평균 CPU 약 `368%`, 평균 RSS `121.3MiB`, 관측 peak RSS 합계 `366.0MiB`였다. Hiqlite에는 OSS lag에 따라 admission을 차단하는 Queqlite `bounded` 대응 mode가 없다.

이 표에서 Hiqlite와 가장 가까운 의미는 같은 1초 주기의 Queqlite `periodic`과 Hiqlite `interval_1000`이지만 동등하지 않다. Hiqlite는 로컬 SSD를 복구 원본으로 사용하고 S3는 주기적 backup이다. Queqlite는 `emptyDir`를 cache로 보고 OSS를 복구 원본으로 사용한다. 약 48배 처리율 차이는 consensus 구현뿐 아니라 native 대 vcluster, binary RPC 대 HTTP/JSON, 로컬 WAL 대 per-flush OSS manifest/CAS 비용을 모두 포함한다.

세 최종 mode 런은 vcluster port-forward와 리소스 수집 경로의 간섭을 성능 결과에서 제거하기 위해 `QUEQLITE_BENCH_RESOURCE_SAMPLING=0`으로 실행했다. CPU/메모리는 비교하지 않으며 기존 2.3절의 정상 수치는 별도 baseline으로만 유지한다.

### 2.6 HaQLite

`미측정`

- 기준 커밋: `db6db54b555a56c7352a5c9d57f1b998da05d199`
- `v0.4.0` 포함 공식 공개 기준에서 곧바로 비교 가능한 측정 수치 없음
- 현재 문서에서 성능 결론을 내리면 안 된다

## 3. 비교 해석

이번 자료로 말할 수 있는 범위는 좁다.

- `Queqlite`는 적어도 현재 실험 조건에서 3노드 쓰기와 노드 교체 중 지속 처리의 초안 수치를 갖고 있다
- `Hiqlite` 수치는 같은 Mac에서 재현했지만 로컬 Raft 디스크 커밋이며 Queqlite의 매 쓰기 object-store `sync`보다 약한 full-cluster 내구성 조건이다. smoke 이후 안정성 이슈도 있어 그대로 가로비교하면 안 된다
- `HaQLite`는 아이디어 자체는 흥미롭지만 이번 기준에서는 빌드/측정 단계가 성립하지 않아 성능 비교 테이블에 넣을 수 없다

따라서 현재 시점의 정직한 요약은 다음이다.

- `Queqlite`: 로컬 측정값 있음
- `Hiqlite`: 로컬 smoke 수치 있음, 내구성 비동등
- `HaQLite`: 비교 수치 없음

## 4. 결함과 재실험 필요 사항

`측정된 사실`

- 노드 교체 런에서 대체 노드 첫 기동이 한 번 실패했다
- 실패 원인은 `command bytes unavailable`

`현재 상태`

- 이 복구 결함은 지금은 수정됐고 테스트도 추가됐다

`수정 후 검증`

- 수정 후 fault benchmark에서 첫 기동 crash는 재현되지 않았다
- 그러나 Ready까지 `47.927s`, 장애 중 오류 `381`, 이후 처리율 미회복이 관측됐다
- 따라서 command 복원 correctness 결함은 수정됐지만 RTO와 부하 회복은 별도 개선 대상이다

`이번 문서에서 하지 않은 것`

- RustFS 장애 주입은 사용자가 제외 요청했다
- 따라서 RustFS 실패 시나리오에 대한 성능/정합성 결론은 없다

## 5. RustFS 사용량 원장과 클라우드 단가

### 5.1 왜 별도 원장이 필요한가

RustFS는 이번 실험에서 `Queqlite` 내장 스토리지가 아니라 로컬 S3 시뮬레이터다. 그래서 아래 두 축을 분리해야 한다.

- `서비스 성능`: `Queqlite` HTTP SQL 벤치 결과
- `객체 저장소 사용량`: RustFS에 실제로 생성된 object 수, 저장 바이트, 요청 수

RustFS는 작업 로그와 audit trail 기능을 제공하지만, 이 문서가 참조한 로그와
인벤토리는 저장소에 포함되지 않았다. 최종 비용 계산을 검증하려면 벤치 결과와 함께
RustFS 로그/인벤토리를 별도 보존해야 한다.

### 5.2 clean metering 사용량

`측정됨`

- 당시 로컬 실행 디렉터리(미포함): `target/queqlite-bench/20260711-175103-52098`
- 조건: `10s`, concurrency `1`, warmup `0`, `389` successful writes
- setup의 DDL/seed 2건까지 합쳐 qlog publication은 `391`건이다
- S3 calls: `8210`, 즉 qlog transaction당 사실상 `21 calls`
- `GET 200`: `4300` (초기 1회를 제외하면 `11/transaction`)
- `PUT 200`: `3128` (`8/transaction`), `PUT 412`: `782` (`2/transaction`); 합계 `10/transaction`
- 요청 바이트: `52,907,199`, 응답 바이트: `96,535,436`
- logical retained: `393 objects / 1,010,012 bytes`

`412`는 조건부 PUT 충돌이며 비용 추정에서는 보수적으로 PUT 요청에 포함했다. 계측용 nginx는 RustFS와 같은 Pod의 sidecar이며 Queqlite 또는 RustFS 제품 구성요소가 아니다. logical inventory는 RustFS 내부 메타데이터/물리 복제 바이트가 아니라 cloud object-store 청구 모델에 대응하는 object payload 합계다.

### 5.3 최종 비용 계산식

월 비용은 아래처럼 분해하는 편이 안전하다.

```text
monthly_cost
  = storage_cost
  + put_list_cost
  + get_cost
  + egress_cost
```

세부식:

```text
storage_cost   = retained_gb_month * storage_usd_per_gb_month
put_list_cost  = (put_count + list_count) / 1000 * put_list_usd_per_1000
get_cost       = get_count / 1000 * get_usd_per_1000
egress_cost    = egress_gb * egress_usd_per_gb
```

세 provider 모두 저장 과금 단위가 `GiB-month`라 입력 decimal GB-month를 변환한다.

```text
retained_gib_month = retained_gb_month * 0.9313225746154785
gcs_storage_cost   = retained_gib_month * 0.020
```

### 5.4 2026-07-12 단가

`측정값이 아니라 가격표 입력값`

- AWS S3 Standard `us-east-1`: `$0.023/GiB-month`, `PUT/LIST $0.005/1k`, `GET $0.0004/1k`
- GCS Standard `us-central1`: `$0.020/GiB-month`, `Class A $0.005/1k`, `Class B $0.0004/1k`
- Azure Blob Hot LRS `East US 2`: `$0.0184/GiB-month`, `PUT/LIST $0.005/1k`, `GET $0.0004/1k`
- egress는 시나리오 의존이라 별도 입력이 필요하다
- Azure 값은 2026-07-12 Retail Prices API의 `General Block Blob v2`, Hot LRS, East US 2 값을 계산기 단위로 정규화해 고정했다

### 5.5 월 비용 시나리오

아래는 **측정 처리율 38.9 tx/s가 30일 내내 지속**, 호출 비율 고정, append-only 선형 증가, 평균 저장량은 월말 저장량의 절반, LIST/DELETE/GC 없음, same-region egress 비용 0이라는 단순 시나리오다.

- 월 writes: `100,828,800`
- 월 PUT-class: `1,008,288,000`
- 월 GET-class: `1,109,116,800`
- 월말 logical data: 약 `260.46 GB`, 평균 `130.23 GB-month`
- 총 object traffic: 약 `38.54 TB/month`

| Provider | Storage | PUT | GET | Total/month |
|---|---:|---:|---:|---:|
| AWS S3 Standard us-east-1 | $2.79 | $5,041.44 | $443.65 | **$5,487.88** |
| GCS Standard us-central1 | $2.43 | $5,041.44 | $443.65 | **$5,487.51** |
| Azure Blob Hot LRS East US 2 | $2.23 | $5,041.44 | $443.65 | **$5,487.32** |

핵심은 저장비가 아니라 호출비다. 현재 sync 경로는 write마다 GC lease와 manifest CAS를 반복하여 약 21번의 object API call과 약 382 KB의 양방향 object traffic을 만든다. 따라서 production 전에 lease 범위 확대, manifest update batching, bounded/periodic durability와 RPO 정책 분리 중 하나가 필요하다.

Azure 가격표는 write/read를 10K 단위로 표시하며, 계산기에는 각각 `$0.05/10K`, `$0.004/10K`를 `$0.005/1K`, `$0.0004/1K`로 정규화했다.

### 5.6 mode별 object call 증폭

이번 세 mode는 benchmark 시작 직전에 meter log를 비웠다. 아래 분모에는 warmup과 측정 구간의 성공 write를 모두 포함하고, PUT에는 성공 PUT과 conditional failure를 포함했다. 가격은 세 cloud 기본표에서 동일한 `PUT $0.005/1k`, `GET $0.0004/1k`를 적용했다.

| Mode | 성공 write | PUT/write | GET/write | 총 call/write | API cost / 1M 성공 write |
|---|---:|---:|---:|---:|---:|
| `sync` | 1,420 | 10.00 | 11.00 | 21.00 | $54.40 |
| `bounded(1s)` | 2,662 | 1.00 | 1.33 | 2.33 | $5.53 |
| `periodic(1s)` | 4,110 | 0.82 | 1.11 | 1.93 | $4.53 |

관측 종료 시 logical retained data는 각각 `3.92MB`, `5.59MB`, `8.85MB`였다. 단기 실행의 고정 manifest 비용과 qlog lifecycle을 포함하므로 이를 월 저장비로 선형 외삽하면 안 된다. 같은 이유로 LIST, DELETE, 장기 compaction/GC 비용은 이 표에 없다. 관측 평균을 단순 환산한 양방향 object traffic은 성공 write 1백만 건당 약 `1,317GB`, `23.0GB`, `22.6GB`였다. sync의 GET 응답량은 qlog history 길이에 따라 커져 안정된 선형 단가가 아니며, same-region egress 비용은 0으로 두었다.

meter는 setup transaction 이후 비워졌고, 종료 시 qlog와 checkpoint의 index/hash가 같아진 뒤 수집됐다. bounded와 periodic은 drain에 각각 1초가 걸렸다. periodic의 PUT에는 관측된 HTTP 502 두 건도 보수적으로 포함했다.

핵심은 `bounded/periodic`이 OSS 호출 증폭을 약 9~11배 줄였다는 점이다. 다만 `bounded(1s)`는 이번 포화 부하에서 33.4%를 503으로 거부해 RPO 상한을 지켰고, `periodic(1s)`는 모두 ACK했지만 OSS가 뒤처질 때 admission을 제한하지 않는다.

### 5.7 재현 명령

당시 로컬 실행 디렉터리가 남아 있는 환경에서만 확인:

```sh
for id in 20260712-022156-66465 20260712-022432-68236 20260712-023024-83282; do
  jq '.warmup, .measurement.totals' "target/queqlite-bench/$id/benchmark.json"
  jq . "target/queqlite-bench/$id/checkpoint-drain.json"
  jq . "target/queqlite-bench/$id/object-usage.json"
done
```

최종 mode 재실행:

```sh
QUEQLITE_BENCH_RESOURCE_SAMPLING=0 QUEQLITE_DURABILITY_MODE=sync \
  scripts/bench-vind.sh --duration 20s --warmup 5s --concurrency 4 --workload write
QUEQLITE_BENCH_RESOURCE_SAMPLING=0 QUEQLITE_DURABILITY_MODE=bounded \
  QUEQLITE_DURABILITY_MAX_LAG=1s \
  scripts/bench-vind.sh --duration 20s --warmup 5s --concurrency 4 --workload write
QUEQLITE_BENCH_RESOURCE_SAMPLING=0 QUEQLITE_DURABILITY_MODE=periodic \
  QUEQLITE_DURABILITY_INTERVAL=1s \
  scripts/bench-vind.sh --duration 20s --warmup 5s --concurrency 4 --workload write
```

월 비용 계산기 실행:

```sh
cargo run --release --manifest-path bench/Cargo.toml --bin queqlite-cost -- \
  --provider aws-s3-standard-us-east-1 \
  --retained-gb-month 100 \
  --put-count 2000 \
  --list-count 1000 \
  --get-count 1000 \
  --egress-gb 10 \
  --egress-usd-per-gb 0.09
```

## 6. 해석상 주의점

- closed-loop 부하다. 클라이언트가 응답을 기다리므로 open-loop 최대 처리량과 다르다
- clean metering 런의 nginx sidecar는 추가 hop이므로 38.9 tx/s를 비계측 기본 성능과 직접 비교하지 않는다. 이 런은 call/byte 비율 산출용이다
- correctness history가 없다. 이번 문서는 성능과 장애 중 응답 특성만 다룬다
- 각 조건당 사실상 한 번의 런만 있다
- Queqlite 히스토그램은 coarse exponential bucket 기반이라 분위수는 정밀 측정이 아니다
- 프로젝트 간 내구성 모델이 동등하지 않다
- `Queqlite`: 이번 mode 표 기준 `sync|bounded(1s)|periodic(1s)`, no PVC, 외부 RustFS 시뮬레이터
- `Hiqlite`: OpenRaft 로컬 영속 로그/상태머신 + S3 백업
- `HaQLite`: SingleWriter lease + S3 WAL shipping 또는 commit-upload 모드
- 따라서 "누가 더 빠르다"보다 "어떤 내구성 조건에서 어떤 공개/실측 데이터가 있나"로 읽는 편이 맞다

## 7. 다음 측정

- rejoin RTO 약 48초의 단계별 profile과 after 처리율 미회복 원인 분석
- 동일 offered-rate sweep과 3회 이상 반복/신뢰구간 추가
- acknowledged write history와 복구 후 전 노드 correctness audit 추가
- HaQLite가 공식 의존성으로 빌드 가능한 시점에 동일 workload 재측정

## 출처

- Hiqlite commit `c8316c53799c509990475ea8e2aa2ef8679e070e`: <https://github.com/sebadob/hiqlite/tree/c8316c53799c509990475ea8e2aa2ef8679e070e>
- HaQLite commit `db6db54b555a56c7352a5c9d57f1b998da05d199`: <https://github.com/russellromney/haqlite/tree/db6db54b555a56c7352a5c9d57f1b998da05d199>
- AWS S3 pricing: <https://aws.amazon.com/s3/pricing/>
- Google Cloud Storage pricing: <https://cloud.google.com/storage/pricing/>
- Azure Blob Storage pricing: <https://azure.microsoft.com/en-us/pricing/details/storage/blobs/>
- RustFS logging and auditing: <https://docs.rustfs.com/features/logging/>
