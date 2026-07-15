# rhiza node RPC equal-TLS benchmark (2026-07-15)

> Historical benchmark: the names, TLS conditions, and measurements below
> describe the experiment as it was run. The runtime candidate was subsequently
> replaced by breaking change with plaintext `tcp-postcard` for one trusted
> Kubernetes-cluster boundary. Do not read `tcp-tls-postcard` below as a current
> selector or deployment contract.

## 결론

동일한 TLS 1.3 서버 인증 조건의 로컬 loopback 진단에서
`tcp-tls-postcard`가 처리량과 p99 지연 모두 6개 payload/concurrency 셀 전부에서
1위를 기록했다. 현재 프로덕션 후보인 `https-json` 대비 6개 셀 기하평균은 처리량
2.546배, p99 지연 3.421배 개선이었다.

이 결과만으로 RecorderRpc를 교체하지 않는다. 측정은 한 호스트·한 프로세스의
transport/codec 분해 벤치이고, Git worktree가 dirty였으며, 별도로 실행한 실제
QuePaxa sync-durability 벤치는 follower checkpoint 검증에 실패했다. 원격 두 호스트,
QuePaxa quorum, qlog, fsync, SQLite/LadybugDB/redb materialization까지 포함한 후보별
통합 벤치가 다음 승격 게이트다.

## 비교 조건

- 후보: `https-json`, `https-postcard`, `https-prost`, `tcp-tls-postcard`,
  `quinn-rpc-stream`, `quinn-lane`
- payload: 128 B, 4096 B
- concurrency: 1, 8, 64
- 각 셀: warmup 4,096회, measurement 60,000회
- 독립 실행: candidate order offset 0, 2, 4의 3회
- 집계: per-run throughput/p50/p95/p99/p999의 중앙값, max의 최악값. 샘플은
  실행 간 합치지 않았다.
- 보안: 실행별 한 개의 자체 서명 인증서와 전용 root, hostname `localhost`,
  server-auth only, TLS 1.3 only, 0-RTT 미사용
- ALPN: HTTPS `http/1.1`, TCP/Quinn `rhiza-bench/1`
- 관찰: HTTPS/TCP는 rustls session에서 TLS 1.3과 ALPN을 확인했고, Quinn은
  handshake data의 ALPN과 QUIC/TLS 1.3 invariant를 확인했다.
- 모든 행에서 warmup/measurement error 0, 측정 중 새 handshake 0,
  negotiation mismatch 0을 요구했다.
- 환경: Apple M3, macOS 26.3 arm64, rustc 1.95.0, AC 전원
- 시작 load average: 3.70 / 6.07 / 6.74; 종료: 8.07 / 7.18 / 7.06
- Git: `06b0860b8a8272d7fa62a498367995587d3b95cc`, dirty

집계기는 `diagnostic_valid=true`, `validation_errors=[]`를 냈다.
`comparison_valid=false`의 유일한 blocker는 dirty worktree이고,
`production_valid=false`는 의도된 상태다. 높은 host load 때문에 숫자는 절대 성능
보증이 아니라 후보 간 진단으로만 사용한다.

## 결과

표의 각 값은 `3-run median throughput ops/s / median p99 us`다.

### 128 B

| Candidate | c=1 | c=8 | c=64 |
|---|---:|---:|---:|
| HTTPS / JSON | 23,603 / 90.5 | 59,135 / 362.2 | 75,104 / 4,075.5 |
| HTTPS / Postcard | 26,548 / 82.2 | 62,695 / 347.9 | 80,960 / 3,069.5 |
| HTTPS / Prost | 26,923 / 84.1 | 65,618 / 299.6 | 81,966 / 2,904.0 |
| Quinn / stream-per-RPC | 27,061 / 72.8 | 43,732 / 499.6 | 89,368 / 1,468.7 |
| Quinn / persistent lane | 36,970 / 60.8 | 63,832 / 348.5 | 109,084 / 1,138.7 |
| **TCP/TLS / Postcard** | **50,968 / 42.5** | **162,003 / 102.1** | **184,605 / 575.7** |

### 4096 B

| Candidate | c=1 | c=8 | c=64 |
|---|---:|---:|---:|
| HTTPS / JSON | 8,272 / 208.5 | 32,768 / 677.4 | 37,322 / 4,548.5 |
| HTTPS / Postcard | 15,046 / 171.6 | 48,069 / 414.1 | 67,434 / 4,124.2 |
| HTTPS / Prost | 16,197 / 130.7 | 53,714 / 411.0 | 71,040 / 2,732.4 |
| Quinn / stream-per-RPC | 13,762 / 165.9 | 31,647 / 587.8 | 37,624 / 4,980.2 |
| Quinn / persistent lane | 14,197 / 166.4 | 34,018 / 662.0 | 44,335 / 2,885.5 |
| **TCP/TLS / Postcard** | **22,177 / 94.2** | **80,443 / 230.2** | **106,168 / 988.4** |

### 6-cell 기하평균, HTTPS/JSON 대비

| Candidate | Throughput 배수 | p99 개선 배수 |
|---|---:|---:|
| HTTPS / JSON | 1.000x | 1.000x |
| HTTPS / Postcard | 1.355x | 1.222x |
| HTTPS / Prost | 1.427x | 1.414x |
| Quinn / stream-per-RPC | 1.085x | 1.221x |
| Quinn / persistent lane | 1.316x | 1.496x |
| **TCP/TLS / Postcard** | **2.546x** | **3.421x** |

`tcp-tls-postcard`는 HTTPS/Prost 대비 처리량 1.784배와 p99 2.419배,
Quinn persistent lane 대비 처리량 1.934배와 p99 2.287배의 6-cell 기하평균
개선을 기록했다. 반면 run별 throughput CV의 후보별 중앙값은 7.29%~14.32%,
최댓값은 31.29%여서 host noise가 작지 않았다.

## 실제 QuePaxa 경로 검증

다음 로컬 3-node sync-durability 실행도 별도로 수행했다.

```sh
RHIZA_BENCH_RESOURCE_SAMPLING=0 \
RHIZA_DURABILITY_MODE=sync \
scripts/bench-vind.sh \
  --duration 20s --warmup 5s --concurrency 4 --workload write
```

HTTP/JSON workload measurement는 3,367/3,367 성공, 오류 0, 168.35 committed
transactions/s, p50 25.6 ms, p95 51.2 ms, p99 102.4 ms였다. 그러나 최종
checkpoint drain에서 세 node의 qlog root는 모두 index 1457로 같았지만 checkpoint는
node 0만 index 1457, node 1과 2는 index 0이었다. 따라서 스크립트는 exit 1을
반환했고 이 숫자를 성공한 sync-durability E2E 결과로 취급할 수 없다. namespace와
vcluster cleanup은 성공했다.

이 실패는 transport 후보 비교가 아니라 현재 HTTP/JSON 통합 경로의 durability
검증 실패다. transport 교체 전 먼저 follower checkpoint 의미와 동기화 정책을
진단해야 한다.

## 증거와 재현

- 하네스: `bench/src/bin/rhiza-transport.rs`
- 3-run runner: `bench/run-rpc-tls.py`
- 실행 지침: `bench/README.md`
- TLS raw/summary directory:
  `/private/tmp/rhiza-rpc-tls-full-final-20260715-163648`
- summary SHA-256:
  `4f7590b441f2c8724c0a0ea63825a4ae7fe3b05db8beba9b685c358867edc41f`
- raw SHA-256:
  - offset 0: `18908494f1180fbf723cbabc163237d0fcbdf1c00ec4349160e3cb90b6154d12`
  - offset 2: `84b0e404b286e11851d7dd6760d021de19153a6f2ac35e1b2b2bc349eabcfbc3`
  - offset 4: `bdf000cdac70a1457166ec9c300905336b38e05e0cca905820bfcc55902dd712`
- benchmark binary SHA-256:
  `5abf8737317ec7281334ac86e613fdcddec1eb861fa38fa74320191c6f144656`
- QuePaxa E2E artifacts:
  `target/rhiza-bench/20260715-071658-95982/artifacts.json`

재현:

```sh
cargo build --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-transport
python3 bench/run-rpc-tls.py --output-dir bench/rpc-tls-results
```

## 다음 승격 게이트

1. follower checkpoint index 0 문제를 진단하고 sync-durability E2E를 통과시킨다.
2. `RecorderRpc`의 HTTP/Prost, TCP/TLS/Postcard, Quinn lane adapter를 같은 semantic
   contract로 구현한다.
3. 깨끗한 commit에서 각 후보를 QuePaxa 3-node quorum과 persistence까지 포함해
   3회 회전 측정한다.
4. 소유권이 확인된 물리 두 호스트에서 RTT/loss를 기록하며 반복한다. 현재는 승인된
   두 번째 호스트와 외부 도달 가능한 peer topology가 없다.
5. 원격 환경에서도 TCP/TLS가 일관되게 이기고 failover/backpressure/cancellation이
   동일할 때만 프로덕션 transport 변경을 검토한다.
