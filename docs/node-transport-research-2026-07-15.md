# rhiza Node Transport Research

Date: 2026-07-15

Status: local transport shootout completed. The current HTTP transport remains
the production choice; the measured hot-path fan-out cost was optimized in
QuePaxa without changing the wire protocol.

Decision update: the current opt-in custom runtime candidate is plaintext
`tcp-postcard` for one trusted Kubernetes-cluster boundary. The mTLS guidance
below remains applicable before cross-cluster, external, or multi-tenant use;
it is not a claim about the current candidate's security.

## Scope and current baseline

rhiza already keeps transport outside QuePaxa's ordering contract through the
[`RecorderRpc`](../crates/rhiza-quepaxa/src/lib.rs) and
[`LogPeer`](../crates/rhiza-node/src/lib.rs) traits. That boundary must remain. A transport may
change framing, connection management, and concurrency, but it must not change
slot, epoch, configuration digest, recovery generation, request identity, or
qlog correctness.

The production peer baseline is currently:

- versioned JSON request and response envelopes over HTTP;
- blocking `reqwest` clients with a 2-second connect timeout and a 10-second
  request timeout;
- bearer peer tokens plus node and recovery-generation headers;
- a reusable HTTP client per peer adapter;
- bounded HTTP bodies and log fetches of at most 1,024 entries; and
- synchronous QuePaxa fan-out that, before this work, spawned an OS thread for
  each recorder call in a broadcast and returned when quorum was reached.

The last item was sometimes abbreviated as "one OS thread per peer", but the
old code actually created per-call fan-out threads. It is now one persistent,
bounded worker per recorder for the hot `record` broadcast. Admission uses
`try_send`; a full or disconnected minority queue fails that peer immediately
instead of blocking healthy quorum dispatch. Each broadcast has its own result
channel, so a late slot response cannot contaminate a later call. The rarer
proof, inspection, and recovery control paths retain their existing fan-out.

Public SQL, Graph, and KV HTTP APIs are a separate compatibility surface. This
research concerns node-to-node recorder and log traffic; it does not require
replacing public HTTP/JSON endpoints.

## Dated primary-source matrix

Versions and dates below are the newest non-yanked crates published as of
2026-07-15. Upstream performance claims establish that a project can be fast;
they do **not** establish performance for QuePaxa, qlog durability, rhiza
payloads, or rhiza fault handling.

| Candidate | Release | Platform and integration boundary | Evidence and risk | Position |
|---|---|---|---|---|
| tonic + hyper | [tonic 0.14.6, 2026-05-07](https://crates.io/crates/tonic/0.14.6); [hyper 1.10.1, 2026-05-29](https://crates.io/crates/hyper/1.10.1) | Tokio, HTTP/2, protobuf, streaming, rustls, health and Tower middleware. The [tonic project](https://github.com/hyperium/tonic) identifies 0.14.x as the released line while `master` prepares breaking changes; [hyper](https://github.com/hyperium/hyper) has broad production use. | Mature operational tooling and the smallest change from rhiza's Tokio/HTTP stack. Protobuf schema evolution, flow-control tuning, and generated-code policy still need explicit ownership. No upstream benchmark is comparable to rhiza. | Provisional production default for the local shootout, not a permanent selection. |
| Quinn | [0.11.11, 2026-06-22](https://crates.io/crates/quinn/0.11.11) | High-level Tokio QUIC; pure Rust; tested on Linux, macOS, and Windows; streams and datagrams. The [official README](https://github.com/quinn-rs/quinn) documents UDP buffer and certificate operational requirements. | Active, portable, and substantially less integration surface than a packet-level stack. Basic benchmarks and fuzz/simulated-I/O tests exist, but no published result represents rhiza. | Primary QUIC challenger. Use raw versioned application streams, not HTTP/3. |
| s2n-quic | [1.83.0, 2026-06-23](https://crates.io/crates/s2n-quic/1.83.0) | Linux, macOS, and Windows; Linux kernel 5.0 or newer. Unix defaults to s2n-tls and Linux to aws-lc-rs crypto, which can require CMake and a C compiler. | The strongest upstream verification program: property/Kani, fuzz, loom, interop, randomized network simulation, flamegraphs, and heap profiles are described in the [CI guide](https://github.com/aws/s2n-quic/blob/main/docs/dev-guide/ci.md). Native crypto/build and provider configuration add supply-chain and CI cost. | Linux-focused QUIC reference challenger after Quinn. |
| quiche | [0.29.3, 2026-07-14](https://crates.io/crates/quiche/0.29.3) | QUIC and HTTP/3 with a low-level packet API. The application owns sockets, the event loop, timers, and flow control. Official CI covers Linux, macOS, Windows, iOS, and Android. | Strong production pedigree at Cloudflare, Android DNS, and curl, documented in the [project README](https://github.com/cloudflare/quiche). That does not offset the amount of transport machinery rhiza would have to own. | Exclude unless packet-level control becomes a demonstrated requirement. |
| h3 / h3-quinn | [0.0.8, 2025-05-06](https://crates.io/crates/h3/0.0.8) | Runtime-independent HTTP/3 layer over Quinn, s2n-quic, or MsQuic. | The [official status](https://github.com/hyperium/h3#status) still calls the crate very experimental, warns of bugs, and allows API changes. | Exclude from the production node transport. |
| tarpc | [0.37.0, 2025-08-10](https://crates.io/crates/tarpc/0.37.0) | Rust-in-code service schema, pluggable `Stream + Sink`, cancellation, deadline propagation, and tracing. See the [official feature list](https://github.com/google/tarpc). | Attractive for a Rust-only prototype, but it has less standardized wire evolution, interoperability, and operational tooling than gRPC. No current official performance study answers this workload. | Prototype fallback, not the production default. |
| Volo | [0.12.3, 2026-03-23](https://crates.io/crates/volo/0.12.3) | gRPC and Thrift framework with an extensible service layer. | The [official README](https://github.com/cloudwego/volo#high-performance) reports 350k QPS on four cores and 440k for a Monoio variant, while explicitly warning that the self-published figures are reference-only. It remains pre-1.0. | Do not add without a rhiza-local result that beats the simpler candidates. |
| Monoio | [0.2.4, 2024-08-20](https://crates.io/crates/monoio/0.2.4) | Thread-per-core runtime: Linux io_uring/epoll and macOS kqueue; no supported Windows path. It replaces rather than extends Tokio. | The official [benchmark](https://github.com/bytedance/monoio/blob/master/docs/en/benchmark.md) used 2021 hardware, a nightly toolchain, and 100-byte microbenchmarks. The [platform guide](https://github.com/bytedance/monoio/blob/master/docs/en/platform-support.md) confirms macOS uses kqueue, so macOS development does not exercise the Linux fast path. | Exclude from this transport change. Revisit only after profiling proves the runtime is the bottleneck. |
| tokio-uring | [0.5.0, 2024-05-27](https://crates.io/crates/tokio-uring/0.5.0) | Linux-only io_uring runtime layered with Tokio; requires a recent kernel and ownership-based I/O buffers. | The [official README](https://github.com/tokio-rs/tokio-uring) calls the project very young and notes that not every io_uring-capable kernel works. It is a substrate, not an RPC stack. | Exclude. |
| Glommio | [0.9.0, 2024-03-25](https://crates.io/crates/glommio/0.9.0) | Linux-only thread-per-core io_uring runtime; kernel 5.8 or newer and at least 512 KiB locked memory per executor. | The [official requirements](https://github.com/DataDog/glommio#supported-linux-kernels) imply a different deployment and sharding model. Release cadence and macOS support do not fit rhiza's current workflow. | Exclude from the current architecture. |
| Compio / low-level io-uring | [Compio 0.19.1, 2026-06-14](https://crates.io/crates/compio/0.19.1); [io-uring 0.7.13, 2026-06-28](https://crates.io/crates/io-uring/0.7.13) | Compio is a non-Tokio completion runtime using IOCP, io_uring, and polling; `io-uring` is a Linux binding, not an RPC layer. | Compio is active but pre-1.0 and has no official result comparable to rhiza. Both choices expand the task into a runtime rewrite. | Record as future research, not a node transport candidate now. |
| Zenoh | [1.9.0, 2026-04-10](https://crates.io/crates/zenoh/1.9.0) | Pub/sub, store/query, compute, optional router, and multi-language bindings. See the [official architecture summary](https://github.com/eclipse-zenoh/zenoh). | It solves an edge data-plane problem and introduces routing and storage/query semantics that overlap QuePaxa and the materializers. | Consider later for non-authoritative event distribution or edge federation, never as the authoritative consensus transport by default. |
| Iroh | [1.0.2, 2026-07-06](https://crates.io/crates/iroh/1.0.2) | Public-key endpoint addressing, QUIC through `noq`, hole punching, and relay fallback. The [official README](https://github.com/n0-computer/iroh) states that its public relay implementation runs in production. | Useful for nodes behind NAT, but discovery and relay behavior are unnecessary dependencies for a fixed server cluster. Its live network measurements are not database RPC benchmarks. | Edge/NAT research only. |
| Rust RDMA | [Sideway 0.4.3, 2026-06-04](https://crates.io/crates/sideway/0.4.3); [async-rdma 0.5.0, 2023-02-01](https://crates.io/crates/async-rdma/0.5.0) | Linux RDMA hardware and `rdma-core`; [Sideway](https://github.com/RDMA-Rust/sideway) is a low-level C API wrapper and explicitly gives no performance guarantee for traditional verbs. [async-rdma](https://github.com/datenlord/async-rdma) provides a Tokio abstraction but requires the RDMA kernel/user-space stack and is stale. | Neither is a complete, cross-platform, production RPC stack. macOS development cannot exercise the target path, and hardware/driver/registration failures become application concerns. | Exclude from the general transport. A Linux/RDMA-only bulk experiment would require a separate operational product profile. |
| eRPC | No Rust crate; [official C++ repository](https://github.com/erpc-io/eRPC) and [NSDI 2019 paper](https://www.usenix.org/conference/nsdi19/presentation/kalia) | Kernel-bypass datacenter RPC over DPDK Ethernet or InfiniBand/RoCE. Requires fast NICs, huge pages, NUMA and firewall setup. | Upstream reports 2.3 us round trips, about 10M 32-byte RPC/s per core, and 5.3 us three-way Raft replication. Those results omit rhiza's wire authentication, qlog durability, materialization, and current hardware, so they are non-comparable. | Architecture reference only. Do not introduce a C++/DPDK sidecar or unsafe FFI path without a separately funded Linux appliance design. |

## Why HTTP/3 is not the QUIC plan

QUIC and HTTP/3 are separate decisions. QuePaxa peer messages need bounded,
versioned request/response and stream framing; they do not require HTTP routing,
headers, caches, or browser interoperability. Adding HTTP/3 would add another
protocol layer while the principal Rust `h3` crate explicitly remains
experimental. The QUIC challenger therefore uses Quinn streams with a rhiza
ALPN and rhiza-owned versioned frames. HTTP/3 can be reconsidered only after
`h3` declares a production stability contract and a concrete public API need
justifies HTTP semantics.

## Why 0-RTT is excluded

TLS and QUIC early data can be replayed. Both [TLS 1.3 section
8](https://www.rfc-editor.org/rfc/rfc8446.html#section-8) and [QUIC-TLS section
8](https://www.rfc-editor.org/rfc/rfc9001.html#section-8) require application
replay defenses. A stable request ID limits duplicate state-machine effects,
but it does not by itself prove that recorder transitions, stale configuration
messages, certificate rotation, or recovery-generation checks are replay-safe.

The first production transport must therefore disable 0-RTT for all peer
operations. Session resumption without early application data is allowed. A
future change may enable 0-RTT only for an explicitly enumerated read-only
operation after a dedicated replay threat model and test suite; consensus
writes, recorder state transitions, proof installation, and configuration
changes remain 1-RTT or later.

## Current transport decision

Do not replace the production transport yet. A loopback micro-shootout compared
the existing Axum plus blocking shared-`reqwest` HTTP/JSON shape, tonic 0.14.6
unary gRPC over one long-lived HTTP/2 channel, and Quinn 0.11.11 using one
bidirectional stream per RPC over one long-lived QUIC connection. Each metric
used a 4,096-request warm-up and 60,000 measured requests; the table reports the
median of three runs. Handshake, consensus, persistence, packet loss, and WAN
delay were intentionally outside this diagnostic run.

| Payload / concurrency | HTTP ops/s (p99) | tonic ops/s (p99) | Quinn ops/s (p99) | Winner |
|---|---:|---:|---:|---|
| 128 B / 1 | 11,903 (237 us) | 8,590 (511 us) | 16,219 (165 us) | Quinn |
| 128 B / 8 | 39,993 (473 us) | 21,904 (736 us) | 18,572 (2,383 us) | HTTP |
| 128 B / 64 | 26,762 (7,398 us) | 23,725 (7,522 us) | 44,089 (3,616 us) | Quinn |
| 4 KiB / 1 | 8,509 (277 us) | 10,105 (188 us) | 10,947 (159 us) | Quinn |
| 4 KiB / 8 | 36,807 (415 us) | 15,266 (1,700 us) | 13,875 (2,390 us) | HTTP |
| 4 KiB / 64 | 28,487 (7,196 us) | 18,843 (11,812 us) | 14,965 (13,320 us) | HTTP |

All 54 measured protocol/size/concurrency metrics completed without an error,
but neither challenger won broadly enough to justify a production migration.
tonic fails the clean-loopback latency gate in several cells, and Quinn's
benefit is workload-dependent while adding UDP, certificate, stream-control,
and observability obligations. Keep the current reusable HTTP client and the
`RecorderRpc` boundary. Retain Quinn as the primary future challenger, using a
private versioned ALPN rather than HTTP/3, only after the end-to-end loss,
reconnect, durability, and soak matrix below is run.

### Separate consensus and bulk channels

Consensus and bulk transfer must not share an unbounded queue or a single
flow-control budget.

| Channel | Traffic | Required behavior |
|---|---|---|
| Consensus | record, inspect, command fetch for a decision, proof install, identity and small control messages | Dedicated connection or connection pool, small maximum frame, no compression, strict per-peer request and byte budgets, priority over bulk, and quorum-early completion. |
| Bulk | qlog catch-up ranges and any future peer snapshot transfer | Separate connection, port, or ALPN; chunked bounded frames; checksum and exact index/hash anchors; resumable offset; rate and byte limits; never consume consensus permits. Remote checkpoints remain in object storage unless separately redesigned. |

No correctness rule may depend on cross-channel arrival order. Every bulk chunk
is bound to cluster, epoch, configuration, recovery generation, starting index,
and previous hash; the receiver applies only a verified contiguous prefix.

## Transport contracts

### Backpressure

- Bound both outstanding request count and encoded bytes per peer and per
  channel. Reject admission before allocating or decoding an oversized body.
- Use a bounded sender queue. Saturation returns an explicit retryable
  overload error; it must not create another thread, task, or connection.
- Reserve consensus capacity so catch-up and a slow minority cannot starve a
  quorum. Keep per-peer fairness and a cluster-wide ceiling.
- Treat transport flow-control windows as a second line of defense, not the
  admission policy.

### Deadlines, cancellation, and retries

- Carry one absolute operation deadline through queue wait, connect, send,
  remote execution, and response decode. Retain separately observable connect,
  per-attempt, and total deadlines.
- Cancellation stops unnecessary local work, but it cannot assume a remote
  recorder did not durably accept a message.
- Every application retry reuses the exact request ID and canonical body.
  Transport retries must not mint a new state-machine request ID.
- Consensus messages remain fenced by slot, epoch, configuration ID and
  digest, recovery generation, and command hash. Retryability never weakens
  those checks.
- Hedge only idempotent operations and preserve the current preferred-first,
  quorum-early semantics.

### mTLS identity and rotation

- Authenticate both directions and bind the certificate identity to the
  configured node identity. Keep protocol authorization separate from TLS
  authentication.
- Use standard `h2` ALPN for tonic, a private versioned ALPN for raw QUIC, and
  an independently versioned message envelope in both cases. Fail closed on an
  unknown envelope or QUIC ALPN version.
- Support an overlap window containing the old and new trust roots, live
  certificate/key reload, explicit activation time, and an expiry alarm.
- Fence a removed node through active configuration state; accepting a still
  cryptographically valid old certificate must not restore membership.
- Never log private keys, bearer tokens, session tickets, or full certificate
  material. Record only bounded identifiers, issuer, serial fingerprint, and
  time-to-expiry where operationally needed.

### Observability

Record the following with bounded labels (`peer`, operation, channel, outcome,
execution profile, transport), never request IDs as metric labels:

- queue depth and queue-wait, connect, TLS, encode, wire, remote, decode, and
  total latency histograms;
- attempts, hedges, cancellations, deadline expirations, overloads, wire
  version rejects, authentication rejects, and retry classes;
- active connections and streams, reconnects, flow-control stalls, bytes and
  frames sent/received, and bulk resume counts;
- quorum response count, minority completion lag, pending background work, and
  consensus-vs-bulk permit use; and
- local qlog/applied index lag and checkpoint lag as gauges, without using
  hashes or request data as high-cardinality labels.

Tracing may carry request ID, slot, and command hash as span fields under the
existing redaction policy. Metrics must remain cardinality-bounded.

## Local shootout and acceptance gates

The harness must run HTTP/JSON baseline, tonic/hyper, Quinn, and optionally
s2n-quic from the same clean commit, release profile, machine allocation,
payload generator, durability mode, and fault schedule. Retain the harness,
raw latency samples, environment, binary hashes, Git state, configuration, and
all errors. Upstream numbers, including eRPC, Volo, Monoio, Cloudflare, and AWS
reports, are explicitly non-comparable and cannot satisfy a gate.

Required workloads:

- recorder unary RPC and an end-to-end QuePaxa write with 128-byte and 4-KiB
  commands at concurrency 1, 8, and 64;
- log catch-up streams alone and concurrently with consensus writes;
- three-, five-, and seven-member configurations where practical;
- clean LAN, 1 ms and 20 ms injected RTT, 0.1% and 1% loss for QUIC/TCP
  comparison, a slow minority, connection reset, peer restart, and certificate
  rotation; and
- at least a 30-minute steady-state soak after short diagnostic runs.

Measure success/error counts, committed operations/s, p50/p95/p99/p99.9/max,
CPU core-seconds, RSS average/peak, allocations if available, wire bytes,
reconnect time, flow-control stalls, queue depth, background-drain time, and
final qlog/applied index and hash agreement.

Initial promotion gates are deliberately conservative:

1. **Correctness:** zero unexpected errors, duplicate effects, divergent
   indices/hashes, authorization bypasses, stale-generation acceptance, or
   unbounded queues in every scenario.
2. **Baseline candidate:** tonic/hyper may advance only if clean-LAN write p99
   and CPU/op are each no more than 10% worse than HTTP/JSON, RSS peak is no
   more than 15% worse, and it passes all backpressure, deadline, rotation, and
   fault tests.
3. **QUIC complexity:** Quinn or s2n-quic may advance only if it passes the same
   limits, has no more than a 5% clean-LAN regression, and improves p99 by at
   least 15% in a declared high-concurrency, loss, or reconnect scenario.
   Otherwise the additional QUIC operations surface is not justified.
4. **Isolation:** concurrent bulk catch-up must reduce consensus throughput by
   less than 10% and must not worsen consensus p99 by more than 20%; no bulk
   sender may consume reserved consensus permits.
5. **Soak:** no monotonic resource growth; RSS in the final 15 minutes must be
   within 5% of the first 15-minute steady-state window after accounting for
   bounded caches, and all background RPCs must drain within their deadline.
6. **Recovery:** peer restart, reset, and certificate rotation must make
   progress no slower than the HTTP/JSON baseline and must preserve exact
   retry identity and final state agreement.

These thresholds are the entry contract for a production decision, not a claim
that any candidate currently passes. If no challenger clears them, retain the
existing transport and address the measured bottleneck directly.
