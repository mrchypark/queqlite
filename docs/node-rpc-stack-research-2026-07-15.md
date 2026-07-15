# rhiza Node RPC and Custom Protocol Research

Date: 2026-07-15

Status: architecture research complete; no production transport migration is
approved by this document. The existing reusable HTTP/JSON peer client remains
the control while the candidates below are measured.

Decision update: the first integrated candidate is now plaintext
`tcp-postcard`, restricted to one trusted Kubernetes-cluster boundary. Its
HELLO token is configuration fencing, not a cryptographic security boundary.
The mTLS design and promotion matrix below are retained as the required
research path for any cross-cluster, externally reachable, or multi-tenant
deployment; they do not describe the current selector.

This report extends [the node transport shootout](node-transport-research-2026-07-15.md).
It covers RPC frameworks, a rhiza-owned wire protocol, codecs, QUIC and TCP
stacks, same-host IPC, kernel acceleration, kernel bypass, RDMA, and the most
relevant 2024–2026 systems research. Public SQL, Graph, and KV HTTP APIs are not
part of this decision.

## Executive decision

Do not jump directly from HTTP/JSON to a bespoke packet stack. Test the
following ladder in order, keeping the QuePaxa `RecorderRpc` and `LogPeer`
contracts unchanged:

1. **HTTP with a binary body.** Keep the current persistent connection and
   replace JSON only inside a private, versioned peer envelope. Compare Prost
   and Postcard. This is the smallest experiment and separates serialization
   cost from transport cost.
2. **rhiza RPC over persistent TCP.** The current single-trusted-cluster
   candidate is plaintext; use a fixed 64-byte base header,
   bounded multiplexing, and independent consensus, control, and bulk lanes.
   Add an authenticated encrypted channel before expanding its trust boundary.
   This is the primary custom-protocol candidate.
3. **rhiza RPC over QUIC.** Reuse exactly the same messages over Quinn. Compare
   one bidirectional stream per RPC with persistent framed streams per lane;
   the earlier Quinn shootout measured only the first topology and had large
   concurrency-8 regressions.
4. **Independent QUIC challengers.** Compare s2n-quic, then `noq`, only if
   Quinn clears correctness but profiling shows an implementation-specific
   ceiling. MsQuic/XDP is a Linux performance profile, not the portable default.
5. **Specialized acceleration.** UDS for colocated nodes, then a separately
   operated Machnet/eBPF, AF_XDP/DPDK, or RDMA profile only after the portable
   stack is proven CPU- or kernel-bound on production hardware.

The current measurements do not select a universal winner. Quinn led some
concurrency-1 and concurrency-64 cells, while HTTP led every concurrency-8
cell and several 4 KiB cells. A custom protocol is therefore a falsifiable
candidate, not a presumed improvement.

## What is shared and what must remain separate

| Layer | Shared by HTTP, TCP, and QUIC | Transport-specific |
|---|---|---|
| QuePaxa semantics | Request identity, slot, configuration epoch and digest, recovery generation, command hash, deadlines, deduplication, quorum-early completion | Nothing; a transport may not weaken these checks |
| Wire model | Versioned operations and responses, maximum sizes, canonical encoding, error taxonomy, golden vectors | HTTP body/content type, TCP framing, QUIC ALPN and stream mapping |
| Admission | Per-peer and global request/byte budgets, fail-fast overload, expired-work rejection | TCP connection queues and QUIC stream/connection flow-control tuning |
| Security | Mutual node authentication, authorization against active membership, certificate overlap and rotation, no 0-RTT writes | rustls-over-TCP versus QUIC TLS integration; fail-closed OS peer credentials for UDS |
| Observability | Queue, encode, wire, remote, decode, total, quorum, retry, and error metrics | TCP retransmit/HOL versus QUIC PTO, stream, migration, and datagram metrics |
| Bulk correctness | Snapshot or qlog identity, offset, length, previous hash, chunk checksum, final object hash, resumability | TCP bulk connection, QUIC bulk stream, or object-store transfer |

The encoder and frame tests should be a small internal crate. Connection
management belongs in `rhiza-node`; QuePaxa must continue to depend only on the
existing transport traits.

## Candidate stack matrix

### Directly benchmark

| Candidate | Why it belongs in the shootout | Main risk | Position |
|---|---|---|---|
| Current HTTP/JSON | Already deployed in the codebase, pooled, bounded, and measured | JSON copies and parsing; HTTP request machinery | Mandatory control |
| HTTP + Prost/Postcard | Isolates codec cost without changing sockets, TLS, retries, or operations | Less gain if HTTP dispatch dominates | First challenger |
| Persistent TCP + rhiza frames | Minimum portable owned RPC stack; the current in-cluster profile is plaintext, while broader trust boundaries require an authenticated encrypted channel | rhiza owns framing, multiplexing, reconnect, backpressure, versioning, fuzzing, tooling, and deployment isolation | Primary custom candidate |
| Quinn 0.11.11 + rhiza frames | Pure Rust, Tokio, mature QUIC implementation; independent streams avoid connection-wide TCP loss HOL | UDP operations, certificates, PTO/flow-control tuning, stream topology | Primary QUIC candidate |
| s2n-quic 1.83.0 | Strong verification, fuzzing, simulation, and interoperability program | Native crypto/build footprint and second QUIC integration | Reference challenger after Quinn |
| `noq` 1.0.1 | Pure-Rust sans-I/O Quinn descendant with experimental multipath and BBRv3 work | Young production history and more integration ownership | Research challenger only |
| UDS + rhiza frames | Same wire and dispatch model without IP/TLS overhead for colocated processes | Same-host only; filesystem permissions and peer credentials | Colocation profile |

The TCP candidate should initially use Tokio rather than introduce a new
runtime. The current trusted-cluster profile is plaintext; a future encrypted
profile should add rustls or an equivalent authenticated channel. Ownership
should be sharded by peer/lane, with a single bounded writer and a response
dispatcher keyed by request ID. If one TCP consensus connection shows
loss-induced HOL, compare two to four sharded consensus connections before
replacing the transport.

### Prototype only after a measured need

| Candidate | Useful property | Why it is not the first choice |
|---|---|---|
| tonic/gRPC | Mature Protobuf evolution, HTTP/2, streaming, health, tracing, and middleware | Already lost several local latency cells; adds generated service and HTTP/2 semantics without proving a gain |
| Cap'n Proto RPC | Capability security and promise pipelining can collapse dependent RPC round trips | QuePaxa calls are mostly independent quorum fan-out; capability lifetime and schema model are unnecessary complexity. Its codec remains testable separately |
| tarpc | Rust-native service definitions, cancellation, deadline propagation, tracing, pluggable transport | Rust-only wire and evolution contract are less standardized; it still leaves durability identity and operational policy to rhiza |
| Volo/Thrift | Active high-performance Rust framework with a Monoio variant | Pre-1.0; published throughput is self-reported and not comparable to durable QuePaxa writes |
| MsQuic 2.5.8 | Mature cross-platform C QUIC, Linux XDP path | C FFI, packaging, crash/debug boundary, and XDP operations; use only if it beats pure Rust materially on target Linux hosts |
| quiche 0.29.3 | Production pedigree and low-level QUIC control | Application owns UDP sockets, timers, pacing, and event-loop integration; excessive ownership for rhiza |
| Monoio, Glommio, Compio, direct io_uring | Thread-per-core or completion I/O can reduce scheduler/syscall costs | A runtime/process architecture change, Linux differences, and immature surfaces. Isolate in a transport process if profiling later justifies it |
| io_uring zero-copy receive / `send_zc` | Can remove payload copies for large transfers | Linux zero-copy receive needs NIC header/data split, flow steering, and RSS; completion and page-pinning overhead make it a poor small-RPC default |
| kTLS + `sendfile`/`splice` | Kernel-assisted encrypted file streaming | Helps bulk checkpoint files, not small recorder calls; needs a separate bulk path and deployment checks |
| Shared memory / iceoryx2 | Very low same-host copy and syscall overhead | A separate ownership, crash recovery, permission, and bounded-ring architecture rather than a UDS transport optimization |

### Exclude from the authoritative consensus path

| Candidate | Exclusion reason |
|---|---|
| Raw UDP or a new reliable-UDP design | rhiza would own congestion control, pacing, loss recovery, reordering, PMTU, anti-amplification, authentication, key rotation, and fairness. That recreates QUIC incorrectly |
| QUIC datagrams | Unreliable and unordered; suitable only for disposable hints or telemetry, never record/propose/commit/proof state |
| QUIC 0-RTT writes | Replayable by protocol design. Request IDs alone do not prove recovery/configuration transitions replay-safe |
| NNG/nanomsg | Messaging patterns are useful, but REQ retries can duplicate requests and its single-outstanding defaults conflict with explicit consensus identity and bounded multiplexing |
| ZeroMQ | Flexible socket patterns hide queues/reconnect behavior rhiza must make explicit; C FFI and no durability semantics |
| NATS | Text-oriented client protocol plus broker/server hop; request/reply and queue semantics add an authority and failure domain outside QuePaxa |
| Zenoh | Excellent edge routing/pub-sub/query system, but discovery, routers, and data-space semantics overlap rather than implement QuePaxa authority |
| Arrow Flight | High-throughput Arrow columnar data over gRPC; wrong data model for small recorder messages and opaque snapshot bytes |
| Connect/Dubbo/general service mesh | Interoperability and service routing are not the bottleneck; proxies add queues, hops, and tail-latency ownership |
| Direct AF_XDP/DPDK/eRPC in the portable product | Huge pages, NIC queues, NUMA, pinned cores, driver compatibility, firewall bypass, and custom observability define a Linux appliance, not an embedded library |
| RDMA/UCX/libfabric as default | Requires registered memory, compatible RNIC/fabric, Linux operations, and a distinct failure model. It may be a datacenter bulk profile, not the general protocol |

## Serialization and message representation

The fast path is small enough that “zero-copy” can cost more than one bounded
copy. Validation, alignment, cache misses, page pinning, buffer lifetime, and
completion processing must be counted. Start with a preallocated contiguous
buffer and one encode/decode pass.

| Codec | Strength | Cost/risk | Recommendation |
|---|---|---|---|
| JSON/Serde | Inspectable, current baseline, easy diagnostics | Larger payloads, parsing and allocation | Keep as control and admin/debug representation |
| Prost/Protobuf | Mature field-number evolution, unknown-field behavior, multi-language tooling | Code generation and decode copies | First production-oriented binary candidate |
| Postcard/Serde | Compact, simple, Rust-native, `no_std` heritage | Schema evolution is rhiza's responsibility; Serde shape changes can break wire compatibility | Fastest minimal candidate with explicit version and golden vectors |
| FlatBuffers | Direct field access without unpacking, schema evolution rules | Builder complexity, validation, random-access cache behavior; little benefit for tiny messages | Benchmark only for larger structured payloads |
| Cap'n Proto serialization | Fast pointer-oriented access and mature schema language | Alignment/segment/lifetime model and amplification limits | Codec-only benchmark; do not adopt its RPC semantics by default |
| rkyv + bytecheck | Archived Rust representation and validated access | Rust layout/toolchain coupling and untrusted-input validation obligations | Same-version trusted bulk cache candidate, not the first durable network wire |
| bincode/CBOR/MessagePack | Familiar Serde integrations | Neither a decisive evolution contract nor a demonstrated advantage over Postcard/Prost | Do not expand the matrix without a profile showing codec dominance |

Every codec must reject unknown operation codes, unsupported required flags,
trailing data, oversize lengths, invalid enums, non-canonical identifiers, and
integer overflows before large allocation. Maintain fixed golden vectors for
every supported wire version plus mutation/property fuzzing of frame and
message decoders.

## Proposed rhiza Wire RPC research draft

This is a benchmarkable draft, not a frozen public protocol.

### Session

1. For any trust boundary broader than the current single trusted Kubernetes
   cluster, establish mTLS and bind the certificate to the configured node identity.
   UDS is the explicit exception: its default is to authenticate with
   `SO_PEERCRED` on Linux or `getpeereid` on BSD/macOS plus filesystem ownership
   and permissions, map that OS identity to active cluster membership, and fail
   closed if the mapping is missing or stale. TLS-over-UDS remains an optional
   deployment profile when certificate identity is required.
2. Negotiate one private ALPN per major wire generation, for example
   `rhiza-peer/1`; fail closed rather than silently falling back to HTTP.
   This ALPN step applies to TLS transports; UDS negotiates the same wire major
   in its bounded `HELLO` after peer-credential authorization.
3. Exchange a bounded `HELLO` containing cluster ID, node ID, supported minor
   features, maximum frame sizes, active configuration identity, recovery
   generation, nonce, and connection lane.
4. Authorize the peer against current membership. A valid certificate from a
   removed node is not authorization.
5. Open independent lanes. TCP uses separate connections; QUIC uses separate
   long-lived streams or per-RPC streams according to the measured topology.

### Fixed base header

Use network byte order and a 64-byte fixed header so a decoder can validate the
frame before allocating the method body:

```text
offset  size  field
0       4     magic = "RHZA"
4       2     wire_major
6       1     message_kind
7       1     flags
8       2     header_len = 64
10      2     reserved = 0
12      4     payload_len
16      16    request_id
32      8     configuration_epoch
40      8     recovery_generation
48      8     consensus_slot_or_log_index
56      4     remaining_deadline_ms (receiver hop-local budget)
60      4     payload_checksum_or_zero
```

Cluster and sender identity are bound by the authenticated session, avoiding
repetition in every small frame. Method payloads still carry and verify the
full configuration ID/digest, command hash, proof anchors, and any other
semantic fence required by QuePaxa. The bulk checksum is for corruption and
resume validation; TLS authentication makes it unnecessary for small control
frames. Unknown versions or nonzero reserved bits fail closed.

### Request and response rules

- One request ID identifies the logical operation across retry, hedge, and
  reconnect; a retry carries byte-identical canonical semantics.
- Maintain a bounded per-session in-flight map. Responses may be out of order
  but must match the request ID and expected operation exactly.
- Include status class and retryability in responses. Transport failure never
  asserts that the remote side did not commit.
- The sender owns the monotonic end-to-end deadline covering its local queue,
  connection acquisition, wire wait, response decode, and total RPC timeout.
  `remaining_deadline_ms` is the receiver's hop-local budget, initialized from
  the sender's remaining allowance when the frame is dispatched; it does not
  require synchronized clocks and does not claim to measure time already spent
  on the wire. The receiver decrements it across receive queueing and processing
  and drops expired work before state-machine execution. Request identity and
  deduplication keep a late response or retry from creating a second effect
  after the sender's monotonic deadline expires.
- Deduplicate by configuration/recovery context, sender, request ID, slot or
  log index, and canonical command digest. Bound dedupe memory by the durable
  qlog horizon rather than an unbounded time cache.
- Do not compress consensus frames. Negotiate compression only per bulk object
  when measured CPU and size justify it.

### Lanes and backpressure

| Lane | Examples | Initial policy |
|---|---|---|
| Consensus | record, decision/proof install, configuration fence | Highest priority; small frames; bounded count and 64–512 KiB queued-byte budget per peer; fail fast on saturation |
| Control/recovery | inspect, identity, command fetch, health, recovery coordination | Independent bounded queue so maintenance cannot block decisions |
| Bulk | qlog range, snapshot/checkpoint bytes | 64–256 KiB chunks, receiver-advertised credit, offset/checksum/final hash, resumable, disk/memory backpressure |

No correctness rule may depend on arrival order across lanes. Do not rely only
on TCP/QUIC flow-control: admission must happen before encoding and allocation.
Piggyback receiver credits and queue pressure on normal responses. This adapts
the receiver-driven ideas from SIRD and overload lessons from Breakwater and
Rajomon without adopting their whole transports.

### QUIC stream topology experiment

The existing loopback shootout opened one bidirectional Quinn stream per RPC.
Test all of the following before deciding QUIC is intrinsically slow or fast:

1. one bidirectional stream per RPC;
2. one persistent framed bidirectional stream per lane with request IDs;
3. two to four persistent consensus streams sharded by request ID; and
4. a separate connection for bulk only if connection-level congestion or
   flow-control coupling is visible.

QUIC's independent streams avoid stream-level loss HOL but still share
connection congestion and socket/runtime work. Datagrams are not a shortcut
for reliable state. Multipath QUIC remains a failover experiment: the current
IETF draft does not specify an application-optimal scheduler, and mixing paths
with different RTT/loss can worsen tail latency. QuePaxa already fans out over
multiple peers, so a second path to one peer is not automatically useful.

## What current research changes for rhiza

| Work | Result to borrow | What not to infer |
|---|---|---|
| [Rakaia, OSDI '26](https://www.usenix.org/conference/osdi26/presentation/yang-rui) | Parse message boundaries early and schedule complete messages; prevent bulk bytes and userspace dispatch queues from hiding urgent RPCs | A kernel parser is not justified for a 3–9-node database before user-space lanes are profiled |
| [BURST, NSDI '26](https://www.usenix.org/conference/nsdi26/technical-sessions) | Lock-free data paths and DSA-assisted copy can approach 400 Gb/s in specialized soft-RDMA | DPDK/DSA results do not transfer to portable TLS, qlog durability, or cloud VMs |
| [Sepia, OSDI '26](https://www.usenix.org/conference/osdi26/presentation/song) | DDIO/cache placement can dominate 200 Gb/s host networking | Cache partitioning is a deployment tuning layer, not an RPC contract |
| [SIRD, NSDI '25](https://www.usenix.org/conference/nsdi25/presentation/prasopoulos) | Receiver-driven credits plus sender feedback protect queues while maintaining utilization | Its 100 Gb/s Caladan environment is not a rhiza result |
| [eTran, NSDI '25](https://www.usenix.org/conference/nsdi25/presentation/chen-zhongjie) | eBPF can supply extensible fast TCP/Homa data paths without hard-coding one transport | It requires a Linux-specialized operational profile and cannot replace application correctness checks |
| [Machnet, 2025 preprint](https://arxiv.org/abs/2502.09281) | A DPDK sidecar can improve a real Go Raft deployment on public-cloud hosts | A preprint and sidecar operational boundary require independent reproduction before adoption |
| [QoS Alignment, NSDI '25](https://www.usenix.org/conference/nsdi25/presentation/buckley) | Per-RPC network priority can matter when the fabric honors DSCP consistently | Marking packets is ineffective or dangerous without end-to-end network policy and policing |
| [eRPC, NSDI '19](https://www.usenix.org/conference/nsdi19/presentation/kalia) | Small messages, bounded sessions, preallocated buffers, and one-copy paths define a useful upper bound | Its DPDK/RDMA microseconds omit portable TLS, recovery, and qlog persistence |
| [Homa in Linux, ATC '21](https://www.usenix.org/conference/atc21/presentation/ousterhout) | Message orientation and size-aware scheduling target short-message tail latency | Deploying a nonstandard datacenter transport is not viable as the default embedded-library path |
| [Cornflakes, SOSP '23](https://www.microsoft.com/en-us/research/publication/cornflakes-zero-copy-serialization-for-microsecond-scale-networking/) | Serialization layout and scatter/gather choices matter for large and nested values | Zero-copy does not guarantee faster 128 B–4 KiB control messages |
| [Breakwater, OSDI '20](https://www.usenix.org/conference/osdi20/presentation/cho) | Credit-based admission should keep queues short under overload | rhiza still needs deterministic peer/global byte limits and consensus correctness |

Consensus systems such as Mu and CURP are not transport optimizations: they
change replication or execution semantics. They require a separate QuePaxa
proof and are excluded from this work.

## Kernel and hardware acceleration decision tree

1. Profile application queue time, encode/decode, allocations, syscalls,
   scheduling, TLS, retransmits/PTO, softirq, and qlog fsync separately.
2. Apply RSS/RFS/XPS, IRQ/core affinity, socket buffers, and bounded batching on
   target Linux hosts before kernel bypass.
3. Try UDP GSO/GRO only for QUIC throughput and bulk. It will not normally
   improve concurrency-1 latency.
4. Try `MSG_ZEROCOPY`, `send_zc`, kTLS, `sendfile`, or io_uring zero-copy only
   for measured payloads above roughly tens of KiB. Linux documents completion,
   pinning, and deferred-copy costs; loopback may deliberately copy.
5. Consider busy polling only in a dedicated-core profile and measure watts as
   well as p99.9.
6. If the portable path remains kernel-bound and the product accepts a Linux
   appliance contract, compare MsQuic XDP before owning AF_XDP directly.
7. Evaluate DPDK/eRPC/Machnet or RDMA/UCX/libfabric only for a separately named
   deployment profile with NIC, NUMA, huge-page, security, and fallback SLOs.

Linux [AF_XDP](https://docs.kernel.org/networking/af_xdp.html) requires UMEM and
RX/TX/fill/completion rings bound to NIC queues. Linux
[zero-copy receive](https://docs.kernel.org/networking/iou-zcrx.html) depends on
NIC header/data split, flow steering, and RSS. Linux
[`MSG_ZEROCOPY`](https://docs.kernel.org/next/networking/msg_zerocopy.html) is a
hint with asynchronous completion, not a guarantee that copying is avoided.
These are deployment architectures, not Cargo dependency swaps.

## Benchmark and falsification plan

### Ladder

Run candidates from the same commit and harness:

1. HTTP/JSON control;
2. HTTP/Postcard and HTTP/Prost;
3. TCP/rustls with Postcard and Prost;
4. Quinn with both stream topologies;
5. s2n-quic and `noq` only against the best QUIC topology;
6. UDS for a declared same-host profile; and
7. MsQuic XDP, Machnet/eBPF/AF_XDP, or RDMA only on representative specialized
   hardware after a profile proves the preceding stack is kernel-bound.

Cap'n Proto, tarpc, and Volo enter only if their distinct property can be tied
to a measured bottleneck. Do not run a giant framework tournament whose result
cannot change the decision.

### Matrix

- Physical two-host test plus 3-, 5-, 7-, and where practical 9-node clusters.
- Payloads 64 B, 128 B, 512 B, 4 KiB, 64 KiB, 1 MiB, and a 64 MiB resumable
  snapshot; concurrency 1, 8, 64, and 256.
- Clean LAN plus 1, 10, and 50 ms RTT; 0.01%, 0.1%, and 1% loss; reordering,
  duplication, asymmetric paths, MTU black-hole, NAT rebinding for QUIC, slow
  minority, disconnect, restart, and certificate rotation.
- Recorder-only and end-to-end durable QuePaxa writes; catch-up/snapshot alone
  and concurrently with consensus; steady state, reconnect, and 30-minute soak.
- TLS/mTLS active for cross-cluster, externally reachable, or multi-tenant
  profiles. The plaintext single-trusted-cluster profile must be labeled and
  measured separately.

Record p50/p95/p99/p99.9/max quorum latency, committed operations/s, CPU
cycles/op, allocations, syscalls, context switches, migrations, softirq and
packet drops, retransmits/PTO, queue depth/wait, bytes on wire, RSS, reconnect
time, certificate-rotation interruption, watts for busy polling, and final
qlog/applied index/hash agreement. Retain raw samples, hardware, kernel/NIC
firmware, offloads, affinity, binary hashes, and all errors.

### Promotion gates

A portable challenger advances only if it:

- has zero unexpected error, duplicate effect, stale-generation acceptance,
  authorization bypass, unbounded queue, or final-state divergence;
- improves concurrency-1 clean-LAN p99 by at least 15% without worsening
  concurrency-64 p99.9 or CPU/op;
- preserves the improvement or has a declared advantage under 0.1% loss,
  reconnect, or slow-minority conditions;
- limits concurrent bulk impact to less than 10% consensus throughput and 20%
  consensus p99, with no use of reserved consensus permits;
- survives restart, deadline, overload, malformed-frame, certificate-rotation,
  and 30-minute soak tests with bounded resources; and
- preserves exact retry identity and durable end state.

A specialized XDP/DPDK/RDMA/runtime profile must improve its declared target by
at least 25–30% because it creates a second deployment, debugging, and fallback
surface. If no candidate clears its gate, keep HTTP and optimize the measured
queue, codec, qlog, or scheduler bottleneck instead.

## Concrete next implementation slice

The smallest useful code change is not a new transport. Add a private binary
peer envelope behind the existing HTTP route and benchmark JSON, Postcard, and
Prost with identical payloads and pooled connections. At the same time, define
the transport-neutral frame/message golden vectors and malformed-input tests.
Only if binary HTTP leaves material transport overhead should the next slice
implement persistent TCP/rustls lanes. Quinn then reuses those messages and
tests stream topology rather than creating a separate semantic stack.

This sequence keeps every experiment reversible, makes failures attributable,
and prevents an impressive packet microbenchmark from silently changing
QuePaxa correctness or rhiza's deployment contract.
