# QuePaxa Paper Conformance and Performance Boundaries

Date: 2026-07-12

Evidence status: benchmark figures in this document are historical observations
from unversioned local runs. The referenced `target/rhiza-bench/...`
directories are not included in the repository and cannot be audited from a
checkout.

Primary source: Tennage et al., [*QuePaxa: Escaping the Tyranny of Timeouts in
Consensus*](https://discovery.ucl.ac.uk/id/eprint/10181480/1/quepaxa.pdf), SOSP
2023.

## Protocol conformance

| Paper property | rhiza sql status |
|---|---|
| A designated first-round proposer can decide after phase 0, one recorder round trip, when its highest-priority proposal reaches a quorum. | Ordinary SQL/KV FastPath returns after that phase-0 recorder quorum. The one-RTT claim is protocol-level; it excludes HTTP handling, SQLite execution, local logging, and configured OSS durability work. |
| If the first round does not decide, round 2 and later are leaderless and fully asynchronous. Each round has four phases and succeeds probabilistically rather than through a timeout-driven view change. | The driver continues through four-phase rounds. Its proposal loop has cancellation but no fixed retry/round cap; phase progress is not a timeout-based liveness proof. |
| Hedging and leader selection reduce redundant work but are not safety or liveness requirements. The paper adapts both with MAB-style tuning. | rhiza sql currently uses a static preferred identity and preferred-first endpoint order; later endpoints are used only for fallback or hedge attempts. MAB leader/hedge tuning remains documentation-only in [`mab-leader-hedge-tuning.md`](mab-leader-hedge-tuning.md). |

rhiza sql adds a configuration-safety boundary beyond the paper's static-membership
model: ordinary commands do not install their proof on a second quorum before
return, but `ConfigChange` decisions do. This distinction must not be summarized
as "every decision waits for proof-quorum installation."

## Performance comparability

The paper reports about 584k commands/s in LAN and 250k commands/s in WAN for
its Go SMR prototype. Those are 17-byte string-KV commands (one-byte operation,
eight-byte key, and eight-byte value), not SQLite transactions over an OSS HTTP
service. Its evaluation batches at both submitters and proposers. Its LAN
optimization broadcasts batch contents from submitters and orders compact batch
IDs; its WAN evaluation additionally pipelines ten slots.

The current rhiza sql benchmark instead measures HTTP SQL transactions, including
one parameterized SQLite `INSERT` per request and rhiza sql's storage/durability
path. The measurements retained in
[`failover-throughput-optimization-2026-07-12.md`](failover-throughput-optimization-2026-07-12.md)
support comparisons between the recorded rhiza sql runs only. They do not prove
the paper's throughput, parity with its prototype, or Raft-equivalent
performance.

## Reported local rhiza sql observation

The local notes for `target/rhiza-bench/20260712-113825-51624` report a
30-second, concurrency-4, periodic-1-second run with one SQL `INSERT` per
request and preferred-first multi-endpoint routing. They report all 9,143
attempts completed with zero errors at 304.77 transactions/s (p50 12.8 ms,
p95 51.2 ms, p99 102.4 ms).

The same notes report 317.4 transactions/s before preferred-pod deletion and
298.316 transactions/s with zero errors during the 20.009-second
deletion/restore window, about a 6.0% reduction. They report that the fault
command succeeded in 34.846 seconds, checkpoint drain waited zero seconds, qlog
and checkpoint ended at index 5193 with the same hash, and the run used 5,259
OSS requests while retaining 29,855,178 bytes in 286 objects.

This unversioned observation is scoped to the stated rhiza sql workload and
topology only. It is neither a reproduction of the paper workload nor evidence
of Raft-equivalent performance.

## Missing performance evidence and work

- Run rhiza sql and Raft on the same command schema, payload size, batching,
  durability, topology, load model, and latency definition before claiming
  Raft-equivalent performance.
- Add and evaluate a paper-grade data path in which submitters disseminate
  batches, consensus orders batch IDs, and missing batch contents are fetched
  before recorder acknowledgement.
- Add bounded multi-slot pipelining and evaluate it separately, especially for
  WAN operation.

Until those exist, the 584k/250k paper results are context and targets, not
rhiza sql benchmark results.
