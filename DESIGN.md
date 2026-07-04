# DESIGN

Decisions and tradeoffs for mini-dynamo. Filled in per tier as architecture
lands. The authoritative feature/design definition is [`docs/spec.md`](docs/spec.md).

## Why LWW instead of vector clocks

*(Tier 2)* Conflict resolution is last-write-wins, keyed on a Lamport logical
clock with a `coordinatorId` lexicographic tiebreak — a total order that makes
every replica converge to the same record for a given set of writes.

**What it trades away:** concurrent writes with equal or skewed clocks can
silently lose an update — the "loser" of the tiebreak is discarded with no
sibling kept for the client to reconcile. Vector clocks would detect that
concurrency and surface siblings; LWW deliberately does not. This is the
accepted simplification for a single-datacenter learning artifact.

## Sloppy quorum: divergence and reconvergence

*(Tier 3)* When intended replicas are unhealthy, writes extend clockwise to the
next healthy nodes, which store a hint identifying the intended owner. This
keeps writes available during failure but lets replicas diverge. Convergence is
restored by three mechanisms, all resolving via the same deterministic LWW rule:
read repair (Tier 2), hinted handoff on recovery (Tier 3), and Merkle-tree
anti-entropy for divergence that outlives hint retention (Tier 4).

## Toolchain notes

- Gradle single module (not Maven multi-module); Spring Boot 3.4.x.
- Internal node-to-node transport is HTTP/JSON via Spring `RestClient`, kept
  behind a transport interface so gRPC can replace it later without touching
  call sites.
