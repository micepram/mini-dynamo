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

## Anti-entropy and tombstone GC

*(Tier 4)* Read repair only fixes replicas that happen to be read, and hinted
handoff only covers nodes that recover within the hint retention window. A node
down longer than that misses writes no other mechanism will deliver. Merkle-tree
anti-entropy closes that gap.

**Protocol.** Each round, for every available peer, a node builds a Merkle tree
over the keys the two share — keys whose preference list contains both nodes, so
each node filters its own store by the (gossiped, agreed) ring. Keys are hashed
into a fixed number of buckets (256); a bucket's leaf digest hashes its keys and
their *versions* (`lamportTs` + `coordinatorId` + `deleted`), never the payload,
since only the version decides LWW. The peer's serialized tree is fetched and
compared: equal roots ⇒ done (the cheap common case). On mismatch the descent
visits only differing subtrees, yielding the differing buckets; the two nodes
exchange just those buckets' records and reconcile each by the same LWW rule —
pulling a winner locally and pushing it back to the peer until both converge. The
transport is separate (`AntiEntropyTransport`) from the replica read/write path.

**Simplifications.** Trees are rebuilt per peer each round rather than maintained
incrementally, and a node reconciles with every available peer (not a random
one). Fine at 3–5 nodes where the root-hash gate makes the in-sync case cheap;
both would change for a large keyspace or cluster.

**Tombstone GC.** A delete is a tombstone so LWW can resolve it against
concurrent writes; once older than `tombstone-gc-ms` it is hard-removed. Age is
measured from when the node first observed the tombstone — a wall clock used only
for GC timing, never as a record timestamp. This is naive: removing a tombstone
the whole cluster hasn't yet seen can let anti-entropy resurrect the key from a
lagging replica still holding the old value. The default retention (24h) is far
longer than convergence, so it is safe in practice; a fully safe GC would remove
only after every replica acknowledges the tombstone.

## Toolchain notes

- Gradle single module (not Maven multi-module); Spring Boot 3.4.x.
- Internal node-to-node transport is HTTP/JSON via Spring `RestClient`, kept
  behind a transport interface so gRPC can replace it later without touching
  call sites.
