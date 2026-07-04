# mini-dynamo: Requirements and Design Specification

A from-scratch reimplementation of the core ideas in Amazon's Dynamo (DeCandia et al., SOSP 2007), scoped as a portfolio and learning project.

**Stack:** Java 21 (LTS), Spring Boot 3.x, Gradle, Docker Compose.
**Conflict model:** Last-write-wins (LWW) with a Lamport logical clock per key and node-id tiebreak.
**Deployment target:** 3 to 5 identical nodes on a single machine via Docker Compose.

This document is the source of truth for implementation. Build in tier order; each tier has acceptance criteria and should be independently demoable.

---

## 1. Design goals and non-goals

**Goals**
- Faithful to Dynamo's decentralized architecture: every node is identical, any node can coordinate a request, there is no master.
- Highly available for writes under single-node failure and simulated partition.
- Eventually consistent: replicas that diverge under failure converge after recovery.
- Readable, well-tested, and documented well enough to serve as a portfolio piece that demonstrates understanding of the underlying theory.

**Non-goals**
- No strong consistency, linearizability, or transactions.
- No cross-datacenter or multi-region logic.
- No authentication, authorization, or encryption.
- No vector clocks or sibling reconciliation. LWW is the deliberate simplification. The design doc must state what this trades away (concurrent writes with equal-or-skewed clocks can lose an update).

---

## 2. Data and consistency model

**Stored record (per key):**
```
Record {
  bytes value          // opaque payload, JSON or string
  long  lamportTs      // logical timestamp assigned at write coordination
  String coordinatorId // node id that coordinated the write, used as tiebreak
  boolean deleted      // tombstone flag
}
```

**LWW resolution rule (deterministic):**
1. Higher `lamportTs` wins.
2. On equal `lamportTs`, higher `coordinatorId` (lexicographic) wins.
   This total order guarantees all replicas converge to the same record for a key given the same set of writes.

**Lamport clock:**
- Each node holds a monotonic counter.
- On coordinating a write: `ts = ++counter`, but first advance `counter = max(counter, anySeenTs)` so the clock respects observed causality.
- The Lamport clock is the sole timestamp source, kept behind a `VersionStamper` interface for testability.

**Deletes** are tombstones carrying a `lamportTs`, replicated on the normal write path, and resolved by LWW like any other write. Tombstones are garbage-collected after a configurable retention window (Tier 4).

---

## 3. Architecture

N identical Spring Boot nodes. Each node exposes a client API and an internal node-to-node API, and runs background tasks for gossip, hint delivery, and anti-entropy.

### 3.1 Package layout
```
com.minidynamo
  ring          // consistent hashing, virtual nodes, preference list
  coordinator   // per-request state machine, quorum collection, read repair
  storage       // StorageEngine interface + InMemory and RocksDB impls
  versioning    // Lamport clock, VersionStamper, LWW resolver
  replication   // internal replica read/write, quorum collector
  membership    // gossip protocol, membership table, failure detector
  failure       // sloppy quorum, hinted handoff store and delivery
  antientropy   // Merkle tree per range, sync protocol (Tier 4)
  api           // REST controllers: client-facing + internal
  config        // N, R, W, vnode count, timeouts, seeds
```

### 3.2 Node-to-node transport
Internal calls use HTTP/JSON via Spring's `RestClient` (blocking) so the whole system is inspectable with curl and readable in logs. gRPC is an acceptable later upgrade but not required. Keep transport behind an interface so it can be replaced.

### 3.3 Persistence
`StorageEngine` interface with two implementations:
- `InMemoryStorageEngine` backed by `ConcurrentHashMap`, used for unit and integration tests.
- `RocksDbStorageEngine` using the RocksDB JNI binding, used for real runs.
  This mirrors Dynamo's pluggable local persistence.

---

## 4. Ring and replication

- Hash space: SHA-256 truncated to a fixed width, or MD5, over the key.
- Each physical node owns `vnodes` virtual nodes (default 128) placed at hashed positions on the ring, for uniform load distribution.
- **Preference list** for a key: walk the ring clockwise from the key's position and collect the first N distinct physical nodes.
- The coordinator for a key is the first node in its preference list, though any node that receives a client request acts as the request coordinator and forwards to the preference list.

**Quorum config (defaults):** N=3, R=2, W=2. Enforce `R + W > N` at startup and log the resulting consistency guarantee. Values are configurable.

---

## 5. Request flows

### 5.1 Write (PUT /kv/{key})
1. Receiving node becomes request coordinator.
2. Compute preference list. If fewer than N intended nodes are healthy, extend clockwise to the next healthy nodes (sloppy quorum).
3. Assign `lamportTs` and set `coordinatorId`.
4. Send replica-write to all N nodes in the (possibly extended) list concurrently. Substitute nodes receive a hint identifying the intended owner.
5. Count the coordinator's own local write toward the quorum. Return success once **W** acks are collected. Remaining replicas are updated asynchronously.
6. On total failure to reach W healthy nodes, return an error.

### 5.2 Read (GET /kv/{key})
1. Coordinator computes preference list and sends replica-read to all N concurrently.
2. Wait for **R** responses.
3. Resolve the returned records by the LWW rule to pick the winner.
4. **Read repair:** if any responding replica returned a stale record, asynchronously push the winning record to it.
5. Return the value, or 404 if the winner is a tombstone or the key is absent.

### 5.3 Delete (DELETE /kv/{key})
Same path as write, producing a tombstone.

---

## 6. Membership and failure detection

- Each node maintains a membership table: `nodeId -> {host, port, state, heartbeat, incarnation}` where state is ALIVE, SUSPECT, or DEAD.
- **Gossip:** every interval (default 1s), each node sends a random subset of its table to a random peer, merging entries by highest heartbeat and incarnation.
- **Bootstrap:** a configured seed node list. New nodes contact a seed to join and learn the ring.
- **Failure detector:** start with a heartbeat-timeout detector (ALIVE to SUSPECT to DEAD). Phi-accrual is an optional Tier 4 upgrade.
- Ring membership changes (join, leave, death) recompute vnode placement and preference lists.

---

## 7. Sloppy quorum and hinted handoff

- When an intended replica is unreachable, the coordinator writes to the next healthy node on the ring, attaching a hint `{intendedNodeId}`.
- Hints live in a separate local hint store, scanned periodically.
- When the intended node is detected ALIVE again, the holder delivers the hinted record and deletes its local hinted copy once delivery succeeds.
- Hint retention is configurable so undeliverable hints are eventually dropped.

---

## 8. Anti-entropy (Tier 4)

- Each node maintains a Merkle tree over the keys in each range it owns.
- Periodically, replicas responsible for the same range exchange root hashes. If roots differ, descend the trees to find differing keys and exchange only those, resolving by LWW.
- This catches divergence that read repair and hinted handoff miss (for example a replica that was down past hint retention).

---

## 9. Configuration

Environment-driven, per node:
```
NODE_ID            unique id
BIND_HOST, PORT
SEEDS              comma-separated seed host:port list
N, R, W            quorum params (default 3, 2, 2)
VNODES             virtual nodes per physical node (default 128)
STORAGE_ENGINE     inmemory | rocksdb
GOSSIP_INTERVAL_MS default 1000
FAILURE_TIMEOUT_MS default 5000
HINT_RETENTION_MS
TOMBSTONE_GC_MS
```

---

## 10. Build order and acceptance criteria

Build strictly in tier order. Do not start a tier until the previous tier's criteria pass.

**Tier 0: Single node**
- `get`, `put`, `delete` over REST against one node.
- `StorageEngine` interface with in-memory and RocksDB impls.
- Acceptance: curl round-trip works; values survive restart on RocksDB.

**Tier 1: Ring and replication**
- Consistent hash ring with virtual nodes, preference list computation.
- N-way replication, configurable N/R/W, any-node coordinator with forwarding.
- Acceptance: a 3-node cluster stores each key on 3 nodes; a write with W=2 succeeds when 2 replicas ack; a read with R=2 returns the value from any node.

**Tier 2: LWW versioning and read repair**
- Lamport clock, `VersionStamper`, LWW resolver, tombstones.
- Read repair pushes the winning record to stale replicas.
- Acceptance: concurrent writes to the same key resolve deterministically across all nodes; a stale replica is repaired after a read.

**Tier 3: Membership and resilience**
- Gossip membership, heartbeat failure detector.
- Sloppy quorum and hinted handoff.
- Acceptance: killing one node does not fail writes (sloppy quorum); when the node returns, hinted writes are delivered and the key set converges.

**Tier 4 (stretch): Convergence and polish**
- Merkle-tree anti-entropy, tombstone GC, phi-accrual detector.
- Optional metrics: Micrometer plus Prometheus and a Grafana dashboard (write and read latency, quorum success rate, hint backlog).
- Acceptance: a replica that missed writes beyond hint retention converges via anti-entropy.

---

## 11. Testing

**Unit**
- Ring assignment determinism and uniform-ish distribution with vnodes.
- Preference list correctness, including sloppy extension.
- Quorum collector completing at exactly W or R responses.
- LWW resolver, including the tiebreak.
- Lamport clock monotonicity and causal advance.
- Hint lifecycle: create, deliver, delete.

**Integration (Testcontainers, JUnit 5)**
- 3-node cluster: write with W=2, read with R=2.
- Kill a node mid-run: verify sloppy quorum keeps writes available and hinted handoff delivers on recovery.
- Read repair convergence.
- Partition simulation (pause a container): verify write availability during the partition and convergence after heal.

**Property-style**
- Apply a random sequence of writes and deletes across nodes with induced failures; assert every key converges to the same final record on all replicas once the cluster is healthy and quiesced.

---

## 12. Portfolio deliverables

- `README.md`: what it is, how to run, architecture diagrams (ring and preference list, write path, read path with repair, hinted handoff, gossip).
- `DESIGN.md`: decisions and tradeoffs, with an explicit section on why LWW was chosen and what it gives up versus vector clocks, and the consequences of sloppy quorum (data divergence and how the system reconverges).
- `Makefile` or scripts: `make up`, `make down`, a node-kill command, and curl-based demo scripts.
- `docker-compose.yml`: 3 to 5 nodes with seed configuration.
- `CLAUDE.md`: implementation notes and conventions for the coding agent.

---

## 13. Tech stack summary

- Java 21 LTS, Spring Boot 3.x, Gradle.
- Spring Web (MVC) for REST, `RestClient` for internal calls.
- RocksDB JNI for persistence, `ConcurrentHashMap` for the in-memory engine.
- Jackson for JSON.
- `CompletableFuture` / `ExecutorService` for the coordinator fan-out and quorum collection.
- JUnit 5 and Testcontainers for tests.
- Micrometer for metrics (optional, Tier 4).
- Docker and Docker Compose for the cluster.

---

## 14. Open defaults to confirm before coding

These are set to sensible defaults; flag any you want changed.
- Internal transport: HTTP/JSON via RestClient (alternative: gRPC).
- Persistence: RocksDB for runs, in-memory for tests.
- Build tool: Gradle (alternative: Maven).
- Cluster size for the demo: 3 nodes (extendable to 5).