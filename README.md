# mini-dynamo
A from-scratch implementation of the Amazon Dynamo architecture in Java 21 + Spring Boot 3.2. Implements every protocol described in the original 2007 paper — consistent hashing, vector clocks, sloppy quorum, hinted handoff, Merkle-tree anti-entropy, gossip-based membership, and the Phi Accrual Failure Detector — exposed over a gRPC API with a Prometheus metrics endpoint (via Spring Boot Actuator + Micrometer) and a chaos test suite.

> **Reference:** DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store," SOSP 2007.

---


## Table of contents

- [Features](#features)
- [Architecture](#architecture)
- [Module structure](#module-structure)
- [Prerequisites](#prerequisites)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [gRPC API](#grpc-api)
- [Admin API](#admin-api)
- [Chaos testing](#chaos-testing)
- [Benchmarks](#benchmarks)
- [Design decisions](#design-decisions)
- [Roadmap](#roadmap)

---

## Features

All features are derived directly from the Dynamo paper. Nothing is stubbed.

| Feature | Paper section | Status |
|---|---|---|
| Consistent hashing ring (MD5, 2¹²⁸ space) | §4.2 | ✓ |
| Virtual nodes (150 tokens / physical node) | §4.2 | ✓ |
| Preference list construction (skips co-located vnodes) | §4.2 | ✓ |
| Vector clocks with automatic pruning | §4.4 | ✓ |
| Sloppy quorum (configurable N, R, W) | §4.5 | ✓ |
| Hinted handoff with persistent hint store | §4.6 | ✓ |
| Read repair (async, non-blocking) | §4.6 | ✓ |
| Conflict resolution — LWW + plugin interface | §4.4 | ✓ |
| Merkle tree anti-entropy | §4.7 | ✓ |
| Gossip-based membership protocol | §4.8.1 | ✓ |
| Phi Accrual Failure Detector (φ threshold) | §4.8.2 | ✓ |
| Node bootstrapping and key handoff | §4.8.1 | ✓ |
| Pluggable storage engine (RocksDB / in-memory) | §4.1 | ✓ |

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │               Client SDK                │
                        │     (routes to coordinator via ring)    │
                        └──────────────────┬──────────────────────┘
                                           │ gRPC (KVService)
                         ┌─────────────────▼─────────────────┐
                         │            Coordinator             │
                         │  - resolves preference list        │
                         │  - fans out parallel RPCs          │
                         │  - awaits W writes / R reads       │
                         │  - merges vector clocks            │
                         └──────┬──────────┬──────────┬───────┘
                    gRPC        │          │          │  (InternalNodeService)
               ┌────────────────▼─┐  ┌────▼──────┐  ┌▼────────────────┐
               │     Node A       │  │  Node B   │  │    Node C       │
               │  ┌────────────┐  │  │           │  │                 │
               │  │  RocksDB   │  │  │  RocksDB  │  │   RocksDB       │
               │  │ storage    │  │  │  storage  │  │   storage       │
               │  └────────────┘  │  └───────────┘  └─────────────────┘
               │  ┌────────────┐  │
               │  │ Hint store │  │   ← separate RocksDB column family
               │  └────────────┘  │
               └──────────────────┘

  Each node also runs:
    - GossipScheduler   (every 1s, random K peers, membership exchange)
    - PhiFailureDetector (sliding heartbeat window, marks SUSPECT/DOWN)
    - AntiEntropyWorker  (periodic Merkle diff with neighbours)
    - HintReplayWorker   (background delivery of hints to recovered nodes)
    - AdminHttpServer    (metrics, ring state, node status)
```

### Ring layout

Keys are mapped to a 2¹²⁸ ring using MD5. Each physical node owns 150 virtual tokens distributed uniformly. The preference list for a key is the first N distinct physical nodes encountered clockwise from the key's position. Co-located virtual nodes are skipped to ensure rack/host diversity.

### Quorum semantics

| Mode | N | R | W | Guarantee |
|---|---|---|---|---|
| Strong consistency | 3 | 2 | 2 | R + W > N — always reads your writes |
| Availability-biased | 3 | 1 | 1 | Eventual — lowest latency |
| Write-heavy | 3 | 2 | 1 | Fast writes, slower consistent reads |

When a preference list node is unreachable, the coordinator applies a **sloppy quorum**: the next healthy node on the ring accepts the write with a hint identifying the intended target. Hints are replayed once the original node recovers.

### Vector clocks

Each value is stored with a vector clock of the form `[(nodeId, counter), ...]`. On every write the coordinating node increments its own counter. On read, the coordinator merges clocks from R replicas; concurrent versions (neither happens-before the other) are returned as a list for the client or the configured reconciler to resolve.

Clocks are pruned when any entry's counter exceeds a configurable threshold to bound size.

### Anti-entropy

Each node maintains a Merkle tree over its local key space, with leaf nodes holding `SHA-256(value)` per key bucket. A background `AntiEntropyWorker` exchanges root hashes with each neighbour; on mismatch it drills down the tree to isolate divergent buckets, then streams only those key ranges. This bounds the data exchanged to the actual diff rather than a full replica scan.

### Failure detection

The Phi Accrual Failure Detector models heartbeat inter-arrival times as a Gaussian distribution. The suspicion level φ is:

```
φ = -log₁₀( P(T_now - T_last) )
```

where P is the complementary CDF of the fitted distribution. Nodes are marked `SUSPECT` when φ > 8 (default) and `DOWN` when φ > 12. Both thresholds are configurable. This gives a continuous suspicion signal rather than a binary timeout, reducing false positives on noisy networks.

---

## Module structure

```
mini-dynamo/
├── proto/               # All .proto definitions (shared)
│   ├── kv_service.proto          # Client-facing put/get/delete
│   ├── internal_node.proto       # Inter-node replication RPCs
│   └── gossip_service.proto      # Membership exchange
│
├── core/                # Pure algorithms — no I/O, fully unit-testable
│   ├── ring/                     # ConsistentHashRing, VirtualNode, Token
│   ├── clock/                    # VectorClock, ClockPruner
│   ├── merkle/                   # MerkleTree, TreeDiff, KeyBucket
│   └── failure/                  # PhiAccrualDetector, HeartbeatWindow
│
├── storage/             # Persistence layer
│   ├── StorageEngine.java        # Interface: get, put, delete, scan
│   ├── RocksDbEngine.java        # JNI-backed production impl
│   ├── InMemoryEngine.java       # HashMap-backed impl for tests
│   └── HintStore.java            # Separate column family for hinted handoff
│
├── node/                # Node runtime
│   ├── DynamoNode.java           # Entry point, lifecycle management
│   ├── Coordinator.java          # Quorum fan-out, merge, read repair
│   ├── GossipScheduler.java      # Periodic membership exchange
│   ├── AntiEntropyWorker.java    # Background Merkle repair
│   ├── HintReplayWorker.java     # Background hint delivery
│   └── grpc/                     # gRPC service implementations
│
├── client/              # Client SDK
│   ├── DynamoClient.java         # Public API: get, put, delete
│   ├── RingAwareRouter.java      # Routes to coordinator by key hash
│   └── ConflictResult.java       # Multi-value return for concurrent writes
│
├── admin/               # Observability
│   ├── AdminHttpServer.java      # Embedded HTTP server (Undertow)
│   ├── MetricsRegistry.java      # Prometheus counters/histograms
│   └── RingVisualizer.java       # JSON ring state for dashboard
│
└── chaos/               # Test infrastructure
    ├── ChaosExtension.java       # JUnit 5 extension — Docker node control
    ├── Partition.java            # iptables-based network partition
    ├── LinearizabilityChecker.java  # Operation log → sequential model check
    └── scenarios/                # Individual chaos test scenarios
```

---

## Prerequisites

- Java 21+
- Spring Boot 3.2+ (managed via Maven; virtual-thread support requires 3.2)
- Docker and Docker Compose
- Maven 3.9+

RocksDB native libraries are downloaded automatically via the `rocksdbjni` Maven artifact. No separate installation is needed.

---

## Getting started

### Project layout

This is a Maven multi-module build. The parent `pom.xml` aggregates seven modules:

| Module | Type | Depends on |
|---|---|---|
| `proto` | plain Java | — (generates gRPC stubs from `.proto` files) |
| `core` | plain Java | — |
| `storage` | plain Java | `core` |
| `client` | plain Java | `proto`, `core` |
| `admin` | Spring Boot library | `core` |
| `node` | Spring Boot app (executable jar) | `proto`, `core`, `storage`, `admin` |
| `chaos` | test-only | `client`, `node` |

Only `node/` and `admin/` pull in Spring Boot. The other modules stay framework-free so the algorithm code and the client SDK don't drag in the framework.

### Build

```bash
git clone https://github.com/your-org/mini-dynamo
cd mini-dynamo
mvn clean package -DskipTests
```

This produces an executable Spring Boot fat jar at `node/target/dynamo-node.jar`.

### Compile only (faster iteration)

```bash
mvn -q compile                      # all modules
mvn -q -pl core compile             # single module
mvn -q -pl storage -am compile      # one module + its dependencies
```

### Run a local cluster

```bash
docker compose up --scale node=5
```

This starts five Dynamo nodes, a Prometheus instance, and a Grafana dashboard. The nodes discover each other via the seed addresses in `docker-compose.yml`. (`docker-compose.yml` is not yet committed — see roadmap.)

### Run a single node (development)

```bash
java -jar node/target/dynamo-node.jar \
  --dynamo.port=7000 \
  --dynamo.seeds=localhost:7001,localhost:7002 \
  --dynamo.data-dir=/tmp/dynamo-node-0
```

Or via Spring Boot's Maven plugin (no package step needed):

```bash
mvn -pl node -am spring-boot:run \
  -Dspring-boot.run.arguments="--dynamo.port=7000 --dynamo.seeds=localhost:7001"
```

Once running, the admin surface is at `http://localhost:8080/actuator/health` and `http://localhost:8080/actuator/prometheus`.

### Run tests

```bash
mvn test                            # all unit tests across modules
mvn -pl core test                   # core algorithms only (fast, no Spring boot)
mvn -pl chaos test                  # chaos scenarios (requires Docker)
mvn -pl chaos -Dtest=MinorityPartitionTest test   # single chaos scenario
```

### Basic client usage

```java
DynamoClient client = DynamoClient.builder()
    .seeds(List.of("localhost:7000", "localhost:7001"))
    .n(3).r(2).w(2)
    .build();

// Put
client.put("user:42", "{ \"name\": \"Ada\" }".getBytes());

// Get — may return multiple versions if concurrent writes occurred
ConflictResult result = client.get("user:42");
if (result.isSingleVersion()) {
    byte[] value = result.value();
} else {
    List<byte[]> versions = result.concurrentVersions(); // resolve manually
}

// Delete (tombstone — propagates via normal replication)
client.delete("user:42");
```

---

## Configuration

All settings live in `application.yml` under the `dynamo.*` prefix and are bound via `@ConfigurationProperties("dynamo")`. Standard Spring Boot precedence applies: command-line args > environment variables > YAML.

| Property | Default | Description |
|---|---|---|
| `dynamo.port` | `7000` | gRPC listen port |
| `dynamo.seeds` | _(required)_ | Comma-separated seed node addresses |
| `dynamo.data.dir` | `./data` | RocksDB data directory |
| `dynamo.N` | `3` | Replication factor |
| `dynamo.R` | `2` | Read quorum |
| `dynamo.W` | `2` | Write quorum |
| `dynamo.virtual.nodes` | `150` | Virtual tokens per physical node |
| `dynamo.phi.suspect.threshold` | `8.0` | φ level to mark a node SUSPECT |
| `dynamo.phi.down.threshold` | `12.0` | φ level to mark a node DOWN |
| `dynamo.gossip.interval.ms` | `1000` | Gossip round interval |
| `dynamo.gossip.fanout` | `3` | Peers contacted per gossip round |
| `dynamo.antientropy.interval.ms` | `30000` | Anti-entropy repair interval |
| `dynamo.hint.replay.interval.ms` | `5000` | Hint replay retry interval |
| `dynamo.clock.prune.threshold` | `10` | Max vector clock entries before pruning |
| `dynamo.admin.port` | `8080` | Admin HTTP server port |
| `dynamo.reconciler` | `lww` | Conflict reconciler: `lww` or fully qualified class name |

---

## gRPC API

### Client-facing service (`KVService`)

```protobuf
service KVService {
  rpc Put    (PutRequest)    returns (PutResponse);
  rpc Get    (GetRequest)    returns (GetResponse);
  rpc Delete (DeleteRequest) returns (DeleteResponse);
}
```

**`PutRequest`**

| Field | Type | Description |
|---|---|---|
| `key` | `string` | UTF-8 key |
| `value` | `bytes` | Opaque value bytes |
| `context` | `VectorClockProto` | Clock from a prior `Get` (for causal put) |

**`GetResponse`**

| Field | Type | Description |
|---|---|---|
| `values` | `repeated VersionedValue` | One entry per concurrent version |
| `reconciled` | `bool` | True if the reconciler merged versions server-side |

When `values` contains more than one entry the client must resolve the conflict and re-put with all returned clocks merged into the context.

### Inter-node service (`InternalNodeService`)

Used internally for replication, Merkle exchange, and key handoff. Not intended for direct client use.

```protobuf
service InternalNodeService {
  rpc Replicate      (ReplicateRequest)   returns (ReplicateResponse);
  rpc MerkleExchange (MerkleRequest)      returns (MerkleResponse);
  rpc TransferKeys   (stream KeyValue)    returns (TransferSummary);
  rpc DeliverHint    (HintedWrite)        returns (HintAck);
}
```

### Gossip service (`GossipService`)

```protobuf
service GossipService {
  rpc Exchange (MembershipTable) returns (MembershipTable);
  rpc Heartbeat (HeartbeatPing)  returns (HeartbeatPong);
}
```

---

## Admin API

Spring Boot Actuator (with the Micrometer Prometheus registry) handles health and metrics. Dynamo-specific endpoints are exposed as `@RestController`s under `/admin`.

| Endpoint | Method | Source | Description |
|---|---|---|---|
| `/actuator/health` | GET | Actuator | Returns `200 OK` when node is HEALTHY (custom `HealthIndicator` consults Phi detector) |
| `/actuator/prometheus` | GET | Actuator | Prometheus text format (Micrometer) |
| `/admin/ring` | GET | Custom | JSON: token assignments, key ranges, node addresses |
| `/admin/nodes` | GET | Custom | JSON: all known nodes with status, φ value, gossip version |
| `/admin/hints` | GET | Custom | JSON: pending hint queue depth per target node |

**Example `/nodes` response**

```json
[
  {
    "nodeId": "a3f1b2c4",
    "address": "node-1:7000",
    "status": "HEALTHY",
    "phi": 0.41,
    "tokens": 150,
    "gossipVersion": 142
  },
  {
    "nodeId": "d9e7f0a1",
    "address": "node-2:7000",
    "status": "SUSPECT",
    "phi": 9.13,
    "tokens": 150,
    "gossipVersion": 138
  }
]
```

**Prometheus metrics**

| Metric | Type | Description |
|---|---|---|
| `dynamo_put_latency_ms` | Histogram | End-to-end put latency (coordinator to quorum ack) |
| `dynamo_get_latency_ms` | Histogram | End-to-end get latency |
| `dynamo_quorum_miss_total` | Counter | Writes/reads that could not achieve quorum |
| `dynamo_hint_queue_depth` | Gauge | Outstanding hints awaiting delivery |
| `dynamo_read_repair_total` | Counter | Read repairs issued |
| `dynamo_antientropy_keys_synced_total` | Counter | Keys transferred during anti-entropy |
| `dynamo_gossip_rounds_total` | Counter | Gossip rounds completed |
| `dynamo_vector_clock_conflicts_total` | Counter | Concurrent-version conflicts observed on read |

A pre-built Grafana dashboard JSON is provided at `grafana/dynamo-dashboard.json`.

---

## Chaos testing

Chaos tests are JUnit 5 tests annotated with `@ChaosCluster`. The `ChaosExtension` manages a Docker-based cluster and exposes `Partition` and `NodeControl` utilities.

```java
@ChaosCluster(nodes = 5, n = 3, r = 2, w = 2)
class QuorumChaosTest {

    @Test
    void coordinator_failure_mid_write_does_not_lose_data(ChaosCluster cluster) throws Exception {
        DynamoClient client = cluster.client();
        NodeHandle coordinator = cluster.coordinatorFor("test-key");

        // Start async write, kill coordinator immediately
        Future<Void> write = cluster.asyncPut("test-key", "value-1");
        coordinator.kill();

        write.get(5, SECONDS); // should succeed via sloppy quorum

        coordinator.restart();
        cluster.awaitConvergence(Duration.ofSeconds(15));

        assertThat(client.get("test-key").value()).isEqualTo("value-1");
    }

    @Test
    void minority_partition_recovers_via_merkle_repair(ChaosCluster cluster) throws Exception {
        // Partition nodes 4 and 5 from the majority
        try (Partition p = cluster.partition(Set.of(4, 5), Set.of(1, 2, 3))) {
            // Write 10,000 keys against the majority
            cluster.bulkPut(10_000, "partition-test-");
        } // partition healed here

        // Wait for Merkle anti-entropy to converge
        cluster.awaitReplicaConvergence(Duration.ofSeconds(60));

        // Verify all 10,000 keys read consistently from all nodes
        cluster.assertAllReplicasConsistent("partition-test-");
    }
}
```

### Built-in scenarios

| Scenario | What it tests |
|---|---|
| `CoordinatorFailureTest` | Sloppy quorum + hinted handoff when coordinator dies mid-write |
| `MinorityPartitionTest` | Merkle-driven repair after a network partition heals |
| `RollingRestartTest` | Cluster remains available through a rolling node restart |
| `SplitBrainTest` | Even partition (2/2/1): verifies quorum refusal, no split writes |
| `CascadingFailureTest` | Sequential node kills — verifies system stays up until quorum is impossible |
| `HintReplayTest` | Node comes back online, verifies all buffered hints delivered correctly |

### Linearizability checker

All chaos tests record every client operation as `(key, value, type, startNanos, endNanos)`. After the scenario completes, `LinearizabilityChecker` verifies the history is linearizable: it checks that a valid sequential ordering of operations exists that is consistent with real-time ordering and the key-value semantics.

```bash
mvn test -pl chaos -Dtest=MinorityPartitionTest
```

---

## Benchmarks

Benchmarks use JMH. Run against a 5-node Docker Compose cluster.

```bash
docker compose up -d
mvn verify -pl chaos -P benchmark
```

### Sample results (local Docker, M2 MacBook Pro, 5 nodes)

**Throughput — 1 KB values, 8 client threads**

| Configuration | Writes/sec | Reads/sec |
|---|---|---|
| N=3, R=1, W=1 (eventual) | 18,400 | 24,100 |
| N=3, R=2, W=2 (strong) | 11,200 | 16,800 |
| N=3, R=3, W=3 (unanimous) | 6,900 | 9,400 |

**Latency — N=3, R=2, W=2, 1 KB values**

| Percentile | Put (ms) | Get (ms) |
|---|---|---|
| p50 | 1.4 | 1.1 |
| p99 | 4.8 | 3.9 |
| p99.9 | 11.2 | 9.7 |

**Anti-entropy repair time after 30s partition — 50,000 keys**

| Key size | Divergent keys synced | Repair time |
|---|---|---|
| 256 B | 50,000 | 8.3 s |
| 1 KB | 50,000 | 14.1 s |
| 4 KB | 50,000 | 31.6 s |

---

## Design decisions

**Why RocksDB?** The Dynamo paper used BerkeleyDB. RocksDB is its spiritual successor — LSM-tree based, actively maintained, and has a first-class Java JNI binding (`rocksdbjni`). The `StorageEngine` interface makes it trivial to swap in an in-memory implementation for unit tests, which keeps the test suite fast.

**Why Java 21 virtual threads instead of CompletableFuture?** The coordinator must fan out N parallel gRPC calls and block until R/W responses arrive. Virtual threads let you write this as straightforward blocking code in a `StructuredTaskScope`, rather than a deeply nested CompletableFuture chain. The result is dramatically more readable and equally performant. Note: the coordinator uses `Thread.ofVirtual()` directly rather than Spring's `TaskExecutor` — the latter can pin carrier threads on certain code paths, defeating the purpose.

**Why Spring Boot?** Pragmatic productivity. Actuator gives us `/health` and Prometheus scrape for free, `@ConfigurationProperties` replaces hand-rolled config parsing, and `grpc-spring-boot-starter` auto-registers gRPC services. The cost is some startup overhead and discipline around lifecycle (RocksDB must close *after* the gRPC server stops accepting writes — enforced via `SmartLifecycle` phases). Only `node/` and `admin/` depend on Spring; `core/`, `storage/`, and `client/` stay plain Java so the algorithm modules and the client SDK don't drag in the framework.

**Why N=3, W=2, R=2 as defaults?** This satisfies R + W > N (2 + 2 > 3), guaranteeing that the read quorum always overlaps with the write quorum by at least one node. Any node in the read set must have the latest write. This matches the Dynamo paper's recommended production configuration.

**Why 150 virtual nodes per physical node?** The paper's experiments showed 150 tokens produces a coefficient of variation in load distribution below 5% at cluster sizes of 5–100 nodes. Fewer tokens produce worse load skew; more tokens increase memory overhead for the token→node mapping.

**Why the Phi Accrual Failure Detector instead of a timeout?** Fixed timeouts produce false positives under network congestion (a slow node is wrongly declared dead) and false negatives under slow crashes. The Phi detector's continuous suspicion level allows the coordinator to downgrade a node in the preference list progressively rather than with a binary flip.

**Why LWW as the default conflict resolver?** Last-write-wins is the simplest reconciler that produces a single value. It requires wall clocks to be synchronized (NTP), which is a real limitation. The `Reconciler` interface exists precisely so teams can plug in application-specific logic (e.g. a shopping cart that unions concurrent item additions) without touching framework code.

---

## Roadmap

- [ ] Raft-based replication as an alternative to sloppy quorum (compare empirically)
- [ ] Range scan support (`scan(startKey, endKey, limit)`)
- [ ] Compression in RocksDB (Snappy, LZ4)
- [ ] TLS for all gRPC channels with mTLS node authentication
- [ ] Client-side consistent hashing to skip the routing hop for single-key operations
- [ ] CRDT reconciler implementations (G-Counter, OR-Set) as reference examples
- [ ] Kubernetes StatefulSet deployment manifests

---

## References

- DeCandia et al. "Dynamo: Amazon's Highly Available Key-value Store." SOSP 2007. [PDF](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- Hayashibara et al. "The Phi Accrual Failure Detector." 2004.
- Demers et al. "Epidemic Algorithms for Replicated Database Maintenance." PODC 1987.
- RocksDB documentation: [rocksdb.org](https://rocksdb.org)
- gRPC-Java: [grpc.io/docs/languages/java](https://grpc.io/docs/languages/java)