package com.minidynamo.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.failure.HintDeliveryService;
import com.minidynamo.failure.HintStore;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.membership.MemberView;
import com.minidynamo.membership.MembershipTable;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.replication.QuorumNotMetException;
import com.minidynamo.ring.Node;
import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.versioning.LamportClock;
import com.minidynamo.versioning.Record;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Property-style convergence (spec §11, CLAUDE.md §8): apply a random sequence of writes and deletes
 * across nodes with induced failures, then assert every key converges to the same final record on
 * all replicas once healthy and quiesced. Deterministic — fixed seed, injected clock. Convergence is
 * carried by full N-way fan-out plus hinted handoff on recovery.
 */
class ConvergencePropertyTest {

    private static final long TIMEOUT = 1000;
    private static final List<String> KEYS = List.of("k0", "k1", "k2", "k3", "k4");

    private final TestClock clock = new TestClock();
    private final Set<Node> down = new HashSet<>();
    private final Map<Node, SimNode> cluster = new LinkedHashMap<>();
    private ExecutorService executor;
    private long heartbeat = 0;

    private static final class TestClock implements LongSupplier {
        long now = 1_000_000;

        @Override
        public long getAsLong() {
            return now;
        }
    }

    /** One simulated node: its own store, hints, membership view, and coordinator. */
    private record SimNode(
            Node id,
            InMemoryStorageEngine store,
            HintStore hints,
            MembershipTable table,
            LocalReplica local,
            Coordinator coordinator,
            HintDeliveryService delivery) {}

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void everyKeyConvergesAfterRandomWritesDeletesAndFailures() {
        executor = Executors.newFixedThreadPool(8);
        MiniDynamoProperties props = new MiniDynamoProperties(
                "n/a", List.of(), 3, 2, 2, 128, "inmemory", 1000, 5000, 3_600_000, 86_400_000);
        List<Node> ids = List.of(new Node("node1", 8080), new Node("node2", 8080), new Node("node3", 8080));
        SimTransport transport = new SimTransport();

        for (Node id : ids) {
            List<Node> seeds = ids.stream().filter(other -> !other.equals(id)).toList();
            MembershipTable table = new MembershipTable(id, seeds, TIMEOUT, 1, clock);
            LamportClock lamport = new LamportClock();
            InMemoryStorageEngine store = new InMemoryStorageEngine();
            HintStore hints = new HintStore();
            LocalReplica local = new LocalReplica(store, lamport);
            MiniDynamoProperties nodeProps = withNodeId(props, id.host());
            Coordinator coordinator = new Coordinator(
                    new ClusterMembership(table, nodeProps), local, hints, transport, executor, lamport, nodeProps);
            cluster.put(id, new SimNode(id, store, hints, table, local, coordinator,
                    new HintDeliveryService(hints, table, transport, nodeProps)));
        }
        transport.bind(cluster);

        Random random = new Random(20260704L); // fixed seed -> deterministic
        for (int i = 0; i < 400; i++) {
            if (i % 15 == 0) {
                // At most one node partitioned so W=2 stays achievable among the survivors.
                down.clear();
                if (random.nextBoolean()) {
                    down.add(ids.get(random.nextInt(ids.size())));
                }
                syncMembership(ids);
            }

            List<SimNode> up = cluster.values().stream().filter(node -> !down.contains(node.id())).toList();
            SimNode coordinator = up.get(random.nextInt(up.size()));
            String key = KEYS.get(random.nextInt(KEYS.size()));
            try {
                if (random.nextInt(100) < 25) {
                    coordinator.coordinator().delete(key);
                } else {
                    coordinator.coordinator().put(key, ("v" + i).getBytes(StandardCharsets.UTF_8));
                }
            } catch (QuorumNotMetException ignored) {
                // A write that could not reach W is allowed to fail; convergence is still asserted.
            }

            if (i % 7 == 0) {
                up.forEach(node -> node.delivery().deliver());
            }
        }

        // Heal the cluster and let hinted handoff drain, then assert every replica agrees per key.
        down.clear();
        syncMembership(ids);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            cluster.values().forEach(node -> node.delivery().deliver());
            for (String key : KEYS) {
                Set<String> versions = new HashSet<>();
                for (SimNode node : cluster.values()) {
                    versions.add(versionOf(node.store().get(key)));
                }
                assertThat(versions).as("replicas agree on key %s", key).hasSize(1);
            }
        });
    }

    /** Drive each node's membership from the `down` set: refresh survivors, age out the partitioned. */
    private void syncMembership(List<Node> ids) {
        clock.now += 3 * TIMEOUT; // everyone becomes stale...
        for (SimNode observer : cluster.values()) {
            for (Node other : ids) {
                if (!other.equals(observer.id()) && !down.contains(other)) {
                    observer.table().merge(new MemberView(other, ++heartbeat, 0)); // ...survivors refreshed to ALIVE
                }
            }
            observer.table().sweep(); // partitioned nodes (not refreshed) -> DEAD
        }
    }

    private static String versionOf(Optional<Record> record) {
        return record.map(r -> r.lamportTs() + "|" + r.coordinatorId() + "|" + r.deleted()).orElse("ABSENT");
    }

    private static MiniDynamoProperties withNodeId(MiniDynamoProperties base, String nodeId) {
        return new MiniDynamoProperties(
                nodeId, base.seeds(), base.n(), base.r(), base.w(), base.vnodes(), base.storageEngine(),
                base.gossipIntervalMs(), base.failureTimeoutMs(), base.hintRetentionMs(), base.tombstoneGcMs());
    }

    /** In-memory transport that routes to peer replicas/hint stores; a partitioned node throws. */
    private final class SimTransport implements InternalTransport {
        private Map<Node, SimNode> nodes;

        void bind(Map<Node, SimNode> nodes) {
            this.nodes = nodes;
        }

        @Override
        public void write(Node node, String key, Record record, List<String> hintFor) {
            if (down.contains(node)) {
                throw new RuntimeException("partitioned: " + node);
            }
            SimNode target = nodes.get(node);
            target.local().apply(key, record);
            long now = System.currentTimeMillis();
            hintFor.forEach(owner -> target.hints().store(Node.parse(owner), key, record, now));
        }

        @Override
        public Optional<Record> read(Node node, String key) {
            if (down.contains(node)) {
                throw new RuntimeException("partitioned: " + node);
            }
            return nodes.get(node).local().read(key);
        }
    }
}
