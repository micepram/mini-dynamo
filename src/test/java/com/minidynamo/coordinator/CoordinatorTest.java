package com.minidynamo.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.replication.QuorumNotMetException;
import com.minidynamo.ring.Node;
import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.versioning.LamportClock;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.web.ServerProperties;

/** N=3 cluster with self local and two peers behind a fake (LWW-merging) transport. */
class CoordinatorTest {

    private static final Node NODE2 = new Node("node2", 8080);
    private static final Node NODE3 = new Node("node3", 8080);

    private final InMemoryStorageEngine selfStorage = new InMemoryStorageEngine();
    private final Map<Node, InMemoryStorageEngine> peers = new HashMap<>();
    private final Set<Node> down = new HashSet<>();
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        peers.put(NODE2, new InMemoryStorageEngine());
        peers.put(NODE3, new InMemoryStorageEngine());
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void writeThenReadReturnsValue() {
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("v"));
        assertThat(coordinator.get("k")).contains(bytes("v"));
    }

    @Test
    void writeSucceedsWithOneReplicaDown() {
        down.add(NODE3); // self + node2 ack -> W=2 met
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("v"));
        assertThat(coordinator.get("k")).contains(bytes("v"));
    }

    @Test
    void writeFailsWhenQuorumUnreachable() {
        down.add(NODE2);
        down.add(NODE3); // only self acks -> W=2 impossible
        assertThatThrownBy(() -> coordinator(3, 2, 2).put("k", bytes("v")))
                .isInstanceOf(QuorumNotMetException.class);
    }

    @Test
    void readFailsWhenQuorumUnreachable() {
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("v"));
        down.add(NODE2);
        down.add(NODE3);
        assertThatThrownBy(() -> coordinator.get("k")).isInstanceOf(QuorumNotMetException.class);
    }

    @Test
    void readResolvesHigherTimestampAcrossReplicas() {
        // Majority holds the newer version; a stale replica must not win the read.
        selfStorage.put("k", Record.value(bytes("old"), 1, "node1"));
        peers.get(NODE2).put("k", Record.value(bytes("new"), 5, "node1"));
        peers.get(NODE3).put("k", Record.value(bytes("new"), 5, "node1"));

        assertThat(coordinator(3, 2, 2).get("k")).contains(bytes("new"));
    }

    @Test
    void readRepairsStaleReplica() {
        selfStorage.put("k", Record.value(bytes("old"), 1, "node1")); // stale
        peers.get(NODE2).put("k", Record.value(bytes("new"), 5, "node1"));
        peers.get(NODE3).put("k", Record.value(bytes("new"), 5, "node1"));

        // R=3 so every replica responds and every stale one is a repair target.
        assertThat(coordinator(3, 3, 1).get("k")).contains(bytes("new"));

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            Record repaired = selfStorage.get("k").orElseThrow();
            assertThat(repaired.lamportTs()).isEqualTo(5);
            assertThat(repaired.value()).isEqualTo(bytes("new"));
        });
    }

    @Test
    void deleteThenGetReturnsEmptyAndConverges() {
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("v"));
        coordinator.delete("k");
        assertThat(coordinator.get("k")).isEmpty();
    }

    @Test
    void laterWriteWinsOverEarlier() {
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("first"));
        coordinator.put("k", bytes("second"));
        assertThat(coordinator.get("k")).contains(bytes("second"));
    }

    private Coordinator coordinator(int n, int r, int w) {
        MiniDynamoProperties props = new MiniDynamoProperties(
                "node1", List.of("node1:8080", "node2:8080", "node3:8080"),
                n, r, w, 128, "inmemory", 1000, 5000, 3_600_000, 86_400_000);
        LamportClock clock = new LamportClock(); // shared by self replica and coordinator (one node)
        LocalReplica local = new LocalReplica(selfStorage, clock);
        return new Coordinator(new ClusterMembership(props, serverOn(8080)), local, new FakeTransport(), executor, clock, props);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ServerProperties serverOn(int port) {
        ServerProperties sp = new ServerProperties();
        sp.setPort(port);
        return sp;
    }

    /** Routes peer calls to in-memory stores with LWW-merge (mirrors a real replica); down -> throw. */
    private final class FakeTransport implements InternalTransport {
        @Override
        public void write(Node node, String key, Record record) {
            if (down.contains(node)) {
                throw new RuntimeException("node down: " + node);
            }
            peers.get(node).merge(key, record, LwwResolver::resolve);
        }

        @Override
        public Optional<Record> read(Node node, String key) {
            if (down.contains(node)) {
                throw new RuntimeException("node down: " + node);
            }
            return peers.get(node).get(key);
        }
    }
}
