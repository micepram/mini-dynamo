package com.minidynamo.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.QuorumNotMetException;
import com.minidynamo.ring.Node;
import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.storage.StorageEngine;
import com.minidynamo.versioning.Record;
import java.nio.charset.StandardCharsets;
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

/** N=3, R=W=2 cluster with self local and two peers behind a fake transport. */
class CoordinatorTest {

    private final InMemoryStorageEngine selfStorage = new InMemoryStorageEngine();
    private final Map<Node, InMemoryStorageEngine> peers = new HashMap<>();
    private final Set<Node> down = new HashSet<>();
    private ExecutorService executor;
    private Coordinator coordinator;

    private static final Node NODE2 = new Node("node2", 8080);
    private static final Node NODE3 = new Node("node3", 8080);

    @BeforeEach
    void setUp() {
        peers.put(NODE2, new InMemoryStorageEngine());
        peers.put(NODE3, new InMemoryStorageEngine());
        executor = Executors.newFixedThreadPool(4);

        MiniDynamoProperties props = new MiniDynamoProperties(
                "node1", List.of("node1:8080", "node2:8080", "node3:8080"),
                3, 2, 2, 128, "inmemory", 1000, 5000, 3_600_000, 86_400_000);
        ClusterMembership membership = new ClusterMembership(props, serverOn(8080));
        coordinator = new Coordinator(membership, selfStorage, new FakeTransport(), executor, props);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void writeThenReadReturnsValue() {
        coordinator.put("k", bytes("v"));
        assertThat(coordinator.get("k")).contains(bytes("v"));
    }

    @Test
    void writeSucceedsWithOneReplicaDown() {
        down.add(NODE3); // self + node2 still ack -> W=2 met
        coordinator.put("k", bytes("v"));
        assertThat(coordinator.get("k")).contains(bytes("v"));
    }

    @Test
    void writeFailsWhenQuorumUnreachable() {
        down.add(NODE2);
        down.add(NODE3); // only self acks -> W=2 impossible
        assertThatThrownBy(() -> coordinator.put("k", bytes("v")))
                .isInstanceOf(QuorumNotMetException.class);
    }

    @Test
    void readReturnsValueEvenWhenSelfIsStale() {
        // Only the peers hold the value; self has nothing. R=2 across peers still returns it.
        peers.get(NODE2).put("k", Record.value(bytes("v"), 1, "node2"));
        peers.get(NODE3).put("k", Record.value(bytes("v"), 1, "node3"));
        assertThat(coordinator.get("k")).contains(bytes("v"));
    }

    @Test
    void readFailsWhenQuorumUnreachable() {
        coordinator.put("k", bytes("v"));
        down.add(NODE2);
        down.add(NODE3); // only self responds -> R=2 impossible
        assertThatThrownBy(() -> coordinator.get("k")).isInstanceOf(QuorumNotMetException.class);
    }

    @Test
    void deleteThenGetReturnsEmpty() {
        coordinator.put("k", bytes("v"));
        coordinator.delete("k");
        assertThat(coordinator.get("k")).isEmpty();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ServerProperties serverOn(int port) {
        ServerProperties sp = new ServerProperties();
        sp.setPort(port);
        return sp;
    }

    /** Routes peer calls to in-memory stores; a "down" node throws (transport failure). */
    private final class FakeTransport implements InternalTransport {
        @Override
        public void write(Node node, String key, Record record) {
            if (down.contains(node)) {
                throw new RuntimeException("node down: " + node);
            }
            peers.get(node).put(key, record);
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
