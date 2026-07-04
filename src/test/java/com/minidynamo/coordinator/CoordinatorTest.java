package com.minidynamo.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** N=3 cluster: self local, two peers behind a fake (LWW-merging, hint-storing) transport. */
class CoordinatorTest {

    private static final Node SELF = new Node("node1", 8080);
    private static final Node NODE2 = new Node("node2", 8080);
    private static final Node NODE3 = new Node("node3", 8080);
    private static final long TIMEOUT = 1000;

    private final InMemoryStorageEngine selfStorage = new InMemoryStorageEngine();
    private final HintStore selfHints = new HintStore();
    private final Map<Node, InMemoryStorageEngine> peers = new HashMap<>();
    private final Map<Node, HintStore> peerHints = new HashMap<>();
    private final Set<Node> down = new HashSet<>();

    private final TestClock clock = new TestClock();
    private ExecutorService executor;
    private MembershipTable table;
    private long heartbeat = 0;

    private static final class TestClock implements LongSupplier {
        long now = 100_000;

        @Override
        public long getAsLong() {
            return now;
        }
    }

    @BeforeEach
    void setUp() {
        peers.put(NODE2, new InMemoryStorageEngine());
        peers.put(NODE3, new InMemoryStorageEngine());
        peerHints.put(NODE2, new HintStore());
        peerHints.put(NODE3, new HintStore());
        executor = Executors.newFixedThreadPool(4);
        table = new MembershipTable(SELF, List.of(NODE2, NODE3), TIMEOUT, 1, clock);
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
    void writeSucceedsWithOneReplicaDownButNotYetDetected() {
        down.add(NODE3); // transport fails though node3 is still ALIVE: strict quorum still met
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("v"));
        assertThat(coordinator.get("k")).contains(bytes("v"));
    }

    @Test
    void writeFailsWhenQuorumUnreachable() {
        down.add(NODE2);
        down.add(NODE3);
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
        selfStorage.put("k", Record.value(bytes("old"), 1, "node1"));
        peers.get(NODE2).put("k", Record.value(bytes("new"), 5, "node1"));
        peers.get(NODE3).put("k", Record.value(bytes("new"), 5, "node1"));

        assertThat(coordinator(3, 2, 2).get("k")).contains(bytes("new"));
    }

    @Test
    void readRepairsStaleReplica() {
        selfStorage.put("k", Record.value(bytes("old"), 1, "node1"));
        peers.get(NODE2).put("k", Record.value(bytes("new"), 5, "node1"));
        peers.get(NODE3).put("k", Record.value(bytes("new"), 5, "node1"));

        assertThat(coordinator(3, 3, 1).get("k")).contains(bytes("new")); // R=3 -> every replica responds

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            Record repaired = selfStorage.get("k").orElseThrow();
            assertThat(repaired.lamportTs()).isEqualTo(5);
            assertThat(repaired.value()).isEqualTo(bytes("new"));
        });
    }

    @Test
    void deleteThenGetReturnsEmpty() {
        Coordinator coordinator = coordinator(3, 2, 2);
        coordinator.put("k", bytes("v"));
        coordinator.delete("k");
        assertThat(coordinator.get("k")).isEmpty();
    }

    @Test
    void sloppyWriteStaysAvailableAndHintsTheDeadOwner() {
        killNode(NODE3);
        Coordinator coordinator = coordinator(3, 2, 2);

        coordinator.put("k", bytes("v")); // self + node2 ack -> W=2 met despite node3 DEAD
        assertThat(coordinator.get("k")).contains(bytes("v"));

        assertThat(hintHolderFor(NODE3)).isNotNull(); // a substitute holds a hint for node3
    }

    @Test
    void hintedHandoffDeliversWhenOwnerRecovers() {
        killNode(NODE3);
        MiniDynamoProperties props = props(3, 2, 2);
        coordinator(props).put("k", bytes("v"));

        HintStore holder = hintHolderFor(NODE3);
        assertThat(holder).isNotNull();

        reviveNode(NODE3);
        HintDeliveryService delivery = new HintDeliveryService(holder, table, new FakeTransport(), props);
        delivery.deliver();

        assertThat(peers.get(NODE3).get("k")).isPresent(); // handed off to the recovered owner
        assertThat(holder.all()).noneMatch(h -> h.intendedNode().equals(NODE3)); // hint cleared
    }

    /** The store (self's or a peer's) that ended up holding a hint for the given owner, or null. */
    private HintStore hintHolderFor(Node owner) {
        if (selfHints.all().stream().anyMatch(h -> h.intendedNode().equals(owner))) {
            return selfHints;
        }
        return peerHints.values().stream()
                .filter(store -> store.all().stream().anyMatch(h -> h.intendedNode().equals(owner)))
                .findFirst()
                .orElse(null);
    }

    private void killNode(Node dead) {
        clock.now += 3 * TIMEOUT;
        // Refresh the other peers so only `dead` ages out to DEAD.
        for (Node peer : List.of(NODE2, NODE3)) {
            if (!peer.equals(dead)) {
                table.merge(new MemberView(peer, ++heartbeat, 0));
            }
        }
        table.sweep();
        down.add(dead);
    }

    private void reviveNode(Node node) {
        table.merge(new MemberView(node, ++heartbeat, 2)); // higher incarnation -> ALIVE
        down.remove(node);
    }

    private Coordinator coordinator(int n, int r, int w) {
        return coordinator(props(n, r, w));
    }

    private Coordinator coordinator(MiniDynamoProperties props) {
        LamportClock lamport = new LamportClock(); // shared by self replica and coordinator (one node)
        LocalReplica local = new LocalReplica(selfStorage, lamport);
        return new Coordinator(
                new ClusterMembership(table, props), local, selfHints, new FakeTransport(), executor, lamport, props);
    }

    private static MiniDynamoProperties props(int n, int r, int w) {
        return new MiniDynamoProperties(
                "node1", List.of("node1:8080", "node2:8080", "node3:8080"),
                n, r, w, 128, "inmemory", 1000, 5000, 3_600_000, 86_400_000);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Peer stores with LWW-merge and hint capture; a "down" node throws (transport failure). */
    private final class FakeTransport implements InternalTransport {
        @Override
        public void write(Node node, String key, Record record, List<String> hintFor) {
            if (down.contains(node)) {
                throw new RuntimeException("node down: " + node);
            }
            peers.get(node).merge(key, record, LwwResolver::resolve);
            hintFor.forEach(owner -> peerHints.get(node).store(Node.parse(owner), key, record, System.currentTimeMillis()));
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
