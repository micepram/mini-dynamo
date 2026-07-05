package com.minidynamo.antientropy;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.membership.MembershipTable;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.ring.Node;
import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.versioning.LamportClock;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Two-node convergence via anti-entropy (spec §8 acceptance): node B has missed writes that read
 * repair and hinted handoff never delivered. A single reconciliation round makes both stores hold
 * the identical LWW winner for every key.
 */
class AntiEntropyServiceTest {

    private static final Node A = new Node("node1", 8080);
    private static final Node B = new Node("node2", 8080);

    private static MiniDynamoProperties props(String nodeId) {
        return new MiniDynamoProperties(
                nodeId, List.of("node1:8080", "node2:8080"),
                3, 2, 2, 128, "inmemory", 1000, 5000, 3_600_000, 86_400_000);
    }

    @Test
    void oneRoundConvergesDivergedReplicas() {
        Router router = new Router();
        TestNode a = new TestNode(A, B, router);
        TestNode b = new TestNode(B, A, router);
        router.register(a);
        router.register(b);

        // A is up to date; B has diverged in every way a replica can:
        a.storage.put("k1", Record.value(bytes("v1"), 5, "node1")); // B holds a stale k1
        b.storage.put("k1", Record.value(bytes("v1old"), 2, "node1"));
        a.storage.put("k2", Record.value(bytes("v2"), 3, "node1")); // B is missing k2
        a.storage.put("k3", Record.tombstone(4, "node1")); // B is missing a delete
        b.storage.put("k4", Record.value(bytes("v4"), 7, "node2")); // A is missing k4

        a.service.syncWith(B);

        // Every key resolves to the LWW winner on both nodes.
        assertConverged(a, b, "k1", 5);
        assertConverged(a, b, "k2", 3);
        assertConverged(a, b, "k3", 4);
        assertConverged(a, b, "k4", 7);
        assertThat(a.storage.get("k3").orElseThrow().deleted()).isTrue();
        assertThat(b.storage.get("k3").orElseThrow().deleted()).isTrue();

        // Idempotent: once converged, a second round finds nothing to exchange.
        long[] treeA = a.service.localTree(B).serialized();
        long[] treeB = b.service.localTree(A).serialized();
        assertThat(MerkleTree.differingBuckets(treeA, treeB)).isEmpty();
    }

    private static void assertConverged(TestNode a, TestNode b, String key, long ts) {
        Record ra = a.storage.get(key).orElseThrow();
        Record rb = b.storage.get(key).orElseThrow();
        assertThat(ra.lamportTs()).isEqualTo(ts);
        assertThat(LwwResolver.sameVersion(ra, rb)).isTrue();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** A node's storage + service, keyed into the router so peers can reach it in-process. */
    private static final class TestNode {
        final Node id;
        final InMemoryStorageEngine storage = new InMemoryStorageEngine();
        final LocalReplica replica;
        final AntiEntropyService service;

        TestNode(Node id, Node peer, Router router) {
            this.id = id;
            this.replica = new LocalReplica(storage, new LamportClock());
            MembershipTable table = new MembershipTable(id, List.of(peer), 5000, 1, () -> 100_000);
            ClusterMembership membership = new ClusterMembership(table, props(id.host()));
            this.service = new AntiEntropyService(storage, membership, replica, router, router, props(id.host()));
        }
    }

    /** Routes anti-entropy and replica-write calls to the target node's in-process components. */
    private static final class Router implements AntiEntropyTransport, InternalTransport {
        private final Map<Node, TestNode> nodes = new HashMap<>();

        void register(TestNode node) {
            nodes.put(node.id, node);
        }

        @Override
        public long[] merkleTree(Node peer, Node self) {
            return nodes.get(peer).service.localTree(self).serialized();
        }

        @Override
        public Map<String, Record> bucketRecords(Node peer, Node self, List<Integer> buckets) {
            return nodes.get(peer).service.recordsInBuckets(self, buckets);
        }

        @Override
        public void write(Node node, String key, Record record, List<String> hintFor) {
            nodes.get(node).replica.apply(key, record);
        }

        @Override
        public Optional<Record> read(Node node, String key) {
            throw new UnsupportedOperationException("not used by anti-entropy");
        }
    }
}
