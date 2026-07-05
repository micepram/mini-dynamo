package com.minidynamo.antientropy;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.versioning.Record;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MerkleTreeTest {

    private static final int BUCKETS = 256;

    private static Map<String, Record> store(String... keys) {
        Map<String, Record> m = new HashMap<>();
        for (String k : keys) {
            m.put(k, Record.value(new byte[] {1}, 1, "node1"));
        }
        return m;
    }

    @Test
    void identicalRecordsProduceEqualRootAndNoDiffs() {
        MerkleTree a = new MerkleTree(store("k1", "k2", "k3"), BUCKETS);
        MerkleTree b = new MerkleTree(store("k1", "k2", "k3"), BUCKETS);

        assertThat(a.root()).isEqualTo(b.root());
        assertThat(MerkleTree.differingBuckets(a.serialized(), b.serialized())).isEmpty();
    }

    @Test
    void aMissingKeyShowsUpAsExactlyItsBucket() {
        Map<String, Record> full = store("k1", "k2", "k3");
        Map<String, Record> missing = store("k1", "k2"); // k3 absent

        MerkleTree a = new MerkleTree(full, BUCKETS);
        MerkleTree b = new MerkleTree(missing, BUCKETS);

        assertThat(a.root()).isNotEqualTo(b.root());
        assertThat(MerkleTree.differingBuckets(a.serialized(), b.serialized()))
                .containsExactly(MerkleTree.bucketOf("k3", BUCKETS));
    }

    @Test
    void differingVersionOfSameKeyShowsUpAsItsBucket() {
        Map<String, Record> older = new HashMap<>(Map.of("k", Record.value(new byte[] {1}, 1, "node1")));
        Map<String, Record> newer = new HashMap<>(Map.of("k", Record.value(new byte[] {1}, 9, "node1")));

        List<Integer> diffs = MerkleTree.differingBuckets(
                new MerkleTree(older, BUCKETS).serialized(), new MerkleTree(newer, BUCKETS).serialized());

        assertThat(diffs).containsExactly(MerkleTree.bucketOf("k", BUCKETS));
    }

    @Test
    void sameValueDifferentPayloadButSameVersionIsConsideredEqual() {
        // LWW identity is (lamportTs, coordinatorId, deleted) — the payload bytes don't affect the digest.
        Map<String, Record> x = Map.of("k", Record.value(new byte[] {1}, 4, "node1"));
        Map<String, Record> y = Map.of("k", Record.value(new byte[] {9, 9}, 4, "node1"));

        assertThat(new MerkleTree(x, BUCKETS).root()).isEqualTo(new MerkleTree(y, BUCKETS).root());
    }
}
