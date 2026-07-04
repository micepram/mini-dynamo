package com.minidynamo.ring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HashRingTest {

    private static final List<Node> THREE =
            List.of(new Node("node1", 8080), new Node("node2", 8080), new Node("node3", 8080));

    @Test
    void preferenceListIsDeterministic() {
        HashRing a = new HashRing(THREE, 128);
        HashRing b = new HashRing(THREE, 128);

        for (String key : List.of("alpha", "bravo", "charlie", "delta")) {
            assertThat(a.preferenceList(key, 3)).isEqualTo(b.preferenceList(key, 3));
        }
    }

    @Test
    void preferenceListReturnsNDistinctNodes() {
        HashRing ring = new HashRing(THREE, 128);

        List<Node> pref = ring.preferenceList("some-key", 3);

        assertThat(pref).hasSize(3).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(THREE);
    }

    @Test
    void preferenceListCappedAtDistinctNodeCount() {
        HashRing ring = new HashRing(THREE, 128);

        // Asking for more than the cluster size yields every distinct node, no more.
        assertThat(ring.preferenceList("k", 5)).hasSize(3);
    }

    @Test
    void preferenceListWrapsAroundTheRing() {
        HashRing ring = new HashRing(THREE, 128);

        // Every key must resolve to a full preference list regardless of where it hashes,
        // which only holds if the walk wraps past the highest vnode back to the lowest.
        for (int i = 0; i < 500; i++) {
            assertThat(ring.preferenceList("key-" + i, 3)).hasSize(3);
        }
    }

    @Test
    void firstPreferenceIsRoughlyUniformAcrossNodes() {
        HashRing ring = new HashRing(THREE, 128);
        Map<Node, Integer> counts = new HashMap<>();

        int keys = 3000;
        for (int i = 0; i < keys; i++) {
            Node primary = ring.preferenceList("key-" + i, 3).get(0);
            counts.merge(primary, 1, Integer::sum);
        }

        // With 128 vnodes each node should own a fair share; allow a generous band.
        assertThat(counts).hasSize(3);
        int expected = keys / 3;
        counts.values().forEach(c -> assertThat(c).isBetween(expected / 2, expected * 3 / 2));
    }

    @Test
    void emptyRingReturnsEmptyPreferenceList() {
        assertThat(new HashRing(List.of(), 128).preferenceList("k", 3)).isEmpty();
    }
}
