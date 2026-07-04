package com.minidynamo.versioning;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class LwwResolverTest {

    private static Record rec(long ts, String coordinator) {
        return Record.value(new byte[] {1}, ts, coordinator);
    }

    @Test
    void higherTimestampWins() {
        Record low = rec(1, "node9");
        Record high = rec(2, "node1");

        assertThat(LwwResolver.resolve(low, high)).isEqualTo(high);
        assertThat(LwwResolver.resolve(high, low)).isEqualTo(high);
    }

    @Test
    void equalTimestampBreaksOnHigherCoordinatorId() {
        Record a = rec(5, "node1");
        Record b = rec(5, "node2");

        // node2 > node1 lexicographically, so b wins regardless of argument order.
        assertThat(LwwResolver.resolve(a, b)).isEqualTo(b);
        assertThat(LwwResolver.resolve(b, a)).isEqualTo(b);
    }

    @Test
    void tombstoneResolvesByTheSameRuleAsAnyWrite() {
        Record value = Record.value(new byte[] {1}, 3, "node1");
        Record laterTombstone = Record.tombstone(4, "node1");

        assertThat(LwwResolver.resolve(value, laterTombstone)).isEqualTo(laterTombstone);
    }

    @Test
    void resolveAcrossCollectionPicksGlobalWinner() {
        Record winner = rec(7, "node1");
        List<Record> records = List.of(rec(1, "node2"), winner, rec(7, "node0"), rec(3, "node9"));

        assertThat(LwwResolver.resolve(records)).contains(winner);
    }

    @Test
    void sameVersionDetectsIdenticalTimestampAndCoordinator() {
        assertThat(LwwResolver.sameVersion(rec(5, "node1"), rec(5, "node1"))).isTrue();
        assertThat(LwwResolver.sameVersion(rec(5, "node1"), rec(5, "node2"))).isFalse();
        assertThat(LwwResolver.sameVersion(rec(5, "node1"), rec(6, "node1"))).isFalse();
    }
}
