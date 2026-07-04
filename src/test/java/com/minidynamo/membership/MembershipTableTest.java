package com.minidynamo.membership;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.ring.Node;
import java.util.List;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class MembershipTableTest {

    private static final Node SELF = new Node("node1", 8080);
    private static final Node PEER = new Node("node2", 8080);
    private static final long TIMEOUT = 1000;

    /** Mutable clock for deterministic failure-detector tests. */
    private static final class TestClock implements LongSupplier {
        long now = 10_000;

        @Override
        public long getAsLong() {
            return now;
        }
    }

    private MembershipTable table(TestClock clock) {
        return new MembershipTable(SELF, List.of(PEER), TIMEOUT, 1, clock);
    }

    @Test
    void mergeAddsUnknownNodeAsAlive() {
        TestClock clock = new TestClock();
        MembershipTable table = table(clock);
        Node node3 = new Node("node3", 8080);

        boolean added = table.merge(new MemberView(node3, 5, 0));

        assertThat(added).isTrue();
        assertThat(table.stateOf(node3)).isEqualTo(MemberState.ALIVE);
        assertThat(table.nodes()).contains(node3);
    }

    @Test
    void mergePrefersHigherIncarnationThenHigherHeartbeat() {
        TestClock clock = new TestClock();
        MembershipTable table = table(clock);

        table.merge(new MemberView(PEER, 5, 1));
        table.merge(new MemberView(PEER, 4, 1)); // lower heartbeat, same incarnation -> ignored
        assertThat(currentHeartbeat(table, PEER)).isEqualTo(5);

        table.merge(new MemberView(PEER, 1, 2)); // higher incarnation wins despite lower heartbeat
        assertThat(currentHeartbeat(table, PEER)).isEqualTo(1);
    }

    @Test
    void sweepTransitionsAliveToSuspectToDead() {
        TestClock clock = new TestClock();
        MembershipTable table = table(clock);
        table.merge(new MemberView(PEER, 1, 0)); // heard now

        clock.now += 500; // within timeout
        table.sweep();
        assertThat(table.stateOf(PEER)).isEqualTo(MemberState.ALIVE);

        clock.now += 1000; // age 1500 > TIMEOUT
        table.sweep();
        assertThat(table.stateOf(PEER)).isEqualTo(MemberState.SUSPECT);

        clock.now += 1000; // age 2500 > 2*TIMEOUT
        table.sweep();
        assertThat(table.stateOf(PEER)).isEqualTo(MemberState.DEAD);
        assertThat(table.isAvailable(PEER)).isFalse();
    }

    @Test
    void freshHeartbeatRevivesADeadMember() {
        TestClock clock = new TestClock();
        MembershipTable table = table(clock);
        clock.now += 5000;
        table.sweep();
        assertThat(table.stateOf(PEER)).isEqualTo(MemberState.DEAD);

        table.merge(new MemberView(PEER, 10, 1)); // recovered, higher incarnation
        assertThat(table.stateOf(PEER)).isEqualTo(MemberState.ALIVE);
        assertThat(table.isAvailable(PEER)).isTrue();
    }

    @Test
    void selfIsAlwaysAliveAndImmuneToPeerViews() {
        TestClock clock = new TestClock();
        MembershipTable table = table(clock);

        table.merge(new MemberView(SELF, 999, 999)); // peer claims something about us
        clock.now += 10_000;
        table.sweep();

        assertThat(table.stateOf(SELF)).isEqualTo(MemberState.ALIVE);
        assertThat(table.isAvailable(SELF)).isTrue();
    }

    @Test
    void bumpSelfHeartbeatIsPublishedInViews() {
        TestClock clock = new TestClock();
        MembershipTable table = table(clock);
        table.bumpSelfHeartbeat();
        table.bumpSelfHeartbeat();

        assertThat(currentHeartbeat(table, SELF)).isEqualTo(2);
    }

    private static long currentHeartbeat(MembershipTable table, Node node) {
        return table.views().stream()
                .filter(v -> v.node().equals(node))
                .map(MemberView::heartbeat)
                .findFirst()
                .orElseThrow();
    }
}
