package com.minidynamo.membership;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.ring.Node;
import java.util.List;
import org.junit.jupiter.api.Test;

class GossipServiceTest {

    private static final Node SELF = new Node("node1", 8080);
    private static final Node PEER = new Node("node2", 8080);
    private static final Node NODE3 = new Node("node3", 8080);

    private static MembershipTable table() {
        return new MembershipTable(SELF, List.of(PEER), 5000, 1, System::currentTimeMillis);
    }

    @Test
    void roundMergesPeerReplyIntoTable() {
        MembershipTable table = table();
        GossipService service = new GossipService(table, (peer, views) -> List.of(new MemberView(NODE3, 7, 2)));

        service.round();

        assertThat(table.nodes()).contains(NODE3);
        assertThat(table.stateOf(NODE3)).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void roundToleratesTransportFailure() {
        MembershipTable table = table();
        GossipService service = new GossipService(table, (peer, views) -> {
            throw new RuntimeException("peer unreachable");
        });

        service.round(); // must not propagate

        // self heartbeat still advanced despite the failed exchange
        long selfHeartbeat = table.views().stream()
                .filter(v -> v.node().equals(SELF))
                .map(MemberView::heartbeat)
                .findFirst()
                .orElseThrow();
        assertThat(selfHeartbeat).isEqualTo(1);
    }

    @Test
    void roundSendsOurViewsToTheChosenPeer() {
        MembershipTable table = table();
        List<MemberView>[] sent = new List[1];
        GossipService service = new GossipService(table, (peer, views) -> {
            sent[0] = views;
            return List.of();
        });

        service.round();

        assertThat(sent[0]).extracting(MemberView::node).contains(SELF, PEER);
    }
}
