package com.minidynamo.membership;

import com.minidynamo.ring.Node;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs one gossip round each interval (spec §6): advance our heartbeat, age peer entries (failure
 * detection), then push-pull our table with a random peer and merge the reply. A failed exchange is
 * ignored — the peer's silence is what the failure detector will eventually act on.
 */
@Component
public class GossipService {

    private static final Logger log = LoggerFactory.getLogger(GossipService.class);

    private final MembershipTable table;
    private final GossipTransport transport;

    public GossipService(MembershipTable table, GossipTransport transport) {
        this.table = table;
        this.transport = transport;
    }

    @Scheduled(fixedDelayString = "${minidynamo.gossip-interval-ms:1000}")
    public void round() {
        table.bumpSelfHeartbeat();
        table.sweep();

        List<Node> peers = table.nodes().stream().filter(node -> !node.equals(table.self())).toList();
        if (peers.isEmpty()) {
            return;
        }
        Node peer = peers.get(ThreadLocalRandom.current().nextInt(peers.size()));
        try {
            transport.exchange(peer, table.views()).forEach(table::merge);
        } catch (RuntimeException e) {
            log.debug("gossip to {} failed: {}", peer.address(), e.getMessage());
        }
    }
}
