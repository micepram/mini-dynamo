package com.minidynamo.membership;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.ring.HashRing;
import com.minidynamo.ring.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The ring and health view the coordinator routes on, backed by the gossip {@link MembershipTable}.
 * The ring is built over <em>all</em> known nodes (a DEAD node keeps its ring positions — its writes
 * are covered by hinted handoff, not reassigned) and rebuilt only when the node set changes. Health
 * for sloppy routing is queried separately via {@link #isAvailable}.
 */
@Component
public class ClusterMembership {

    private static final Logger log = LoggerFactory.getLogger(ClusterMembership.class);

    private final MembershipTable table;
    private final int vnodes;

    private volatile HashRing ring;
    private volatile long ringVersion = -1;

    public ClusterMembership(MembershipTable table, MiniDynamoProperties props) {
        if (props.r() + props.w() <= props.n()) {
            throw new IllegalStateException(
                    "R+W must be > N for quorum overlap (R=" + props.r() + ", W=" + props.w() + ", N=" + props.n() + ")");
        }
        this.table = table;
        this.vnodes = props.vnodes();
        rebuild();
        log.info(
                "Cluster membership: self={} members={} N={} R={} W={} (R+W>N ⇒ quorums overlap)",
                table.self().address(), table.nodes(), props.n(), props.r(), props.w());
    }

    private synchronized void rebuild() {
        this.ring = new HashRing(table.nodes(), vnodes);
        this.ringVersion = table.nodeSetVersion();
    }

    public HashRing ring() {
        if (table.nodeSetVersion() != ringVersion) {
            rebuild();
        }
        return ring;
    }

    public Node self() {
        return table.self();
    }

    public boolean isAvailable(Node node) {
        return table.isAvailable(node);
    }

    public MembershipTable table() {
        return table;
    }
}
