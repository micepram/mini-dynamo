package com.minidynamo.membership;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.ring.HashRing;
import com.minidynamo.ring.Node;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.stereotype.Component;

/**
 * Cluster view and the {@link HashRing} built from it. Tier 1 is <em>static</em>: members are
 * {@code SEEDS} ∪ self, so {@code SEEDS} must enumerate every node's address. Gossip (Tier 3)
 * makes this mutable and rebuilds the ring on change — hence {@code ring} is {@code volatile}.
 *
 * <p>Convention: a node's advertised host is its {@code node-id} (the Docker service name), so
 * self's address is {@code node-id:server-port}.
 */
@Component
public class ClusterMembership {

    private static final Logger log = LoggerFactory.getLogger(ClusterMembership.class);

    private final Node self;
    private volatile HashRing ring;

    public ClusterMembership(MiniDynamoProperties props, ServerProperties server) {
        if (props.nodeId() == null || props.nodeId().isBlank()) {
            throw new IllegalStateException("minidynamo.node-id is required");
        }
        if (props.r() + props.w() <= props.n()) {
            throw new IllegalStateException(
                    "R+W must be > N for quorum overlap (R=" + props.r() + ", W=" + props.w() + ", N=" + props.n() + ")");
        }

        int port = server.getPort() == null ? 8080 : server.getPort();
        this.self = new Node(props.nodeId(), port);

        Map<String, Node> members = new LinkedHashMap<>();
        members.put(self.address(), self);
        for (String seed : props.seeds()) {
            Node node = Node.parse(seed);
            members.put(node.address(), node);
        }
        this.ring = new HashRing(members.values(), props.vnodes());

        log.info(
                "Cluster membership: self={} members={} N={} R={} W={} (R+W>N ⇒ read/write quorums overlap)",
                self.address(), members.keySet(), props.n(), props.r(), props.w());
    }

    public Node self() {
        return self;
    }

    public HashRing ring() {
        return ring;
    }
}
