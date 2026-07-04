package com.minidynamo.config;

import com.minidynamo.membership.MembershipTable;
import com.minidynamo.ring.Node;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.web.ServerProperties;

/** Builds the {@link MembershipTable} from config (spec §6, §9). */
@Configuration
public class MembershipConfig {

    @Bean
    public MembershipTable membershipTable(MiniDynamoProperties props, ServerProperties server) {
        if (props.nodeId() == null || props.nodeId().isBlank()) {
            throw new IllegalStateException("minidynamo.node-id is required");
        }
        int port = server.getPort() == null ? 8080 : server.getPort();
        Node self = new Node(props.nodeId(), port);
        List<Node> seeds = props.seeds().stream().map(Node::parse).toList();
        // Incarnation is boot wall-clock time: a fresh value each restart lets a recovered node
        // override the stale DEAD entry its peers still hold. (Failure timing only — never record LWW.)
        long incarnation = System.currentTimeMillis();
        return new MembershipTable(self, seeds, props.failureTimeoutMs(), incarnation, System::currentTimeMillis);
    }
}
