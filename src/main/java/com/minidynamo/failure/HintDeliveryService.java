package com.minidynamo.failure;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.MembershipTable;
import com.minidynamo.replication.InternalTransport;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically hands off held hints once their intended owner is ALIVE again (spec §7): deliver the
 * record, then drop the local copy on success. Expired hints are dropped regardless. A failed
 * delivery is left in place to retry next round.
 */
@Component
public class HintDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(HintDeliveryService.class);

    private final HintStore hints;
    private final MembershipTable membership;
    private final InternalTransport transport;
    private final long retentionMs;

    public HintDeliveryService(
            HintStore hints, MembershipTable membership, InternalTransport transport, MiniDynamoProperties props) {
        this.hints = hints;
        this.membership = membership;
        this.transport = transport;
        this.retentionMs = props.hintRetentionMs();
    }

    @Scheduled(fixedDelayString = "${minidynamo.gossip-interval-ms:1000}")
    public void deliver() {
        long now = System.currentTimeMillis();
        hints.dropExpired(now, retentionMs);

        for (Hint hint : hints.all()) {
            if (!membership.isAvailable(hint.intendedNode())) {
                continue; // owner still down — keep the hint
            }
            try {
                transport.write(hint.intendedNode(), hint.key(), hint.record(), List.of());
                hints.remove(hint);
                log.info("hint delivered to {} key {}", hint.intendedNode().address(), hint.key());
            } catch (RuntimeException e) {
                log.debug("hint delivery to {} failed, will retry", hint.intendedNode().address());
            }
        }
    }
}
