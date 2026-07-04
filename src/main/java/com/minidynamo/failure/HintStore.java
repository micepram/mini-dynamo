package com.minidynamo.failure;

import com.minidynamo.ring.Node;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Local store of undelivered hints (spec §7), separate from the main data store. In-memory and
 * keyed by {@code intendedNode|key} so at most one (LWW-winning) hint is held per owner+key.
 *
 * <p>ponytail: in-memory — hints are lost if the holder itself restarts before delivery. Anti-entropy
 * (Tier 4) is the backstop for divergence hints don't cover; persist the hint store only if that gap
 * matters.
 */
@Component
public class HintStore {

    private static final Logger log = LoggerFactory.getLogger(HintStore.class);

    private final ConcurrentHashMap<String, Hint> hints = new ConcurrentHashMap<>();

    private static String key(Node intended, String key) {
        return intended.address() + "|" + key;
    }

    public void store(Node intendedNode, String key, Record record, long nowMillis) {
        hints.merge(
                key(intendedNode, key),
                new Hint(intendedNode, key, record, nowMillis),
                (current, incoming) -> LwwResolver.wins(incoming.record(), current.record()) ? incoming : current);
        log.info("hint stored for {} key {}", intendedNode.address(), key);
    }

    public Collection<Hint> all() {
        return List.copyOf(hints.values());
    }

    public void remove(Hint hint) {
        hints.remove(key(hint.intendedNode(), hint.key()), hint);
    }

    /** Drop hints older than the retention window (spec §7). */
    public void dropExpired(long nowMillis, long retentionMs) {
        hints.values().removeIf(hint -> nowMillis - hint.storedAtMillis() > retentionMs);
    }

    public int size() {
        return hints.size();
    }
}
