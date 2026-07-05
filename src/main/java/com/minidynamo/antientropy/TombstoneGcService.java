package com.minidynamo.antientropy;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.storage.StorageEngine;
import com.minidynamo.versioning.Record;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Tombstone garbage collection (spec §2, §8). A delete leaves a tombstone {@link Record} so LWW can
 * resolve it against concurrent writes; once it has outlived the retention window it is hard-removed
 * to reclaim space. Age is measured from when this node first observed the tombstone — a wall clock
 * used only for GC timing, never as a record timestamp source.
 *
 * <p>ponytail: naive age-based GC. Removing a tombstone the whole cluster hasn't yet seen can let
 * anti-entropy resurrect the key from a lagging replica that still holds the old value. The default
 * retention (24h) is far longer than convergence, so this is safe in practice; a fully safe GC would
 * remove only after every replica acknowledges the tombstone. Upgrade there if it ever matters.
 */
@Component
public class TombstoneGcService {

    private static final Logger log = LoggerFactory.getLogger(TombstoneGcService.class);

    private final StorageEngine storage;
    private final long retentionMs;

    // key -> wall-clock millis this node first saw the tombstone. Guarded by ConcurrentHashMap.
    private final ConcurrentHashMap<String, Long> firstSeen = new ConcurrentHashMap<>();

    public TombstoneGcService(StorageEngine storage, MiniDynamoProperties props) {
        this.storage = storage;
        this.retentionMs = props.tombstoneGcMs();
    }

    @Scheduled(fixedDelayString = "${minidynamo.gossip-interval-ms:1000}")
    public void gc() {
        gc(System.currentTimeMillis());
    }

    /** Package-private so a test can drive GC at a controlled wall-clock time. */
    void gc(long now) {
        Map<String, Record> entries = storage.entries();
        // Forget keys that are no longer tombstones (a later write revived them) so their age resets.
        firstSeen.keySet().removeIf(k -> {
            Record r = entries.get(k);
            return r == null || !r.deleted();
        });
        for (Map.Entry<String, Record> e : entries.entrySet()) {
            if (!e.getValue().deleted()) {
                continue;
            }
            long seen = firstSeen.computeIfAbsent(e.getKey(), k -> now);
            if (now - seen >= retentionMs) {
                storage.remove(e.getKey());
                firstSeen.remove(e.getKey());
                log.info("tombstone GC removed key {}", e.getKey());
            }
        }
    }
}
