package com.minidynamo.replication;

import com.minidynamo.storage.StorageEngine;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import com.minidynamo.versioning.VersionStamper;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * A replica's local view of the store (spec §2, §5). Every replica write — whether from the
 * coordinator's self-write, a peer's internal call, or read repair — goes through {@link #apply},
 * which advances the Lamport clock to the observed timestamp and LWW-merges so only a winning
 * record is kept. Reads also observe the stored timestamp so the clock respects causality.
 */
@Component
public class LocalReplica {

    private final StorageEngine storage;
    private final VersionStamper clock;

    public LocalReplica(StorageEngine storage, VersionStamper clock) {
        this.storage = storage;
        this.clock = clock;
    }

    /** Apply an incoming record with LWW semantics; returns the record now stored. */
    public Record apply(String key, Record incoming) {
        clock.observe(incoming.lamportTs());
        return storage.merge(key, incoming, LwwResolver::resolve);
    }

    public Optional<Record> read(String key) {
        Optional<Record> record = storage.get(key);
        record.ifPresent(r -> clock.observe(r.lamportTs()));
        return record;
    }
}
