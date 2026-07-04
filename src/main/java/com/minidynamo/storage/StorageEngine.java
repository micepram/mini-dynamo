package com.minidynamo.storage;

import com.minidynamo.versioning.Record;
import java.util.Map;
import java.util.Optional;

/**
 * Pluggable local persistence (spec §3.3). A raw key→{@link Record} store: it does not
 * apply LWW itself — conflict resolution lives at the coordinator/replica-write layer.
 * Deletes are writes of a tombstone {@link Record}, never hard removes on the write path.
 */
public interface StorageEngine {

    Optional<Record> get(String key);

    void put(String key, Record record);

    /** Snapshot of all entries, including tombstones. Used by tests and (later) anti-entropy. */
    Map<String, Record> entries();
}
