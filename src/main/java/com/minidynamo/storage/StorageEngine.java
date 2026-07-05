package com.minidynamo.storage;

import com.minidynamo.versioning.Record;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 * Pluggable local persistence (spec §3.3). {@code put} is a raw overwrite; the replica write
 * path instead uses {@link #merge} to apply LWW atomically. Deletes are writes of a tombstone
 * {@link Record}, never hard removes on the write path.
 */
public interface StorageEngine {

    Optional<Record> get(String key);

    void put(String key, Record record);

    /**
     * Atomically store the winner of {@code resolver.apply(incoming, current)} (or {@code incoming}
     * if the key is absent) and return the stored record. Atomicity prevents concurrent replica
     * writes to the same key from losing an update.
     */
    Record merge(String key, Record incoming, BinaryOperator<Record> resolver);

    /** Snapshot of all entries, including tombstones. Used by tests and anti-entropy. */
    Map<String, Record> entries();

    /**
     * Hard-remove a key. Used only by tombstone GC after the retention window (spec §8) — never on
     * the write path, where a delete is a tombstone {@link Record}.
     */
    void remove(String key);
}
