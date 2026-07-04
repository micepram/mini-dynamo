package com.minidynamo.storage;

import com.minidynamo.versioning.Record;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;

/** {@link ConcurrentHashMap}-backed engine for unit and integration tests (spec §3.3). */
public class InMemoryStorageEngine implements StorageEngine {

    private final ConcurrentHashMap<String, Record> map = new ConcurrentHashMap<>();

    @Override
    public Optional<Record> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public void put(String key, Record record) {
        map.put(key, record);
    }

    @Override
    public Record merge(String key, Record incoming, BinaryOperator<Record> resolver) {
        // ConcurrentHashMap.merge applies the remap atomically; args are (current, incoming).
        return map.merge(key, incoming, (current, given) -> resolver.apply(given, current));
    }

    @Override
    public Map<String, Record> entries() {
        return Map.copyOf(map);
    }
}
