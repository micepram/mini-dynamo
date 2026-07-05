package com.minidynamo.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidynamo.versioning.Record;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/** RocksDB JNI-backed engine for real runs (spec §3.3). Records are Jackson-serialized. */
public class RocksDbStorageEngine implements StorageEngine, Closeable {

    static {
        RocksDB.loadLibrary();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RocksDB db;

    public RocksDbStorageEngine(Path directory) {
        try {
            directory.toFile().mkdirs();
            Options options = new Options().setCreateIfMissing(true);
            this.db = RocksDB.open(options, directory.toString());
        } catch (RocksDBException e) {
            throw new IllegalStateException("Failed to open RocksDB at " + directory, e);
        }
    }

    @Override
    public Optional<Record> get(String key) {
        try {
            byte[] raw = db.get(bytes(key));
            return raw == null ? Optional.empty() : Optional.of(deserialize(raw));
        } catch (RocksDBException e) {
            throw new IllegalStateException("RocksDB get failed for key " + key, e);
        }
    }

    @Override
    public void put(String key, Record record) {
        try {
            db.put(bytes(key), serialize(record));
        } catch (RocksDBException e) {
            throw new IllegalStateException("RocksDB put failed for key " + key, e);
        }
    }

    @Override
    public synchronized Record merge(String key, Record incoming, BinaryOperator<Record> resolver) {
        // ponytail: coarse per-engine lock for read-modify-write. Fine single-process; stripe
        // per key if merge throughput ever matters.
        Record current = get(key).orElse(null);
        Record winner = current == null ? incoming : resolver.apply(incoming, current);
        put(key, winner);
        return winner;
    }

    @Override
    public Map<String, Record> entries() {
        Map<String, Record> out = new HashMap<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.put(new String(it.key(), StandardCharsets.UTF_8), deserialize(it.value()));
            }
        }
        return Map.copyOf(out);
    }

    @Override
    public void remove(String key) {
        try {
            db.delete(bytes(key));
        } catch (RocksDBException e) {
            throw new IllegalStateException("RocksDB delete failed for key " + key, e);
        }
    }

    @Override
    public void close() {
        db.close();
    }

    private static byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] serialize(Record record) {
        try {
            return MAPPER.writeValueAsBytes(record);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Record deserialize(byte[] raw) {
        try {
            return MAPPER.readValue(raw, Record.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
