package com.minidynamo.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.versioning.Record;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDbStorageEngineTest extends StorageEngineContract {

    @TempDir Path dir;
    private RocksDbStorageEngine engine;

    @BeforeEach
    void setUp() {
        engine = new RocksDbStorageEngine(dir.resolve("db"));
    }

    @AfterEach
    void tearDown() {
        engine.close();
    }

    @Override
    protected StorageEngine engine() {
        return engine;
    }

    @Test
    void valuesSurviveReopen() {
        Path db = dir.resolve("persist");
        try (RocksDbStorageEngine first = new RocksDbStorageEngine(db)) {
            first.put("k", Record.value("durable".getBytes(StandardCharsets.UTF_8), 3, "node1"));
        }

        try (RocksDbStorageEngine reopened = new RocksDbStorageEngine(db)) {
            assertThat(reopened.get("k")).isPresent();
            assertThat(reopened.get("k").get().value())
                    .isEqualTo("durable".getBytes(StandardCharsets.UTF_8));
            assertThat(reopened.get("k").get().lamportTs()).isEqualTo(3);
        }
    }
}
