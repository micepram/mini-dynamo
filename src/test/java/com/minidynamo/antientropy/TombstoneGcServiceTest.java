package com.minidynamo.antientropy;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.versioning.Record;
import org.junit.jupiter.api.Test;

class TombstoneGcServiceTest {

    private static final long RETENTION = 10_000;

    private final InMemoryStorageEngine storage = new InMemoryStorageEngine();
    private final TombstoneGcService gc = new TombstoneGcService(storage, props());

    private static MiniDynamoProperties props() {
        return new MiniDynamoProperties(
                "node1", java.util.List.of("node1:8080"),
                3, 2, 2, 128, "inmemory", 1000, 5000, 3_600_000, RETENTION);
    }

    @Test
    void tombstoneIsRemovedOnlyAfterRetention() {
        storage.put("k", Record.tombstone(1, "node1"));

        gc.gc(1_000); // first seen at t=1000
        assertThat(storage.get("k")).isPresent(); // within retention — kept

        gc.gc(1_000 + RETENTION); // age == retention
        assertThat(storage.get("k")).isEmpty(); // hard-removed
    }

    @Test
    void liveValuesAreNeverRemoved() {
        storage.put("k", Record.value(new byte[] {1}, 1, "node1"));

        gc.gc(1_000);
        gc.gc(1_000 + 10 * RETENTION);

        assertThat(storage.get("k")).isPresent();
    }

    @Test
    void aRevivedKeyResetsItsTombstoneAge() {
        storage.put("k", Record.tombstone(1, "node1"));
        gc.gc(1_000); // first seen at t=1000

        storage.put("k", Record.value(new byte[] {1}, 5, "node1")); // revived — no longer a tombstone
        gc.gc(2_000);
        storage.put("k", Record.tombstone(9, "node1")); // deleted again

        gc.gc(2_500); // only 500ms since the new tombstone — must survive
        assertThat(storage.get("k")).isPresent();

        gc.gc(2_000 + RETENTION); // still short of the revived tombstone's retention (first seen ~2500)
        assertThat(storage.get("k")).isPresent();
    }
}
