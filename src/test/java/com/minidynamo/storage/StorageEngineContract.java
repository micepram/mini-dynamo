package com.minidynamo.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Behavior every {@link StorageEngine} must satisfy. Subclasses supply the engine. */
abstract class StorageEngineContract {

    protected abstract StorageEngine engine();

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putThenGetReturnsStoredRecord() {
        engine().put("k", Record.value(utf8("v"), 1, "node1"));

        Optional<Record> got = engine().get("k");

        assertThat(got).isPresent();
        assertThat(got.get().value()).isEqualTo(utf8("v"));
        assertThat(got.get().lamportTs()).isEqualTo(1);
        assertThat(got.get().coordinatorId()).isEqualTo("node1");
        assertThat(got.get().deleted()).isFalse();
    }

    @Test
    void putOverwritesPreviousRecord() {
        engine().put("k", Record.value(utf8("old"), 1, "node1"));
        engine().put("k", Record.value(utf8("new"), 2, "node1"));

        assertThat(engine().get("k")).get().extracting(Record::value).isEqualTo(utf8("new"));
    }

    @Test
    void getAbsentKeyReturnsEmpty() {
        assertThat(engine().get("missing")).isEmpty();
    }

    @Test
    void tombstoneIsStoredAndRetrievableAsDeleted() {
        engine().put("k", Record.tombstone(5, "node1"));

        Optional<Record> got = engine().get("k");

        assertThat(got).isPresent();
        assertThat(got.get().deleted()).isTrue();
        assertThat(got.get().lamportTs()).isEqualTo(5);
    }

    @Test
    void mergeKeepsTheLwwWinnerAndReturnsIt() {
        engine().put("k", Record.value(utf8("old"), 3, "node1"));

        // Incoming loses (lower ts) -> stored record unchanged.
        Record afterLoser = engine().merge("k", Record.value(utf8("stale"), 1, "node1"), LwwResolver::resolve);
        assertThat(afterLoser.value()).isEqualTo(utf8("old"));
        assertThat(engine().get("k")).get().extracting(Record::value).isEqualTo(utf8("old"));

        // Incoming wins (higher ts) -> stored record replaced.
        Record afterWinner = engine().merge("k", Record.value(utf8("new"), 9, "node1"), LwwResolver::resolve);
        assertThat(afterWinner.value()).isEqualTo(utf8("new"));
        assertThat(engine().get("k")).get().extracting(Record::value).isEqualTo(utf8("new"));
    }

    @Test
    void mergeOnAbsentKeyStoresIncoming() {
        Record stored = engine().merge("fresh", Record.value(utf8("v"), 1, "node1"), LwwResolver::resolve);

        assertThat(stored.value()).isEqualTo(utf8("v"));
        assertThat(engine().get("fresh")).isPresent();
    }

    @Test
    void removeHardDeletesAKey() {
        engine().put("k", Record.tombstone(5, "node1"));

        engine().remove("k");

        assertThat(engine().get("k")).isEmpty();
    }

    @Test
    void entriesSnapshotIncludesTombstonesAndValues() {
        engine().put("a", Record.value(utf8("1"), 1, "node1"));
        engine().put("b", Record.tombstone(2, "node1"));

        assertThat(engine().entries()).containsOnlyKeys("a", "b");
        assertThat(engine().entries().get("b").deleted()).isTrue();
    }
}
