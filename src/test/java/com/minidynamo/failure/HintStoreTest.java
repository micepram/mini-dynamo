package com.minidynamo.failure;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import org.junit.jupiter.api.Test;

class HintStoreTest {

    private static final Node OWNER = new Node("node3", 8080);

    @Test
    void storesAndReturnsAHint() {
        HintStore store = new HintStore();
        store.store(OWNER, "k", Record.value(new byte[] {1}, 1, "node1"), 1000);

        assertThat(store.all()).singleElement().satisfies(hint -> {
            assertThat(hint.intendedNode()).isEqualTo(OWNER);
            assertThat(hint.key()).isEqualTo("k");
        });
    }

    @Test
    void keepsOnlyTheLwwWinnerPerOwnerAndKey() {
        HintStore store = new HintStore();
        store.store(OWNER, "k", Record.value(new byte[] {1}, 1, "node1"), 1000);
        store.store(OWNER, "k", Record.value(new byte[] {2}, 5, "node1"), 1000); // newer wins
        store.store(OWNER, "k", Record.value(new byte[] {3}, 2, "node1"), 1000); // older ignored

        assertThat(store.all()).singleElement().satisfies(hint -> assertThat(hint.record().lamportTs()).isEqualTo(5));
    }

    @Test
    void removeDeletesAStoredHint() {
        HintStore store = new HintStore();
        store.store(OWNER, "k", Record.value(new byte[] {1}, 1, "node1"), 1000);

        store.remove(store.all().iterator().next());

        assertThat(store.all()).isEmpty();
        assertThat(store.size()).isZero();
    }

    @Test
    void dropExpiredRemovesHintsPastRetention() {
        HintStore store = new HintStore();
        store.store(OWNER, "old", Record.value(new byte[] {1}, 1, "node1"), 1000);
        store.store(OWNER, "fresh", Record.value(new byte[] {1}, 1, "node1"), 9000);

        store.dropExpired(10_000, 2000); // retention 2s: age(old)=9000 dropped, age(fresh)=1000 kept

        assertThat(store.all()).singleElement().satisfies(hint -> assertThat(hint.key()).isEqualTo("fresh"));
    }
}
