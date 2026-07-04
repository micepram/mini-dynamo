package com.minidynamo.versioning;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LamportClockTest {

    @Test
    void tickIsStrictlyMonotonic() {
        LamportClock clock = new LamportClock();

        assertThat(clock.tick()).isEqualTo(1);
        assertThat(clock.tick()).isEqualTo(2);
        assertThat(clock.tick()).isEqualTo(3);
    }

    @Test
    void observeAdvancesToSeenTimestamp() {
        LamportClock clock = new LamportClock();

        clock.observe(41);
        assertThat(clock.tick()).isEqualTo(42); // advanced past the observed value
    }

    @Test
    void observeNeverMovesTheClockBackwards() {
        LamportClock clock = new LamportClock();
        clock.tick(); // 1
        clock.tick(); // 2

        clock.observe(1); // older than current -> ignored

        assertThat(clock.current()).isEqualTo(2);
        assertThat(clock.tick()).isEqualTo(3);
    }

    @Test
    void concurrentTicksProduceUniqueMonotonicValues() throws Exception {
        LamportClock clock = new LamportClock();
        int n = 1000;

        List<CompletableFuture<Long>> futures = new ArrayList<>();
        IntStream.range(0, n).forEach(i -> futures.add(CompletableFuture.supplyAsync(clock::tick)));
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get();

        List<Long> values = futures.stream().map(CompletableFuture::join).sorted().toList();
        assertThat(values).doesNotHaveDuplicates();
        assertThat(values.get(values.size() - 1)).isEqualTo(n);
    }
}
