package com.minidynamo.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class QuorumCollectorTest {

    private static CompletableFuture<String> ok(String v) {
        return CompletableFuture.completedFuture(v);
    }

    private static CompletableFuture<String> fail() {
        return CompletableFuture.failedFuture(new RuntimeException("replica down"));
    }

    @Test
    void completesAtExactlyThresholdAndIgnoresStragglers() {
        CompletableFuture<String> straggler = new CompletableFuture<>(); // never completes
        List<CompletableFuture<String>> futures = List.of(ok("a"), ok("b"), straggler);

        CompletableFuture<List<String>> result = QuorumCollector.collect(futures, 2);

        assertThat(result.join()).containsExactlyInAnyOrder("a", "b");
        assertThat(result).isCompleted();
    }

    @Test
    void failsWhenThresholdCanNoLongerBeReached() {
        // 3 replicas, W=2, two fail -> only 1 possible success, quorum impossible.
        CompletableFuture<List<String>> result = QuorumCollector.collect(List.of(ok("a"), fail(), fail()), 2);

        assertThatThrownBy(result::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(QuorumNotMetException.class);
    }

    @Test
    void succeedsDespiteSomeFailuresWhenThresholdStillReachable() {
        CompletableFuture<List<String>> result = QuorumCollector.collect(List.of(ok("a"), fail(), ok("c")), 2);

        assertThat(result.join()).containsExactlyInAnyOrder("a", "c");
    }

    @Test
    void thresholdExceedingReplicaCountFailsImmediately() {
        CompletableFuture<List<String>> result = QuorumCollector.collect(List.of(ok("a")), 2);

        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void zeroThresholdCompletesEmptyImmediately() {
        assertThat(QuorumCollector.collect(List.of(ok("a")), 0).join()).isEmpty();
    }
}
