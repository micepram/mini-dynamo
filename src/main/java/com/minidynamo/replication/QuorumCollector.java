package com.minidynamo.replication;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Completes as soon as {@code threshold} of the given futures succeed (spec §5, CLAUDE.md §7).
 * Stragglers are not awaited — they may still complete in the background for read repair and
 * durability. If enough futures fail that {@code threshold} can no longer be reached, the
 * result completes exceptionally with {@link QuorumNotMetException}.
 */
public final class QuorumCollector {

    private QuorumCollector() {}

    public static <T> CompletableFuture<List<T>> collect(List<CompletableFuture<T>> futures, int threshold) {
        CompletableFuture<List<T>> result = new CompletableFuture<>();
        int total = futures.size();

        if (threshold <= 0) {
            result.complete(List.of());
            return result;
        }
        if (threshold > total) {
            result.completeExceptionally(
                    new QuorumNotMetException(threshold, total, "threshold exceeds replica count"));
            return result;
        }

        ConcurrentLinkedQueue<T> successes = new ConcurrentLinkedQueue<>();
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        for (CompletableFuture<T> future : futures) {
            future.whenComplete((value, error) -> {
                if (error != null) {
                    int f = failed.incrementAndGet();
                    // Once too many have failed, the remaining successes cannot reach threshold.
                    if (total - f < threshold) {
                        result.completeExceptionally(
                                new QuorumNotMetException(threshold, total - f, "not enough healthy replicas"));
                    }
                } else {
                    successes.add(value);
                    if (succeeded.incrementAndGet() == threshold) {
                        result.complete(List.copyOf(successes));
                    }
                }
            });
        }
        return result;
    }
}
