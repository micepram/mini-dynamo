package com.minidynamo.versioning;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Lamport logical clock (spec §2). {@code observe} pulls the counter up to any timestamp seen
 * (via reads or replicated writes); {@code tick} increments it for a new write. Because every
 * observed timestamp advances the counter, a coordinated write is stamped higher than anything
 * the node has seen. Thread-safe via {@link AtomicLong}.
 */
@Component
public class LamportClock implements VersionStamper {

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public long tick() {
        return counter.incrementAndGet();
    }

    @Override
    public void observe(long seenTs) {
        counter.updateAndGet(current -> Math.max(current, seenTs));
    }

    @Override
    public long current() {
        return counter.get();
    }
}
