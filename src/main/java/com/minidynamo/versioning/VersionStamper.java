package com.minidynamo.versioning;

/**
 * Logical timestamp source (spec §2). The sole source of write ordering — no wall clock.
 * Kept behind an interface for testability and so the Lamport implementation can be swapped.
 */
public interface VersionStamper {

    /** Advance the clock and return a fresh timestamp for coordinating a write. */
    long tick();

    /** Advance the clock to respect an observed timestamp: {@code counter = max(counter, seenTs)}. */
    void observe(long seenTs);

    long current();
}
