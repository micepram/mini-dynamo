package com.minidynamo.versioning;

import java.util.Collection;
import java.util.Optional;

/**
 * The single, deterministic last-write-wins rule (spec §2, CLAUDE.md §2). Higher {@code lamportTs}
 * wins; ties break on higher {@code coordinatorId} (lexicographic). This total order guarantees
 * every replica converges to the same record. Reused everywhere resolution happens: coordinator
 * read resolution, replica-write merge, read repair, and anti-entropy.
 */
public final class LwwResolver {

    private LwwResolver() {}

    /** True if {@code candidate} strictly wins over {@code incumbent} under the LWW order. */
    public static boolean wins(Record candidate, Record incumbent) {
        if (candidate.lamportTs() != incumbent.lamportTs()) {
            return candidate.lamportTs() > incumbent.lamportTs();
        }
        return candidate.coordinatorId().compareTo(incumbent.coordinatorId()) > 0;
    }

    /** The winner of two records (returns {@code b} on an exact tie — the records are equivalent). */
    public static Record resolve(Record a, Record b) {
        return wins(a, b) ? a : b;
    }

    /** The winner across a set of records, or empty if the set is empty. */
    public static Optional<Record> resolve(Collection<Record> records) {
        return records.stream().reduce(LwwResolver::resolve);
    }

    /** Whether two records represent the same logical version (same timestamp and coordinator). */
    public static boolean sameVersion(Record a, Record b) {
        return a.lamportTs() == b.lamportTs() && a.coordinatorId().equals(b.coordinatorId());
    }
}
