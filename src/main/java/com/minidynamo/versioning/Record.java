package com.minidynamo.versioning;

/**
 * Immutable stored value for a key (spec §2). {@code value} is an opaque payload;
 * a delete is a {@code deleted} tombstone carrying the same {@code lamportTs} machinery
 * as any other write, resolved by LWW.
 *
 * <p>ponytail: {@code value} is a {@code byte[]}, so record-generated {@code equals}/
 * {@code hashCode} are identity-based on the array. Callers that need value equality
 * compare fields explicitly (see the storage contract test). Upgrade to a wrapper only
 * if value-equality is needed on the hot path.
 */
public record Record(byte[] value, long lamportTs, String coordinatorId, boolean deleted) {

    public static Record value(byte[] value, long lamportTs, String coordinatorId) {
        return new Record(value, lamportTs, coordinatorId, false);
    }

    public static Record tombstone(long lamportTs, String coordinatorId) {
        return new Record(new byte[0], lamportTs, coordinatorId, true);
    }
}
