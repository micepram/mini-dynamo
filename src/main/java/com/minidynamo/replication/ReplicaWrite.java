package com.minidynamo.replication;

import com.minidynamo.versioning.Record;
import java.util.List;

/**
 * The body of an internal replica write (spec §5.1). The substitute stores {@code record} as its
 * own replica and, for each address in {@code hintFor}, a hint identifying an intended owner that
 * was unreachable — to be handed off when that owner recovers. {@code hintFor} is empty for a
 * normal write, read repair, or hint delivery.
 */
public record ReplicaWrite(Record record, List<String> hintFor) {

    public ReplicaWrite {
        hintFor = hintFor == null ? List.of() : List.copyOf(hintFor);
    }
}
