package com.minidynamo.antientropy;

import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import java.util.List;
import java.util.Map;

/**
 * Node-to-node anti-entropy calls (spec §8), kept separate from the replica read/write transport so
 * the two concerns evolve independently. {@code self} is the caller's address, which the peer uses to
 * compute the symmetric set of keys the two nodes share (both appear in the key's preference list).
 */
public interface AntiEntropyTransport {

    /** The peer's serialized Merkle tree over the keys it shares with {@code self}. */
    long[] merkleTree(Node peer, Node self);

    /** The peer's records for the given buckets, among the keys it shares with {@code self}. */
    Map<String, Record> bucketRecords(Node peer, Node self, List<Integer> buckets);
}
