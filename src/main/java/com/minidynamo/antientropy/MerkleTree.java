package com.minidynamo.antientropy;

import com.minidynamo.ring.HashRing;
import com.minidynamo.versioning.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Merkle tree over a node's records, partitioned into a fixed number of hash buckets (spec §8).
 * Two replicas of the same range build trees over the same bucket space, so their trees are
 * structurally identical and comparable node-by-node. Comparing roots is the fast "in sync" gate;
 * on mismatch the descent visits only the subtrees that differ, yielding the differing bucket
 * indices — the keys that actually need to be exchanged.
 *
 * <p>Each key maps to a bucket by {@link HashRing#hash(String)} mod {@code buckets}. A bucket's leaf
 * digest is a hash of its keys and their <em>versions</em> (lamportTs + coordinatorId + deleted) —
 * the value payload is irrelevant to LWW, so it is left out. Internal nodes fold their children.
 *
 * <p>The tree is stored in a heap array: {@code tree[1]} is the root, internal nodes occupy
 * {@code 1..buckets-1}, and leaf bucket {@code b} sits at {@code tree[buckets + b]}.
 */
public final class MerkleTree {

    private final long[] tree;

    public MerkleTree(Map<String, Record> records, int buckets) {
        if (Integer.bitCount(buckets) != 1) {
            throw new IllegalArgumentException("buckets must be a power of two: " + buckets);
        }
        this.tree = build(records, buckets);
    }

    public long root() {
        return tree[1];
    }

    /** The heap array (root at index 1), as exchanged with a peer. */
    public long[] serialized() {
        return tree.clone();
    }

    private static long[] build(Map<String, Record> records, int buckets) {
        // Sort keys within each bucket so the digest is independent of iteration order.
        List<TreeMap<String, Record>> byBucket = new ArrayList<>(buckets);
        for (int b = 0; b < buckets; b++) {
            byBucket.add(new TreeMap<>());
        }
        for (Map.Entry<String, Record> e : records.entrySet()) {
            byBucket.get(bucketOf(e.getKey(), buckets)).put(e.getKey(), e.getValue());
        }
        long[] tree = new long[2 * buckets];
        for (int b = 0; b < buckets; b++) {
            tree[buckets + b] = leafDigest(byBucket.get(b));
        }
        for (int i = buckets - 1; i >= 1; i--) {
            tree[i] = combine(tree[2 * i], tree[2 * i + 1]);
        }
        return tree;
    }

    private static long leafDigest(TreeMap<String, Record> bucket) {
        if (bucket.isEmpty()) {
            return 0L;
        }
        StringBuilder sb = new StringBuilder();
        // Length-prefix the key so distinct (key, version) sets can't collide by concatenation.
        bucket.forEach((k, r) -> sb.append(k.length()).append(':').append(k)
                .append('|').append(r.lamportTs())
                .append('|').append(r.coordinatorId())
                .append('|').append(r.deleted())
                .append(';'));
        return HashRing.hash(sb.toString());
    }

    /** FNV-style mix of two child digests. Deterministic; order-sensitive (left vs right). */
    private static long combine(long left, long right) {
        return (left * 1099511628211L) ^ right;
    }

    public static int bucketOf(String key, int buckets) {
        return (int) Long.remainderUnsigned(HashRing.hash(key), buckets);
    }

    /**
     * Descend two serialized trees of equal size and return the indices of the leaf buckets whose
     * digests differ. Subtrees with matching digests are skipped at their common ancestor — the
     * Merkle short-circuit that keeps comparison cheap when replicas are mostly in sync.
     */
    public static List<Integer> differingBuckets(long[] a, long[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("tree size mismatch: " + a.length + " vs " + b.length);
        }
        int buckets = a.length / 2;
        List<Integer> diffs = new ArrayList<>();
        descend(a, b, 1, buckets, diffs);
        return diffs;
    }

    private static void descend(long[] a, long[] b, int i, int buckets, List<Integer> diffs) {
        if (a[i] == b[i]) {
            return; // subtree identical
        }
        if (i >= buckets) {
            diffs.add(i - buckets); // leaf
            return;
        }
        descend(a, b, 2 * i, buckets, diffs);
        descend(a, b, 2 * i + 1, buckets, diffs);
    }
}
