package com.minidynamo.ring;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Consistent-hash ring with virtual nodes (spec §4). Immutable: a membership change
 * builds a new ring. Each physical {@link Node} owns {@code vnodes} positions, placed by
 * hashing {@code address#i} with SHA-256 (truncated to a long) for uniform distribution.
 */
public final class HashRing {

    private final NavigableMap<Long, Node> ring = new TreeMap<>();
    private final int vnodes;

    public HashRing(Collection<Node> nodes, int vnodes) {
        this.vnodes = vnodes;
        for (Node node : nodes) {
            for (int i = 0; i < vnodes; i++) {
                ring.put(hash(node.address() + "#" + i), node);
            }
        }
    }

    /**
     * Preference list for a key (spec §4): walk the ring clockwise from the key's position
     * and collect the first {@code n} distinct physical nodes. Returns fewer than {@code n}
     * only if the ring has fewer than {@code n} distinct nodes.
     */
    public List<Node> preferenceList(String key, int n) {
        List<Node> result = new ArrayList<>();
        if (ring.isEmpty()) {
            return result;
        }
        long start = hash(key);
        // tailMap(start) is the clockwise segment from the key; then wrap through the whole ring.
        List<Node> walk = new ArrayList<>(ring.tailMap(start, true).values());
        walk.addAll(ring.values());
        for (Node node : walk) {
            if (!result.contains(node)) {
                result.add(node);
                if (result.size() == n) {
                    break;
                }
            }
        }
        return result;
    }

    public int vnodes() {
        return vnodes;
    }

    /** SHA-256 of the input, first 8 bytes folded into a long. Deterministic. Reused by anti-entropy bucketing. */
    public static long hash(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xffL);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
