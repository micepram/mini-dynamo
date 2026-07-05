package com.minidynamo.antientropy;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.ring.HashRing;
import com.minidynamo.ring.Node;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import com.minidynamo.storage.StorageEngine;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Merkle-tree anti-entropy (spec §8). Each round, for every available peer, build a tree over the
 * keys the two nodes share (both appear in the key's preference list), fetch the peer's tree, and
 * compare. Equal roots ⇒ nothing to do. Otherwise descend to the differing buckets, exchange only
 * those keys' records, and reconcile each by the same LWW rule used everywhere else — pulling a
 * winner into the local store and pushing it back to the peer as needed until both converge.
 *
 * <p>This catches divergence that read repair and hinted handoff miss, e.g. a replica that was down
 * past hint retention (spec §8 acceptance).
 *
 * <p>ponytail: rebuilds trees per peer each round rather than maintaining them incrementally, and
 * reconciles with every available peer (not a random one). Fine for a 3–5 node local cluster where
 * the root-hash gate makes the common in-sync case cheap; revisit if the keyspace or cluster grows.
 */
@Component
public class AntiEntropyService {

    /** Power of two. Internal detail, not a tuning knob for this scale. */
    static final int BUCKETS = 256;

    private static final Logger log = LoggerFactory.getLogger(AntiEntropyService.class);

    private final StorageEngine storage;
    private final ClusterMembership membership;
    private final LocalReplica local;
    private final AntiEntropyTransport digestTransport;
    private final InternalTransport transport;
    private final int n;

    public AntiEntropyService(
            StorageEngine storage,
            ClusterMembership membership,
            LocalReplica local,
            AntiEntropyTransport digestTransport,
            InternalTransport transport,
            MiniDynamoProperties props) {
        this.storage = storage;
        this.membership = membership;
        this.local = local;
        this.digestTransport = digestTransport;
        this.transport = transport;
        this.n = props.n();
    }

    @Scheduled(fixedDelayString = "${minidynamo.gossip-interval-ms:1000}")
    public void reconcile() {
        Node self = membership.self();
        for (Node peer : membership.table().nodes()) {
            if (peer.equals(self) || !membership.isAvailable(peer)) {
                continue;
            }
            try {
                syncWith(peer);
            } catch (RuntimeException e) {
                log.debug("anti-entropy with {} failed: {}", peer.address(), e.toString());
            }
        }
    }

    /** Reconcile the shared keyspace with one peer. Package-private so tests can drive one round. */
    void syncWith(Node peer) {
        long[] mine = localTree(peer).serialized();
        long[] theirs = digestTransport.merkleTree(peer, membership.self());
        if (mine.length != theirs.length) {
            return; // bucket-count mismatch (shouldn't happen with identical code) — skip
        }
        List<Integer> diffs = MerkleTree.differingBuckets(mine, theirs);
        if (diffs.isEmpty()) {
            return; // roots (and all subtrees) match — replicas are in sync
        }
        Map<String, Record> mineRecs = recordsInBuckets(peer, diffs);
        Map<String, Record> theirRecs = digestTransport.bucketRecords(peer, membership.self(), diffs);

        Set<String> keys = new HashSet<>(mineRecs.keySet());
        keys.addAll(theirRecs.keySet());
        for (String key : keys) {
            Record mineRec = mineRecs.get(key);
            Record theirRec = theirRecs.get(key);
            Record winner = mineRec == null ? theirRec
                    : theirRec == null ? mineRec
                    : LwwResolver.resolve(mineRec, theirRec);
            if (mineRec == null || !LwwResolver.sameVersion(mineRec, winner)) {
                local.apply(key, winner); // pull: we were stale or missing
            }
            if (theirRec == null || !LwwResolver.sameVersion(theirRec, winner)) {
                transport.write(peer, key, winner, List.of()); // push: they were stale or missing
            }
        }
        log.info("anti-entropy with {} reconciled {} keys across {} buckets", peer.address(), keys.size(), diffs.size());
    }

    // --- local digest side, also served to peers via AntiEntropyController ---

    /** This node's Merkle tree over the keys it shares with {@code peer}. */
    public MerkleTree localTree(Node peer) {
        return new MerkleTree(sharedRecords(peer), BUCKETS);
    }

    /** This node's records in the given buckets, among the keys it shares with {@code peer}. */
    public Map<String, Record> recordsInBuckets(Node peer, List<Integer> buckets) {
        Set<Integer> want = new HashSet<>(buckets);
        Map<String, Record> out = new HashMap<>();
        for (Map.Entry<String, Record> e : sharedRecords(peer).entrySet()) {
            if (want.contains(MerkleTree.bucketOf(e.getKey(), BUCKETS))) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    /** Keys whose preference list contains both this node and {@code peer} — the set they co-own. */
    private Map<String, Record> sharedRecords(Node peer) {
        Node self = membership.self();
        HashRing ring = membership.ring();
        Map<String, Record> shared = new HashMap<>();
        for (Map.Entry<String, Record> e : storage.entries().entrySet()) {
            List<Node> pref = ring.preferenceList(e.getKey(), n);
            if (pref.contains(self) && pref.contains(peer)) {
                shared.put(e.getKey(), e.getValue());
            }
        }
        return shared;
    }
}
