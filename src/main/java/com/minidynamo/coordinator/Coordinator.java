package com.minidynamo.coordinator;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.failure.HintStore;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.replication.QuorumCollector;
import com.minidynamo.replication.QuorumNotMetException;
import com.minidynamo.ring.Node;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import com.minidynamo.versioning.VersionStamper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Per-request coordination (spec §5, §7). Any node coordinates: it stamps a Lamport timestamp and
 * fans out to the first N <em>available</em> nodes clockwise (sloppy quorum). When an intended owner
 * is DEAD, a substitute takes its slot and holds a hint for it. Writes return at W acks, reads at R
 * responses; reads resolve by LWW and asynchronously repair stale responders.
 */
@Component
public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final ClusterMembership membership;
    private final LocalReplica local;
    private final HintStore hints;
    private final InternalTransport transport;
    private final ExecutorService executor;
    private final VersionStamper clock;
    private final String coordinatorId;
    private final int n;
    private final int r;
    private final int w;

    public Coordinator(
            ClusterMembership membership,
            LocalReplica local,
            HintStore hints,
            InternalTransport transport,
            ExecutorService coordinatorExecutor,
            VersionStamper clock,
            MiniDynamoProperties props) {
        this.membership = membership;
        this.local = local;
        this.hints = hints;
        this.transport = transport;
        this.executor = coordinatorExecutor;
        this.clock = clock;
        this.coordinatorId = props.nodeId();
        this.n = props.n();
        this.r = props.r();
        this.w = props.w();
    }

    public void put(String key, byte[] value) {
        write(key, Record.value(value, clock.tick(), coordinatorId));
    }

    public void delete(String key) {
        write(key, Record.tombstone(clock.tick(), coordinatorId));
    }

    /** Returns the value, or empty if the key is absent or resolves to a tombstone (→ 404). */
    public Optional<byte[]> get(String key) {
        List<Node> targets = availableTargets(key);
        List<CompletableFuture<NodeRead>> reads = targets.stream()
                .map(node -> CompletableFuture.supplyAsync(() -> new NodeRead(node, readReplica(node, key)), executor))
                .toList();

        List<NodeRead> responses = join(QuorumCollector.collect(reads, r));
        responses.forEach(nr -> nr.record().ifPresent(rec -> clock.observe(rec.lamportTs())));

        Optional<Record> winner = LwwResolver.resolve(
                responses.stream().flatMap(nr -> nr.record().stream()).toList());
        winner.ifPresent(w -> readRepair(key, w, responses));

        return winner.filter(record -> !record.deleted()).map(Record::value);
    }

    private void write(String key, Record record) {
        List<Node> ordered = clockwiseNodes(key);
        List<Node> intended = ordered.stream().limit(n).toList();
        List<Node> targets = ordered.stream().filter(membership::isAvailable).limit(n).toList();
        Map<Node, List<String>> hintsByTarget = assignHints(intended, targets, ordered);

        List<CompletableFuture<Node>> writes = targets.stream()
                .map(node -> CompletableFuture.supplyAsync(
                        () -> {
                            writeReplica(node, key, record, hintsByTarget.getOrDefault(node, List.of()));
                            return node;
                        },
                        executor))
                .toList();

        join(QuorumCollector.collect(writes, w));
    }

    /** First N available nodes clockwise from the key (sloppy quorum set). */
    private List<Node> availableTargets(String key) {
        return clockwiseNodes(key).stream().filter(membership::isAvailable).limit(n).toList();
    }

    private List<Node> clockwiseNodes(String key) {
        int total = membership.table().nodes().size();
        return membership.ring().preferenceList(key, total);
    }

    /** Assign each unreachable intended owner to the nearest clockwise target, which holds its hint. */
    private Map<Node, List<String>> assignHints(List<Node> intended, List<Node> targets, List<Node> ordered) {
        Map<Node, List<String>> byTarget = new HashMap<>();
        Set<Node> targetSet = new HashSet<>(targets);
        for (Node owner : intended) {
            if (!membership.isAvailable(owner)) {
                Node holder = nearestClockwiseTarget(owner, ordered, targetSet);
                if (holder != null) {
                    byTarget.computeIfAbsent(holder, key -> new ArrayList<>()).add(owner.address());
                }
            }
        }
        return byTarget;
    }

    private Node nearestClockwiseTarget(Node from, List<Node> ordered, Set<Node> targets) {
        int index = ordered.indexOf(from);
        if (index < 0) {
            return null;
        }
        int size = ordered.size();
        for (int step = 1; step <= size; step++) {
            Node candidate = ordered.get((index + step) % size);
            if (targets.contains(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    /** Push the winning record to any responder that returned a stale or missing record (spec §5.2). */
    private void readRepair(String key, Record winner, List<NodeRead> responses) {
        for (NodeRead response : responses) {
            boolean upToDate = response.record().map(rec -> LwwResolver.sameVersion(rec, winner)).orElse(false);
            if (!upToDate) {
                CompletableFuture.runAsync(() -> writeReplica(response.node(), key, winner, List.of()), executor)
                        .whenComplete((v, error) -> {
                            if (error != null) {
                                log.debug("read repair to {} failed for key {}", response.node().address(), key);
                            }
                        });
            }
        }
    }

    private void writeReplica(Node node, String key, Record record, List<String> hintFor) {
        if (node.equals(membership.self())) {
            local.apply(key, record);
            long now = System.currentTimeMillis();
            hintFor.forEach(owner -> hints.store(Node.parse(owner), key, record, now));
        } else {
            transport.write(node, key, record, hintFor);
        }
    }

    private Optional<Record> readReplica(Node node, String key) {
        if (node.equals(membership.self())) {
            return local.read(key);
        }
        return transport.read(node, key);
    }

    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof QuorumNotMetException q) {
                throw q;
            }
            throw e;
        }
    }

    /** A replica's response to a read: the node and the record it returned (empty = absent). */
    private record NodeRead(Node node, Optional<Record> record) {}
}
