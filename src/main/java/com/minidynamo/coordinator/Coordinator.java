package com.minidynamo.coordinator;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.replication.QuorumCollector;
import com.minidynamo.replication.QuorumNotMetException;
import com.minidynamo.ring.Node;
import com.minidynamo.versioning.LwwResolver;
import com.minidynamo.versioning.Record;
import com.minidynamo.versioning.VersionStamper;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Per-request coordination (spec §5). Any node coordinates: it stamps a Lamport timestamp, fans
 * out to all N replicas, and returns at the quorum (W acks / R responses). Reads resolve the
 * responses by the LWW rule and asynchronously repair any replica that returned a stale record.
 */
@Component
public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final ClusterMembership membership;
    private final LocalReplica local;
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
            InternalTransport transport,
            ExecutorService coordinatorExecutor,
            VersionStamper clock,
            MiniDynamoProperties props) {
        this.membership = membership;
        this.local = local;
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
        List<Node> preferenceList = membership.ring().preferenceList(key, n);
        List<CompletableFuture<NodeRead>> reads = preferenceList.stream()
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
        List<Node> preferenceList = membership.ring().preferenceList(key, n);
        List<CompletableFuture<Node>> writes = preferenceList.stream()
                .map(node -> CompletableFuture.supplyAsync(
                        () -> {
                            writeReplica(node, key, record);
                            return node;
                        },
                        executor))
                .toList();

        join(QuorumCollector.collect(writes, w));
    }

    /** Push the winning record to any responder that returned a stale or missing record (spec §5.2). */
    private void readRepair(String key, Record winner, List<NodeRead> responses) {
        for (NodeRead response : responses) {
            boolean upToDate = response.record().map(rec -> LwwResolver.sameVersion(rec, winner)).orElse(false);
            if (!upToDate) {
                CompletableFuture.runAsync(() -> writeReplica(response.node(), key, winner), executor)
                        .whenComplete((v, error) -> {
                            if (error != null) {
                                log.debug("read repair to {} failed for key {}", response.node().address(), key);
                            }
                        });
            }
        }
    }

    private void writeReplica(Node node, String key, Record record) {
        if (node.equals(membership.self())) {
            local.apply(key, record);
        } else {
            transport.write(node, key, record);
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
