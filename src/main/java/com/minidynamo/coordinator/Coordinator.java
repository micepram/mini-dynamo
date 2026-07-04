package com.minidynamo.coordinator;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.QuorumCollector;
import com.minidynamo.replication.QuorumNotMetException;
import com.minidynamo.ring.Node;
import com.minidynamo.storage.StorageEngine;
import com.minidynamo.versioning.Record;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Per-request coordination (spec §5). Any node can coordinate: it computes the preference list,
 * fans out replica reads/writes to all N nodes concurrently, and returns as soon as the quorum
 * ({@code W} acks / {@code R} responses) is met. Self is written/read locally; peers over the
 * {@link InternalTransport}. The coordinator's local write naturally counts toward W.
 */
@Component
public class Coordinator {

    private final ClusterMembership membership;
    private final StorageEngine storage;
    private final InternalTransport transport;
    private final ExecutorService executor;
    private final String coordinatorId;
    private final int n;
    private final int r;
    private final int w;

    // ponytail: Tier 1 placeholder timestamp source. Tier 2 replaces this with the Lamport
    // VersionStamper (advance to max(local, seen) then ++). Fine while there are no conflicts.
    private final AtomicLong ts = new AtomicLong();

    public Coordinator(
            ClusterMembership membership,
            StorageEngine storage,
            InternalTransport transport,
            ExecutorService coordinatorExecutor,
            MiniDynamoProperties props) {
        this.membership = membership;
        this.storage = storage;
        this.transport = transport;
        this.executor = coordinatorExecutor;
        this.coordinatorId = props.nodeId();
        this.n = props.n();
        this.r = props.r();
        this.w = props.w();
    }

    public void put(String key, byte[] value) {
        write(key, Record.value(value, ts.incrementAndGet(), coordinatorId));
    }

    public void delete(String key) {
        write(key, Record.tombstone(ts.incrementAndGet(), coordinatorId));
    }

    /** Returns the value, or empty if the key is absent or resolves to a tombstone (→ 404). */
    public Optional<byte[]> get(String key) {
        List<Node> preferenceList = membership.ring().preferenceList(key, n);
        List<CompletableFuture<Optional<Record>>> reads = preferenceList.stream()
                .map(node -> CompletableFuture.supplyAsync(() -> readReplica(node, key), executor))
                .toList();

        List<Optional<Record>> responses = join(QuorumCollector.collect(reads, r));
        // Tier 1: return any present, non-tombstone record. Tier 2 replaces with LWW resolution
        // across all responses plus read repair of stale replicas.
        return responses.stream()
                .flatMap(Optional::stream)
                .filter(record -> !record.deleted())
                .map(Record::value)
                .findFirst();
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

    private void writeReplica(Node node, String key, Record record) {
        if (node.equals(membership.self())) {
            storage.put(key, record);
        } else {
            transport.write(node, key, record);
        }
    }

    private Optional<Record> readReplica(Node node, String key) {
        if (node.equals(membership.self())) {
            return storage.get(key);
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
}
