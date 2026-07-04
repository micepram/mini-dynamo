package com.minidynamo.replication;

import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import java.util.Optional;

/**
 * Node-to-node replica calls (spec §3.2). Kept behind an interface so the HTTP/JSON
 * implementation can be swapped (e.g. for gRPC) without touching coordinator logic.
 * Both methods throw on transport failure (a down/unreachable replica); an <em>absent</em>
 * key is a successful {@link Optional#empty()} read, distinct from a failure.
 */
public interface InternalTransport {

    void write(Node node, String key, Record record);

    Optional<Record> read(Node node, String key);
}
