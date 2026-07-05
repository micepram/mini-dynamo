package com.minidynamo.replication;

import com.minidynamo.antientropy.AntiEntropyTransport;
import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * HTTP/JSON transport over Spring {@link RestClient} (spec §3.2, §8). Implements both the replica
 * read/write transport and the anti-entropy transport.
 */
@Component
public class RestClientTransport implements InternalTransport, AntiEntropyTransport {

    private static final ParameterizedTypeReference<Map<String, Record>> RECORD_MAP =
            new ParameterizedTypeReference<>() {};

    private final RestClient client;

    public RestClientTransport(RestClient internalRestClient) {
        this.client = internalRestClient;
    }

    @Override
    public void write(Node node, String key, Record record, List<String> hintFor) {
        client.put()
                .uri(node.baseUrl() + "/internal/kv/{key}", key)
                .contentType(MediaType.APPLICATION_JSON)
                .body(new ReplicaWrite(record, hintFor))
                .retrieve()
                .toBodilessEntity();
    }

    @Override
    public Optional<Record> read(Node node, String key) {
        // 200 -> record; 204 -> absent (body is null); connection/5xx errors throw.
        return Optional.ofNullable(
                client.get().uri(node.baseUrl() + "/internal/kv/{key}", key).retrieve().body(Record.class));
    }

    @Override
    public long[] merkleTree(Node peer, Node self) {
        return client.get()
                .uri(peer.baseUrl() + "/internal/antientropy/tree?peer={p}", self.address())
                .retrieve()
                .body(long[].class);
    }

    @Override
    public Map<String, Record> bucketRecords(Node peer, Node self, List<Integer> buckets) {
        String csv = buckets.stream().map(String::valueOf).collect(Collectors.joining(","));
        return client.get()
                .uri(peer.baseUrl() + "/internal/antientropy/records?peer={p}&buckets={b}", self.address(), csv)
                .retrieve()
                .body(RECORD_MAP);
    }
}
