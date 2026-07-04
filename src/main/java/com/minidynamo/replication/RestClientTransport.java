package com.minidynamo.replication;

import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import java.util.Optional;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** HTTP/JSON {@link InternalTransport} over Spring {@link RestClient} (spec §3.2). */
@Component
public class RestClientTransport implements InternalTransport {

    private final RestClient client;

    public RestClientTransport(RestClient internalRestClient) {
        this.client = internalRestClient;
    }

    @Override
    public void write(Node node, String key, Record record) {
        client.put()
                .uri(node.baseUrl() + "/internal/kv/{key}", key)
                .contentType(MediaType.APPLICATION_JSON)
                .body(record)
                .retrieve()
                .toBodilessEntity();
    }

    @Override
    public Optional<Record> read(Node node, String key) {
        // 200 -> record; 204 -> absent (body is null); connection/5xx errors throw.
        return Optional.ofNullable(
                client.get().uri(node.baseUrl() + "/internal/kv/{key}", key).retrieve().body(Record.class));
    }
}
