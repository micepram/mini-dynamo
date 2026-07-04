package com.minidynamo.api;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.storage.StorageEngine;
import com.minidynamo.versioning.Record;
import java.io.IOException;
import java.io.InputStream;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Client-facing key-value API (spec §5). Tier 0 is single-node: writes stamp {@code ts=0}
 * and {@code coordinatorId=nodeId} (the Lamport {@code VersionStamper} arrives in Tier 2).
 * A delete writes a tombstone, never a hard remove. 200 on hit, 404 on absent/tombstone.
 */
@RestController
@RequestMapping("/kv")
public class KvController {

    private final StorageEngine storage;
    private final String nodeId;

    public KvController(StorageEngine storage, MiniDynamoProperties props) {
        this.storage = storage;
        this.nodeId = props.nodeId();
    }

    @GetMapping("/{key}")
    public ResponseEntity<byte[]> get(@PathVariable String key) {
        return storage.get(key)
                .filter(record -> !record.deleted())
                .map(record -> ResponseEntity.ok(record.value()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> put(@PathVariable String key, InputStream body) throws IOException {
        // Read the raw request stream so any Content-Type works (curl's default form encoding
        // otherwise gets consumed by Spring's form converter before a byte[] @RequestBody).
        storage.put(key, Record.value(body.readAllBytes(), 0, nodeId));
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) {
        storage.put(key, Record.tombstone(0, nodeId));
        return ResponseEntity.ok().build();
    }
}
