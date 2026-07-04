package com.minidynamo.api;

import com.minidynamo.replication.LocalReplica;
import com.minidynamo.versioning.Record;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Node-to-node replica API (spec §3.2). Writes go through {@link LocalReplica#apply} so the
 * Lamport clock advances and LWW-merge keeps only a winning record. A read returns 200 with the
 * record, or 204 when the key is absent (distinct from a transport failure).
 */
@RestController
@RequestMapping("/internal/kv")
public class InternalKvController {

    private final LocalReplica replica;

    public InternalKvController(LocalReplica replica) {
        this.replica = replica;
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> write(@PathVariable String key, @RequestBody Record record) {
        replica.apply(key, record);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{key}")
    public ResponseEntity<Record> read(@PathVariable String key) {
        return replica.read(key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }
}
