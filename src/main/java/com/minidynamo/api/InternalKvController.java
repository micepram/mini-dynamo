package com.minidynamo.api;

import com.minidynamo.storage.StorageEngine;
import com.minidynamo.versioning.Record;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Node-to-node replica API (spec §3.2). Operates on raw {@link Record}s: the coordinator has
 * already stamped versioning. A read returns 200 with the record, or 204 when the key is absent
 * (distinct from a transport failure). Tier 2 will make the write path LWW-merge.
 */
@RestController
@RequestMapping("/internal/kv")
public class InternalKvController {

    private final StorageEngine storage;

    public InternalKvController(StorageEngine storage) {
        this.storage = storage;
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> write(@PathVariable String key, @RequestBody Record record) {
        storage.put(key, record);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{key}")
    public ResponseEntity<Record> read(@PathVariable String key) {
        return storage.get(key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }
}
