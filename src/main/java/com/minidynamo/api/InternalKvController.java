package com.minidynamo.api;

import com.minidynamo.failure.HintStore;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.replication.ReplicaWrite;
import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Node-to-node replica API (spec §3.2, §7). A write LWW-merges the record into the main store via
 * {@link LocalReplica} and stores a hint for each unreachable owner named in {@code hintFor}. A read
 * returns 200 with the record, or 204 when the key is absent (distinct from a transport failure).
 */
@RestController
@RequestMapping("/internal/kv")
public class InternalKvController {

    private final LocalReplica replica;
    private final HintStore hints;

    public InternalKvController(LocalReplica replica, HintStore hints) {
        this.replica = replica;
        this.hints = hints;
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> write(@PathVariable String key, @RequestBody ReplicaWrite body) {
        replica.apply(key, body.record());
        long now = System.currentTimeMillis();
        for (String intended : body.hintFor()) {
            hints.store(Node.parse(intended), key, body.record(), now);
        }
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{key}")
    public ResponseEntity<Record> read(@PathVariable String key) {
        return replica.read(key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }
}
