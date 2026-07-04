package com.minidynamo.api;

import com.minidynamo.coordinator.Coordinator;
import com.minidynamo.replication.QuorumNotMetException;
import java.io.IOException;
import java.io.InputStream;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Client-facing key-value API (spec §5). Any node coordinates the request. 200 on success,
 * 404 on absent/tombstoned keys, 503 when a quorum cannot be met.
 */
@RestController
@RequestMapping("/kv")
public class KvController {

    private final Coordinator coordinator;

    public KvController(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @GetMapping("/{key}")
    public ResponseEntity<byte[]> get(@PathVariable String key) {
        return coordinator.get(key)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> put(@PathVariable String key, InputStream body) throws IOException {
        // Raw request stream so any Content-Type works (see application.yml formcontent note).
        coordinator.put(key, body.readAllBytes());
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) {
        coordinator.delete(key);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(QuorumNotMetException.class)
    public ResponseEntity<Void> quorumNotMet() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }
}
