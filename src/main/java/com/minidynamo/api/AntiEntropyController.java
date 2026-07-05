package com.minidynamo.api;

import com.minidynamo.antientropy.AntiEntropyService;
import com.minidynamo.ring.Node;
import com.minidynamo.versioning.Record;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Node-to-node anti-entropy API (spec §8). A peer fetches this node's Merkle tree, then the records
 * in the buckets that differed. {@code peer} is the caller's address, so both sides filter to the
 * same shared keyspace (keys whose preference list contains both nodes).
 */
@RestController
@RequestMapping("/internal/antientropy")
public class AntiEntropyController {

    private final AntiEntropyService service;

    public AntiEntropyController(AntiEntropyService service) {
        this.service = service;
    }

    @GetMapping("/tree")
    public long[] tree(@RequestParam String peer) {
        return service.localTree(Node.parse(peer)).serialized();
    }

    @GetMapping("/records")
    public Map<String, Record> records(@RequestParam String peer, @RequestParam List<Integer> buckets) {
        return service.recordsInBuckets(Node.parse(peer), buckets);
    }
}
