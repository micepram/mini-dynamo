package com.minidynamo.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Single binding point for all custom settings (CLAUDE.md §5). Every {@code minidynamo.*}
 * environment variable maps here; no {@code @Value} reads are scattered elsewhere.
 *
 * <p>Startup validation ({@code R+W>N}, required fields) is added in Tier 1.
 */
@ConfigurationProperties(prefix = "minidynamo")
public record MiniDynamoProperties(
        String nodeId,
        List<String> seeds,
        int n,
        int r,
        int w,
        int vnodes,
        String storageEngine,
        long gossipIntervalMs,
        long failureTimeoutMs,
        long hintRetentionMs,
        long tombstoneGcMs) {

    public MiniDynamoProperties {
        seeds = seeds == null ? List.of() : List.copyOf(seeds);
        if (n == 0) n = 3;
        if (r == 0) r = 2;
        if (w == 0) w = 2;
        if (vnodes == 0) vnodes = 128;
        if (storageEngine == null) storageEngine = "rocksdb";
        if (gossipIntervalMs == 0) gossipIntervalMs = 1000;
        if (failureTimeoutMs == 0) failureTimeoutMs = 5000;
        if (hintRetentionMs == 0) hintRetentionMs = 3_600_000;
        if (tombstoneGcMs == 0) tombstoneGcMs = 86_400_000;
    }
}
