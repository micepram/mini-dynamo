package com.minidynamo.config;

import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.storage.RocksDbStorageEngine;
import com.minidynamo.storage.StorageEngine;
import java.nio.file.Path;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Selects the {@link StorageEngine} implementation from config (spec §3.3, §9). */
@Configuration
public class StorageConfig {

    @Bean
    public StorageEngine storageEngine(MiniDynamoProperties props) {
        if ("inmemory".equalsIgnoreCase(props.storageEngine())) {
            return new InMemoryStorageEngine();
        }
        String node = props.nodeId() == null ? "node" : props.nodeId();
        return new RocksDbStorageEngine(Path.of("data", node));
    }
}
