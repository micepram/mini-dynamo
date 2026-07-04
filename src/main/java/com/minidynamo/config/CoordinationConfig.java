package com.minidynamo.config;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/** Beans for the coordinator fan-out and internal transport (CLAUDE.md §4, §7). */
@Configuration
public class CoordinationConfig {

    /** Bounded pool for concurrent replica fan-out (CLAUDE.md §7). */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService coordinatorExecutor() {
        return Executors.newFixedThreadPool(16);
    }

    /** Short timeouts so an unreachable replica fails fast rather than pinning a fan-out thread. */
    @Bean
    public RestClient internalRestClient() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(1));
        factory.setReadTimeout(Duration.ofSeconds(2));
        return RestClient.builder().requestFactory(factory).build();
    }
}
