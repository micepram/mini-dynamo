package com.minidynamo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Tier 1 acceptance (spec §10): a 3-node cluster replicates each key, a W=2 write succeeds, and a
 * R=2 read returns the value from any coordinator. Auto-skips when Docker is unavailable so the
 * fast unit suite still runs.
 */
@Testcontainers(disabledWithoutDocker = true)
class ClusterIntegrationTest {

    private static final WaitStrategy STARTED =
            Wait.forLogMessage(".*Started MiniDynamoApplication.*", 1).withStartupTimeout(Duration.ofMinutes(4));

    @Container
    static ComposeContainer cluster = new ComposeContainer(new File("docker-compose.yml"))
            .withExposedService("node1", 8080, STARTED)
            .withExposedService("node2", 8080, STARTED)
            .withExposedService("node3", 8080, STARTED)
            .withLocalCompose(true);

    private final RestClient http = RestClient.create();

    private String base(String service) {
        return "http://" + cluster.getServiceHost(service, 8080) + ":" + cluster.getServicePort(service, 8080);
    }

    private void put(String service, String key, String value) {
        http.put().uri(base(service) + "/kv/" + key).body(value).retrieve().toBodilessEntity();
    }

    private String get(String service, String key) {
        return http.get().uri(base(service) + "/kv/" + key).retrieve().body(String.class);
    }

    @Test
    void writeOnOneNodeIsReadableFromEveryOtherCoordinator() {
        put("node1", "color", "blue"); // W=2 quorum via node1

        // R=2 read resolves the same value regardless of which node coordinates.
        assertThat(get("node1", "color")).isEqualTo("blue");
        assertThat(get("node2", "color")).isEqualTo("blue");
        assertThat(get("node3", "color")).isEqualTo("blue");
    }

    @Test
    void laterWriteFromAnotherCoordinatorWins() {
        put("node1", "leader", "alice"); // earlier Lamport ts
        put("node2", "leader", "bob"); // later ts on a different coordinator

        // LWW: the later write converges everywhere, regardless of read coordinator.
        assertThat(get("node1", "leader")).isEqualTo("bob");
        assertThat(get("node2", "leader")).isEqualTo("bob");
        assertThat(get("node3", "leader")).isEqualTo("bob");
    }

    @Test
    void deleteConvergesAcrossTheCluster() {
        put("node2", "temp", "value");
        assertThat(get("node3", "temp")).isEqualTo("value");

        http.delete().uri(base("node2") + "/kv/temp").retrieve().toBodilessEntity();

        // The tombstone (higher ts) wins on every replica; every coordinator now 404s.
        for (String node : new String[] {"node1", "node2", "node3"}) {
            assertThatThrownBy(() -> get(node, "temp"))
                    .isInstanceOf(RestClientResponseException.class)
                    .satisfies(e -> assertThat(((RestClientResponseException) e).getStatusCode().value()).isEqualTo(404));
        }
    }
}
