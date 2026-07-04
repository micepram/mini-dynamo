package com.minidynamo.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.coordinator.Coordinator;
import com.minidynamo.failure.HintStore;
import com.minidynamo.membership.ClusterMembership;
import com.minidynamo.membership.MembershipTable;
import com.minidynamo.replication.InternalTransport;
import com.minidynamo.replication.LocalReplica;
import com.minidynamo.ring.Node;
import com.minidynamo.storage.InMemoryStorageEngine;
import com.minidynamo.versioning.LamportClock;
import com.minidynamo.versioning.Record;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class KvControllerTest {

    /** Single-node coordinator: quorum met locally (N=R=W=1). */
    private static MockMvc singleNode() {
        return mvc(1, 1, 1);
    }

    /** N=3,R=W=2 but only one node in the ring: every request fails quorum. */
    private static MockMvc underQuorum() {
        return mvc(3, 2, 2);
    }

    private static MockMvc mvc(int n, int r, int w) {
        MiniDynamoProperties props = new MiniDynamoProperties(
                "node1", List.of(), n, r, w, 128, "inmemory", 1000, 5000, 3_600_000, 86_400_000);
        LamportClock clock = new LamportClock();
        MembershipTable table = new MembershipTable(
                new Node("node1", 8080), List.of(), 5000, 1, System::currentTimeMillis);
        Coordinator coordinator = new Coordinator(
                new ClusterMembership(table, props),
                new LocalReplica(new InMemoryStorageEngine(), clock),
                new HintStore(),
                unreachableTransport(),
                Executors.newSingleThreadExecutor(),
                clock,
                props);
        return MockMvcBuilders.standaloneSetup(new KvController(coordinator)).build();
    }

    private static InternalTransport unreachableTransport() {
        return new InternalTransport() {
            @Override
            public void write(Node node, String key, Record record, List<String> hintFor) {
                throw new UnsupportedOperationException("no peers in single-node test");
            }

            @Override
            public Optional<Record> read(Node node, String key) {
                throw new UnsupportedOperationException("no peers in single-node test");
            }
        };
    }

    @Test
    void getMissingKeyReturns404() throws Exception {
        singleNode().perform(get("/kv/missing")).andExpect(status().isNotFound());
    }

    @Test
    void putThenGetReturnsValue() throws Exception {
        MockMvc mvc = singleNode();
        mvc.perform(put("/kv/foo").content("bar")).andExpect(status().isOk());
        mvc.perform(get("/kv/foo")).andExpect(status().isOk()).andExpect(content().string("bar"));
    }

    @Test
    void deleteThenGetReturns404() throws Exception {
        MockMvc mvc = singleNode();
        mvc.perform(put("/kv/foo").content("bar")).andExpect(status().isOk());
        mvc.perform(delete("/kv/foo")).andExpect(status().isOk());
        mvc.perform(get("/kv/foo")).andExpect(status().isNotFound());
    }

    @Test
    void returns503WhenQuorumCannotBeMet() throws Exception {
        underQuorum().perform(put("/kv/foo").content("bar")).andExpect(status().isServiceUnavailable());
    }
}
