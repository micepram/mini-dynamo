package com.minidynamo.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.minidynamo.config.MiniDynamoProperties;
import com.minidynamo.storage.InMemoryStorageEngine;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class KvControllerTest {

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        MiniDynamoProperties props =
                new MiniDynamoProperties(
                        "node1", List.of("localhost:8080"), 0, 0, 0, 0, "inmemory", 0, 0, 0, 0);
        mvc = MockMvcBuilders.standaloneSetup(new KvController(new InMemoryStorageEngine(), props))
                .build();
    }

    @Test
    void getMissingKeyReturns404() throws Exception {
        mvc.perform(get("/kv/missing")).andExpect(status().isNotFound());
    }

    @Test
    void putThenGetReturnsValue() throws Exception {
        mvc.perform(put("/kv/foo").content("bar")).andExpect(status().isOk());
        mvc.perform(get("/kv/foo")).andExpect(status().isOk()).andExpect(content().string("bar"));
    }

    @Test
    void deleteThenGetReturns404() throws Exception {
        mvc.perform(put("/kv/foo").content("bar")).andExpect(status().isOk());
        mvc.perform(delete("/kv/foo")).andExpect(status().isOk());
        mvc.perform(get("/kv/foo")).andExpect(status().isNotFound());
    }
}
