package com.minidynamo.storage;

import org.junit.jupiter.api.BeforeEach;

class InMemoryStorageEngineTest extends StorageEngineContract {

    private InMemoryStorageEngine engine;

    @BeforeEach
    void setUp() {
        engine = new InMemoryStorageEngine();
    }

    @Override
    protected StorageEngine engine() {
        return engine;
    }
}
