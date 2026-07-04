package com.minidynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.minidynamo.config.MiniDynamoProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {"minidynamo.node-id=node1", "minidynamo.seeds=localhost:8080"})
class MiniDynamoApplicationTests {

    @Autowired MiniDynamoProperties props;

    @Test
    void contextLoadsAndAppliesDefaults() {
        assertThat(props.nodeId()).isEqualTo("node1");
        assertThat(props.n()).isEqualTo(3);
        assertThat(props.r()).isEqualTo(2);
        assertThat(props.w()).isEqualTo(2);
        assertThat(props.vnodes()).isEqualTo(128);
    }
}
