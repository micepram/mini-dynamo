package io.minidynamo.node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
        "io.minidynamo.node",
        "io.minidynamo.admin"
})
public class DynamoNodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamoNodeApplication.class, args);
    }
}
