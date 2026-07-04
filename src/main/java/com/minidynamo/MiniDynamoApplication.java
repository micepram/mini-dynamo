package com.minidynamo;

import com.minidynamo.config.MiniDynamoProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(MiniDynamoProperties.class)
public class MiniDynamoApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniDynamoApplication.class, args);
    }
}
