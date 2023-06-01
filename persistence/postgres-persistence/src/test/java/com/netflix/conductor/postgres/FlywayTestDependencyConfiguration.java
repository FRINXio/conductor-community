package com.netflix.conductor.postgres;

import org.springframework.context.annotation.Bean;

// FIXME remove in the next version. Once any migration problems are solved

public class FlywayTestDependencyConfiguration {

    @Bean(name = "flyway")
    public Object flyway() {
        return new Object();
    }
}
