/*
 * Copyright 2022 Orkes, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.orkes.conductor.dao.postgres.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.postgres.config.PostgresProperties;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayConfigurationCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({PostgresProperties.class})
@ConditionalOnProperty(name = "conductor.archive.db.type", havingValue = "postgres")
public class PostgresArchiveDAOConfiguration {

    private final DataSource dataSource;

    private final ObjectMapper objectMapper;

    public PostgresArchiveDAOConfiguration(
            ObjectMapper objectMapper,
            DataSource dataSource) {

        this.objectMapper = objectMapper;
        this.dataSource = dataSource;
    }

//    @Bean
//    @Qualifier("primaryExecutionDAO")
//    @ConditionalOnProperty(name = "conductor.archive.db.enabled", havingValue = "true")
//    public ExecutionDAO getPrimaryExecutionDAO() {
//        return primaryExecutionDAO;
//    }

    @Bean(initMethod = "migrate", name = "flyway")
    @PostConstruct
    public Flyway flywayForArchiveDb() {
        return Flyway.configure()
                .locations("classpath:db/migration_archive_postgres")
                .schemas("archive")
                .dataSource(dataSource)
                .baselineOnMigrate(true)
                .mixed(true)
                .outOfOrder(true)
                .load();
    }

    @Bean(name = "flywayInitializer")
    public FlywayConfigurationCustomizer flywayConfigurationCustomizer() {
        // override the default location.
        return configuration ->
                configuration.locations("classpath:db/migration_archive_postgres");
    }

    @Bean
    @DependsOn({"flyway", "flywayInitializer"})
    @ConditionalOnProperty(value = "conductor.archive.db.type", havingValue = "postgres")
    public PostgresArchiveDAO getPostgresArchiveDAO(
            @Qualifier("searchDatasource") DataSource searchDatasource) {
        return new PostgresArchiveDAO(objectMapper, dataSource, searchDatasource);
    }
}
