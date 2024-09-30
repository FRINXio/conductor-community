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

import com.netflix.conductor.postgres.config.PostgresProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;;

@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({PostgresProperties.class})
@Import(DataSourceAutoConfiguration.class)
@ConditionalOnProperty(name = "conductor.archive.db.type", havingValue = "postgres")
public class PostgresArchiveDatasourceConfiguration {

    private final Environment environment;

    public PostgresArchiveDatasourceConfiguration(Environment environment) {
        this.environment = environment;
    }

    @Bean
    @Qualifier("searchDatasource")
    public DataSource searchDatasource(DataSource defaultDatasource) {
        String url = environment.getProperty("spring.search-datasource.url");
        String user = environment.getProperty("spring.search-datasource.username");
        String password = environment.getProperty("spring.search-datasource.password");
        String maxPoolSizeString =
                environment.getProperty("spring.search-datasource.hikari.maximum-pool-size");

        if (Strings.isEmpty(url)) {
            return defaultDatasource;
        }
        log.info("Configuring searchDatasource with {}", url);

        int maxPoolSize = 10;
        if (Strings.isNotEmpty(maxPoolSizeString)) {
            try {
                maxPoolSize = Integer.parseInt(maxPoolSizeString);
            } catch (Exception e) {
            }
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setAutoCommit(true);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setDriverClassName("org.postgresql.Driver");
        return new HikariDataSource(config);
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            @Qualifier("searchDatasource") DataSource searchDatasource) {
        LocalContainerEntityManagerFactoryBean factoryBean =
                new LocalContainerEntityManagerFactoryBean();
        factoryBean.setDataSource(searchDatasource);
        factoryBean.setPackagesToScan("com.netflix");

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(true);
        vendorAdapter.setShowSql(true);

        factoryBean.setJpaVendorAdapter(vendorAdapter);
        factoryBean
                .getJpaPropertyMap()
                .put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        return factoryBean;
    }

    @Bean
    public PlatformTransactionManager transactionManager(
            EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
