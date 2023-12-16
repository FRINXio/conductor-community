package com.netflix.conductor.postgres.dao;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.postgres.config.PostgresConfiguration;
import org.flywaydb.core.Flyway;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@ContextConfiguration(
        classes = {
                TestObjectMapperConfiguration.class,
                PostgresConfiguration.class,
                FlywayAutoConfiguration.class,
        })
@RunWith(SpringRunner.class)
@SpringBootTest
public class PostgresLockDAOTest {
    @Autowired
    private PostgresLockDAO lockDAO;

    @Autowired
    Flyway flyway;

    // clean the database between tests.
    @Before
    public void before() {
        flyway.clean();
        flyway.migrate();
    }
}