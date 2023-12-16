package com.netflix.conductor.postgres.dao;

import com.netflix.conductor.common.config.TestObjectMapperConfiguration;
import com.netflix.conductor.postgres.config.PostgresConfiguration;
import org.flywaydb.core.Flyway;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

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

    @After
    public void tearDown() {
        // Clean caches between tests as they are shared globally
        lockDAO.scheduledFutures().values().forEach(f -> f.stream().forEach(s -> s.cancel(false)));
        lockDAO.scheduledFutures().clear();
    }

    @Test
    public void testAcquireLock() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
    }

    @Test
    public void testAcquireLockTwice() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        acquiredLock = lockDAO.acquireLock("testLock", 2500, TimeUnit.MILLISECONDS);
        assertFalse(acquiredLock);
    }

    @Test
    public void testAcquireLockTwiceWithDifferentLockNames() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        acquiredLock = lockDAO.acquireLock("testLock2", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
    }

    @Test
    public void testReentrantLockWithLessReleasesThanAcquires() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);

        assertEquals(2, lockDAO.scheduledFutures().get("testLock").size());

        lockDAO.releaseLock("testLock");

        assertEquals(1, lockDAO.scheduledFutures().get("testLock").size());
    }

    @Test
    public void testReentrantLock() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);

        assertEquals(2, lockDAO.scheduledFutures().get("testLock").size());

        lockDAO.releaseLock("testLock");
        lockDAO.releaseLock("testLock");

        assertEquals(0, lockDAO.scheduledFutures().get("testLock").size());
    }

    @Test
    public void testReleaseLock() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        lockDAO.releaseLock("testLock");
        assertEquals(0, lockDAO.scheduledFutures().get("testLock").size());
    }

    @Test
    public void testReleaseLockTwice() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        lockDAO.releaseLock("testLock");
        lockDAO.releaseLock("testLock");
        assertEquals(0, lockDAO.scheduledFutures().get("testLock").size());
    }

    @Test
    public void testDeleteLock() {
        boolean acquiredLock = lockDAO.acquireLock("testLock", 5000, TimeUnit.MILLISECONDS);
        assertTrue(acquiredLock);
        lockDAO.deleteLock("testLock");
        assertEquals(0, lockDAO.scheduledFutures().get("testLock").size());
    }
}