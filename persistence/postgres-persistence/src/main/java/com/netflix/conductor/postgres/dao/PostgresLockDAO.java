package com.netflix.conductor.postgres.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.postgres.util.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import javax.sql.DataSource;
import java.util.concurrent.*;

public class PostgresLockDAO extends PostgresBaseDAO implements Lock {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLockDAO.class);
    private static final ConcurrentHashMap<String, ScheduledFuture<?>> SCHEDULEDFUTURES =
            new ConcurrentHashMap<>();
    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("PostgresLock-scheduler");
    private static final ThreadFactory THREAD_FACTORY =
            runnable -> new Thread(THREAD_GROUP, runnable);
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(1, THREAD_FACTORY);
    protected PostgresLockDAO(RetryTemplate retryTemplate, ObjectMapper objectMapper, DataSource dataSource) {
        super(retryTemplate, objectMapper, dataSource);
    }

    @Override
    public void acquireLock(String lockId) {
        LOGGER.trace("Acquiring lock {}", lockId);
        // create code for acquiring advisory lock in postgres
        final String ADVISORY_LOCK = "SELECT pg_advisory_lock (?)";

        // execute the query
        queryWithTransaction(ADVISORY_LOCK, (q) -> {
            q.addParameter(lockId);
            q.executeQuery();

            return q;
        });
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, TimeUnit unit) {
        final String TRY_ADVISORY_LOCK =
                "DECLARE\n" +
                        "  lock_acquired boolean;\n" +
                        "  start_time timestamp;\n" +
                        "BEGIN\n" +
                        "  start_time := now();\n" +
                        "\n" +
                        "  lock_acquired := pg_try_advisory_lock(?);\n" +
                        "\n" +
                        "WHILE NOT lock_acquired AND start_time + INTERVAL '? ?' < now() LOOP\n" +
                        "  PERFORM pg_sleep(0.5);\n" +
                        "  lock_acquired := pg_try_advisory_lock(?);\n" +
                        "END LOOP;\n" +
                        "\n" +
                        "SELECT lock_acquired;\n" +
                        "\n" +
                        "END;";

        Query query = queryWithTransaction(TRY_ADVISORY_LOCK, (q) -> {
            q.addParameter(lockId);
            q.addParameter(timeToTry);
            q.addParameter(unit.toString());
            q.addParameter(lockId);

            return q;
        });

        return query.executeAndFetchFirst(Boolean.class);
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit) {
        final String TRY_ADVISORY_LOCK =
                "DECLARE\n" +
                        "  lock_acquired boolean;\n" +
                        "  start_time timestamp;\n" +
                        "BEGIN\n" +
                        "  start_time := now();\n" +
                        "\n" +
                        "  lock_acquired := pg_try_advisory_lock(?);\n" +
                        "\n" +
                        "WHILE NOT lock_acquired AND start_time + INTERVAL '? ?' < now() LOOP\n" +
                        "  PERFORM pg_sleep(0.5);\n" +
                        "  lock_acquired := pg_try_advisory_lock(?);\n" +
                        "END LOOP;\n" +
                        "\n" +
                        "SELECT lock_acquired;\n" +
                        "\n" +
                        "END;";

        Query query = queryWithTransaction(TRY_ADVISORY_LOCK, (q) -> {
            q.addParameter(lockId);
            q.addParameter(timeToTry);
            q.addParameter(unit.toString());
            q.addParameter(lockId);
            q.executeQuery();

            return q;
        });

        SCHEDULEDFUTURES.put(
                lockId, SCHEDULER.schedule(() -> deleteLock(lockId), leaseTime, unit)
        );

        return query.executeAndFetchFirst(Boolean.class);
    }

    @Override
    public void releaseLock(String lockId) {
        final String ADVISORY_UNLOCK = "SELECT pg_advisory_unlock (?)";

        queryWithTransaction(ADVISORY_UNLOCK, (q) -> {
            q.addParameter(lockId);
            q.executeQuery();

            return q;
        });

        SCHEDULEDFUTURES.remove(lockId);
    }

    @Override
    public void deleteLock(String lockId) {
        releaseLock(lockId);
    }

}
