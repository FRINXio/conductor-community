package com.netflix.conductor.postgres.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.annotations.VisibleForTesting;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.postgres.util.Query;
import org.springframework.retry.support.RetryTemplate;

import javax.sql.DataSource;
import java.util.Queue;
import java.util.concurrent.*;

public class PostgresLockDAO extends PostgresBaseDAO implements Lock {
    private static final ConcurrentHashMap<String, Queue<ScheduledFuture<?>>> SCHEDULEDFUTURES =
            new ConcurrentHashMap<>();
    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("PostgresLock-scheduler");
    private static final ThreadFactory THREAD_FACTORY =
            runnable -> new Thread(THREAD_GROUP, runnable);
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(1, THREAD_FACTORY);
    private static final long DEFAULT_LEASE_TIME = 5000;
    private static final long DEFAULT_TIME_TO_TRY = 500;

    public PostgresLockDAO(RetryTemplate retryTemplate, ObjectMapper objectMapper, DataSource dataSource){
        super(retryTemplate, objectMapper, dataSource);

        logger.debug(PostgresLockDAO.class.getName() + " is ready to serve");
    }

    @Override
    public void acquireLock(String lockId) {
        acquireLock(lockId, DEFAULT_TIME_TO_TRY, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, TimeUnit unit) {
        return acquireLock(lockId, timeToTry, DEFAULT_LEASE_TIME, unit);
    }

    @Override
    public boolean acquireLock(String lockId, long timeToTry, long leaseTime, TimeUnit unit) {
        final String TRY_ADVISORY_LOCK = "CALL acquire_advisory_lock(?, ?, ?)";
        int hashedLockId = hashStringToInt(lockId);

        Query query = queryWithTransaction(TRY_ADVISORY_LOCK, (q) -> {
            q.addParameter(hashedLockId);
            q.addParameter(timeToTry);
            q.addParameter(unit.toString());
            q.executeQuery();

            return q;
        });

        boolean lockAcquired = query.executeAndFetchFirst(Boolean.class);

        if (lockAcquired) {
            if (!SCHEDULEDFUTURES.containsKey(lockId)) {
                SCHEDULEDFUTURES.put(lockId, new ConcurrentLinkedQueue<>());
                SCHEDULEDFUTURES.get(lockId).add(SCHEDULER.schedule(() -> releaseLock(lockId), leaseTime, unit));
            } else {
                Queue<ScheduledFuture<?>> scheduledFutures = SCHEDULEDFUTURES.get(lockId);
                scheduledFutures.add(SCHEDULER.schedule(() -> releaseLock(lockId), leaseTime, unit));
            }
        }

        return lockAcquired;
    }

    @Override
    public void releaseLock(String lockId) {
        final String ADVISORY_UNLOCK = "SELECT pg_advisory_unlock (?)";
        int hashedLockId = hashStringToInt(lockId);

        queryWithTransaction(ADVISORY_UNLOCK, (q) -> {
            q.addParameter(hashedLockId);
            q.executeQuery();

            return q;
        });

        if (SCHEDULEDFUTURES.containsKey(lockId)) {
            Queue<ScheduledFuture<?>> scheduledFutures = SCHEDULEDFUTURES.get(lockId);
            ScheduledFuture<?> scheduledFuture;

            if ((scheduledFuture = scheduledFutures.poll()) != null) {
                scheduledFuture.cancel(false);
            }
        } else {
            logger.warn("Lock {} does not exist", lockId);
        }
    }

    @Override
    public void deleteLock(String lockId) {
        releaseLock(lockId);
    }

    private int hashStringToInt(String str) {
        int hash = 5381;
        int i = -1;
        while (i < str.length() - 1) {
            i += 1;
            hash = (hash * 33) ^ str.charAt(i);
        }
        return hash >>> 0;
    }

    @VisibleForTesting
    ConcurrentHashMap<String, Queue<ScheduledFuture<?>>> scheduledFutures() {
        return SCHEDULEDFUTURES;
    }
}
