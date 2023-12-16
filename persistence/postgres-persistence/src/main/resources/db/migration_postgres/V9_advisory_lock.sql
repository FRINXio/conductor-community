CREATE OR REPLACE PROCEDURE acquire_advisory_lock(lock_id INTEGER, time_to_try INTEGER, unit VARCHAR DEFAULT 'SECOND')
LANGUAGE PLPGSQL
AS $$
BEGIN
  DECLARE
lock_acquired BOOLEAN;
    start_time TIMESTAMP;

BEGIN
        -- Set the start time to the current timestamp
        start_time := NOW();

        -- Attempt to acquire the advisory lock
        lock_acquired := pg_try_advisory_lock(lock_id);

        -- While the lock is not acquired and the timeout has not expired
        WHILE NOT lock_acquired AND start_time + INTERVAL time_to_try || ' ' || unit < NOW() LOOP
          -- Sleep for 0.5 seconds (500 milliseconds)
          PERFORM pg_sleep(0.5);

          -- Reattempt to acquire the lock
          lock_acquired := pg_try_advisory_lock(lock_id);
END LOOP;

        -- Return the lock acquired status
SELECT lock_acquired;
END;
END;
$$
