/*
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
package com.netflix.conductor.postgres.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;

@ConfigurationProperties("conductor.postgres")
public class PostgresProperties {

    @Value("${conductor.outbox.table.enabled:false}")
    private boolean outboxEnabled;

    @Value("${spring.postgreslock.datasource.url}")
    private String dbUrl;

    @Value("${spring.postgreslock.datasource.username}")
    private String dbUsername;

    @Value("${spring.postgreslock.datasource.password}")
    private String dbPassword;

    @Value("${spring.postgreslock.datasource.leaseTime}")
    private long DEFAULT_LEASE_TIME;

    @Value("${spring.postgreslock.datasource.timeToTry}")
    private long DEFAULT_TIME_TO_TRY;

    /** The time in seconds after which the in-memory task definitions cache will be invalidated */
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration taskDefCacheRefreshInterval = Duration.ofSeconds(60);

    /** The time in seconds after which the queue details cache will be invalidated */
    @DurationUnit(ChronoUnit.MILLIS)
    private Duration queueDetailsCacheRefreshInterval = Duration.ofMillis(200);

    private boolean cachingEnabled = true;

    private Integer deadlockRetryMax = 3;

    public String schema = "public";

    public Duration getTaskDefCacheRefreshInterval() {
        return taskDefCacheRefreshInterval;
    }

    public void setTaskDefCacheRefreshInterval(Duration taskDefCacheRefreshInterval) {
        this.taskDefCacheRefreshInterval = taskDefCacheRefreshInterval;
    }

    public Integer getDeadlockRetryMax() {
        return deadlockRetryMax;
    }

    public void setDeadlockRetryMax(Integer deadlockRetryMax) {
        this.deadlockRetryMax = deadlockRetryMax;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Duration getQueueDetailsCacheRefreshInterval() {
        return queueDetailsCacheRefreshInterval;
    }

    public void setQueueDetailsCacheRefreshInterval(Duration queueDetailsCacheRefreshInterval) {
        this.queueDetailsCacheRefreshInterval = queueDetailsCacheRefreshInterval;
    }

    public boolean isCachingEnabled() {
        return cachingEnabled;
    }

    public void setCachingEnabled(boolean cachingEnabled) {
        this.cachingEnabled = cachingEnabled;
    }

    public boolean isOutboxEnabled() { return outboxEnabled; }

    public String getLockDbUrl() {
        return dbUrl;
    }

    public String getLockDbUsername() {
        return dbUsername;
    }

    public String getLockDbPassword() {
        return dbPassword;
    }

    public long getDefaultLeaseTime() {
        return DEFAULT_LEASE_TIME;
    }

    public long getDefaultTimeToTry() {
        return DEFAULT_TIME_TO_TRY;
    }
}
