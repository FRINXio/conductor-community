/*
 * Copyright 2023 Netflix, Inc.
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
package com.netflix.conductor.postgres.config.outbox;

import java.sql.Connection;
import java.util.List;
import javax.sql.DataSource;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.exception.NonTransientException;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.postgres.dao.PostgresBaseDAO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class OutboxExecutionDAO extends PostgresBaseDAO implements ExecutionDAO, RateLimitingDAO,
        PollDataDAO, ConcurrentExecutionLimitDAO {

    private final ExecutionDAO delegate;
    private final ObjectMapper objectMapper;
    private final String TOPIC_ID = "workflow.status";
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxExecutionDAO.class);
    private final String INSERT_DATA = "INSERT INTO outbox_table "
            + "(ID, aggregateType, aggregateId, payload, eventType) "
            + "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";

    public OutboxExecutionDAO(
            RetryTemplate retryTemplate,
            ObjectMapper objectMapper,
            DataSource dataSource,
            ExecutionDAO executionDAO) {
        super(retryTemplate, objectMapper, dataSource);
        this.delegate = executionDAO;
        this.objectMapper = objectMapper;
        LOGGER.info("Outbox service initialized");
    }

    @Override
    public List<TaskModel> getPendingTasksByWorkflow(String taskName, String workflowId) {
        return delegate.getPendingTasksByWorkflow(taskName, workflowId);
    }

    @Override
    public List<TaskModel> getTasks(String taskType, String startKey, int count) {
        return delegate.getTasks(taskType, startKey, count);
    }

    @Override
    public List<TaskModel> createTasks(List<TaskModel> tasks) { return delegate.createTasks(tasks);}

    @Override
    public void updateTask(TaskModel task) {
        updateTasksOutboxTable(task);
        delegate.updateTask(task);
    }

    @Override
    @Deprecated
    public boolean exceedsInProgressLimit(TaskModel task) {
        return delegate.exceedsInProgressLimit(task);
    }

    @Override
    public boolean removeTask(String taskId) {
        return delegate.removeTask(taskId);
    }

    @Override
    public TaskModel getTask(String taskId) {
        return delegate.getTask(taskId);
    }

    @Override
    public List<TaskModel> getTasks(List<String> taskIds) {
        return delegate.getTasks(taskIds);
    }

    @Override
    public List<TaskModel> getPendingTasksForTaskType(String taskType) {
        return delegate.getPendingTasksForTaskType(taskType);
    }

    @Override
    public List<TaskModel> getTasksForWorkflow(String workflowId) {
        return delegate.getTasksForWorkflow(workflowId);
    }

    @Override
    public String createWorkflow(WorkflowModel workflow) {
        return delegate.createWorkflow(workflow);
    }

    @Override
    public String updateWorkflow(WorkflowModel workflow) {
        String workflowId = delegate.updateWorkflow(workflow);
        updateWorkflowOutboxTable(workflow);
        return workflowId;
    }

    private void updateWorkflowOutboxTable(WorkflowModel workflow) {
        Preconditions.checkNotNull(workflow, "workflow object cannot be null");
        boolean terminal = workflow.getStatus().isTerminal();

        try {
            withTransaction(
                    tx -> {
                        if (terminal) {
                            writeWorkflowToOutbox(tx, workflow);
                        }
                    });
        } catch (NonTransientException e) {
            LOGGER.error("Writing workflow with id: {} to outbox table failed", workflow.getWorkflowId(), e);
        }
    }

    private void updateTasksOutboxTable(TaskModel task) {
        boolean terminal = task.getStatus().isTerminal();

        try {
            withTransaction(
                    connection -> {
                        if (terminal) {
                            validateTask(task);
                            writeTaskToOutbox(connection, task);
                        }
                    });
        } catch (NonTransientException e) {
            LOGGER.error("Writing task with id: {} to outbox table failed", task.getTaskId(), e);
        }
    }

    /**
     * Writes task data into outbox table:
     * @see #INSERT_DATA contains columns in outbox table to be filled
     * ID - task ID
     * aggregateType - part of the topic name
     * aggregateId - workflow id of specific task later concatenated with topic name
     * payload - json containing workflow id, task id and status of the task
     * evenType - task/workflow update state differentiator
     * Debezium connect tracks changes in outbox table and reuses them
     * for generating kafka messages.
     * Data are afterward removed from db in the same transaction.
     */
    private void writeTaskToOutbox(Connection connection, TaskModel task) {
        final String status = task.getStatus().toString();
        final var payload = taskPayload(task.getWorkflowInstanceId(), task.getTaskId(), status);

        execute(
                connection,
                INSERT_DATA,
                query ->
                        query.addParameter(task.getTaskId())
                                .addParameter(TOPIC_ID)
                                .addParameter(task.getWorkflowInstanceId())
                                .addJsonParameter(payload)
                                .addParameter("taskStatusUpdate")
                                .executeUpdate());
        LOGGER.info("Task with id: {} written in the outbox table.", task.getTaskId());
        removeEntityFromOutbox(connection, task.getTaskId());
    }

    private void removeEntityFromOutbox(Connection connection, String id) {
        final String REMOVE_TASK = "DELETE FROM outbox_table WHERE ID = ?";
        execute(connection, REMOVE_TASK, q -> q.addParameter(id).executeDelete());
        LOGGER.info("Entity with id: {} removed from outbox table.", id);
    }

    private ObjectNode taskPayload(String wfId, String taskId, String status) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("taskId", taskId);
        payload.put("workflowId", wfId);
        payload.put("status", status);
        return payload;
    }

    /**
     * Same as in case of
     * {@link #writeTaskToOutbox(Connection, TaskModel)}
     * with different parameters
     */
    private void writeWorkflowToOutbox(Connection connection, WorkflowModel workflow) {
        final String status = workflow.getStatus().toString();
        final var payload = workflowPayload(workflow.getWorkflowId(), workflow.getParentWorkflowId(),
                status, workflow.getWorkflowDefinition().toString());

        execute(
                connection,
                INSERT_DATA,
                query ->
                        query.addParameter(workflow.getWorkflowId())
                                .addParameter(TOPIC_ID)
                                .addParameter(workflow.getWorkflowId())
                                .addJsonParameter(payload)
                                .addParameter("workflowStatusUpdate")
                                .executeUpdate());
        LOGGER.info("Workflow with id: {} written in the outbox table.", workflow.getWorkflowId());
        removeEntityFromOutbox(connection, workflow.getWorkflowId());
    }

    private ObjectNode workflowPayload(String wfId, String parentId,
                                       String status, String workflowType) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("workflowId", wfId);
        payload.put("parentId", parentId);
        payload.put("status", status);
        payload.put("workflowType", workflowType);
        return payload;
    }

    @Override
    public boolean removeWorkflow(String workflowId) {
        return delegate.removeWorkflow(workflowId);
    }

    @Override
    public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
        return delegate.removeWorkflowWithExpiry(workflowId, ttlSeconds);
    }

    @Override
    public void removeFromPendingWorkflow(String workflowType, String workflowId) {
        delegate.removeFromPendingWorkflow(workflowType, workflowId);
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId) {
        return delegate.getWorkflow(workflowId);
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
        return delegate.getWorkflow(workflowId, includeTasks);
    }

    @Override
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        return delegate.getRunningWorkflowIds(workflowName, version);
    }

    @Override
    public List<WorkflowModel> getPendingWorkflowsByType(String workflowName, int version) {
        return delegate.getPendingWorkflowsByType(workflowName, version);
    }

    @Override
    public long getPendingWorkflowCount(String workflowName) {
        return delegate.getPendingWorkflowCount(workflowName);
    }

    @Override
    public long getInProgressTaskCount(String taskDefName) {
        return delegate.getInProgressTaskCount(taskDefName);
    }

    @Override
    public List<WorkflowModel> getWorkflowsByType(
            String workflowName, Long startTime, Long endTime) {
        return delegate.getWorkflowsByType(workflowName, startTime, endTime);
    }

    @Override
    public List<WorkflowModel> getWorkflowsByCorrelationId(
            String workflowName, String correlationId, boolean includeTasks) {
        return delegate.getWorkflowsByCorrelationId(workflowName, correlationId, includeTasks);
    }

    @Override
    public boolean canSearchAcrossWorkflows() {
        return delegate.canSearchAcrossWorkflows();
    }

    @Override
    public boolean addEventExecution(EventExecution eventExecution) {
        return delegate.addEventExecution(eventExecution);
    }

    @Override
    public void updateEventExecution(EventExecution eventExecution) {
        delegate.updateEventExecution(eventExecution);
    }

    @Override
    public void removeEventExecution(EventExecution eventExecution) {
        delegate.removeEventExecution(eventExecution);
    }

    @Override
    public boolean exceedsRateLimitPerFrequency(TaskModel task, TaskDef taskDef) {
        return ((RateLimitingDAO) delegate).exceedsRateLimitPerFrequency(task, taskDef);
    }

    @Override
    public void addTaskToLimit(TaskModel task) {
        ((ConcurrentExecutionLimitDAO) delegate).addTaskToLimit(task);
    }

    @Override
    public void removeTaskFromLimit(TaskModel task) {
        ((ConcurrentExecutionLimitDAO) delegate).removeTaskFromLimit(task);
    }

    @Override
    public boolean exceedsLimit(TaskModel task) {
        return ((ConcurrentExecutionLimitDAO) delegate).exceedsLimit(task);
    }

    @Override
    public void updateLastPollData(String taskDefName, String domain, String workerId) {
        ((PollDataDAO) delegate).updateLastPollData(taskDefName, domain, workerId);
    }

    @Override
    public PollData getPollData(String taskDefName, String domain) {
        return ((PollDataDAO) delegate).getPollData(taskDefName, domain);
    }

    @Override
    public List<PollData> getPollData(String taskDefName) {
        return ((PollDataDAO) delegate).getPollData(taskDefName);
    }

    @Override
    public List<PollData> getAllPollData() {
        return ((PollDataDAO) delegate).getAllPollData();
    }

    private void validateTask(TaskModel task) {
        Preconditions.checkNotNull(task, "Task object cannot be null");
        Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
        Preconditions.checkNotNull(task.getReferenceTaskName(), "Task reference name cannot be null");
    }
}