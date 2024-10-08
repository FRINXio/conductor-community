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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.postgres.dao.PostgresBaseDAO;
import io.orkes.conductor.dao.archive.ArchiveDAO;
import io.orkes.conductor.dao.archive.DocumentStoreDAO;
import io.orkes.conductor.dao.archive.ScrollableSearchResult;
import io.orkes.conductor.dao.archive.WorkflowModelSummary;
import io.orkes.conductor.dao.indexer.WorkflowIndex;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.retry.support.RetryTemplate;

@Slf4j
public class PostgresArchiveDAO extends PostgresBaseDAO implements ArchiveDAO, DocumentStoreDAO {

    private static final String GET_WORKFLOW =
            "SELECT json_data FROM archive.workflow_archive WHERE workflow_id = ? FOR SHARE SKIP LOCKED";

    private static final String GET_WORKFLOW_FALLBACK =
            "SELECT * FROM archive.workflow_archive WHERE workflow_id = ? FOR SHARE SKIP LOCKED";

    private static final String REMOVE_WORKFLOW =
            "DELETE FROM archive.workflow_archive WHERE workflow_id = ?";
    public static final String UNKNOWN = PostgresArchiveDAO.class.getName() + "_UNKNOWN";
    public static final String GET_WORKFLOW_FAMILY = "WITH RECURSIVE workflow_hierarchy AS (" +
            "   SELECT" +
            "       workflow_id, parent_workflow_id, json_data, status from archive.workflow_archive where workflow_id = ?" +
            "   UNION ALL" +
            "   SELECT child_wf.workflow_id workflow_id, child_wf.parent_workflow_id, child_wf.json_data, child_wf.status" +
            "   FROM archive.workflow_archive AS child_wf" +
            "      JOIN workflow_hierarchy parent_wf ON child_wf.parent_workflow_id = parent_wf.workflow_id" +
            ")\n" +
            "SELECT workflow_id, parent_workflow_id, json_data, status FROM workflow_hierarchy;";
    public static final String GET_WORKFLOW_FAMILY_SUMMARY = "WITH RECURSIVE workflow_hierarchy AS (" +
            "   SELECT" +
            "       workflow_id, parent_workflow_id, status," +
            "   CASE" +
            "       WHEN json_data LIKE '%\"completedWithErrors\":true%' THEN true" +
            "       WHEN json_data LIKE '%\"completedWithErrors\":false%' THEN false" +
            "       ELSE NULL"  +
            "   END AS completed_with_errors" +
            "   from archive.workflow_archive where workflow_id = ?" +
            "   UNION ALL" +
            "   SELECT child_wf.workflow_id workflow_id, child_wf.parent_workflow_id, child_wf.status," +
            "   CASE" +
            "       WHEN child_wf.json_data LIKE '%\"completedWithErrors\":true%' THEN true" +
            "       WHEN child_wf.json_data LIKE '%\"completedWithErrors\":false%' THEN false" +
            "       ELSE null" +
            "   END AS completed_with_errors" +
            "   FROM archive.workflow_archive AS child_wf" +
            "      JOIN workflow_hierarchy parent_wf ON child_wf.parent_workflow_id = parent_wf.workflow_id" +
            ")\n" +
            "SELECT workflow_id, parent_workflow_id, status, completed_with_errors FROM workflow_hierarchy;";
    public static final String GET_WORKFLOW_PATH = "WITH RECURSIVE workflow_path AS (" +
            "   SELECT" +
            "       workflow_id, parent_workflow_id from archive.workflow_archive child_wf where workflow_id = ?" +
            "   UNION ALL" +
            "   SELECT parent_wf.workflow_id workflow_id, parent_wf.parent_workflow_id" +
            "   FROM archive.workflow_archive AS parent_wf" +
            "      JOIN workflow_path child_wf ON child_wf.parent_workflow_id = parent_wf.workflow_id" +
            ")\n" +
            "SELECT workflow_id, parent_workflow_id FROM workflow_path;";

    private final DataSource searchDatasource;

    public PostgresArchiveDAO(
            ObjectMapper objectMapper,
            @Qualifier("searchDatasource") DataSource searchDatasource) {
        super(RetryTemplate.defaultInstance(), objectMapper, searchDatasource);
        this.searchDatasource = searchDatasource;

        log.info("Using {} as search datasource", searchDatasource);

        try (Connection conn = searchDatasource.getConnection()) {
            log.info("Using {} as search datasource", conn.getMetaData().getURL());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void createOrUpdateWorkflow(WorkflowModel workflow) {

        String INSERT_OR_UPDATE_LATEST =
                "INSERT INTO "
                        + "archive.workflow_archive"
                        + " as wf"
                        + "(workflow_id, created_on, modified_on, correlation_id, workflow_name, status, index_data, created_by, json_data, parent_workflow_id, rbac_labels) "
                        + "VALUES "
                        + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  "
                        + "ON CONFLICT (workflow_id) DO "
                        + "UPDATE SET modified_on = ?, status = ?, index_data = ?, json_data = ? "
                        + "WHERE wf.modified_on < ? ;";

        try (Connection connection = searchDatasource.getConnection()) {
            connection.setAutoCommit(true);
            PreparedStatement statement = connection.prepareStatement(INSERT_OR_UPDATE_LATEST);

            WorkflowIndex index = new WorkflowIndex(workflow, 200, 50);
            Collection<String> indexData = index.toIndexWords();

            int indx = 1;
            long updatedTime = workflow.getUpdatedTime() == null ? 0 : workflow.getUpdatedTime();

            // Insert values

            statement.setString(indx++, workflow.getWorkflowId());
            statement.setLong(indx++, workflow.getCreateTime());
            statement.setLong(indx++, updatedTime);
            statement.setString(indx++, workflow.getCorrelationId());
            statement.setString(indx++, workflow.getWorkflowName());
            statement.setString(indx++, workflow.getStatus().toString());
            statement.setArray(
                    indx++, connection.createArrayOf("text", indexData.toArray(new String[0])));
            statement.setString(indx++, workflow.getCreatedBy());

            String workflowJson = null;
            if (workflow.getStatus().isTerminal()) {
                workflowJson = objectMapper.writeValueAsString(workflow);
            }
            statement.setString(indx++, workflowJson);
            statement.setString(indx++, workflow.getParentWorkflowId() == null ? null : workflow.getParentWorkflowId());
            statement.setArray(indx++, connection.createArrayOf(
                    "text", getLabels(workflow.getWorkflowDefinition().getDescription())));

            // Update values
            statement.setLong(indx++, updatedTime);
            statement.setString(indx++, workflow.getStatus().toString());
            statement.setArray(
                    indx++, connection.createArrayOf("text", indexData.toArray(new String[0])));
            statement.setString(indx++, workflowJson);
            statement.setLong(indx, updatedTime);

            statement.executeUpdate();

        } catch (Exception e) {
            log.error(
                    "Error updating workflow {} - {}", workflow.getWorkflowId(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean removeWorkflow(String workflowId) {
        boolean removed = false;
        WorkflowModel workflow = this.getWorkflow(workflowId, true);
        if (workflow != null) {
            withTransaction(
                    connection ->
                            execute(
                                    connection,
                                    REMOVE_WORKFLOW,
                                    q -> q.addParameter(workflowId).executeDelete()));
            removed = true;
        }
        return removed;
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW);
            statement.setString(1, workflowId);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                byte[] json = rs.getBytes("json_data");
                if (json == null || json.length == 0) {
                    return getWorkflowFallback(connection, workflowId);
                }
                var data = objectMapper.readValue(json, WorkflowModel.class);
                if (!includeTasks) {
                    data.setTasks(Collections.emptyList());
                }
                return data;
            }
        } catch (Exception e) {
            log.error("Error reading workflow - " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Create artificial workflow object from the metadata stored in archive. This is a fallback in
     * case json_data is unavailable. This workflow only contains the very basic parameters. No
     * tasks, inputs, outputs etc.
     */
    private WorkflowModel getWorkflowFallback(Connection connection, String workflowId)
            throws Exception {
        PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_FALLBACK);
        statement.setString(1, workflowId);
        ResultSet rs = statement.executeQuery();
        if (rs.next()) {
            final WorkflowDef workflowDefinition = new WorkflowDef();
            workflowDefinition.setName(rs.getString("workflow_name"));

            final WorkflowModel workflowModel = new WorkflowModel();
            workflowModel.setWorkflowId(workflowId);
            workflowModel.setParentWorkflowId(rs.getString("parent_workflow_id"));
            workflowModel.setWorkflowDefinition(workflowDefinition);
            workflowModel.setCorrelationId(rs.getString("correlation_id"));
            workflowModel.setStatus(WorkflowModel.Status.valueOf(rs.getString("status")));
            workflowModel.setCreateTime(rs.getLong("created_on"));
            workflowModel.setCreatedBy(rs.getString("created_by"));
            return workflowModel;
        }

        return null;
    }


    @Override
    public List<String> getWorkflowPath(String workflowId) {
        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(GET_WORKFLOW_PATH);
            statement.setString(1, workflowId);
            ResultSet rs = statement.executeQuery();
            List<String> results = new LinkedList<>();
            String parentWfId = null;
            while (rs.next()) {
                String wfId = rs.getString("workflow_id");
                parentWfId = rs.getString("parent_workflow_id");
                results.add(wfId);
            }

            if (!com.google.common.base.Strings.isNullOrEmpty(parentWfId)) {
                // couldn't find complete path
                results.add(UNKNOWN);
            }

            Collections.reverse(results);
            return results;

        } catch (Exception e) {
            log.error("Error reading workflow - " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getWorkflowIdsByType(String workflowName, Long startTime, Long endTime) {
        String query =
                "workflowType IN ("
                        + workflowName
                        + ") AND startTime>"
                        + startTime
                        + " AND startTime< "
                        + endTime;
        ScrollableSearchResult<String> result = searchWorkflows(query, "*", 0, 100_000);
        return result.getResults();
    }

    @Override
    public List<String> getWorkflowIdsByCorrelationId(
            String workflowName,
            String correlationId,
            boolean includeClosed,
            boolean includeTasks) {
        String query =
                "workflowType = '"
                        + workflowName.trim()
                        + "' AND correlationId = '"
                        + correlationId.trim()
                        + "'";
        if (!includeClosed) {
            query += " AND status IN (" + WorkflowModel.Status.RUNNING + ")";
        }
        ScrollableSearchResult<String> result = searchWorkflows(query, null, 0, 100_000);
        return result.getResults();
    }

    // Search
    public ScrollableSearchResult<String> searchWorkflows(
            String query, String freeText, int start, int count) {

        if (query == null) query = "";
        if (freeText == null) freeText = "";

        log.debug(
                "search. query = {}, fulltext={}, limit={}, start= {}",
                query,
                freeText,
                count,
                start);

        SearchQuery parsedQuery = SearchQuery.parse(query);
        SearchResult<String> results =
                search("archive.workflow_archive", parsedQuery, freeText.trim(), count, start);
        ScrollableSearchResult<String> scrollableSearchResult = new ScrollableSearchResult<>();
        scrollableSearchResult.setResults(results.getResults());
        return scrollableSearchResult;
    }

    private SearchResult<String> search(
            String tableName, SearchQuery query, String freeText, int limit, int start) {

        List<String> workflowNames = query.getWorkflowNames();
        List<String> workflowIds = query.getWorkflowIds();
        List<String> correlationIds = query.getCorrelationIds();
        List<String> statuses = query.getStatuses();
        long startTime = query.getFromStartTime();
        long endTime = query.getToStartTime();
        if (endTime == 0) {
            endTime = System.currentTimeMillis();
        }

        // Task specific
        List<String> taskIds = query.getTaskIds();
        List<String> taskTypes = query.getTaskTypes();

        String WHERE_CLAUSE = "from  " + tableName;

        WHERE_CLAUSE += " WHERE 1=1 ";

        String SELECT_QUERY = "select  workflow_id, created_on ";

        String JOINER = " AND ";
        if (workflowNames != null && !workflowNames.isEmpty()) {
            WHERE_CLAUSE += JOINER + "workflow_name LIKE ANY (?) ";
        }

        if (taskTypes != null && !taskTypes.isEmpty()) {
            WHERE_CLAUSE += JOINER + "task_type = ANY (?) ";
        }

        if (workflowIds != null && !workflowIds.isEmpty()) {
            WHERE_CLAUSE += JOINER + "workflow_id = ANY (?) ";
            JOINER = " AND ";
        }

        if (taskIds != null && !taskIds.isEmpty()) {
            WHERE_CLAUSE += JOINER + "task_id = ANY (?) ";
            JOINER = " AND ";
        }

        if (statuses != null && !statuses.isEmpty()) {
            WHERE_CLAUSE += JOINER + "status = ANY (?) ";
            JOINER = " AND ";
        }

        if (correlationIds != null && !correlationIds.isEmpty()) {
            WHERE_CLAUSE += JOINER + "correlation_id = ANY (?) ";
            JOINER = " AND ";
        }

        if (startTime > 0) {
            WHERE_CLAUSE += JOINER + "created_on BETWEEN ? AND ? ";
            JOINER = " AND ";
        }

        if (Strings.isNotBlank(freeText) && !"*".equals(freeText)) {
            WHERE_CLAUSE += JOINER + " index_data @> ? ";
        }

        String SEARCH_QUERY =
                SELECT_QUERY
                        + " "
                        + WHERE_CLAUSE
                        + " order by created_on desc limit "
                        + limit
                        + " offset "
                        + start;
        log.debug(SEARCH_QUERY);

        SearchResult<String> result = new SearchResult<>();
        result.setResults(new ArrayList<>());

        try (Connection conn = searchDatasource.getConnection()) {
            PreparedStatement pstmt = conn.prepareStatement(SEARCH_QUERY);
            int indx = 1;
            if (workflowNames != null && !workflowNames.isEmpty()) {
                pstmt.setArray(
                        indx++,
                        conn.createArrayOf("VARCHAR", workflowNames.toArray(new String[0])));
            }
            if (taskTypes != null && !taskTypes.isEmpty()) {
                pstmt.setArray(
                        indx++, conn.createArrayOf("VARCHAR", taskTypes.toArray(new String[0])));
            }

            if (workflowIds != null && !workflowIds.isEmpty()) {
                pstmt.setArray(
                        indx++, conn.createArrayOf("VARCHAR", workflowIds.toArray(new String[0])));
            }

            if (taskIds != null && !taskIds.isEmpty()) {
                pstmt.setArray(
                        indx++, conn.createArrayOf("VARCHAR", taskIds.toArray(new String[0])));
            }

            if (statuses != null && !statuses.isEmpty()) {
                pstmt.setArray(
                        indx++, conn.createArrayOf("VARCHAR", statuses.toArray(new String[0])));
            }

            if (correlationIds != null && !correlationIds.isEmpty()) {
                pstmt.setArray(
                        indx++,
                        conn.createArrayOf("VARCHAR", correlationIds.toArray(new String[0])));
            }

            if (startTime > 0) {
                pstmt.setLong(indx++, startTime);
                pstmt.setLong(indx++, endTime);
            }

            if (Strings.isNotBlank(freeText) && !"*".equals(freeText)) {
                String[] textArray = freeText.toLowerCase().split(" ");
                pstmt.setArray(indx++, conn.createArrayOf("text", textArray));
            }

            result.setTotalHits(0);
            long countStart = System.currentTimeMillis();
            ResultSet rs = pstmt.executeQuery();
            log.debug(
                    "search query took {} ms to execute",
                    (System.currentTimeMillis() - countStart));
            while (rs.next()) {
                String workflowId = rs.getString("workflow_id");
                result.getResults().add(workflowId);
            }

        } catch (SQLException sqlException) {
            log.error(sqlException.getMessage(), sqlException);
            throw new RuntimeException(sqlException);
        }

        return result;
    }

    // Private Methods
    @Override
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        String GET_TASK = "SELECT seq, log, created_on FROM archive.task_logs WHERE task_id = ? order by seq";
        return queryWithTransaction(
                GET_TASK,
                q -> {
                    List<TaskExecLog> taskExecLogs =
                            q.addParameter(taskId)
                                    .executeAndFetch(
                                            resultSet -> {
                                                List<TaskExecLog> logs = new ArrayList<>();
                                                while (resultSet.next()) {
                                                    TaskExecLog log = new TaskExecLog();
                                                    log.setTaskId(taskId);
                                                    log.setLog(resultSet.getString(2));
                                                    log.setCreatedTime(resultSet.getLong(3));
                                                    logs.add(log);
                                                }
                                                return logs;
                                            });
                    return taskExecLogs;
                });
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> logs) {
        String INSERT_STMT = "INSERT INTO archive.task_logs (task_id, log, created_on) values(?,?,?)";
        for (TaskExecLog taskExecLog : logs) {
            withTransaction(
                    tx -> {
                        execute(
                                tx,
                                INSERT_STMT,
                                q ->
                                        q

                                                // Insert values
                                                .addParameter(taskExecLog.getTaskId())
                                                .addParameter(taskExecLog.getLog())
                                                .addParameter(taskExecLog.getCreatedTime())

                                                // execute
                                                .executeUpdate());
                    });
        }
    }

    @Override
    public List<WorkflowModel> getWorkflowFamily(String workflowId, boolean summaryOnly) {
        String select = summaryOnly ? GET_WORKFLOW_FAMILY_SUMMARY : GET_WORKFLOW_FAMILY;
        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(select);
            statement.setString(1, workflowId);
            ResultSet rs = statement.executeQuery();
            List<WorkflowModel> results = new ArrayList<>();
            while (rs.next()) {
                String wfId = rs.getString("workflow_id");
                String parentWfId = rs.getString("parent_workflow_id");
                String status = rs.getString("status");
                String json = summaryOnly ? "" : rs.getString("json_data");
                Boolean completedWithErrors = false;
                if (com.google.common.base.Strings.isNullOrEmpty(json)) {
                    if (summaryOnly) {
                        completedWithErrors = rs.getBoolean("completed_with_errors");
                        if (rs.wasNull()) {
                            completedWithErrors = null;
                        }
                    }
                    results.add(new WorkflowModelSummary(wfId, parentWfId, status,  completedWithErrors));
                } else {
                    results.add(objectMapper.readValue(json, WorkflowModel.class));
                }
            }

            Collections.reverse(results);
            return results;

        } catch (Exception e) {
            log.error("Error reading workflow - " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private String[] getLabels(String description) {
        if (description == null || description.isEmpty()) {
            return new String[0];
        }
        try {
            JsonNode jsonDescription = objectMapper.readTree(description);
            JsonNode labels = jsonDescription.get("labels");

            if (labels != null && labels.isArray()) {
                String[] labelsArray = new String[labels.size()];
                for (int i = 0; i < labels.size(); i++) {
                    labelsArray[i] = labels.get(i).asText();
                }
                return labelsArray;
            }
        } catch (JsonProcessingException e) {
            log.error("Unable to get labels from {}", description, e);

        }
        return new String[0];
    }

    @Override
    public List<String> getUserWorkflowIds(List<String> roles) {
        final String GET_USER_WORKFLOWS = "SELECT " +
                "workflow_id FROM archive.workflow_archive " +
                "WHERE rbac_labels && ARRAY[?]::text[];";

        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(GET_USER_WORKFLOWS);
            String[] labels = roles.toArray(new String[0]);
            statement.setArray(1, connection.createArrayOf("text", labels));
            ResultSet rs = statement.executeQuery();
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            return ids;
        } catch (SQLException e) {
            log.error("Unable to get workflow ids accessible by user", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public SearchResult<String> getSearchResultIds(List<String> roles) {
        SearchResult<String> result = new SearchResult<>();
        List<String> userIds = getUserWorkflowIds(roles);
        result.setResults(userIds);
        return result;
    }

    @Override
    public boolean exists(Object[] args) {
        String wfId = (String) args[0];

        final String WF_EXIST = "SELECT EXISTS (" +
                "SELECT 1 FROM archive.workflow_archive " +
                "WHERE workflow_id=?) " +
                "AS exist;";

        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(WF_EXIST);
            statement.setString(1, wfId);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            log.error("Error checking existence  of workflow {}. {}", wfId, e.getMessage());
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public boolean hasAccess(Object[] args, List<String> roles) {
        String id = (String) args[0];
        final String IS_VALID_FAMILY_ID = "SELECT EXISTS (" +
                "SELECT 1 FROM archive.workflow_archive " +
                "WHERE workflow_id=? AND rbac_labels && ARRAY[?]::text[]) " +
                "AS exist;";

        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(IS_VALID_FAMILY_ID);
            statement.setString(1, id);
            String[] labels = roles.toArray(new String[0]);
            statement.setArray(2, connection.createArrayOf("text", labels));
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            log.error("Error checking access of workflow {}. {}", id, e.getMessage());
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public List<String> getPresentIds(List<String> ids) {
        final String GET_PRESENT_IDS = "SELECT workflow_id " +
                "FROM archive.workflow_archive " +
                "WHERE workflow_id = ANY(?);";
        try (Connection connection = searchDatasource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(GET_PRESENT_IDS);
            String[] passedIds = ids.toArray(new String[0]);
            statement.setArray(1, connection.createArrayOf("text", passedIds));
            ResultSet rs = statement.executeQuery();
            List<String> presentIds = new ArrayList<>();
            while (rs.next()) {
                presentIds.add(rs.getString(1));
            }
            return presentIds;
        } catch (SQLException e) {
            log.error("Error getting workflow ids. {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}