package org.apache.cockpit.integration.service;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskExecutionVO;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.connectors.api.common.metrics.TaskInfo;

import java.util.List;
import java.util.Map;

/**
 * Task Execution Service Interface
 * Responsible for managing the entire lifecycle of task execution, including start, cancel, status query, etc.
 */
public interface TaskExecutionService extends IService<TaskExecutionPO> {

    /**
     * Execute specified task definition
     * Create execution record and start task execution based on task definition ID
     *
     * @param definitionId  Task definition ID
     * @param executionMode Execution mode
     */
    void execute(String definitionId, ExecutionMode executionMode);

    void batchExecute(List<String> definitionIds, ExecutionMode executionMode);


    /**
     * Cancel ongoing task execution
     * Attempt to cancel the task corresponding to the specified execution ID.
     * Only tasks in RUNNING or STARTING status can be cancelled.
     *
     * @param executionId Task execution record ID
     * @return true - cancellation successful, false - cancellation failed or task cannot be cancelled
     * @throws RuntimeException Thrown when execution record does not exist or error occurs during cancellation
     */
    boolean cancel(String executionId);

    void batchExecute(List<String> definitionIds);

    /**
     * Get task execution status
     * Query the current status of the task corresponding to the specified execution ID
     *
     * @param executionId Task execution record ID
     * @return Task execution status, returns null if execution record does not exist
     */
    TaskExecutionStatus getStatus(String executionId);

    /**
     * Get engine task detailed information
     * Get detailed information of the underlying engine execution task, including running metrics, progress, etc.
     *
     * @param executionId Task execution record ID
     * @return Engine task information object, returns null if task does not exist or is not associated with engine task
     */
    TaskInfo getEngineTaskInfo(String executionId);

    /**
     * Clean up completed task execution records
     * Clean up completed (successful, failed, cancelled) task execution records from specified days ago.
     * Used for historical data cleanup.
     *
     * @param days Retention days, completed records older than these days will be cleaned up
     * @throws RuntimeException Thrown when database error occurs during cleanup process
     */
    void cleanupCompletedExecutions(int days);

    /**
     * Query execution records by task definition
     * Get all execution records for the specified task definition, sorted by start time in descending order
     *
     * @param definitionId Task definition ID
     * @return List of task execution records, sorted from newest to oldest by start time
     */
    List<TaskExecutionPO> getExecutionsByDefinition(String definitionId);

    /**
     * Get metrics summary for tasks
     *
     * @param timeRange Time range for metrics
     * @param taskType  Type of task
     * @return Map containing metrics summary data
     */
    Map<String, Object> getMetricsSummary(String timeRange, String taskType);

    /**
     * Get synchronization trend data
     *
     * @param timeRange Time range for trend analysis
     * @return Map containing synchronization trend data
     */
    Map<String, Object> getSyncTrend(String timeRange);

    /**
     * Get execution information by definition ID
     *
     * @param definitionId Task definition ID
     * @return List of task execution VO objects
     */
    List<TaskExecutionVO> getExecutionInfoByDefinitionId(String definitionId);

    /**
     * Get task execution by ID
     *
     * @param id Task execution ID
     * @return Task execution VO object
     */
    TaskExecutionVO selectById(String id);

    /**
     * Get task execution log
     *
     * @param id Task execution ID
     * @return Task execution log content
     */
    String getTaskLog(String id);

}