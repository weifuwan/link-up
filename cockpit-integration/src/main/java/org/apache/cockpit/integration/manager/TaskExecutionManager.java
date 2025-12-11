package org.apache.cockpit.integration.manager;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskDefinitionPO;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.common.utils.DateTimeUtils;
import org.apache.cockpit.connectors.api.CockpitEngine;
import org.apache.cockpit.connectors.api.common.metrics.TaskInfo;
import org.apache.cockpit.integration.handler.TaskExecutionStatusCallbackHandler;
import org.apache.cockpit.integration.service.TaskDefinitionService;
import org.apache.cockpit.integration.service.TaskScheduleService;
import org.apache.cockpit.persistence.integration.TaskExecutionMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class TaskExecutionManager extends ServiceImpl<TaskExecutionMapper, TaskExecutionPO> {

    @Value("${integration.log.path:D:\\log}")
    private String logPath;

    @Resource
    private TaskDefinitionService taskDefinitionService;

    @Resource
    private TaskScheduleService taskScheduleService;

    @Resource
    private TaskExecutionStatusCallbackHandler statusCallbackHandler;

    private final ConcurrentHashMap<String, String> taskIdMapping = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskInfo> engineTaskInfoMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        recoverInterruptedTasks();
        log.info("TaskExecutionManager initialized");
    }

    @PreDestroy
    public void destroy() {
        log.info("TaskExecutionManager destroyed");
    }

    private void recoverInterruptedTasks() {
        try {
            LambdaQueryWrapper<TaskExecutionPO> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.in(TaskExecutionPO::getStatus,
                    TaskExecutionStatus.RUNNING, TaskExecutionStatus.STARTING);

            List<TaskExecutionPO> interruptedTasks = list(queryWrapper);
            for (TaskExecutionPO task : interruptedTasks) {
                task.setStatus(TaskExecutionStatus.FAILED);
                task.setEndTime(new Date());
                updateById(task);
                log.warn("Recovered interrupted task: {} -> FAILED", task.getId());
            }
        } catch (Exception e) {
            log.error("Error recovering interrupted tasks", e);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void execute(String definitionId, ExecutionMode executionMode) {
        TaskDefinitionPO taskDefinition = taskDefinitionService.getById(definitionId);
        if (taskDefinition == null) {
            throw new RuntimeException("Task definition does not exist");
        }

        TaskExecutionPO execution = createTaskExecution(taskDefinition);

        try {
            ConfigParseOptions options = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON);
            Config engineConfig = ConfigFactory.parseString(taskDefinition.getTaskParams(), options);

            CockpitEngine engine = CockpitEngine.getInstance();
            String folderPath = String.format("%s%s%s_%s", logPath, File.separator,
                    execution.getDefinitionId(), execution.getId());

            File dir = new File(folderPath);
            if (!dir.isDirectory()) {
                dir.mkdirs();
            }

            execution.setLogPath(folderPath + File.separator + generateLogDefaultFileName());

            String engineTaskId = engine.startAsync(engineConfig, execution.getId(),
                    execution, statusCallbackHandler);

            taskIdMapping.put(execution.getId(), engineTaskId);
            engineTaskInfoMap.put(execution.getId(), engine.getTaskInfo(engineTaskId));

            execution.setStatus(TaskExecutionStatus.RUNNING);
            execution.setEngineTaskId(engineTaskId);
            execution.setExecutionMode(executionMode);
            updateById(execution);

            log.info("Task execution started - executionId: {}, engineTaskId: {}, definition: {}",
                    execution.getId(), engineTaskId, definitionId);

        } catch (Exception e) {
            execution.setStatus(TaskExecutionStatus.FAILED);
            execution.setEndTime(new Date());
            updateById(execution);

            log.error("Task execution failed - executionId: {}, definition: {}",
                    execution.getId(), definitionId, e);
            throw new RuntimeException("Task execution failed: " + e.getMessage(), e);
        }
    }

    /**
     * Batch execute multiple task definitions
     *
     * @param definitionIds List of task definition IDs to execute
     * @param executionMode Execution mode
     */
    @Transactional(rollbackFor = Exception.class)
    public void batchExecute(List<String> definitionIds, ExecutionMode executionMode) {
        if (CollectionUtils.isEmpty(definitionIds)) {
            throw new RuntimeException("Task definition IDs cannot be empty");
        }

        log.info("Starting batch execution for {} task definitions", definitionIds.size());

        // Validate all task definitions exist before starting any execution
        validateAndGetTaskDefinitions(definitionIds);

        int successCount = 0;
        int failCount = 0;
        List<String> failedDefinitions = new ArrayList<>();

        for (String definitionId : definitionIds) {
            try {
                log.info("Executing task definition: {}", definitionId);
                execute(definitionId, executionMode);
                successCount++;
                log.info("Successfully started execution for task definition: {}", definitionId);
            } catch (Exception e) {
                failCount++;
                failedDefinitions.add(definitionId);
                log.error("Failed to execute task definition: {}, error: {}", definitionId, e.getMessage(), e);
            }
        }

        log.info("Batch execution completed - Success: {}, Failed: {}", successCount, failCount);

        if (failCount > 0) {
            throw new RuntimeException(String.format("Batch execution completed with %d failures. Failed definitions: %s",
                    failCount, failedDefinitions));
        }
    }

    /**
     * Validate task definitions and return the list of valid ones
     */
    /**
     * Validate task definitions and check if there are any running tasks
     * Throws exception if any task definition does not exist or has running tasks
     */
    private void validateAndGetTaskDefinitions(List<String> definitionIds) {
        List<TaskDefinitionPO> taskDefinitions = new ArrayList<>();
        List<String> invalidDefinitions = new ArrayList<>();
        List<String> runningTasks = new ArrayList<>();

        for (String definitionId : definitionIds) {
            TaskDefinitionPO taskDefinition = taskDefinitionService.getById(definitionId);
            if (taskDefinition == null) {
                invalidDefinitions.add(definitionId);
            } else {
                // Check if there are any running tasks for this definition
                LambdaQueryWrapper<TaskExecutionPO> queryWrapper = new LambdaQueryWrapper<>();
                queryWrapper.eq(TaskExecutionPO::getDefinitionId, definitionId)
                        .in(TaskExecutionPO::getStatus,
                                TaskExecutionStatus.RUNNING, TaskExecutionStatus.STARTING);

                long runningCount = count(queryWrapper);
                if (runningCount > 0) {
                    runningTasks.add(taskDefinition.getName() + "(" + definitionId + ")");
                } else {
                    taskDefinitions.add(taskDefinition);
                }
            }
        }

        if (!invalidDefinitions.isEmpty()) {
            throw new RuntimeException("The following task definitions do not exist: " + invalidDefinitions);
        }

        if (!runningTasks.isEmpty()) {
            throw new RuntimeException("The following tasks are currently running and cannot be started again: " + runningTasks);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean cancel(String executionId) {
        TaskExecutionPO execution = getById(executionId);
        if (execution == null) {
            throw new RuntimeException("Task execution record does not exist");
        }

        if (execution.getStatus() != TaskExecutionStatus.RUNNING &&
                execution.getStatus() != TaskExecutionStatus.STARTING) {
            log.warn("Task cannot be cancelled in current status: {}", execution.getStatus());
            return false;
        }

        try {
            CockpitEngine engine = CockpitEngine.getInstance();
            String engineTaskId = taskIdMapping.get(executionId);

            boolean cancelled = false;
            if (engineTaskId != null) {
                cancelled = engine.cancel(engineTaskId, true);
            }

            if (cancelled) {
                execution.setStatus(TaskExecutionStatus.CANCELLED);
                execution.setEndTime(new Date());
                updateById(execution);

                taskIdMapping.remove(executionId);
                engineTaskInfoMap.remove(executionId);

                log.info("Task execution cancelled - executionId: {}, engineTaskId: {}",
                        executionId, engineTaskId);
                return true;
            } else {
                log.warn("Failed to cancel task - executionId: {}, engineTaskId: {}",
                        executionId, engineTaskId);
                return false;
            }
        } catch (Exception e) {
            log.error("Error cancelling task - executionId: {}", executionId, e);
            throw new RuntimeException("Task cancellation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Batch cancel multiple task executions
     *
     * @param executionIds List of execution IDs to cancel
     * @return Batch cancellation result with success and failure counts
     */
    @Transactional(rollbackFor = Exception.class)
    public BatchCancelResult batchCancel(List<String> executionIds) {
        if (CollectionUtils.isEmpty(executionIds)) {
            throw new RuntimeException("Execution IDs cannot be empty");
        }

        log.info("Starting batch cancellation for {} task executions", executionIds.size());

        // Validate all execution records exist before starting any cancellation
        validateExecutionRecords(executionIds);

        int successCount = 0;
        int failCount = 0;
        List<String> failedExecutions = new ArrayList<>();

        for (String executionId : executionIds) {
            try {
                log.info("Cancelling task execution: {}", executionId);
                boolean cancelled = cancel(executionId);
                if (cancelled) {
                    successCount++;
                    log.info("Successfully cancelled task execution: {}", executionId);
                } else {
                    failCount++;
                    failedExecutions.add(executionId);
                    log.warn("Failed to cancel task execution: {}", executionId);
                }
            } catch (Exception e) {
                failCount++;
                failedExecutions.add(executionId);
                log.error("Error cancelling task execution: {}, error: {}", executionId, e.getMessage(), e);
            }
        }

        log.info("Batch cancellation completed - Success: {}, Failed: {}", successCount, failCount);

        return new BatchCancelResult(successCount, failCount, failedExecutions);
    }


    /**
     * Validate that all execution records exist and are in runnable state for cancellation
     */
    private void validateExecutionRecords(List<String> executionIds) {
        List<String> invalidExecutions = new ArrayList<>();
        List<String> notRunningExecutions = new ArrayList<>();

        for (String executionId : executionIds) {
            TaskExecutionPO execution = getById(executionId);
            if (execution == null) {
                invalidExecutions.add(executionId);
            } else {
                // Check if the execution is in a state that can be cancelled
                if (execution.getStatus() != TaskExecutionStatus.RUNNING &&
                        execution.getStatus() != TaskExecutionStatus.STARTING) {
                    notRunningExecutions.add(executionId + "(" + execution.getStatus() + ")");
                }
            }
        }

        if (!invalidExecutions.isEmpty()) {
            throw new RuntimeException("The following execution records do not exist: " + invalidExecutions);
        }

        if (!notRunningExecutions.isEmpty()) {
            throw new RuntimeException("The following executions are not in running state and cannot be cancelled: " + notRunningExecutions);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void cleanupCompletedExecutions(int days) {
        try {
            // Cleanup logic remains unchanged
            // ...
        } catch (Exception e) {
            log.error("Error cleaning up completed task executions", e);
            throw new RuntimeException("Failed to clean up task execution records: " + e.getMessage(), e);
        }
    }

    private TaskExecutionPO createTaskExecution(TaskDefinitionPO taskDefinitionPO) {
        TaskExecutionPO execution = new TaskExecutionPO();
        execution.setDefinitionId(taskDefinitionPO.getId());
        execution.setTaskName(taskDefinitionPO.getName());
        execution.setTaskParams(taskDefinitionPO.getTaskParams());
        execution.setStatus(TaskExecutionStatus.STARTING);
        execution.setStartTime(new Date());
        execution.setTaskExecuteType(taskDefinitionPO.getTaskExecuteType());
        execution.initInsert();
        save(execution);
        return execution;
    }

    private String generateLogDefaultFileName() {
        return "sync_" + DateTimeUtils.formatMillisDefault() + ".log";
    }

    public TaskInfo getEngineTaskInfo(String executionId) {
        return engineTaskInfoMap.get(executionId);
    }

    /**
     * Result class for batch cancellation operation
     */
    public static class BatchCancelResult {
        private final int successCount;
        private final int failCount;
        private final List<String> failedExecutions;

        public BatchCancelResult(int successCount, int failCount, List<String> failedExecutions) {
            this.successCount = successCount;
            this.failCount = failCount;
            this.failedExecutions = failedExecutions;
        }

        public int getSuccessCount() {
            return successCount;
        }

        public int getFailCount() {
            return failCount;
        }

        public List<String> getFailedExecutions() {
            return failedExecutions;
        }

        public boolean isCompleteSuccess() {
            return failCount == 0;
        }
    }
}