package org.apache.cockpit.controller.integration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.result.Result;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.bean.vo.integration.TaskExecutionVO;
import org.apache.cockpit.common.enums.integration.ExecutionMode;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.connectors.api.common.metrics.TaskInfo;
import org.apache.cockpit.integration.manager.TaskExecutionManager;
import org.apache.cockpit.integration.service.TaskExecutionService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/task-execution")
@Api(tags = "Task Execution Management")
public class TaskExecutionController {

    @Resource
    private TaskExecutionService taskExecutionService;

    @Resource
    private TaskExecutionManager taskExecutionManager;

    @PostMapping("/batch-execute")
    @ApiOperation("Batch Execute Tasks")
    public Result<String> batchExecuteTasks(
            @ApiParam(value = "Task Definition IDs", required = true)
            @RequestBody List<String> definitionIds) {
        try {
            if (definitionIds == null || definitionIds.isEmpty()) {
                return Result.buildFailure("Task definition IDs cannot be empty");
            }
            taskExecutionManager.batchExecute(definitionIds, ExecutionMode.MANUAL);
            return Result.buildSuc("Batch task execution started successfully");
        } catch (Exception e) {
            log.error("Batch execute tasks failed - definitionIds: {}", definitionIds, e);
            return Result.buildFailure("Batch task execution failed: " + e.getMessage());
        }
    }

    @PostMapping("/batch-cancel")
    @ApiOperation("Batch Cancel Task Executions")
    public Result<TaskExecutionManager.BatchCancelResult> batchCancelExecutions(
            @ApiParam(value = "Task Execution IDs", required = true)
            @RequestBody List<String> executionIds) {
        try {
            if (executionIds == null || executionIds.isEmpty()) {
                return Result.buildFailure("Task execution IDs cannot be empty");
            }
            TaskExecutionManager.BatchCancelResult result = taskExecutionManager.batchCancel(executionIds);
            return Result.buildSuc(result);
        } catch (Exception e) {
            log.error("Batch cancel executions failed - executionIds: {}", executionIds, e);
            return Result.buildFailure("Batch task cancellation failed: " + e.getMessage());
        }
    }

    @GetMapping("/{definitionId}/execute")
    @ApiOperation("Execute Task")
    public Result<String> executeTask(
            @ApiParam(value = "Task Definition ID", required = true)
            @PathVariable String definitionId) {
        taskExecutionService.execute(definitionId, ExecutionMode.MANUAL);
        return Result.buildSuc("Task started successfully");
    }

    @GetMapping("/{executionId}/cancel")
    @ApiOperation("Cancel Task Execution")
    public Result<Boolean> cancelExecution(
            @ApiParam(value = "Task Execution ID", required = true)
            @PathVariable String executionId) {
        try {
            boolean result = taskExecutionService.cancel(executionId);
            if (result) {
                return Result.buildSuc(true);
            } else {
                return Result.buildFailure("Task cancellation failed");
            }
        } catch (Exception e) {
            log.error("Cancel task execution failed - executionId: {}", executionId, e);
            return Result.buildFailure("Task cancellation failed: " + e.getMessage());
        }
    }

    @GetMapping("/{executionId}/status")
    @ApiOperation("Get Task Execution Status")
    public Result<TaskExecutionStatus> getExecutionStatus(
            @ApiParam(value = "Task Execution ID", required = true)
            @PathVariable String executionId) {
        try {
            TaskExecutionStatus status = taskExecutionService.getStatus(executionId);
            if (status != null) {
                return Result.buildSuc(status);
            } else {
                return Result.buildFailure("Task execution record does not exist");
            }
        } catch (Exception e) {
            log.error("Get execution status failed - executionId: {}", executionId, e);
            return Result.buildFailure("Failed to get task status: " + e.getMessage());
        }
    }

    @GetMapping("/{executionId}/engine-info")
    @ApiOperation("Get Engine Task Information")
    public Result<TaskInfo> getEngineTaskInfo(
            @ApiParam(value = "Task Execution ID", required = true)
            @PathVariable String executionId) {
        try {
            TaskInfo taskInfo = taskExecutionService.getEngineTaskInfo(executionId);
            if (taskInfo != null) {
                return Result.buildSuc(taskInfo);
            } else {
                return Result.buildFailure("Engine task information does not exist");
            }
        } catch (Exception e) {
            log.error("Get engine task info failed - executionId: {}", executionId, e);
            return Result.buildFailure("Failed to get engine task information: " + e.getMessage());
        }
    }

    @GetMapping("/definition/{definitionId}")
    @ApiOperation("Query Execution Records by Task Definition ID")
    public Result<List<TaskExecutionPO>> getExecutionsByDefinition(
            @ApiParam(value = "Task Definition ID", required = true)
            @PathVariable String definitionId) {
        try {
            List<TaskExecutionPO> executions = taskExecutionService.getExecutionsByDefinition(definitionId);
            return Result.buildSuc(executions);
        } catch (Exception e) {
            log.error("Get executions by definition failed - definitionId: {}", definitionId, e);
            return Result.buildFailure("Failed to query execution records: " + e.getMessage());
        }
    }

    @GetMapping("/{id}")
    @ApiOperation("Get Task Execution Details")
    public Result<TaskExecutionVO> getExecutionDetail(
            @ApiParam(value = "Task Execution ID", required = true)
            @PathVariable String id) {
        return Result.buildSuc(taskExecutionService.selectById(id));
    }

    @GetMapping("/taskLog/{id}")
    @ApiOperation("Get Task Execution Log")
    public Result<String> getTaskLog(@PathVariable("id") String id) {
        return Result.buildSuc(taskExecutionService.getTaskLog(id));
    }

    @DeleteMapping("/cleanup")
    @ApiOperation("Clean Up Completed Task Execution Records")
    public Result<String> cleanupExecutions(
            @ApiParam(value = "Retention Days", defaultValue = "30")
            @RequestParam(defaultValue = "30") int days) {
        try {
            if (days < 1) {
                return Result.buildFailure("Retention days must be greater than 0");
            }
            taskExecutionService.cleanupCompletedExecutions(days);
            return Result.buildSuc("Cleanup completed");
        } catch (Exception e) {
            log.error("Cleanup executions failed - days: {}", days, e);
            return Result.buildFailure("Failed to clean up execution records: " + e.getMessage());
        }
    }

    @GetMapping("/summary")
    @ApiOperation("Get Metrics Summary")
    public Result<Map<String, Object>> getMetricsSummary(
            @RequestParam String timeRange,
            @RequestParam(required = false) String taskType) {
        return Result.buildSuc(taskExecutionService.getMetricsSummary(timeRange, taskType));
    }

    @GetMapping("/sync-trend")
    @ApiOperation("Get Synchronization Trend")
    public Result<Map<String, Object>> getSyncTrend(@RequestParam String timeRange) {
        return Result.buildSuc(taskExecutionService.getSyncTrend(timeRange));
    }

    @GetMapping("/execution-info/{id}")
    @ApiOperation("Get Execution Information by Definition ID")
    public Result<List<TaskExecutionVO>> getExecutionInfoByDefinitionId(@PathVariable String id) {
        return Result.buildSuc(taskExecutionService.getExecutionInfoByDefinitionId(id));
    }
}