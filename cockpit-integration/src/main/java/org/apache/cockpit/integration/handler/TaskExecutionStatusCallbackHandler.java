package org.apache.cockpit.integration.handler;


import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.connectors.api.common.metrics.TaskExecutionStatusCallback;
import org.apache.cockpit.persistence.integration.TaskExecutionMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Slf4j
@Component
public class TaskExecutionStatusCallbackHandler extends ServiceImpl<TaskExecutionMapper, TaskExecutionPO>
        implements TaskExecutionStatusCallback {

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void onTaskCompleted(String executionId) {
        updateTaskStatus(executionId, TaskExecutionStatus.COMPLETED, null);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void onTaskFailed(String executionId, Throwable error) {
        updateTaskStatus(executionId, TaskExecutionStatus.FAILED, error.getMessage());
    }

    @Override
    public void updateRunningToFailed(String executionId, Throwable error) {
        updateRunningToFailed(executionId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void onTaskCancelled(String executionId) {
        updateTaskStatus(executionId, TaskExecutionStatus.CANCELLED, "任务被取消");
    }

    @Override
    public void onTaskSourceTotalRecord(String taskId, Long totalRecord) {
        updateTaskField(taskId, "sourceTotalRecord", totalRecord);
    }

    @Override
    public void onTaskSinkTotalRecord(String taskId, Long totalRecord) {
        updateTaskField(taskId, "sinkTotalRecord", totalRecord);
    }

    @Override
    public void onTaskSourceTotalBytes(String taskId, Long totalBytes) {
        updateTaskField(taskId, "sourceTotalBytes", totalBytes);
    }

    @Override
    public void onTaskSinkTotalBytes(String taskId, Long totalBytes) {
        updateTaskField(taskId, "sinkTotalBytes", totalBytes);
    }

    private void updateTaskStatus(String executionId, TaskExecutionStatus status, String errorMessage) {
        try {
            TaskExecutionPO execution = getById(executionId);
            if (execution != null) {
                execution.setStatus(status);
                execution.setEndTime(new Date());
                updateById(execution);

                log.info("Task execution {} updated to {} - executionId: {}",
                        executionId, status, executionId);
            }
        } catch (Exception e) {
            log.error("Error updating task status for execution: {}", executionId, e);
        }
    }

    private void updateRunningToFailed(String executionId) {
        try {
            TaskExecutionPO execution = getById(executionId);
            if (execution != null && execution.getStatus() == TaskExecutionStatus.RUNNING) {
                execution.setStatus(TaskExecutionStatus.FAILED);
                execution.setEndTime(new Date());
                updateById(execution);

                log.info("Task execution {} updated to {} - executionId: {}",
                        executionId, TaskExecutionStatus.FAILED, executionId);
            }
        } catch (Exception e) {
            log.error("Error updating task status for execution: {}", executionId, e);
        }
    }

    private void updateTaskField(String executionId, String field, Object value) {
        try {
            TaskExecutionPO execution = getById(executionId);
            if (execution != null) {
                switch (field) {
                    case "sourceTotalRecord":
                        execution.setSourceTotalRecord((Long) value);
                        break;
                    case "sinkTotalRecord":
                        execution.setSinkTotalRecord((Long) value);
                        break;
                    case "sourceTotalBytes":
                        execution.setSourceTotalBytes((Long) value);
                        break;
                    case "sinkTotalBytes":
                        execution.setSinkTotalBytes((Long) value);
                        break;
                }
                updateById(execution);
                log.info("Task execution {} {} updated to {} - executionId: {}",
                        executionId, field, value, executionId);
            }
        } catch (Exception e) {
            log.error("Error updating task {} for execution: {}", field, executionId, e);
        }
    }
}
