package org.apache.cockpit.connectors.api.common.metrics;

import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.connectors.api.jdbc.flow.SourceFlowLifeCycle;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.util.concurrent.Future;

// 任务信息类
public  class TaskInfo {
    private final String taskId;
    private volatile TaskExecutionStatus status;
    private volatile long startTime;
    private volatile long endTime;
    private volatile Throwable error;
    private volatile Future<?> future;
    private volatile SourceFlowLifeCycle<SeaTunnelRow, SourceSplit> flowLifeCycle;

    public TaskInfo(String taskId) {
        this.taskId = taskId;
        this.status = TaskExecutionStatus.STARTING;
    }

    // getter 和 setter 方法
    public String getTaskId() { return taskId; }
    public TaskExecutionStatus getStatus() { return status; }
    public void setStatus(TaskExecutionStatus status) { this.status = status; }
    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public Throwable getError() { return error; }
    public void setError(Throwable error) { this.error = error; }
    public Future<?> getFuture() { return future; }
    public void setFuture(Future<?> future) { this.future = future; }
    public SourceFlowLifeCycle<SeaTunnelRow, SourceSplit> getFlowLifeCycle() { return flowLifeCycle; }
    public void setFlowLifeCycle(SourceFlowLifeCycle<SeaTunnelRow, SourceSplit> flowLifeCycle) { this.flowLifeCycle = flowLifeCycle; }

    public long getDuration() {
        if (startTime == 0) return 0;
        if (endTime == 0) return System.currentTimeMillis() - startTime;
        return endTime - startTime;
    }
}

