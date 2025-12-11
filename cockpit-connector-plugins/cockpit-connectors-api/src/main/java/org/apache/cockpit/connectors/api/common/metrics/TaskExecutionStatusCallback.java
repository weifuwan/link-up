package org.apache.cockpit.connectors.api.common.metrics;

// 添加任务状态回调接口
public interface TaskExecutionStatusCallback {
    void onTaskCompleted(String taskId);

    void onTaskFailed(String taskId, Throwable error);

    void updateRunningToFailed(String taskId, Throwable error);

    void onTaskCancelled(String taskId);

    void onTaskSourceTotalRecord(String taskId, Long totalRecord);

    void onTaskSinkTotalRecord(String taskId, Long totalRecord);

    void onTaskSourceTotalBytes(String taskId, Long totalBytes);

    void onTaskSinkTotalBytes(String taskId, Long totalBytes);
}