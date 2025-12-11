package org.apache.cockpit.connectors.api.common.metrics;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class TaskHistory {

    private static final TaskHistory INSTANCE = new TaskHistory();
    private final ScheduledExecutorService scheduler;
    private final Map<String, TaskStats> taskStatsMap;
    private final AtomicLong totalTasks;
    private final AtomicLong completedTasks;
    private final AtomicLong failedTasks;
    private final AtomicLong cancelledTasks;
    private volatile boolean isRunning;

    // 任务统计信息
    public static class TaskStats {
        private final String taskId;
        private final long startTime;
        private volatile long endTime;
        private volatile TaskExecutionStatus status;
        private volatile long recordsProcessed;
        private volatile long bytesProcessed;
        private volatile Throwable lastError;

        public TaskStats(String taskId, TaskExecutionStatus status) {
            this.taskId = taskId;
            this.status = status;
            this.startTime = System.currentTimeMillis();
            this.recordsProcessed = 0;
            this.bytesProcessed = 0;
        }


        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public TaskExecutionStatus getStatus() {
            return status;
        }

        public void setStatus(TaskExecutionStatus status) {
            this.status = status;
        }

        public void incrementRecordsProcessed(long count) {
            this.recordsProcessed += count;
        }


        public void incrementBytesProcessed(long bytes) {
            this.bytesProcessed += bytes;
        }

        public Throwable getLastError() {
            return lastError;
        }

        public void setLastError(Throwable lastError) {
            this.lastError = lastError;
        }

        public long getDuration() {
            if (endTime == 0) {
                return System.currentTimeMillis() - startTime;
            }
            return endTime - startTime;
        }

        public double getRecordsPerSecond() {
            long duration = getDuration();
            if (duration == 0) return 0;
            return (recordsProcessed * 1000.0) / duration;
        }

        public double getBytesPerSecond() {
            long duration = getDuration();
            if (duration == 0) return 0;
            return (bytesProcessed * 1000.0) / duration;
        }

        @Override
        public String toString() {
            return String.format("Task[%s] Status: %s, Duration: %s, Records: %d (%.1f rec/s), Bytes: %s (%.1f MB/s)",
                    taskId.substring(0, 8), // 只显示前8位ID
                    status,
                    formatDuration(getDuration()),
                    recordsProcessed,
                    getRecordsPerSecond(),
                    formatBytes(bytesProcessed),
                    getBytesPerSecond() / (1024 * 1024));
        }
    }

    private TaskHistory() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TaskHistory-Printer");
            t.setDaemon(true);
            return t;
        });
        this.taskStatsMap = new ConcurrentHashMap<>();
        this.totalTasks = new AtomicLong(0);
        this.completedTasks = new AtomicLong(0);
        this.failedTasks = new AtomicLong(0);
        this.cancelledTasks = new AtomicLong(0);
        this.isRunning = false;
    }

    public static TaskHistory getInstance() {
        return INSTANCE;
    }

    /**
     * 开始定时打印任务统计信息
     */
    public void start() {
        start(1, TimeUnit.MINUTES);
    }

    /**
     * 开始定时打印任务统计信息
     */
    public void start(long interval, TimeUnit timeUnit) {
        if (isRunning) {
            log.warn("TaskHistory is already running");
            return;
        }

        isRunning = true;
        scheduler.scheduleAtFixedRate(this::printStatistics, 0, interval, timeUnit);
        log.info("TaskHistory started with interval: {} {}", interval, timeUnit);
    }

    /**
     * 停止定时打印
     */
    public void stop() {
        if (!isRunning) {
            return;
        }

        isRunning = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("TaskHistory stopped");
    }

    /**
     * 注册新任务
     */
    public void registerTask(String taskId, TaskExecutionStatus status) {
        TaskStats stats = new TaskStats(taskId, status);
        taskStatsMap.put(taskId, stats);
        totalTasks.incrementAndGet();
        log.debug("Registered new task: {}", taskId);
    }

    /**
     * 更新任务状态
     */
    public void updateTaskStatus(String taskId, TaskExecutionStatus status) {
        TaskStats stats = taskStatsMap.get(taskId);
        if (stats != null) {
            stats.setStatus(status);
            if (status == TaskExecutionStatus.COMPLETED) {
                stats.setEndTime(System.currentTimeMillis());
                completedTasks.incrementAndGet();
            } else if (status == TaskExecutionStatus.FAILED) {
                stats.setEndTime(System.currentTimeMillis());
                failedTasks.incrementAndGet();
            } else if (status == TaskExecutionStatus.CANCELLED) {
                stats.setEndTime(System.currentTimeMillis());
                cancelledTasks.incrementAndGet();
            }
        }
    }

    /**
     * 更新任务处理记录数
     */
    public void updateRecordsProcessed(String taskId, long records) {
        TaskStats stats = taskStatsMap.get(taskId);
        if (stats != null) {
            stats.incrementRecordsProcessed(records);
        }
    }

    /**
     * 更新任务处理字节数
     */
    public void updateBytesProcessed(String taskId, long bytes) {
        TaskStats stats = taskStatsMap.get(taskId);
        if (stats != null) {
            stats.incrementBytesProcessed(bytes);
        }
    }

    /**
     * 更新任务错误信息
     */
    public void updateTaskError(String taskId, Throwable error) {
        TaskStats stats = taskStatsMap.get(taskId);
        if (stats != null) {
            stats.setLastError(error);
        }
    }

    /**
     * 移除任务统计信息
     */
    public void removeTask(String taskId) {
        taskStatsMap.remove(taskId);
        log.debug("Removed task statistics: {}", taskId);
    }

    /**
     * 获取任务统计信息
     */
    public TaskStats getTaskStats(String taskId) {
        return taskStatsMap.get(taskId);
    }

    /**
     * 获取所有任务统计信息
     */
    public Map<String, TaskStats> getAllTaskStats() {
        return new ConcurrentHashMap<>(taskStatsMap);
    }

    /**
     * 打印统计信息
     */
    private void printStatistics() {
        if (taskStatsMap.isEmpty()) {
            log.info("=== No active tasks ===");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n=== Task Execution Statistics ===\n");

        // 总体统计
        sb.append(String.format("Total Tasks: %d, Completed: %d, Failed: %d, Cancelled: %d, Running: %d\n",
                totalTasks.get(),
                completedTasks.get(),
                failedTasks.get(),
                cancelledTasks.get(),
                getRunningTaskCount()));

        // 按状态分组显示任务
        Map<TaskExecutionStatus, List<TaskStats>> tasksByStatus = groupTasksByStatus();

        for (Map.Entry<TaskExecutionStatus, List<TaskStats>> entry : tasksByStatus.entrySet()) {
            TaskExecutionStatus status = entry.getKey();
            List<TaskStats> tasks = entry.getValue();

            sb.append(String.format("\n%s Tasks (%d):\n", status, tasks.size()));

            for (TaskStats task : tasks) {
                sb.append("  ").append(task.toString()).append("\n");

                // 对于失败的任务，显示错误信息
                if (status == TaskExecutionStatus.FAILED && task.getLastError() != null) {
                    String errorMsg = task.getLastError().getMessage();
                    if (errorMsg != null && errorMsg.length() > 100) {
                        errorMsg = errorMsg.substring(0, 100) + "...";
                    }
                    sb.append(String.format("    Error: %s\n", errorMsg));
                }
            }
        }

        // 性能汇总
        sb.append("\nPerformance Summary:\n");
        double avgRecordsPerSec = calculateAverageRecordsPerSecond();
        double avgBytesPerSec = calculateAverageBytesPerSecond();
        sb.append(String.format("Average Throughput: %.1f records/sec, %.1f MB/sec\n",
                avgRecordsPerSec, avgBytesPerSec / (1024 * 1024)));

        sb.append("=================================\n");

        log.info(sb.toString());
    }

    /**
     * 按状态分组任务
     */
    private Map<TaskExecutionStatus, List<TaskStats>> groupTasksByStatus() {
        Map<TaskExecutionStatus, List<TaskStats>> result = new LinkedHashMap<>();

        // 按重要顺序排列状态
        result.put(TaskExecutionStatus.FAILED, new ArrayList<>());
        result.put(TaskExecutionStatus.RUNNING, new ArrayList<>());
        result.put(TaskExecutionStatus.CANCELLED, new ArrayList<>());
        result.put(TaskExecutionStatus.COMPLETED, new ArrayList<>());
        result.put(TaskExecutionStatus.STARTING, new ArrayList<>());

        for (TaskStats stats : taskStatsMap.values()) {
            List<TaskStats> list = result.get(stats.getStatus());
            if (list != null) {
                list.add(stats);
            }
        }

        // 移除空列表
        result.entrySet().removeIf(entry -> entry.getValue().isEmpty());

        return result;
    }

    /**
     * 获取运行中任务数量
     */
    private long getRunningTaskCount() {
        return taskStatsMap.values().stream()
                .filter(stats -> stats.getStatus() == TaskExecutionStatus.RUNNING)
                .count();
    }

    /**
     * 计算平均记录处理速度
     */
    private double calculateAverageRecordsPerSecond() {
        return taskStatsMap.values().stream()
                .mapToDouble(TaskStats::getRecordsPerSecond)
                .average()
                .orElse(0.0);
    }

    /**
     * 计算平均字节处理速度
     */
    private double calculateAverageBytesPerSecond() {
        return taskStatsMap.values().stream()
                .mapToDouble(TaskStats::getBytesPerSecond)
                .average()
                .orElse(0.0);
    }

    /**
     * 格式化持续时间
     */
    private static String formatDuration(long durationMs) {
        if (durationMs < 1000) {
            return durationMs + "ms";
        } else if (durationMs < 60000) {
            return String.format("%.1fs", durationMs / 1000.0);
        } else {
            long minutes = durationMs / 60000;
            long seconds = (durationMs % 60000) / 1000;
            return String.format("%dm %ds", minutes, seconds);
        }
    }

    /**
     * 格式化字节大小
     */
    private static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    /**
     * 清理已完成的任务统计
     */
    public void cleanupCompletedTasks() {
        int initialSize = taskStatsMap.size();
        taskStatsMap.entrySet().removeIf(entry -> {
            TaskExecutionStatus status = entry.getValue().getStatus();
            return status == TaskExecutionStatus.COMPLETED ||
                    status == TaskExecutionStatus.FAILED ||
                    status == TaskExecutionStatus.CANCELLED;
        });
        int removed = initialSize - taskStatsMap.size();
        if (removed > 0) {
            log.info("Cleaned up {} completed task statistics", removed);
        }
    }
}