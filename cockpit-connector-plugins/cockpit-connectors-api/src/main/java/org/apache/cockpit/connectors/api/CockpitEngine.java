package org.apache.cockpit.connectors.api;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.common.log.Logger;
import org.apache.cockpit.connectors.api.common.metrics.*;
import org.apache.cockpit.connectors.api.jdbc.flow.SourceFlowLifeCycle;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class CockpitEngine {

    public static final int STRING_BUFFER = 4096;
    private final TaskHistory taskHistory = TaskHistory.getInstance();

    private final Map<String, TaskExecutionStatusCallback> taskCallbacks = new ConcurrentHashMap<>();

    // 任务存储
    private final Map<String, TaskInfo> taskMap = new ConcurrentHashMap<>();
    private final ExecutorService taskExecutor = Executors.newCachedThreadPool();

    // 单例模式
    private static final CockpitEngine INSTANCE = new CockpitEngine();

    public static CockpitEngine getInstance() {
        return INSTANCE;
    }

    private CockpitEngine() {
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * 启动任务 - 异步执行
     */
    public String startAsync(Config config, String taskId, TaskExecutionPO execution, TaskExecutionStatusCallback callback) {
        TaskInfo taskInfo = new TaskInfo(taskId);
        taskInfo.setStartTime(System.currentTimeMillis());
        taskInfo.setStatus(TaskExecutionStatus.RUNNING);

        taskHistory.registerTask(taskId, TaskExecutionStatus.RUNNING);

        // 保存回调
        if (callback != null) {
            taskCallbacks.put(taskId, callback);
        }

        Future<?> future = taskExecutor.submit(() -> {
            try {
                execute(config, taskInfo, execution.getLogPath(), callback);
                taskInfo.setStatus(TaskExecutionStatus.COMPLETED);

                // 回调通知任务完成
                if (callback != null) {
                    callback.onTaskCompleted(taskId);
                }
                taskHistory.updateTaskStatus(taskId, TaskExecutionStatus.COMPLETED);

            } catch (Exception e) {
                taskInfo.setStatus(TaskExecutionStatus.FAILED);
                taskInfo.setError(e);

                // 回调通知任务失败
                if (callback != null) {
                    callback.onTaskFailed(taskId, e);
                }
                taskHistory.updateTaskStatus(taskId, TaskExecutionStatus.FAILED);
                log.error("Task {} failed: {}", taskId, e.getMessage(), e);
            } finally {
                taskInfo.setEndTime(System.currentTimeMillis());
                // 清理回调
                taskCallbacks.remove(taskId);
            }
        });

        taskInfo.setFuture(future);
        taskMap.put(taskId, taskInfo);

        log.info("Task {} started successfully", taskId);
        return taskId;
    }

    /**
     * 取消任务
     */
    public boolean cancel(String taskId) {
        return cancel(taskId, false);
    }


    /**
     * 取消任务
     *
     * @param interrupt 是否中断执行
     */
    // 在 cancel 方法中也添加回调
    public boolean cancel(String taskId, boolean interrupt) {
        TaskInfo taskInfo = taskMap.get(taskId);
        if (taskInfo == null) {
            log.warn("Task {} not found", taskId);
            return false;
        }

        if (taskInfo.getStatus() == TaskExecutionStatus.COMPLETED ||
                taskInfo.getStatus() == TaskExecutionStatus.FAILED) {
            log.info("Task {} is already finished with status: {}", taskId, taskInfo.getStatus());
            return false;
        }

        // 尝试取消 Future
        if (taskInfo.getFuture() != null) {
            boolean cancelled = taskInfo.getFuture().cancel(interrupt);
            if (cancelled) {
                taskInfo.setStatus(TaskExecutionStatus.CANCELLED);
                taskInfo.setEndTime(System.currentTimeMillis());

                // 回调通知任务取消
                TaskExecutionStatusCallback callback = taskCallbacks.get(taskId);
                if (callback != null) {
                    callback.onTaskCancelled(taskId);
                    taskCallbacks.remove(taskId);
                }

                taskHistory.updateTaskStatus(taskId, TaskExecutionStatus.CANCELLED);

                // 关闭任务资源
                if (taskInfo.getFlowLifeCycle() != null) {
                    try {
                        taskInfo.getFlowLifeCycle().close();
                    } catch (Exception e) {
                        log.warn("Error while closing flow lifecycle for task {}", taskId, e);
                    }
                }

                log.info("Task {} cancelled successfully", taskId);
                return true;
            }
        }

        log.warn("Failed to cancel task {}", taskId);
        return false;
    }

    /**
     * 获取任务状态
     */
    public TaskExecutionStatus getStatus(String taskId) {
        TaskInfo taskInfo = taskMap.get(taskId);
        return taskInfo != null ? taskInfo.getStatus() : null;
    }

    /**
     * 获取任务信息
     */
    public TaskInfo getTaskInfo(String taskId) {
        return taskMap.get(taskId);
    }

    /**
     * 获取所有任务信息
     */
    public Map<String, TaskInfo> getAllTasks() {
        return new ConcurrentHashMap<>(taskMap);
    }

    /**
     * 等待任务完成
     */
    public boolean waitForCompletion(String taskId, long timeout, TimeUnit unit) throws InterruptedException {
        TaskInfo taskInfo = taskMap.get(taskId);
        if (taskInfo == null || taskInfo.getFuture() == null) {
            return false;
        }

        try {
            taskInfo.getFuture().get(timeout, unit);
            return true;
        } catch (java.util.concurrent.TimeoutException e) {
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            return true; // 任务已完成（可能失败）
        }
    }

    /**
     * 清理已完成的任务
     */
    public void cleanupCompletedTasks() {
        taskMap.entrySet().removeIf(entry -> {
            TaskExecutionStatus status = entry.getValue().getStatus();
            return status == TaskExecutionStatus.COMPLETED ||
                    status == TaskExecutionStatus.FAILED ||
                    status == TaskExecutionStatus.CANCELLED;
        });
        log.info("Completed tasks cleaned up");
    }

    /**
     * 清理指定任务
     */
    public boolean removeTask(String taskId) {
        TaskInfo removed = taskMap.remove(taskId);
        if (removed != null) {
            log.info("Task {} removed from management", taskId);
            return true;
        }
        return false;
    }


    /**
     * 关闭引擎
     */
    public void shutdown() {
        log.info("Shutting down CockpitEngine...");

        // 取消所有运行中的任务
        for (Map.Entry<String, TaskInfo> entry : taskMap.entrySet()) {
            TaskInfo taskInfo = entry.getValue();
            if (taskInfo.getStatus() == TaskExecutionStatus.RUNNING) {
                cancel(entry.getKey(), true);
            }
        }

        // 关闭线程池
        taskExecutor.shutdown();
        try {
            if (!taskExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                taskExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            taskExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("CockpitEngine shutdown completed");
    }

    private void execute(Config config, TaskInfo taskInfo, String logPath, TaskExecutionStatusCallback callback) throws Exception {
        Logger logger = null;
        SourceFlowLifeCycle<SeaTunnelRow, SourceSplit> sourceFlowLifeCycle = null;
        try {
            logger = new Logger(logPath);
            MetricsContext metricsContext = new SeaTunnelMetricsContext();
            sourceFlowLifeCycle =
                    new SourceFlowLifeCycle<>(config.getConfig("source"), metricsContext, logger);
            // 保存 flowLifeCycle 引用以便取消时能关闭资源
            taskInfo.setFlowLifeCycle(sourceFlowLifeCycle);
            sourceFlowLifeCycle.init();
            sourceFlowLifeCycle.open();
            logger.log("input params config : " + config.root().render());
            sourceFlowLifeCycle.initCollector(config.getConfig("sink"));
            sourceFlowLifeCycle.collect();


            Counter sourceReceivedCount = (Counter) metricsContext.getAllMetrics().get("SourceReceivedCount");
            Counter sourceReceivedBytes = (Counter) metricsContext.getAllMetrics().get("SourceReceivedBytes");
            Counter sinkWriteCount = (Counter) metricsContext.getAllMetrics().get("SinkWriteCount");
            Counter sinkWriteBytes = (Counter) metricsContext.getAllMetrics().get("SinkWriteBytes");
            callback.onTaskSourceTotalRecord(taskInfo.getTaskId(), sourceReceivedCount.getCount());
            callback.onTaskSourceTotalBytes(taskInfo.getTaskId(), sourceReceivedBytes.getCount());
            callback.onTaskSinkTotalRecord(taskInfo.getTaskId(), sinkWriteCount.getCount());
            callback.onTaskSinkTotalBytes(taskInfo.getTaskId(), sinkWriteBytes.getCount());


            taskHistory.updateRecordsProcessed(taskInfo.getTaskId(), sourceReceivedCount.getCount());
            taskHistory.updateBytesProcessed(taskInfo.getTaskId(), sourceReceivedBytes.getCount());
            // 更新任务状态为完成
            taskHistory.updateTaskStatus(taskInfo.getTaskId(), TaskExecutionStatus.COMPLETED);

        } catch (Exception e) {
            // 更新任务状态为失败
            taskHistory.updateTaskStatus(taskInfo.getTaskId(), TaskExecutionStatus.FAILED);
            taskHistory.updateTaskError(taskInfo.getTaskId(), e);
            assert logger != null;
            logger.log("\n\n经Cockpit智能分析,该任务最可能的错误原因是:\n" + trace(e));
            throw new RuntimeException("经Cockpit智能分析,该任务最可能的错误原因是 " + e, e);
        } finally {
            try {
                callback.updateRunningToFailed(taskInfo.getTaskId(), null);
                log.info("==关闭所有资源==");
                if (sourceFlowLifeCycle != null) {
                    sourceFlowLifeCycle.close();
                }
            } catch (Exception e) {
                if (logger != null) {
                    logger.log("\n\n经Cockpit智能分析,该任务最可能的错误原因是:\n" + trace(e));
                }
                // 更新任务状态为失败
                taskHistory.updateTaskStatus(taskInfo.getTaskId(), TaskExecutionStatus.FAILED);
                taskHistory.updateTaskError(taskInfo.getTaskId(), e);
                throw new RuntimeException("经Cockpit智能分析,该任务最可能的错误原因是 " + e, e);
            }
        }
    }

    public static String trace(Throwable ex) {
        StringWriter sw = new StringWriter(STRING_BUFFER);
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        return sw.toString();
    }
}