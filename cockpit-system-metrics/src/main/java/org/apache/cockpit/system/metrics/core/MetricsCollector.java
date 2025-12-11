package org.apache.cockpit.system.metrics.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.springframework.stereotype.Component;

import java.lang.management.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
@Component
public class MetricsCollector {

    private LocalDateTime lastCollectTime;
    private final Map<String, Long> lastGcCounts = new ConcurrentHashMap<>();

    public MetricsJvmPO collectJvmMetrics(String applicationName, String instanceId) {
        try {
            MetricsJvmPO metrics = new MetricsJvmPO();
            metrics.setApplicationName(applicationName);
            metrics.setInstanceId(instanceId);
            metrics.setCollectTime(LocalDateTime.now());

            // 采集内存指标
            collectMemoryMetrics(metrics);

            // 采集GC指标
            collectGcMetrics(metrics);

            // 采集线程指标
            collectThreadMetrics(metrics);

            // 采集类加载指标
            collectClassLoadingMetrics(metrics);

            // 计算使用率
            calculateUsage(metrics);

            log.debug("成功采集JVM指标数据, 应用: {}, 实例: {}", applicationName, instanceId);
            return metrics;
        } catch (Exception e) {
            log.error("采集JVM指标数据失败, 应用: {}, 实例: {}", applicationName, instanceId, e);
            return null;
        }
    }

    private void collectMemoryMetrics(MetricsJvmPO metrics) {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        // 堆内存
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        metrics.setHeapMemoryInit(heapUsage.getInit());
        metrics.setHeapMemoryUsed(heapUsage.getUsed());
        metrics.setHeapMemoryCommitted(heapUsage.getCommitted());

        long heapMax = heapUsage.getMax();
        if (heapMax == -1) {
            heapMax = getHeapMaxFromRuntime();
        }
        metrics.setHeapMemoryMax(heapMax);

        // 非堆内存
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();
        metrics.setNonheapMemoryInit(nonHeapUsage.getInit());
        metrics.setNonheapMemoryUsed(nonHeapUsage.getUsed());
        metrics.setNonheapMemoryCommitted(nonHeapUsage.getCommitted());
        long nonHeapMax = nonHeapUsage.getMax();
        if (nonHeapMax == -1) {
            nonHeapMax = getNonHeapMaxEstimate();
        }
        metrics.setNonheapMemoryMax(nonHeapMax);

        // 计算总内存使用
        long totalUsed = heapUsage.getUsed() + nonHeapUsage.getUsed();
        long totalCommitted = heapUsage.getCommitted() + nonHeapUsage.getCommitted();
        long totalMax = heapMax + nonHeapMax;

        metrics.setTotalMemoryUsed(totalUsed);
        metrics.setTotalMemoryCommitted(totalCommitted);
        metrics.setTotalMemoryMax(totalMax);

        // 内存池详细信息
        collectMemoryPoolMetrics(metrics);
    }

    private long getHeapMaxFromRuntime() {
        return Runtime.getRuntime().maxMemory();
    }

    private long getNonHeapMaxEstimate() {
        try {
            List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
            for (String arg : inputArguments) {
                if (arg.startsWith("-XX:MaxMetaspaceSize=")) {
                    String sizeStr = arg.substring("-XX:MaxMetaspaceSize=".length());
                    return parseMemorySize(sizeStr);
                }
            }
        } catch (Exception e) {
            // 忽略异常
        }
        return 512 * 1024 * 1024L;
    }

    private long parseMemorySize(String sizeStr) {
        if (sizeStr == null || sizeStr.isEmpty()) {
            return 0L;
        }

        sizeStr = sizeStr.toLowerCase();
        try {
            if (sizeStr.endsWith("g")) {
                return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 1)) * 1024 * 1024 * 1024;
            } else if (sizeStr.endsWith("m")) {
                return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 1)) * 1024 * 1024;
            } else if (sizeStr.endsWith("k")) {
                return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 1)) * 1024;
            } else {
                return Long.parseLong(sizeStr);
            }
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private void collectMemoryPoolMetrics(MetricsJvmPO metrics) {
        List<MemoryPoolMXBean> memoryPools = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean pool : memoryPools) {
            MemoryUsage usage = pool.getUsage();
            String poolName = pool.getName();

            if (poolName.contains("Eden")) {
                metrics.setEdenMemoryUsed(usage.getUsed());
                metrics.setEdenMemoryMax(usage.getMax());
            } else if (poolName.contains("Survivor")) {
                metrics.setSurvivorMemoryUsed(usage.getUsed());
                metrics.setSurvivorMemoryMax(usage.getMax());
            } else if (poolName.contains("Old") || poolName.contains("Tenured")) {
                metrics.setOldgenMemoryUsed(usage.getUsed());
                metrics.setOldgenMemoryMax(usage.getMax());
            } else if (poolName.contains("Metaspace")) {
                metrics.setMetaspaceMemoryUsed(usage.getUsed());
                metrics.setMetaspaceMemoryMax(usage.getMax());
            }
        }
    }

    private void collectGcMetrics(MetricsJvmPO metrics) {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long youngCount = 0;
        long youngTime = 0;
        long oldCount = 0;
        long oldTime = 0;
        long fullCount = 0;
        long fullTime = 0;

        Map<String, Long> lastGcCounts = getLastGcCounts();
        Map<String, Long> currentGcCounts = new HashMap<>();

        for (GarbageCollectorMXBean gc : gcBeans) {
            String gcName = gc.getName();
            long count = gc.getCollectionCount();
            long time = gc.getCollectionTime();

            currentGcCounts.put(gcName, count);

            if (isYoungGC(gcName)) {
                youngCount += count;
                youngTime += time;
            } else if (isOldGC(gcName)) {
                oldCount += count;
                oldTime += time;
            }

            if (isFullGC(gcName)) {
                fullCount += count;
                fullTime += time;
            }
        }

        long youngCountIncrement = calculateIncrement(lastGcCounts, currentGcCounts, this::isYoungGC);
        long oldCountIncrement = calculateIncrement(lastGcCounts, currentGcCounts, this::isOldGC);
        long fullCountIncrement = calculateIncrement(lastGcCounts, currentGcCounts, this::isFullGC);

        saveCurrentGcCounts(currentGcCounts);

        metrics.setGcYoungCount(youngCount);
        metrics.setGcYoungTime(youngTime);
        metrics.setGcOldCount(oldCount);
        metrics.setGcOldTime(oldTime);
        metrics.setGcFullCount(fullCount);
        metrics.setGcFullTime(fullTime);
        metrics.setGcYoungCountIncrement(youngCountIncrement);
        metrics.setGcOldCountIncrement(oldCountIncrement);
        metrics.setGcFullCountIncrement(fullCountIncrement);

        calculateGcFrequency(metrics);
    }

    private boolean isYoungGC(String gcName) {
        return gcName.contains("Young") ||
                gcName.contains("Eden") ||
                gcName.contains("ParNew") ||
                gcName.contains("Copy") ||
                gcName.contains("Scavenge") ||
                "PS Scavenge".equals(gcName) ||
                "G1 Young Generation".equals(gcName);
    }

    private boolean isOldGC(String gcName) {
        return gcName.contains("Old") ||
                gcName.contains("Tenured") ||
                gcName.contains("MarkSweep") ||
                "PS MarkSweep".equals(gcName) ||
                "G1 Old Generation".equals(gcName);
    }

    private boolean isFullGC(String gcName) {
        if (gcName.contains("MarkSweep") ||
                gcName.contains("Full GC") ||
                gcName.contains("Global GC") ||
                "PS MarkSweep".equals(gcName) ||
                "ConcurrentMarkSweep".equals(gcName) ||
                "G1 Old Generation".equals(gcName)) {
            return true;
        }

        if ("G1 Young Generation".equals(gcName)) {
            return false;
        }
        if ("G1 Old Generation".equals(gcName)) {
            return true;
        }

        return false;
    }

    private long calculateIncrement(Map<String, Long> lastCounts, Map<String, Long> currentCounts,
                                    Function<String, Boolean> gcTypeFilter) {
        return currentCounts.entrySet().stream()
                .filter(entry -> gcTypeFilter.apply(entry.getKey()))
                .mapToLong(entry -> {
                    String gcName = entry.getKey();
                    long currentCount = entry.getValue();
                    long lastCount = lastCounts.getOrDefault(gcName, 0L);

                    if (currentCount < lastCount) {
                        return currentCount;
                    }
                    return currentCount - lastCount;
                })
                .sum();
    }

    private void calculateGcFrequency(MetricsJvmPO metrics) {
        LocalDateTime now = LocalDateTime.now();
        if (lastCollectTime != null) {
            long seconds = Duration.between(lastCollectTime, now).getSeconds();
            if (seconds > 0) {
                double youngFreq = (double) metrics.getGcYoungCountIncrement() / seconds * 60;
                double oldFreq = (double) metrics.getGcOldCountIncrement() / seconds * 60;
                double fullFreq = (double) metrics.getGcFullCountIncrement() / seconds * 60;

                metrics.setGcYoungFrequency(BigDecimal.valueOf(youngFreq));
                metrics.setGcOldFrequency(BigDecimal.valueOf(oldFreq));
                metrics.setGcFullFrequency(BigDecimal.valueOf(fullFreq));
            }
        }
        lastCollectTime = now;
    }

    private void collectThreadMetrics(MetricsJvmPO metrics) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        metrics.setThreadCount(threadMXBean.getThreadCount());
        metrics.setThreadPeakCount(threadMXBean.getPeakThreadCount());
        metrics.setThreadDaemonCount(threadMXBean.getDaemonThreadCount());
        metrics.setThreadStartedCount(threadMXBean.getTotalStartedThreadCount());

        int runnableCount = 0;
        int blockedCount = 0;
        int waitingCount = 0;
        int timedWaitingCount = 0;

        long[] threadIds = threadMXBean.getAllThreadIds();
        for (long threadId : threadIds) {
            ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId);
            if (threadInfo != null) {
                switch (threadInfo.getThreadState()) {
                    case RUNNABLE:
                        runnableCount++;
                        break;
                    case BLOCKED:
                        blockedCount++;
                        break;
                    case WAITING:
                        waitingCount++;
                        break;
                    case TIMED_WAITING:
                        timedWaitingCount++;
                        break;
                }
            }
        }

        metrics.setThreadRunnableCount(runnableCount);
        metrics.setThreadBlockedCount(blockedCount);
        metrics.setThreadWaitingCount(waitingCount);
        metrics.setThreadTimedWaitingCount(timedWaitingCount);

        long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
        metrics.setThreadDeadlockCount(deadlockedThreads != null ? deadlockedThreads.length : 0);
    }

    private void collectClassLoadingMetrics(MetricsJvmPO metrics) {
        ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();

        metrics.setClassesLoaded(classLoadingMXBean.getLoadedClassCount());
        metrics.setClassesUnloaded(classLoadingMXBean.getUnloadedClassCount());
        metrics.setClassesTotalLoaded(classLoadingMXBean.getTotalLoadedClassCount());
    }

    private void calculateUsage(MetricsJvmPO metrics) {
        if (metrics.getHeapMemoryMax() > 0) {
            BigDecimal heapUsage = BigDecimal.valueOf(metrics.getHeapMemoryUsed())
                    .divide(BigDecimal.valueOf(metrics.getHeapMemoryMax()), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100));
            metrics.setHeapMemoryUsage(heapUsage);
        }

        if (metrics.getNonheapMemoryMax() > 0) {
            BigDecimal nonHeapUsage = BigDecimal.valueOf(metrics.getNonheapMemoryUsed())
                    .divide(BigDecimal.valueOf(metrics.getNonheapMemoryMax()), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100));
            metrics.setNonheapMemoryUsage(nonHeapUsage);
        }
    }

    private Map<String, Long> getLastGcCounts() {
        return new HashMap<>(lastGcCounts);
    }

    private void saveCurrentGcCounts(Map<String, Long> currentCounts) {
        lastGcCounts.clear();
        lastGcCounts.putAll(currentCounts);
    }
}
