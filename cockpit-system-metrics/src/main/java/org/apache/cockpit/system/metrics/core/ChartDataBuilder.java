package org.apache.cockpit.system.metrics.core;


import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.apache.cockpit.common.bean.vo.metrics.LineChartVO;
import org.apache.cockpit.common.bean.vo.metrics.LineSeriesVO;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class ChartDataBuilder {

    public LineChartVO buildMemoryUsageChart(List<MetricsJvmPO> metricsList, String applicationName) {
        LineChartVO chartVO = new LineChartVO();
        chartVO.setTitle("内存使用趋势 - " + applicationName);

        List<String> xAxis = metricsList.stream()
                .map(po -> po.getCollectTime().format(DateTimeFormatter.ofPattern("yyyy年MM月dd HH:mm:00")))
                .collect(Collectors.toList());
        chartVO.setXAxis(xAxis);

        List<LineSeriesVO> series = new ArrayList<>();

        series.add(LineSeriesVO.builder()
                .name("JVM内存最大值")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getTotalMemoryMax() != null ?
                                convertBytesToMB(po.getTotalMemoryMax()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        series.add(LineSeriesVO.builder()
                .name("堆内存使用量")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getHeapMemoryUsed() != null ?
                                convertBytesToMB(po.getHeapMemoryUsed()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        series.add(LineSeriesVO.builder()
                .name("非堆内存使用量")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getNonheapMemoryUsed() != null ?
                                convertBytesToMB(po.getNonheapMemoryUsed()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        if (hasEdenData(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("Eden区使用量")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getEdenMemoryUsed() != null ?
                                    convertBytesToMB(po.getEdenMemoryUsed()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        if (hasOldGenData(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("老年代使用量")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getOldgenMemoryUsed() != null ?
                                    convertBytesToMB(po.getOldgenMemoryUsed()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        if (hasMetaspaceData(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("元空间使用量")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getMetaspaceMemoryUsed() != null ?
                                    convertBytesToMB(po.getMetaspaceMemoryUsed()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        chartVO.setSeries(series);
        return chartVO;
    }

    private BigDecimal convertBytesToMB(Long bytes) {
        if (bytes == null || bytes == 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(bytes / 1024.0 / 1024.0)
                .setScale(2, RoundingMode.HALF_UP);
    }

    private boolean hasEdenData(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getEdenMemoryUsed() != null && po.getEdenMemoryUsed() > 0);
    }

    private boolean hasOldGenData(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getOldgenMemoryUsed() != null && po.getOldgenMemoryUsed() > 0);
    }

    private boolean hasMetaspaceData(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getMetaspaceMemoryUsed() != null && po.getMetaspaceMemoryUsed() > 0);
    }

    public LocalDateTime calculateStartTime(String duration, LocalDateTime endTime) {
        switch (duration) {
            case "1h":
                return endTime.minusHours(1);
            case "6h":
                return endTime.minusHours(6);
            case "12h":
                return endTime.minusHours(12);
            case "1d":
                return endTime.minusDays(1);
            case "7d":
                return endTime.minusDays(7);
            case "30d":
                return endTime.minusDays(30);
            default:
                return endTime.minusHours(1);
        }
    }
    /**
     * 构建线程使用情况折线图数据
     */
    public LineChartVO buildThreadUsageChart(List<MetricsJvmPO> metricsList, String applicationName) {
        LineChartVO chartVO = new LineChartVO();
        chartVO.setTitle("线程使用趋势 - " + applicationName);

        // 构建X轴数据（时间点）
        List<String> xAxis = metricsList.stream()
                .map(po -> po.getCollectTime().format(DateTimeFormatter.ofPattern("yyyy年MM月dd HH:mm:00")))
                .collect(Collectors.toList());
        chartVO.setXAxis(xAxis);

        // 构建系列数据
        List<LineSeriesVO> series = new ArrayList<>();

        // 总线程数
        series.add(LineSeriesVO.builder()
                .name("总线程数")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getThreadCount() != null ?
                                BigDecimal.valueOf(po.getThreadCount()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        // 峰值线程数
        series.add(LineSeriesVO.builder()
                .name("峰值线程数")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getThreadPeakCount() != null ?
                                BigDecimal.valueOf(po.getThreadPeakCount()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        // 守护线程数
        series.add(LineSeriesVO.builder()
                .name("守护线程数")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getThreadDaemonCount() != null ?
                                BigDecimal.valueOf(po.getThreadDaemonCount()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        // RUNNABLE状态线程数
        series.add(LineSeriesVO.builder()
                .name("运行中线程")
                .type("line")
                .data(metricsList.stream()
                        .map(po -> po.getThreadRunnableCount() != null ?
                                BigDecimal.valueOf(po.getThreadRunnableCount()) : BigDecimal.ZERO)
                        .collect(Collectors.toList()))
                .build());

        // BLOCKED状态线程数
        if (hasBlockedThreads(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("阻塞线程")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getThreadBlockedCount() != null ?
                                    BigDecimal.valueOf(po.getThreadBlockedCount()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        // WAITING状态线程数
        if (hasWaitingThreads(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("等待线程")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getThreadWaitingCount() != null ?
                                    BigDecimal.valueOf(po.getThreadWaitingCount()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        // TIMED_WAITING状态线程数
        if (hasTimedWaitingThreads(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("限时等待线程")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getThreadTimedWaitingCount() != null ?
                                    BigDecimal.valueOf(po.getThreadTimedWaitingCount()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        // 死锁线程数
        if (hasDeadlockThreads(metricsList)) {
            series.add(LineSeriesVO.builder()
                    .name("死锁线程")
                    .type("line")
                    .data(metricsList.stream()
                            .map(po -> po.getThreadDeadlockCount() != null ?
                                    BigDecimal.valueOf(po.getThreadDeadlockCount()) : BigDecimal.ZERO)
                            .collect(Collectors.toList()))
                    .build());
        }

        chartVO.setSeries(series);
        return chartVO;
    }

    /**
     * 检查是否有阻塞线程数据
     */
    private boolean hasBlockedThreads(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getThreadBlockedCount() != null && po.getThreadBlockedCount() > 0);
    }

    /**
     * 检查是否有等待线程数据
     */
    private boolean hasWaitingThreads(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getThreadWaitingCount() != null && po.getThreadWaitingCount() > 0);
    }

    /**
     * 检查是否有限时等待线程数据
     */
    private boolean hasTimedWaitingThreads(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getThreadTimedWaitingCount() != null && po.getThreadTimedWaitingCount() > 0);
    }

    /**
     * 检查是否有死锁线程数据
     */
    private boolean hasDeadlockThreads(List<MetricsJvmPO> metricsList) {
        return metricsList.stream()
                .anyMatch(po -> po.getThreadDeadlockCount() != null && po.getThreadDeadlockCount() > 0);
    }


}
