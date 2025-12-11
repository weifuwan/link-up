package org.apache.cockpit.integration.metrics;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.integration.TaskExecutionPO;
import org.apache.cockpit.common.enums.integration.TaskExecutionStatus;
import org.apache.cockpit.persistence.integration.TaskExecutionMapper;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TaskMetricsService extends ServiceImpl<TaskExecutionMapper, TaskExecutionPO> {

    public Map<String, Object> getMetricsSummary(String timeRange, String taskType) {
        Map<String, Object> result = new HashMap<>();

        try {
            LocalDateTime endTime = LocalDateTime.now();
            LocalDateTime startTime = calculateStartTime(endTime, timeRange);

            LambdaQueryWrapper<TaskExecutionPO> queryWrapper = buildMetricsQueryWrapper(startTime, endTime, taskType);
            List<TaskExecutionPO> executions = list(queryWrapper);

            calculateMetrics(result, executions);

        } catch (Exception e) {
            log.error("Error getting metrics summary", e);
            throw new RuntimeException("获取指标汇总失败: " + e.getMessage(), e);
        }

        return result;
    }

    public Map<String, Object> getSyncTrend(String timeRange) {
        Map<String, Object> result = new HashMap<>();

        try {
            LocalDateTime endTime = LocalDateTime.now();
            LocalDateTime startTime = calculateStartTime(endTime, timeRange);

            LambdaQueryWrapper<TaskExecutionPO> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.between(TaskExecutionPO::getStartTime, startTime, endTime)
                    .isNotNull(TaskExecutionPO::getEndTime);

            List<TaskExecutionPO> executions = list(queryWrapper);
            Map<String, List<TaskExecutionPO>> groupedByTime = groupExecutionsByTime(executions, timeRange);

            populateTrendData(result, groupedByTime);

        } catch (Exception e) {
            log.error("Error getting sync trend data", e);
            throw new RuntimeException("获取同步趋势数据失败: " + e.getMessage(), e);
        }

        return result;
    }

    // 其他私有方法保持不变，只是移动到这个类中
    private LambdaQueryWrapper<TaskExecutionPO> buildMetricsQueryWrapper(
            LocalDateTime startTime, LocalDateTime endTime, String taskType) {
        LambdaQueryWrapper<TaskExecutionPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.between(TaskExecutionPO::getStartTime, startTime, endTime);

        if (taskType != null && !"ALL".equals(taskType)) {
            queryWrapper.eq(TaskExecutionPO::getTaskExecuteType, taskType);
        }
        return queryWrapper;
    }

    private void calculateMetrics(Map<String, Object> result, List<TaskExecutionPO> executions) {
        long totalRecords = executions.stream()
                .filter(e -> e.getSinkTotalRecord() != null)
                .mapToLong(TaskExecutionPO::getSinkTotalRecord)
                .sum();

        long totalBytes = executions.stream()
                .filter(e -> e.getSinkTotalBytes() != null)
                .mapToLong(TaskExecutionPO::getSinkTotalBytes)
                .sum();

        long totalTasks = executions.size();

        long successTasks = executions.stream()
                .filter(e -> TaskExecutionStatus.COMPLETED.equals(e.getStatus()))
                .count();

        DecimalFormat df = new DecimalFormat("#.##");
        double totalRecordsInTenThousand = Double.parseDouble(df.format(totalRecords / 10000.0));
        double totalBytesInMB = Double.parseDouble(df.format(totalBytes / (1024.0 * 1024.0)));

        result.put("totalRecords", totalRecordsInTenThousand);
        result.put("totalBytes", totalBytesInMB);
        result.put("totalTasks", totalTasks);
        result.put("successTasks", successTasks);
    }

    private void populateTrendData(Map<String, Object> result,
                                   Map<String, List<TaskExecutionPO>> groupedData) {
        result.put("recordsTrend", calculateRecordsTrend(groupedData));
        result.put("bytesTrend", calculateBytesTrend(groupedData));
        result.put("recordsSpeedTrend", calculateRecordsSpeedTrend(groupedData));
        result.put("bytesSpeedTrend", calculateBytesSpeedTrend(groupedData));
    }

    /**
     * 计算开始时间
     */
    private LocalDateTime calculateStartTime(LocalDateTime endTime, String timeRange) {
        switch (timeRange) {
            case "week":
                return endTime.minusWeeks(1);
            case "48h":
                return endTime.minusHours(48);
            case "24h":
            default:
                return endTime.minusHours(24);
        }
    }

    /**
     * 按时间分组执行记录
     */
    private Map<String, List<TaskExecutionPO>> groupExecutionsByTime(List<TaskExecutionPO> executions, String timeRange) {
        DateTimeFormatter formatter;

        switch (timeRange) {
            case "week":
                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                break;
            case "48h":
                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00");
                break;
            case "24h":
            default:
                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00");
                break;
        }

        return executions.stream()
                .collect(Collectors.groupingBy(execution -> {
                    LocalDateTime startTime = execution.getStartTime().toInstant()
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime();
                    return startTime.format(formatter);
                }));
    }

    /**
     * 计算记录数趋势
     */
    private List<Map<String, Object>> calculateRecordsTrend(Map<String, List<TaskExecutionPO>> groupedData) {
        return groupedData.entrySet().stream()
                .map(entry -> {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("date", entry.getKey());

                    long totalRecords = entry.getValue().stream()
                            .filter(e -> e.getSinkTotalRecord() != null)
                            .mapToLong(TaskExecutionPO::getSinkTotalRecord)
                            .sum();
                    // 转换为万条
                    double value = totalRecords / 10000.0;
                    dataPoint.put("value", Math.round(value * 100) / 100.0); // 保留两位小数

                    return dataPoint;
                })
                .sorted(Comparator.comparing(dataPoint -> (String) dataPoint.get("date")))
                .collect(Collectors.toList());
    }

    /**
     * 计算字节数趋势
     */
    private List<Map<String, Object>> calculateBytesTrend(Map<String, List<TaskExecutionPO>> groupedData) {
        return groupedData.entrySet().stream()
                .map(entry -> {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("date", entry.getKey());

                    List<Long> bytesList = entry.getValue().stream()
                            .map(TaskExecutionPO::getSinkTotalBytes)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    // 计算当天的平均字节数
                    double averageBytes = bytesList.stream()
                            .mapToLong(Long::longValue)
                            .average()
                            .orElse(0.0);

                    // 转换为MB
                    double value = averageBytes / (1024.0 * 1024.0);
                    dataPoint.put("value", Math.round(value * 100) / 100.0); // 保留两位小数
                    dataPoint.put("count", bytesList.size()); // 可选：包含数据点数量

                    return dataPoint;
                })
                .sorted(Comparator.comparing(dataPoint -> (String) dataPoint.get("date")))
                .collect(Collectors.toList());
    }

    private List<Map<String, Object>> calculateRecordsSpeedTrend(Map<String, List<TaskExecutionPO>> groupedData) {
        return groupedData.entrySet().stream()
                .map(entry -> {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("date", entry.getKey());

                    List<Double> speeds = entry.getValue().stream()
                            .filter(e -> e.getSinkTotalRecord() != null && e.getStartTime() != null && e.getEndTime() != null)
                            .mapToDouble(e -> {
                                long records = e.getSinkTotalRecord();
                                long duration = e.getEndTime().getTime() - e.getStartTime().getTime();
                                if (duration > 0) {
                                    return records * 1000.0 / duration; // 记录数/秒
                                }
                                return 0;
                            })
                            .boxed()
                            .collect(Collectors.toList());

                    // 计算当天的平均速率
                    double averageSpeed = speeds.stream()
                            .mapToDouble(Double::doubleValue)
                            .average()
                            .orElse(0.0);

                    dataPoint.put("value", Math.round(averageSpeed * 100) / 100.0); // 保留两位小数
                    dataPoint.put("count", speeds.size()); // 可选：包含数据点数量

                    return dataPoint;
                })
                .sorted(Comparator.comparing(dataPoint -> (String) dataPoint.get("date")))
                .collect(Collectors.toList());
    }

    /**
     * 计算字节数速率趋势
     */
    private List<Map<String, Object>> calculateBytesSpeedTrend(Map<String, List<TaskExecutionPO>> groupedData) {
        return groupedData.entrySet().stream()
                .map(entry -> {
                    Map<String, Object> dataPoint = new HashMap<>();
                    dataPoint.put("date", entry.getKey());

                    double totalSpeed = entry.getValue().stream()
                            .filter(e -> e.getSinkTotalBytes() != null && e.getStartTime() != null && e.getEndTime() != null)
                            .mapToDouble(e -> {
                                long bytes = e.getSinkTotalBytes();
                                long duration = e.getEndTime().getTime() - e.getStartTime().getTime();
                                if (duration > 0) {
                                    return (bytes / (1024.0 * 1024.0)) * 1000.0 / duration; // MB/秒
                                }
                                return 0;
                            })
                            .sum();

                    dataPoint.put("value", Math.round(totalSpeed * 100) / 100.0); // 保留两位小数

                    return dataPoint;
                })
                .sorted(Comparator.comparing(dataPoint -> (String) dataPoint.get("date")))
                .collect(Collectors.toList());
    }
}
