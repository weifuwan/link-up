package org.apache.cockpit.controller.metrics;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.result.Result;
import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.apache.cockpit.common.bean.vo.metrics.*;
import org.apache.cockpit.system.metrics.scheduler.MetricsCollectionScheduler;
import org.apache.cockpit.system.metrics.service.MetricsJvmService;
import org.apache.commons.lang.StringUtils;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/metrics/jvm")
@Slf4j
public class MetricsJvmController {

    @Resource
    private MetricsJvmService metricsJvmService;

    @Resource
    private MetricsCollectionScheduler metricsCollectionScheduler;

    @GetMapping("/memory-usage")
    @ApiOperation("获取内存使用情况折线图数据")
    public Result<LineChartVO> getMemoryUsage(
            @RequestParam String applicationName,
            @RequestParam(required = false) String instanceId,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "1h") String duration) {
        return Result.buildSuc(metricsJvmService.getMemoryUsage(applicationName, startTime, endTime, duration));
    }

    @GetMapping("/thread-usage")
    public Result<LineChartVO> getThreadUsage(
            @RequestParam String applicationName,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "1h") String duration) {

        LineChartVO chartData = metricsJvmService.getThreadUsage(applicationName, startTime, endTime, duration);
        return Result.buildSuc(chartData);
    }

    /**
     * 获取JVM指标列表
     */
    @GetMapping("/list")
    public Result<List<MetricsJvmPO>> getJvmMetricsList(
            @RequestParam String applicationName,
            @RequestParam(required = false) String instanceId,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime,
            @RequestParam(defaultValue = "1") Integer pageNum,
            @RequestParam(defaultValue = "100") Integer pageSize) {


        Page<MetricsJvmPO> page = new Page<>(pageNum, pageSize);
        LambdaQueryWrapper<MetricsJvmPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(MetricsJvmPO::getApplicationName, applicationName)
                .between(MetricsJvmPO::getCollectTime, startTime, endTime)
                .orderByDesc(MetricsJvmPO::getCollectTime);

        if (StringUtils.isNotBlank(instanceId)) {
            queryWrapper.eq(MetricsJvmPO::getInstanceId, instanceId);
        }

        Page<MetricsJvmPO> result = metricsJvmService.page(page, queryWrapper);
        return Result.buildSuc(result.getRecords());

    }

    /**
     * 获取最新的JVM指标
     */
    @GetMapping("/latest")
    public Result<MetricsJvmVO> getLatestJvmMetrics(
            @RequestParam String applicationName,
            @RequestParam String instanceId) {


        MetricsJvmVO metrics = metricsJvmService.getLatestMetrics(applicationName, instanceId);
        if (metrics == null) {
            return Result.buildFailure("未找到JVM指标数据");
        }
        return Result.buildSuc(metrics);

    }

    /**
     * 获取JVM指标趋势数据
     */
    @GetMapping("/trend")
    public Result<JvmTrendVO> getJvmTrend(
            @RequestParam String applicationName,
            @RequestParam String instanceId,
            @RequestParam(defaultValue = "1h") String duration) {

        try {
            LocalDateTime endTime = LocalDateTime.now();
            LocalDateTime startTime = calculateStartTime(duration, endTime);

            List<MetricsJvmPO> metricsList = metricsJvmService.queryByApplicationAndTime(
                    applicationName, startTime, endTime);

            JvmTrendVO trendVO = buildJvmTrendVO(metricsList);
            return Result.buildSuc(trendVO);
        } catch (Exception e) {
            log.error("获取JVM指标趋势失败", e);
            return Result.buildFailure("获取JVM指标趋势失败");
        }
    }

    /**
     * 手动触发JVM指标采集
     */
    @PostMapping("/collect")
    public Result<Boolean> triggerJvmCollection() {
        try {
            boolean success = metricsCollectionScheduler.triggerCollection();
            if (success) {
                return Result.buildSuc(true);
            } else {
                return Result.buildFailure("JVM指标采集触发失败");
            }
        } catch (Exception e) {
            log.error("手动触发JVM指标采集失败", e);
            return Result.buildFailure("手动触发JVM指标采集失败");
        }
    }

    /**
     * 获取JVM指标统计信息
     */
    @GetMapping("/statistics")
    public Result<JvmStatisticsVO> getJvmStatistics(
            @RequestParam String applicationName,
            @RequestParam String instanceId,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime) {

        try {
            List<MetricsJvmPO> metricsList = metricsJvmService.queryByApplicationAndTime(
                    applicationName, startTime, endTime);

            JvmStatisticsVO statistics = calculateJvmStatistics(metricsList);
            return Result.buildSuc(statistics);
        } catch (Exception e) {
            log.error("获取JVM指标统计失败", e);
            return Result.buildFailure("获取JVM指标统计失败");
        }
    }

    /**
     * 删除JVM指标数据
     */
    @DeleteMapping("/{id}")
    public Result<Boolean> deleteJvmMetrics(@PathVariable Long id) {
        try {
            boolean success = metricsJvmService.removeById(id);
            if (success) {
                return Result.buildSuc(true);
            } else {
                return Result.buildFailure("删除失败");
            }
        } catch (Exception e) {
            log.error("删除JVM指标数据失败, id: {}", id, e);
            return Result.buildFailure("删除JVM指标数据失败");
        }
    }

    /**
     * 批量删除JVM指标数据
     */
    @DeleteMapping("/batch")
    public Result<Boolean> batchDeleteJvmMetrics(@RequestBody List<Long> ids) {
        try {
            boolean success = metricsJvmService.removeByIds(ids);
            if (success) {
                return Result.buildSuc(true);
            } else {
                return Result.buildFailure("批量删除失败");
            }
        } catch (Exception e) {
            log.error("批量删除JVM指标数据失败, ids: {}", ids, e);
            return Result.buildFailure("批量删除JVM指标数据失败");
        }
    }

    /**
     * 获取应用实例列表
     */
    @GetMapping("/instances")
    public Result<List<InstanceVO>> getApplicationInstances(@RequestParam String applicationName) {
        try {
            LambdaQueryWrapper<MetricsJvmPO> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.select(MetricsJvmPO::getInstanceId)
                    .eq(MetricsJvmPO::getApplicationName, applicationName)
                    .groupBy(MetricsJvmPO::getInstanceId);

            List<MetricsJvmPO> list = metricsJvmService.list(queryWrapper);
            List<InstanceVO> instances = list.stream()
                    .map(po -> {
                        InstanceVO vo = new InstanceVO();
                        vo.setInstanceId(po.getInstanceId());
                        vo.setLastCollectTime(po.getCollectTime());
                        return vo;
                    })
                    .collect(Collectors.toList());

            return Result.buildSuc(instances);
        } catch (Exception e) {
            log.error("获取应用实例列表失败", e);
            return Result.buildFailure("获取应用实例列表失败");
        }
    }

    private LocalDateTime calculateStartTime(String duration, LocalDateTime endTime) {
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

    private JvmTrendVO buildJvmTrendVO(List<MetricsJvmPO> metricsList) {
        JvmTrendVO trendVO = new JvmTrendVO();

        List<String> timePoints = metricsList.stream()
                .map(po -> po.getCollectTime().format(DateTimeFormatter.ofPattern("HH:mm")))
                .collect(Collectors.toList());

        List<BigDecimal> heapUsageList = metricsList.stream()
                .map(MetricsJvmPO::getHeapMemoryUsage)
                .collect(Collectors.toList());

        List<BigDecimal> nonHeapUsageList = metricsList.stream()
                .map(MetricsJvmPO::getNonheapMemoryUsage)
                .collect(Collectors.toList());

        List<Integer> threadCountList = metricsList.stream()
                .map(MetricsJvmPO::getThreadCount)
                .collect(Collectors.toList());

        trendVO.setTimePoints(timePoints);
        trendVO.setHeapUsage(heapUsageList);
        trendVO.setNonHeapUsage(nonHeapUsageList);
        trendVO.setThreadCount(threadCountList);

        return trendVO;
    }

    private JvmStatisticsVO calculateJvmStatistics(List<MetricsJvmPO> metricsList) {
        if (metricsList.isEmpty()) {
            return new JvmStatisticsVO();
        }

        JvmStatisticsVO statistics = new JvmStatisticsVO();

        // 堆内存统计
        statistics.setHeapUsageAvg(metricsList.stream()
                .map(MetricsJvmPO::getHeapMemoryUsage)
                .filter(Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .divide(BigDecimal.valueOf(metricsList.size()), 2, RoundingMode.HALF_UP));

        statistics.setHeapUsageMax(metricsList.stream()
                .map(MetricsJvmPO::getHeapMemoryUsage)
                .filter(Objects::nonNull)
                .max(BigDecimal::compareTo)
                .orElse(BigDecimal.ZERO));

        // 线程统计
        statistics.setThreadCountAvg(metricsList.stream()
                .mapToInt(MetricsJvmPO::getThreadCount)
                .average()
                .orElse(0.0));

        statistics.setThreadCountMax(metricsList.stream()
                .mapToInt(MetricsJvmPO::getThreadCount)
                .max()
                .orElse(0));

        // GC统计
        long totalGcCount = metricsList.stream()
                .mapToLong(po -> (po.getGcYoungCount() != null ? po.getGcYoungCount() : 0) +
                        (po.getGcOldCount() != null ? po.getGcOldCount() : 0))
                .sum();

        statistics.setTotalGcCount(totalGcCount);
        statistics.setMetricsCount(metricsList.size());

        return statistics;
    }
}
