package org.apache.cockpit.system.metrics.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.apache.cockpit.common.bean.vo.metrics.LineChartVO;
import org.apache.cockpit.common.bean.vo.metrics.MetricsJvmVO;
import org.apache.cockpit.common.utils.ConvertUtil;
import org.apache.cockpit.persistence.metrics.MetricsJvmMapper;
import org.apache.cockpit.system.metrics.core.ChartDataBuilder;
import org.apache.cockpit.system.metrics.core.MetricsCollector;
import org.apache.cockpit.system.metrics.service.MetricsJvmService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class MetricsJvmServiceImpl extends ServiceImpl<MetricsJvmMapper, MetricsJvmPO> implements MetricsJvmService {

    @Resource
    private MetricsCollector metricsCollector;

    @Resource
    private ChartDataBuilder chartDataBuilder;

    @Override
    public MetricsJvmPO collectJvmMetrics(String applicationName, String instanceId) {
        return metricsCollector.collectJvmMetrics(applicationName, instanceId);
    }

    @Override
    public boolean saveJvmMetrics(MetricsJvmPO metrics) {
        if (metrics == null) {
            return false;
        }
        return this.save(metrics);
    }

    @Override
    public List<MetricsJvmPO> queryByApplicationAndTime(String applicationName, LocalDateTime startTime, LocalDateTime endTime) {
        LambdaQueryWrapper<MetricsJvmPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(MetricsJvmPO::getApplicationName, applicationName)
                .between(MetricsJvmPO::getCollectTime, startTime, endTime)
                .orderByAsc(MetricsJvmPO::getCollectTime);
        return this.list(queryWrapper);
    }

    @Override
    public MetricsJvmVO getLatestMetrics(String applicationName, String instanceId) {
        LambdaQueryWrapper<MetricsJvmPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(MetricsJvmPO::getApplicationName, applicationName)
                .eq(MetricsJvmPO::getInstanceId, instanceId)
                .orderByDesc(MetricsJvmPO::getCollectTime)
                .last("LIMIT 1");
        MetricsJvmPO po = this.getOne(queryWrapper);
        return ConvertUtil.sourceToTarget(po, MetricsJvmVO.class);
    }

    @Override
    public int cleanExpiredData(LocalDateTime expireTime) {
        LambdaQueryWrapper<MetricsJvmPO> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.lt(MetricsJvmPO::getCollectTime, expireTime);
        return this.getBaseMapper().delete(queryWrapper);
    }

    @Override
    public LineChartVO getMemoryUsage(String applicationName, LocalDateTime startTime, LocalDateTime endTime, String duration) {

        if (startTime == null || endTime == null) {
            endTime = LocalDateTime.now();
            startTime = chartDataBuilder.calculateStartTime(duration, endTime);
        }

        List<MetricsJvmPO> metricsList = queryByApplicationAndTime(
                applicationName, startTime, endTime);

        if (metricsList.isEmpty()) {
            return LineChartVO.builder()
                    .xAxis(new ArrayList<>())
                    .series(new ArrayList<>())
                    .title("内存使用趋势 - " + applicationName)
                    .build();
        }

        return chartDataBuilder.buildMemoryUsageChart(metricsList, applicationName);
    }

    @Override
    public LineChartVO getThreadUsage(String applicationName, LocalDateTime startTime, LocalDateTime endTime, String duration) {
        if (startTime == null || endTime == null) {
            endTime = LocalDateTime.now();
            startTime = chartDataBuilder.calculateStartTime(duration, endTime);
        }

        List<MetricsJvmPO> metricsList = queryByApplicationAndTime(
                applicationName, startTime, endTime);
        if (metricsList.isEmpty()) {
            return LineChartVO.builder()
                    .xAxis(new ArrayList<>())
                    .series(new ArrayList<>())
                    .title("线程使用趋势 - " + applicationName)
                    .build();
        }

        return chartDataBuilder.buildThreadUsageChart(metricsList, applicationName);

    }
}