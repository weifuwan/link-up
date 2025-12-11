package org.apache.cockpit.system.metrics.service;

import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.apache.cockpit.common.bean.vo.metrics.LineChartVO;
import org.apache.cockpit.common.bean.vo.metrics.MetricsJvmVO;

import java.time.LocalDateTime;
import java.util.List;

public interface MetricsJvmService extends IService<MetricsJvmPO> {

    /**
     * 采集JVM指标数据
     */
    MetricsJvmPO collectJvmMetrics(String applicationName, String instanceId);

    /**
     * 批量保存JVM指标数据
     */
    boolean saveJvmMetrics(MetricsJvmPO metrics);

    /**
     * 根据应用名称和时间范围查询JVM指标
     */
    List<MetricsJvmPO> queryByApplicationAndTime(String applicationName, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * 获取最新的JVM指标数据
     */
    MetricsJvmVO getLatestMetrics(String applicationName, String instanceId);

    /**
     * 清理过期数据
     */
    int cleanExpiredData(LocalDateTime expireTime);

    /**
     * 获取内存使用情况折线图数据
     */
    LineChartVO getMemoryUsage(String applicationName, LocalDateTime startTime, LocalDateTime endTime, String duration);

    /**
     * 获取线程使用情况折线图数据
     */
    LineChartVO getThreadUsage(String applicationName, LocalDateTime startTime, LocalDateTime endTime, String duration);
}
