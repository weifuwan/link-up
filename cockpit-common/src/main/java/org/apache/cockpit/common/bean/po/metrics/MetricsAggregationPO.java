package org.apache.cockpit.common.bean.po.metrics;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;
import org.apache.cockpit.common.enums.metrics.AggregationTypeEnum;
import org.apache.cockpit.common.enums.metrics.MetricTypeEnum;

import java.math.BigDecimal;
import java.time.LocalDateTime;


@Data
@TableName("t_cockpit_metrics_aggregation")
@EqualsAndHashCode(callSuper = true)
public class MetricsAggregationPO extends BasePO {

    /**
     * 应用名称
     */
    private String applicationName;

    /**
     * 实例ID
     */
    private String instanceId;

    /**
     * 指标类型
     */
    private MetricTypeEnum metricType;

    /**
     * 指标字段
     */
    private String metricField;

    /**
     * 聚合类型
     */
    private AggregationTypeEnum aggregationType;

    // 聚合数据
    private BigDecimal valueMin;
    private BigDecimal valueMax;
    private BigDecimal valueAvg;
    private BigDecimal valueSum;
    private Long valueCount;

    /**
     * 聚合时间点
     */
    private LocalDateTime aggregationTime;

    /**
     * 时间桶标识
     */
    private String timeBucket;
}
