package org.apache.cockpit.common.bean.vo.metrics;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class JvmStatisticsVO {
    private BigDecimal heapUsageAvg;
    private BigDecimal heapUsageMax;
    private BigDecimal nonHeapUsageAvg;
    private BigDecimal nonHeapUsageMax;
    private double threadCountAvg;
    private int threadCountMax;
    private long totalGcCount;
    private int metricsCount;
}
