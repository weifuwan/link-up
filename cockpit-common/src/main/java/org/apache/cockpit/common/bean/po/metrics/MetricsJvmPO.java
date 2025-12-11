package org.apache.cockpit.common.bean.po.metrics;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("t_cockpit_metrics_jvm")
@EqualsAndHashCode(callSuper = true)
public class MetricsJvmPO extends BasePO {

    /**
     * 应用名称
     */
    private String applicationName;

    /**
     * 实例ID(IP:PORT)
     */
    private String instanceId;

    // 堆内存指标
    private Long heapMemoryInit;
    private Long heapMemoryUsed;
    private Long heapMemoryCommitted;
    private Long heapMemoryMax;
    private BigDecimal heapMemoryUsage;
    private Long totalMemoryUsed;
    private Long totalMemoryCommitted;
    private Long totalMemoryMax;

    // 非堆内存指标
    private Long nonheapMemoryInit;
    private Long nonheapMemoryUsed;
    private Long nonheapMemoryCommitted;
    private Long nonheapMemoryMax;
    private BigDecimal nonheapMemoryUsage;

    // 内存池详细指标
    private Long edenMemoryUsed;
    private Long edenMemoryMax;
    private Long survivorMemoryUsed;
    private Long survivorMemoryMax;
    private Long oldgenMemoryUsed;
    private Long oldgenMemoryMax;
    private Long metaspaceMemoryUsed;
    private Long metaspaceMemoryMax;

    // GC相关指标
    private Long gcYoungCount;
    private Long gcYoungTime;
    private Long gcOldCount;
    private Long gcOldTime;
    private Long gcLastDuration;
    private Long gcFullCount;
    private Long gcFullTime;
    private Long gcYoungCountIncrement;
    private Long gcOldCountIncrement;
    private Long gcFullCountIncrement;
    private BigDecimal gcYoungFrequency;
    private BigDecimal gcOldFrequency;
    private BigDecimal gcFullFrequency;

    // 线程相关指标
    private Integer threadCount;
    private Integer threadPeakCount;
    private Integer threadDaemonCount;
    private Long threadStartedCount;
    private Integer threadDeadlockCount;
    private Integer threadRunnableCount;
    private Integer threadBlockedCount;
    private Integer threadWaitingCount;
    private Integer threadTimedWaitingCount;

    // 类加载指标
    private Integer classesLoaded;
    private Long classesUnloaded;
    private Long classesTotalLoaded;

    // 编译指标
    private Long compilationTime;

    /**
     * 数据采集时间
     */
    private LocalDateTime collectTime;
}