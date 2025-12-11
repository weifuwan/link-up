package org.apache.cockpit.common.bean.po.metrics;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.cockpit.common.bean.po.BasePO;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("t_cockpit_metrics_system")
@EqualsAndHashCode(callSuper = true)
public class MetricsSystemPO extends BasePO {

    /**
     * 应用名称
     */
    private String applicationName;

    /**
     * 实例ID(IP:PORT)
     */
    private String instanceId;

    /**
     * 主机IP地址
     */
    private String hostIp;

    // CPU相关指标
    private BigDecimal cpuUsage;
    private BigDecimal cpuSystemUsage;
    private BigDecimal cpuUserUsage;
    private BigDecimal cpuIdleUsage;
    private BigDecimal cpuLoad1min;
    private BigDecimal cpuLoad5min;
    private BigDecimal cpuLoad15min;
    private Integer cpuCores;

    // 内存相关指标
    private Long memoryTotal;
    private Long memoryUsed;
    private Long memoryFree;
    private BigDecimal memoryUsage;
    private Long memoryBuffer;
    private Long memoryCached;
    private Long swapTotal;
    private Long swapUsed;
    private Long swapFree;
    private BigDecimal swapUsage;

    // 磁盘相关指标
    private Long diskTotal;
    private Long diskUsed;
    private Long diskFree;
    private BigDecimal diskUsage;
    private Long diskReadRate;
    private Long diskWriteRate;
    private BigDecimal diskQueueDepth;

    // 网络相关指标
    private Long networkBytesReceived;
    private Long networkBytesSent;
    private Long networkPacketsReceived;
    private Long networkPacketsSent;
    private Long networkErrorsIn;
    private Long networkErrorsOut;

    /**
     * 数据采集时间
     */
    private LocalDateTime collectTime;
}
