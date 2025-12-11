package org.apache.cockpit.common.bean.vo.metrics;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class MetricsJvmVO {

    /**
     * 应用名称
     */
    private String applicationName;

    /**
     * 实例ID(IP:PORT)
     */
    private String instanceId;


    private BigDecimal heapMemoryUsage;
    private Long gcFullCount;
    private Long gcFullTime;
    private Integer threadBlockedCount;

    /**
     * 数据采集时间
     */
    @JsonFormat(pattern = "yyyy年MM月dd HH:mm:ss", timezone = "GMT+8")
    private LocalDateTime collectTime;

}