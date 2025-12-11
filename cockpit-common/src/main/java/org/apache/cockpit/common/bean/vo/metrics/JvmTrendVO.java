package org.apache.cockpit.common.bean.vo.metrics;

import lombok.Data;

import java.math.BigDecimal;
import java.util.List;
@Data
public class JvmTrendVO {
    private List<String> timePoints;
    private List<BigDecimal> heapUsage;
    private List<BigDecimal> nonHeapUsage;
    private List<Integer> threadCount;
    private List<Long> gcCount;
}
