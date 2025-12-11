package org.apache.cockpit.common.bean.vo.metrics;

import lombok.Data;

import java.time.LocalDateTime;
@Data
public class InstanceVO {
    private String instanceId;
    private LocalDateTime lastCollectTime;
}
