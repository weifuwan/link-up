package org.apache.cockpit.common.enums.integration;

import com.baomidou.mybatisplus.annotation.EnumValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ExecutionMode {
    MANUAL("MANUAL", "手动执行"),
    SCHEDULED("SCHEDULED", "调度执行");

    @EnumValue
    private final String code;
    private final String description;
}
