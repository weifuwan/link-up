package org.apache.cockpit.common.enums.integration;

import com.baomidou.mybatisplus.annotation.EnumValue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * 调度状态枚举
 */
@Getter
@AllArgsConstructor
public enum ScheduleStatusEnum {
    STOPPED("STOPPED","已停止"),
    RUNNING("RUNNING","运行中"),
    PAUSED("PAUSED","已暂停"),
    ERROR("ERROR","错误状态");

    @EnumValue
    private final String code;
    private final String description;

}