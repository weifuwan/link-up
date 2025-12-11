package org.apache.cockpit.common.enums.integration;

import com.baomidou.mybatisplus.annotation.EnumValue;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum TaskExecutionTypeEnum {
    SINGLE_TABLE("SINGLE_TABLE", "单表同步"),
    SINGLE_TABLE_CUSTOM("SINGLE_TABLE_CUSTOM", "单表自定义"),
    MULTI_TABLE("MULTI_TABLE", "多表同步"),
    ;

    @EnumValue
    private final String code;
    private final String description;
}
