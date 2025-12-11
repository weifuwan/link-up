package org.apache.cockpit.common.enums.metrics;

public enum AlertLevelEnum {
    INFO("INFO", "信息"),
    WARNING("WARNING", "警告"),
    ERROR("ERROR", "错误"),
    CRITICAL("CRITICAL", "严重");

    private final String code;
    private final String desc;

    AlertLevelEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}