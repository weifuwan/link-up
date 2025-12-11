package org.apache.cockpit.common.enums.metrics;

public enum AlertStatusEnum {
    TRIGGERED("TRIGGERED", "已触发"),
    RESOLVED("RESOLVED", "已恢复"),
    ACKNOWLEDGED("ACKNOWLEDGED", "已确认");

    private final String code;
    private final String desc;

    AlertStatusEnum(String code, String desc) {
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
