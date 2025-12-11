package org.apache.cockpit.common.enums.metrics;

public enum AggregationTypeEnum {
    HOUR("HOUR", "小时聚合"),
    DAY("DAY", "天聚合");

    private final String code;
    private final String desc;

    AggregationTypeEnum(String code, String desc) {
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

