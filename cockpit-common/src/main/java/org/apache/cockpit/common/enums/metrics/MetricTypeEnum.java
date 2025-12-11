package org.apache.cockpit.common.enums.metrics;

public enum MetricTypeEnum {
    SYSTEM("system", "系统指标"),
    JVM("jvm", "JVM指标"),
    BUSINESS("business", "业务指标");

    private final String code;
    private final String desc;

    MetricTypeEnum(String code, String desc) {
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