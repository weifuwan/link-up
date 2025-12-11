package org.apache.cockpit.common.enums.metrics;

public enum ComparisonOperatorEnum {
    GT(">", "大于"),
    LT("<", "小于"),
    GTE(">=", "大于等于"),
    LTE("<=", "小于等于"),
    EQ("=", "等于"),
    NEQ("!=", "不等于");

    private final String code;
    private final String desc;

    ComparisonOperatorEnum(String code, String desc) {
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