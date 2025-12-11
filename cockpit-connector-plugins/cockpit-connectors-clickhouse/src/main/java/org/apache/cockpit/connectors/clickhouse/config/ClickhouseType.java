package org.apache.cockpit.connectors.clickhouse.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class ClickhouseType {

    public static final String STRING = "String";
    public static final String TINYINT = "Int8";
    public static final String SMALLINT = "Int16";
    public static final String INT = "Int32";
    public static final String BIGINT = "Int64";
    public static final String FLOAT = "Float32";
    public static final String BOOLEAN = "Bool";
    public static final String DOUBLE = "Float64";
    public static final String DATE = "Date";
    public static final String DateTime64 = "DateTime64";
    public static final String MAP = "Map";
    public static final String ARRAY = "Array";
    public static final String DECIMAL = "Decimal";
    private String type;
    private Map<String, Object> options;
}
