package org.apache.cockpit.connectors.api.jdbc.converter;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

import java.io.Serializable;

@Data
@Builder
public class BasicTypeDefine<T> implements Serializable {
    protected String name;
    // e.g. `varchar(10)` for MySQL
    protected String columnType;
    // e.g. `varchar` for MySQL
    protected String dataType;
    // It's jdbc sql type(java.sql.Types) not SeaTunnel SqlType
    protected int sqlType;
    protected T nativeType;
    // e.g. `varchar` length is 10
    protected Long length;
    // e.g. `decimal(10, 2)` precision is 10
    protected Long precision;
    // e.g. `decimal(10, 2)` scale is 2 or timestamp(6) scale is 6
    protected Integer scale;
    // e.g. `tinyint unsigned` is true
    protected boolean unsigned;
    @Builder.Default protected boolean nullable = true;
    protected Object defaultValue;
    protected String comment;

    @Tolerate
    public BasicTypeDefine() {}
}
