package org.apache.cockpit.connectors.api.type;

/** The sql type of {@link SeaTunnelDataType}. */
public enum SqlType {
    ARRAY,
    MAP,
    STRING,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    NULL,
    BYTES,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMP_TZ,
    BINARY_VECTOR,
    FLOAT_VECTOR,
    FLOAT16_VECTOR,
    BFLOAT16_VECTOR,
    SPARSE_FLOAT_VECTOR,
    ROW,
    MULTIPLE_ROW;
}
