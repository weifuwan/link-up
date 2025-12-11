package org.apache.cockpit.connectors.api.catalog.exception;

/**
 * SeaTunnel connector error code interface
 *
 */
@Deprecated
public enum CommonErrorCodeDeprecated implements SeaTunnelErrorCode {
    REFLECT_CLASS_OPERATION_FAILED("COMMON-03", "Reflect class operation failed"),
    SERIALIZE_OPERATION_FAILED("COMMON-04", "Serialize class operation failed"),
    UNSUPPORTED_OPERATION("COMMON-05", "Unsupported operation"),
    ILLEGAL_ARGUMENT("COMMON-06", "Illegal argument"),
    UNSUPPORTED_DATA_TYPE("COMMON-07", "Unsupported data type"),
    SQL_OPERATION_FAILED(
            "COMMON-08", "Sql operation failed, such as (execute,addBatch,close) etc..."),
    TABLE_SCHEMA_GET_FAILED("COMMON-09", "Get table schema from upstream data failed"),
    FLUSH_DATA_FAILED("COMMON-10", "Flush data operation that in sink connector failed"),
    WRITER_OPERATION_FAILED(
            "COMMON-11", "Sink writer operation failed, such as (open, close) etc..."),
    READER_OPERATION_FAILED(
            "COMMON-12", "Source reader operation failed, such as (open, close) etc..."),
    HTTP_OPERATION_FAILED(
            "COMMON-13", "Http operation failed, such as (open, close, response) etc..."),
    KERBEROS_AUTHORIZED_FAILED("COMMON-14", "Kerberos authorized failed"),
    CLASS_NOT_FOUND("COMMON-15", "Class load operation failed");

    private final String code;
    private final String description;

    CommonErrorCodeDeprecated(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
