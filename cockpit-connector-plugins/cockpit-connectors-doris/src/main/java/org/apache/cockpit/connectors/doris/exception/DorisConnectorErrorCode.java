package org.apache.cockpit.connectors.doris.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;

public enum DorisConnectorErrorCode implements SeaTunnelErrorCode {
    STREAM_LOAD_FAILED("Doris-01", "stream load error"),
    COMMIT_FAILED("Doris-02", "commit error"),
    REST_SERVICE_FAILED("Doris-03", "rest service error"),
    ROUTING_FAILED("Doris-04", "routing error"),
    ARROW_READ_FAILED("Doris-05", "arrow read error"),
    BACKEND_CLIENT_FAILED("Doris-06", "backend client error"),
    ROW_BATCH_GET_FAILED("Doris-07", "row batch get error"),
    SCHEMA_FAILED("Doirs-08", "get schema error"),
    SCAN_BATCH_FAILED("Doris-09", "scan batch error"),
    RESOURCE_CLOSE_FAILED("Doris-10", "resource close failed"),
    SCHEMA_CHANGE_FAILED("Doris-11", "schema change failed"),
    SHOULD_NEVER_HAPPEN("Doris-00", "Should Never Happen !");

    private final String code;
    private final String description;

    DorisConnectorErrorCode(String code, String description) {
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
