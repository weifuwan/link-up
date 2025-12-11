package org.apache.cockpit.connectors.starrocks.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;

public enum StarRocksConnectorErrorCode implements SeaTunnelErrorCode {
    FLUSH_DATA_FAILED("STARROCKS-01", "Flush batch data to sink connector failed"),
    WRITE_RECORDS_FAILED("STARROCKS-02", "Writing records to StarRocks failed."),
    CLOSE_BE_READER_FAILED("STARROCKS-03", "Close StarRocks BE reader failed"),
    CREATE_BE_READER_FAILED("STARROCKS-04", "Create StarRocks BE reader failed"),
    SCAN_BE_DATA_FAILED("STARROCKS-05", "Scan data from StarRocks BE failed"),
    QUEST_QUERY_PLAN_FAILED("STARROCKS-06", "Request query Plan failed"),
    READER_ARROW_DATA_FAILED("STARROCKS-07", "Read Arrow data failed"),
    HOST_IS_NULL("STARROCKS-08", "Read Arrow data failed");

    private final String code;
    private final String description;

    StarRocksConnectorErrorCode(String code, String description) {
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
