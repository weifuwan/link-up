package org.apache.cockpit.connectors.influxdb.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;

public enum InfluxdbConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECT_FAILED(
            "INFLUXDB-01", "Connect influxdb failed, due to influxdb version info is unknown"),
    GET_COLUMN_INDEX_FAILED("INFLUXDB-02", "Get column index of query result exception");

    private final String code;
    private final String description;

    InfluxdbConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getErrorMessage() {
        return SeaTunnelErrorCode.super.getErrorMessage();
    }
}
