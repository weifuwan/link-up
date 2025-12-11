package org.apache.cockpit.connectors.influxdb.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class InfluxdbConnectorException extends SeaTunnelRuntimeException {

    public InfluxdbConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public InfluxdbConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public InfluxdbConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
