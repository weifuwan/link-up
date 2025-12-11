package org.apache.cockpit.connectors.hive3.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class HiveConnectorException extends SeaTunnelRuntimeException {
    public HiveConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public HiveConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public HiveConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
