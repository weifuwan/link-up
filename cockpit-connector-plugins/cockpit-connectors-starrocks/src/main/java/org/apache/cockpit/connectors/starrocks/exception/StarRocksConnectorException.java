package org.apache.cockpit.connectors.starrocks.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class StarRocksConnectorException extends SeaTunnelRuntimeException {

    private boolean reCreateLabel;

    public StarRocksConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public StarRocksConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, boolean reCreateLabel) {
        super(seaTunnelErrorCode, errorMessage);
        this.reCreateLabel = reCreateLabel;
    }

    public StarRocksConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public StarRocksConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }

    public boolean needReCreateLabel() {
        return reCreateLabel;
    }
}
