package org.apache.cockpit.connectors.doris.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class DorisConnectorException extends SeaTunnelRuntimeException {
    private boolean reCreateLabel;

    public DorisConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public DorisConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, boolean reCreateLabel) {
        super(seaTunnelErrorCode, errorMessage);
        this.reCreateLabel = reCreateLabel;
    }

    public DorisConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public DorisConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }

    public boolean needReCreateLabel() {
        return reCreateLabel;
    }
}
