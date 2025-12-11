package org.apache.cockpit.connectors.api.serialization.text;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class SeaTunnelTextFormatException extends SeaTunnelRuntimeException {
    public SeaTunnelTextFormatException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public SeaTunnelTextFormatException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public SeaTunnelTextFormatException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
