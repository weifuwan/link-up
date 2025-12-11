package org.apache.cockpit.connectors.api.jdbc.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;

public class JdbcConnectorException extends SeaTunnelRuntimeException {
    public JdbcConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public JdbcConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public JdbcConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
