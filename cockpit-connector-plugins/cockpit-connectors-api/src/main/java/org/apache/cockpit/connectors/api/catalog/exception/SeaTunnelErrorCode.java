package org.apache.cockpit.connectors.api.catalog.exception;

/** SeaTunnel connector error code interface */
public interface SeaTunnelErrorCode {
    /**
     * Get error code
     *
     * @return error code
     */
    String getCode();

    /**
     * Get error description
     *
     * @return error description
     */
    String getDescription();

    default String getErrorMessage() {
        return String.format("ErrorCode:[%s], ErrorDescription:[%s]", getCode(), getDescription());
    }
}
