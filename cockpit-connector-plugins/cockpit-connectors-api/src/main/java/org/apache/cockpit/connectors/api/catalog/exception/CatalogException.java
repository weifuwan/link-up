package org.apache.cockpit.connectors.api.catalog.exception;


/** A catalog-related, runtime exception. */
public class CatalogException extends SeaTunnelRuntimeException {

    /** @param message the detail message. */
    public CatalogException(String message) {
        super(SeaTunnelAPIErrorCode.CATALOG_INITIALIZE_FAILED, message);
    }

    /** @param cause the cause. */
    public CatalogException(Throwable cause) {
        super(SeaTunnelAPIErrorCode.CATALOG_INITIALIZE_FAILED, cause);
    }

    /**
     * @param message the detail message.
     * @param cause the cause.
     */
    public CatalogException(String message, Throwable cause) {
        super(SeaTunnelAPIErrorCode.CATALOG_INITIALIZE_FAILED, message, cause);
    }
}
