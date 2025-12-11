package org.apache.cockpit.connectors.api.catalog.exception;



public class DatabaseAlreadyExistException extends SeaTunnelRuntimeException {
    private static final String MSG = "Database %s already exist in Catalog %s.";

    public DatabaseAlreadyExistException(String catalogName, String databaseName) {
        this(catalogName, databaseName, null);
    }

    public DatabaseAlreadyExistException(String catalogName, String databaseName, Throwable cause) {
        super(
                SeaTunnelAPIErrorCode.DATABASE_ALREADY_EXISTED,
                String.format(MSG, databaseName, catalogName),
                cause);
    }
}
