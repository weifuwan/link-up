package org.apache.cockpit.connectors.api.catalog.exception;



/** Exception for trying to operate on a database that doesn't exist. */
public class DatabaseNotExistException extends SeaTunnelRuntimeException {
    private static final String MSG = "Database %s does not exist in Catalog %s.";

    public DatabaseNotExistException(String catalogName, String databaseName, Throwable cause) {
        super(
                SeaTunnelAPIErrorCode.DATABASE_NOT_EXISTED,
                String.format(MSG, databaseName, catalogName),
                cause);
    }

    public DatabaseNotExistException(String catalogName, String databaseName) {
        this(catalogName, databaseName, null);
    }
}
