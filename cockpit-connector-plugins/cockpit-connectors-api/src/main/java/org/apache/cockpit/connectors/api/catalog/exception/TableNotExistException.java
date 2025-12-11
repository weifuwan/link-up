package org.apache.cockpit.connectors.api.catalog.exception;


import org.apache.cockpit.connectors.api.catalog.TablePath;

/**
 * Exception for trying to operate on a table that doesn't exist.
 */
public class TableNotExistException extends SeaTunnelRuntimeException {

    private static final String MSG = "Table %s does not exist in Catalog %s.";

    public TableNotExistException(String catalogName, TablePath tablePath) {
        this(catalogName, tablePath, null);
    }

    public TableNotExistException(String catalogName, TablePath tablePath, Throwable cause) {
        super(
                SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                String.format(MSG, tablePath.getFullName(), catalogName),
                cause);
    }
}
