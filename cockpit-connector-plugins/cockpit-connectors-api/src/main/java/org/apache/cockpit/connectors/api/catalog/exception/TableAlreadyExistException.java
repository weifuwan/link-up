package org.apache.cockpit.connectors.api.catalog.exception;


import org.apache.cockpit.connectors.api.catalog.TablePath;

public class TableAlreadyExistException extends SeaTunnelRuntimeException {
    private static final String MSG = "Table %s already exist in Catalog %s.";

    public TableAlreadyExistException(String catalogName, TablePath tablePath) {
        this(catalogName, tablePath, null);
    }

    public TableAlreadyExistException(String catalogName, TablePath tablePath, Throwable cause) {
        super(
                SeaTunnelAPIErrorCode.TABLE_ALREADY_EXISTED,
                String.format(MSG, tablePath.getFullName(), catalogName),
                cause);
    }
}
